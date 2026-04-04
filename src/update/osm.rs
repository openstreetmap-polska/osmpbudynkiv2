use std::collections::HashSet;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use flate2::read::GzDecoder;
use tracing::info;

use crate::config::Config;
use crate::download::download_file;
use crate::osm::kvstore::RocksDB;
use crate::osm::replication::{
    ChangeAction, OsmChange, RelationChange, WayChange, parse_osc, parse_state_txt,
    sequence_to_path,
};
use crate::osm::{encoding, kvstore};

pub fn update(
    conn: &Connection,
    kv: &RocksDB,
    _config: &Config,
    replication_base_url: &str,
) -> Result<()> {
    let current_seq = get_current_sequence(conn)?;
    info!(current_seq, "Current replication sequence");

    let latest_seq = fetch_latest_sequence(replication_base_url)?;
    info!(latest_seq, "Latest available sequence");

    if current_seq >= latest_seq {
        info!("Database is up to date");
        return Ok(());
    }

    let pending = latest_seq - current_seq;
    info!(pending, "Sequences to apply");

    for seq in (current_seq + 1)..=latest_seq {
        apply_sequence(conn, kv, seq, replication_base_url)?;

        if (seq - current_seq) % 100 == 0 {
            info!(
                seq,
                progress = format!("{}/{}", seq - current_seq, pending),
                "Progress"
            );
        }
    }

    info!(final_seq = latest_seq, "OSM update complete");
    Ok(())
}

fn get_current_sequence(conn: &Connection) -> Result<u64> {
    let result: Result<String, _> = conn.query_row(
        "SELECT value FROM metadata WHERE key = 'osm_replication_sequence'",
        [],
        |row| row.get(0),
    );

    match result {
        Ok(val) => val.parse().context("Invalid sequence number in metadata"),
        Err(_) => {
            bail!("No replication sequence number found in metadata. Run 'import osm' first.")
        }
    }
}

fn fetch_latest_sequence(replication_base_url: &str) -> Result<u64> {
    let url = format!("{replication_base_url}/state.txt");
    let state_path = download_file(&url, Path::new("./data/replication"))?;
    let text = std::fs::read_to_string(&state_path).context("Failed to read state.txt")?;
    let _ = std::fs::remove_file(&state_path);
    parse_state_txt(&text)
}

fn apply_sequence(
    conn: &Connection,
    kv: &RocksDB,
    seq: u64,
    replication_base_url: &str,
) -> Result<()> {
    let path = sequence_to_path(seq);
    let url = format!("{replication_base_url}/{path}");

    let osc_gz_path = download_file(&url, Path::new("./data/replication"))?;
    let osc_xml = decompress_gz(&osc_gz_path)?;
    let _ = std::fs::remove_file(&osc_gz_path);

    let changes = parse_osc(&osc_xml)?;
    apply_changes(conn, kv, &changes)?;

    conn.execute(
        "DELETE FROM metadata WHERE key = 'osm_replication_sequence'",
        [],
    )?;
    conn.execute(
        "INSERT INTO metadata (key, value) VALUES ('osm_replication_sequence', ?)",
        [&seq.to_string()],
    )?;

    Ok(())
}

fn decompress_gz(path: &Path) -> Result<String> {
    let file = std::fs::File::open(path).with_context(|| format!("Failed to open {path:?}"))?;
    let mut decoder = GzDecoder::new(file);
    let mut xml = String::new();
    decoder
        .read_to_string(&mut xml)
        .context("Failed to decompress gzip")?;
    Ok(xml)
}

fn apply_changes(conn: &Connection, kv: &RocksDB, changes: &OsmChange) -> Result<()> {
    let mut affected_way_ids: HashSet<i64> = HashSet::new();
    let mut affected_relation_ids: HashSet<i64> = HashSet::new();

    // --- Apply node changes ---
    for node in &changes.nodes {
        match node.action {
            ChangeAction::Delete => {
                let way_ids = kvstore::get_node_to_ways(kv, node.id)?;
                affected_way_ids.extend(&way_ids);
                for &wid in &way_ids {
                    kvstore::remove_node_to_ways(kv, node.id, wid)?;
                }
                kvstore::delete_node(kv, node.id)?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'node'",
                    [node.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                kvstore::put_node(kv, node.id, node.lon, node.lat)?;
                let way_ids = kvstore::get_node_to_ways(kv, node.id)?;
                affected_way_ids.extend(&way_ids);
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'node'",
                    [node.id],
                )?;
                if let Some(hn) = tag_value(&node.tags, "addr:housenumber") {
                    let street = tag_value(&node.tags, "addr:street");
                    let city = tag_value(&node.tags, "addr:city")
                        .or_else(|| tag_value(&node.tags, "addr:place"));
                    let postcode = tag_value(&node.tags, "addr:postcode");
                    conn.execute(
                        "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
                         VALUES (?, 'node', ?, ?, ?, ?, ST_Point(?, ?))",
                        duckdb::params![node.id, hn, street, city, postcode, node.lon, node.lat],
                    )?;
                }
            }
        }
    }

    // --- Apply way changes ---
    for way in &changes.ways {
        match way.action {
            ChangeAction::Delete => {
                if let Some(old_node_ids) = kvstore::get_way(kv, way.id)? {
                    for &nid in &old_node_ids {
                        kvstore::remove_node_to_ways(kv, nid, way.id)?;
                    }
                }
                let rel_ids = kvstore::get_way_to_relations(kv, way.id)?;
                affected_relation_ids.extend(&rel_ids);
                kvstore::delete_way(kv, way.id)?;
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                if let Some(old_node_ids) = kvstore::get_way(kv, way.id)? {
                    for &nid in &old_node_ids {
                        kvstore::remove_node_to_ways(kv, nid, way.id)?;
                    }
                }
                kvstore::put_way(kv, way.id, &way.node_refs)?;
                for &nid in &way.node_refs {
                    kvstore::add_node_to_ways(kv, nid, way.id)?;
                }
                let rel_ids = kvstore::get_way_to_relations(kv, way.id)?;
                affected_relation_ids.extend(&rel_ids);
                affected_way_ids.insert(way.id);
            }
        }
    }

    // --- Apply relation changes ---
    for rel in &changes.relations {
        match rel.action {
            ChangeAction::Delete => {
                if let Some(old_members) = kvstore::get_relation(kv, rel.id)? {
                    for (ref_id, member_type, _) in &old_members {
                        if *member_type == encoding::encode_member_type("way") {
                            kvstore::remove_way_to_relations(kv, *ref_id, rel.id)?;
                        }
                    }
                }
                kvstore::delete_relation(kv, rel.id)?;
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                if let Some(old_members) = kvstore::get_relation(kv, rel.id)? {
                    for (ref_id, member_type, _) in &old_members {
                        if *member_type == encoding::encode_member_type("way") {
                            kvstore::remove_way_to_relations(kv, *ref_id, rel.id)?;
                        }
                    }
                }
                let members: Vec<(i64, u8, u8)> = rel
                    .members
                    .iter()
                    .map(|m| {
                        (
                            m.member_ref,
                            encoding::encode_member_type(&m.member_type),
                            encoding::encode_member_role(&m.role),
                        )
                    })
                    .collect();
                kvstore::put_relation(kv, rel.id, &members)?;
                for m in &rel.members {
                    if m.member_type == "way" {
                        kvstore::add_way_to_relations(kv, m.member_ref, rel.id)?;
                    }
                }
                affected_relation_ids.insert(rel.id);
            }
        }
    }

    // --- Rebuild affected way geometries ---
    for &way_id in &affected_way_ids {
        rebuild_way_geometry(conn, kv, way_id, &changes.ways)?;
    }

    // Cascade way changes to relations
    for &way_id in &affected_way_ids {
        let rel_ids = kvstore::get_way_to_relations(kv, way_id)?;
        affected_relation_ids.extend(&rel_ids);
    }

    // --- Rebuild affected relation geometries ---
    for &relation_id in &affected_relation_ids {
        rebuild_relation_geometry(conn, kv, relation_id, &changes.relations)?;
    }

    Ok(())
}

fn tag_value(tags: &[(String, String)], key: &str) -> Option<String> {
    tags.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}

fn rebuild_way_geometry(
    conn: &Connection,
    kv: &RocksDB,
    way_id: i64,
    way_changes: &[WayChange],
) -> Result<()> {
    if kvstore::get_way(kv, way_id)?.is_none() {
        return Ok(());
    }

    // Determine tags: from the change if directly affected, else from DuckDB existence.
    // For indirectly affected ways, check DuckDB BEFORE deleting old entries.
    let way_change = way_changes.iter().find(|w| w.id == way_id);
    let (building_tag, housenumber, street, city, postcode) = match way_change {
        Some(wc) => (
            tag_value(&wc.tags, "building"),
            tag_value(&wc.tags, "addr:housenumber"),
            tag_value(&wc.tags, "addr:street"),
            tag_value(&wc.tags, "addr:city").or_else(|| tag_value(&wc.tags, "addr:place")),
            tag_value(&wc.tags, "addr:postcode"),
        ),
        None => {
            let has_building: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way')",
                    [way_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            let has_address: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way')",
                    [way_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);

            if !has_building && !has_address {
                return Ok(());
            }
            (
                if has_building {
                    Some("yes".to_string())
                } else {
                    None
                },
                if has_address {
                    Some(String::new())
                } else {
                    None
                },
                None,
                None,
                None,
            )
        }
    };

    if building_tag.is_none() && housenumber.is_none() {
        return Ok(());
    }

    conn.execute(
        "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;

    if building_tag.is_some() {
        let building = building_tag.as_deref().unwrap_or("yes");
        let building_sql = building.replace('\'', "''");
        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
             SELECT {way_id}, 'way', '{building_sql}',
                    ST_MakePolygon(ST_GeomFromWKB(resolve_way_coords({way_id})))
             WHERE resolve_way_coords({way_id}) IS NOT NULL
               AND ST_NPoints(ST_GeomFromWKB(resolve_way_coords({way_id}))) >= 4"
        ))?;
    }

    if housenumber.is_some() {
        conn.execute(
            "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
             SELECT ?, 'way', ?, ?, ?, ?,
                    ST_Centroid(ST_GeomFromWKB(resolve_way_coords(?)))
             WHERE resolve_way_coords(?) IS NOT NULL",
            duckdb::params![way_id, housenumber, street, city, postcode, way_id, way_id],
        )?;
    }

    Ok(())
}

fn rebuild_relation_geometry(
    conn: &Connection,
    kv: &RocksDB,
    relation_id: i64,
    relation_changes: &[RelationChange],
) -> Result<()> {
    let members = match kvstore::get_relation(kv, relation_id)? {
        Some(m) => m,
        None => return Ok(()),
    };

    // Determine tags: from the change if directly affected, else from DuckDB existence.
    // Check DuckDB BEFORE deleting old entries.
    let rel_change = relation_changes.iter().find(|r| r.id == relation_id);
    let (building_tag, housenumber, street, city, postcode) = match rel_change {
        Some(rc) => (
            tag_value(&rc.tags, "building"),
            tag_value(&rc.tags, "addr:housenumber"),
            tag_value(&rc.tags, "addr:street"),
            tag_value(&rc.tags, "addr:city").or_else(|| tag_value(&rc.tags, "addr:place")),
            tag_value(&rc.tags, "addr:postcode"),
        ),
        None => {
            let has_building: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation')",
                    [relation_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            let has_address: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation')",
                    [relation_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);

            if !has_building && !has_address {
                return Ok(());
            }
            (
                if has_building {
                    Some("yes".to_string())
                } else {
                    None
                },
                if has_address {
                    Some(String::new())
                } else {
                    None
                },
                None,
                None,
                None,
            )
        }
    };

    if building_tag.is_none() && housenumber.is_none() {
        return Ok(());
    }

    conn.execute(
        "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
        [relation_id],
    )?;
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
        [relation_id],
    )?;

    // Build a VALUES list of way members: (way_id, role)
    let way_members: Vec<(i64, &str)> = members
        .iter()
        .filter(|(_, member_type, _)| *member_type == encoding::encode_member_type("way"))
        .map(|(ref_id, _, role)| (*ref_id, encoding::decode_member_role(*role)))
        .collect();

    if way_members.is_empty() {
        return Ok(());
    }

    let values_sql: String = way_members
        .iter()
        .map(|(wid, role)| format!("({wid}, '{role}')"))
        .collect::<Vec<_>>()
        .join(", ");

    if building_tag.is_some() {
        let building = building_tag.as_deref().unwrap_or("yes");
        let building_sql = building.replace('\'', "''");
        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
             WITH way_members(way_id, member_role) AS (VALUES {values_sql}),
             way_geoms AS (
                 SELECT way_id, member_role,
                        ST_GeomFromWKB(resolve_way_coords(way_id)) AS line_geom
                 FROM way_members
                 WHERE resolve_way_coords(way_id) IS NOT NULL
             ),
             outer_polys AS (
                 SELECT ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
                 FROM way_geoms
                 WHERE (member_role = 'outer' OR member_role = '')
                   AND ST_NPoints(line_geom) >= 4
             ),
             inner_polys AS (
                 SELECT ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
                 FROM way_geoms
                 WHERE member_role = 'inner'
                   AND ST_NPoints(line_geom) >= 4
             )
             SELECT
                 {relation_id}, 'relation', '{building_sql}',
                 CASE
                     WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                     ELSE o.outer_geom
                 END
             FROM outer_polys o
             LEFT JOIN inner_polys i ON true
             WHERE o.outer_geom IS NOT NULL"
        ))?;
    }

    if housenumber.is_some() {
        let hn_sql = housenumber
            .as_deref()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());
        let street_sql = street
            .as_deref()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());
        let city_sql = city
            .as_deref()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());
        let postcode_sql = postcode
            .as_deref()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());

        conn.execute_batch(&format!(
            "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
             WITH way_members(way_id, member_role) AS (VALUES {values_sql}),
             way_geoms AS (
                 SELECT ST_GeomFromWKB(resolve_way_coords(way_id)) AS line_geom
                 FROM way_members
                 WHERE resolve_way_coords(way_id) IS NOT NULL
             )
             SELECT {relation_id}, 'relation', {hn_sql}, {street_sql}, {city_sql}, {postcode_sql},
                    ST_Centroid(ST_Collect(list(line_geom)))
             FROM way_geoms"
        ))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::db::init_db;
    use crate::osm::kvstore;
    use crate::osm::replication::NodeChange;

    fn setup_test_db_and_kv() -> Result<(Connection, Arc<RocksDB>, tempfile::TempDir)> {
        let tmpdir = tempfile::tempdir()?;
        let kv = Arc::new(kvstore::open(tmpdir.path(), 8, 4)?);
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, Some(kv.clone()))?;

        // Seed KV store with test data
        kvstore::put_node(&kv, 1, 20.0, 50.0)?;
        kvstore::put_node(&kv, 2, 20.001, 50.0)?;
        kvstore::put_node(&kv, 3, 20.001, 50.001)?;
        kvstore::put_node(&kv, 4, 20.0, 50.001)?;

        kvstore::put_way(&kv, 100, &[1, 2, 3, 4, 1])?;
        for &nid in &[1i64, 2, 3, 4] {
            kvstore::add_node_to_ways(&kv, nid, 100)?;
        }

        // Seed DuckDB with existing building geometry
        conn.execute_batch(
            "INSERT INTO osm_buildings VALUES (100, 'way', 'yes', ST_MakePolygon(ST_MakeLine(
                list_value(ST_Point(20.0, 50.0), ST_Point(20.001, 50.0),
                           ST_Point(20.001, 50.001), ST_Point(20.0, 50.001),
                           ST_Point(20.0, 50.0))
            )));
            INSERT INTO metadata VALUES ('osm_replication_sequence', '1000');",
        )?;

        Ok((conn, kv, tmpdir))
    }

    #[test]
    fn test_apply_node_create() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Create,
                id: 10,
                lon: 21.0,
                lat: 51.0,
                tags: vec![
                    ("addr:housenumber".into(), "5".into()),
                    ("addr:street".into(), "Nowa".into()),
                ],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        // Node should be in RocksDB
        let coords = kvstore::get_node(&kv, 10)?.unwrap();
        assert!((coords.0 - 21.0).abs() < 1e-9);

        // Address should be in DuckDB
        let hn: String = conn.query_row(
            "SELECT housenumber FROM osm_addresses WHERE osm_id = 10 AND osm_type = 'node'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(hn, "5");

        Ok(())
    }

    #[test]
    fn test_apply_node_delete() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let create = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Create,
                id: 20,
                lon: 21.0,
                lat: 51.0,
                tags: vec![("addr:housenumber".into(), "10".into())],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &create)?;

        let delete = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Delete,
                id: 20,
                lon: 0.0,
                lat: 0.0,
                tags: vec![],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &delete)?;

        assert!(kvstore::get_node(&kv, 20)?.is_none());

        let addr_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_addresses WHERE osm_id = 20",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(addr_count, 0);

        Ok(())
    }

    #[test]
    fn test_apply_node_modify_cascades_to_way() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Modify,
                id: 1,
                lon: 20.0005,
                lat: 50.0005,
                tags: vec![],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        // Node should be updated in RocksDB
        let (lon, lat) = kvstore::get_node(&kv, 1)?.unwrap();
        assert!((lon - 20.0005).abs() < 1e-9);
        assert!((lat - 50.0005).abs() < 1e-9);

        // Building geometry should have been rebuilt
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1, "Building should still exist after node modify");

        Ok(())
    }

    #[test]
    fn test_apply_way_delete() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Delete,
                id: 100,
                node_refs: vec![],
                tags: vec![],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        let building_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(building_count, 0);

        assert!(kvstore::get_way(&kv, 100)?.is_none());

        Ok(())
    }
}
