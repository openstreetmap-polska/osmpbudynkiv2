use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use flate2::read::GzDecoder;
use tracing::info;

use crate::download::download_file;
use crate::osm::replication::{
    ChangeAction, NodeChange, OsmChange, RelationChange, WayChange, parse_osc, parse_state_txt,
    sequence_to_path,
};

pub fn update(conn: &Connection, replication_base_url: &str) -> Result<()> {
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
        apply_sequence(conn, seq, replication_base_url)?;

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
    // Remove cached file so next call fetches fresh state
    let _ = std::fs::remove_file(&state_path);
    parse_state_txt(&text)
}

fn apply_sequence(conn: &Connection, seq: u64, replication_base_url: &str) -> Result<()> {
    let path = sequence_to_path(seq);
    let url = format!("{replication_base_url}/{path}");

    let osc_gz_path = download_file(&url, Path::new("./data/replication"))?;
    let osc_xml = decompress_gz(&osc_gz_path)?;
    // Clean up after decompression
    let _ = std::fs::remove_file(&osc_gz_path);

    let changes = parse_osc(&osc_xml)?;
    apply_changes(conn, &changes)?;

    // Update sequence number
    conn.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES ('osm_replication_sequence', ?)",
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

fn apply_changes(conn: &Connection, changes: &OsmChange) -> Result<()> {
    apply_node_changes(conn, &changes.nodes)?;
    apply_way_changes(conn, &changes.ways)?;
    apply_relation_changes(conn, &changes.relations)?;
    Ok(())
}

fn apply_node_changes(conn: &Connection, nodes: &[NodeChange]) -> Result<()> {
    for node in nodes {
        match node.action {
            ChangeAction::Delete => {
                conn.execute("DELETE FROM osm_nodes WHERE node_id = ?", [node.id])?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'node'",
                    [node.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                // Upsert node coordinates
                conn.execute(
                    "INSERT OR REPLACE INTO osm_nodes (node_id, lon, lat) VALUES (?, ?, ?)",
                    duckdb::params![node.id, node.lon, node.lat],
                )?;

                // Remove old address entry if any
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'node'",
                    [node.id],
                )?;

                // If this node has an address, insert it
                let housenumber = node.tags.iter().find(|(k, _)| k == "addr:housenumber");
                if let Some((_, hn)) = housenumber {
                    let street = tag_value(&node.tags, "addr:street");
                    let city = tag_value(&node.tags, "addr:city");
                    let postcode = tag_value(&node.tags, "addr:postcode");
                    conn.execute(
                        "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
                         VALUES (?, 'node', ?, ?, ?, ?, ST_Point(?, ?))",
                        duckdb::params![node.id, hn, street, city, postcode, node.lon, node.lat],
                    )?;
                }

                // Update geometries of ways that reference this node
                update_ways_referencing_node(conn, node.id)?;
            }
        }
    }
    Ok(())
}

fn apply_way_changes(conn: &Connection, ways: &[WayChange]) -> Result<()> {
    for way in ways {
        match way.action {
            ChangeAction::Delete => {
                conn.execute("DELETE FROM osm_way_nodes WHERE way_id = ?", [way.id])?;
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute("DELETE FROM osm_way_tags WHERE way_id = ?", [way.id])?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                // Update way-node references
                conn.execute("DELETE FROM osm_way_nodes WHERE way_id = ?", [way.id])?;
                for (pos, &node_ref) in way.node_refs.iter().enumerate() {
                    conn.execute(
                        "INSERT INTO osm_way_nodes (way_id, node_id, position) VALUES (?, ?, ?)",
                        duckdb::params![way.id, node_ref, (pos + 1) as i32],
                    )?;
                }

                // Clean old geometry entries
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute("DELETE FROM osm_way_tags WHERE way_id = ?", [way.id])?;

                let building = tag_value(&way.tags, "building");
                let housenumber = tag_value(&way.tags, "addr:housenumber");

                if building.is_some() || housenumber.is_some() {
                    // Reconstruct way tags as a MAP literal for osm_way_tags
                    let tag_pairs: Vec<String> = way
                        .tags
                        .iter()
                        .map(|(k, v)| format!("'{k}': '{v}'"))
                        .collect();
                    let map_literal = format!("MAP {{{}}}", tag_pairs.join(", "));
                    conn.execute(
                        &format!(
                            "INSERT INTO osm_way_tags (way_id, tags) VALUES ({}, {})",
                            way.id, map_literal
                        ),
                        [],
                    )?;
                }

                // Rebuild geometry for this specific way
                rebuild_way_geometry(conn, way.id)?;
            }
        }
    }
    Ok(())
}

fn apply_relation_changes(conn: &Connection, relations: &[RelationChange]) -> Result<()> {
    for rel in relations {
        match rel.action {
            ChangeAction::Delete => {
                conn.execute("DELETE FROM osm_relations WHERE relation_id = ?", [rel.id])?;
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_relation_tags WHERE relation_id = ?",
                    [rel.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                // Update relation members
                conn.execute("DELETE FROM osm_relations WHERE relation_id = ?", [rel.id])?;
                for (pos, member) in rel.members.iter().enumerate() {
                    conn.execute(
                        "INSERT INTO osm_relations (relation_id, member_id, member_type, member_role, position) VALUES (?, ?, ?, ?, ?)",
                        duckdb::params![rel.id, member.member_ref, member.member_type, member.role, (pos + 1) as i32],
                    )?;
                }

                // Clean old entries
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_relation_tags WHERE relation_id = ?",
                    [rel.id],
                )?;

                let building = tag_value(&rel.tags, "building");
                let housenumber = tag_value(&rel.tags, "addr:housenumber");
                let street = tag_value(&rel.tags, "addr:street");
                let city = tag_value(&rel.tags, "addr:city");
                let postcode = tag_value(&rel.tags, "addr:postcode");

                if building.is_some() || housenumber.is_some() {
                    conn.execute(
                        "INSERT INTO osm_relation_tags (relation_id, building, housenumber, street, city, postcode) VALUES (?, ?, ?, ?, ?, ?)",
                        duckdb::params![rel.id, building, housenumber, street, city, postcode],
                    )?;
                }

                // Rebuild relation geometry
                rebuild_relation_geometry(conn, rel.id)?;
            }
        }
    }
    Ok(())
}

fn tag_value(tags: &[(String, String)], key: &str) -> Option<String> {
    tags.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}

fn update_ways_referencing_node(conn: &Connection, node_id: i64) -> Result<()> {
    // Find all ways that reference this node
    let mut stmt = conn.prepare("SELECT DISTINCT way_id FROM osm_way_nodes WHERE node_id = ?")?;
    let way_ids: Vec<i64> = stmt
        .query_map([node_id], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for way_id in way_ids {
        rebuild_way_geometry(conn, way_id)?;
    }

    Ok(())
}

fn rebuild_way_geometry(conn: &Connection, way_id: i64) -> Result<()> {
    // Check if this way has relevant tags
    let has_tags: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM osm_way_tags WHERE way_id = ?",
            [way_id],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !has_tags {
        return Ok(());
    }

    // Remove old geometry entries for this way
    conn.execute(
        "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;

    // Rebuild building geometry
    conn.execute(
        &format!(
            "
            INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
            SELECT
                w.way_id AS osm_id,
                'way' AS osm_type,
                w.building,
                ST_MakePolygon(
                    ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY wn.position))
                ) AS geom
            FROM (
                SELECT way_id, element_at(tags, 'building')[1] AS building
                FROM osm_way_tags WHERE way_id = {way_id}
            ) w
            JOIN osm_way_nodes wn ON w.way_id = wn.way_id
            JOIN osm_nodes n ON wn.node_id = n.node_id
            WHERE w.building IS NOT NULL
            GROUP BY w.way_id, w.building
            HAVING COUNT(*) >= 4
            "
        ),
        [],
    )?;

    // Rebuild address geometry
    conn.execute(
        &format!(
            "
            INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
            SELECT
                w.way_id AS osm_id,
                'way' AS osm_type,
                element_at(w.tags, 'addr:housenumber')[1] AS housenumber,
                element_at(w.tags, 'addr:street')[1] AS street,
                element_at(w.tags, 'addr:city')[1] AS city,
                element_at(w.tags, 'addr:postcode')[1] AS postcode,
                ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
            FROM osm_way_tags w
            JOIN osm_way_nodes wn ON w.way_id = wn.way_id
            JOIN osm_nodes n ON wn.node_id = n.node_id
            WHERE w.way_id = {way_id}
              AND element_at(w.tags, 'addr:housenumber')[1] IS NOT NULL
            GROUP BY w.way_id, element_at(w.tags, 'addr:housenumber')[1],
                     element_at(w.tags, 'addr:street')[1],
                     element_at(w.tags, 'addr:city')[1],
                     element_at(w.tags, 'addr:postcode')[1]
            "
        ),
        [],
    )?;

    Ok(())
}

fn rebuild_relation_geometry(conn: &Connection, relation_id: i64) -> Result<()> {
    // Check if this relation has relevant tags
    let has_tags: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM osm_relation_tags WHERE relation_id = ?",
            [relation_id],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !has_tags {
        return Ok(());
    }

    // Building geometry from relation
    conn.execute(
        &format!(
            "
            INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
            WITH rel_way_lines AS (
                SELECT
                    r.relation_id,
                    r.member_role,
                    ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY wn.position)) AS line_geom
                FROM osm_relations r
                JOIN osm_way_nodes wn ON r.member_id = wn.way_id
                JOIN osm_nodes n ON wn.node_id = n.node_id
                WHERE r.relation_id = {relation_id}
                  AND r.member_type = 'way'
                GROUP BY r.relation_id, r.member_id, r.member_role
                HAVING COUNT(*) >= 2
            ),
            outer_polys AS (
                SELECT relation_id, ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
                FROM rel_way_lines
                WHERE (member_role = 'outer' OR member_role = '')
                  AND ST_NPoints(line_geom) >= 4
                GROUP BY relation_id
            ),
            inner_polys AS (
                SELECT relation_id, ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
                FROM rel_way_lines
                WHERE member_role = 'inner'
                  AND ST_NPoints(line_geom) >= 4
                GROUP BY relation_id
            )
            SELECT
                o.relation_id AS osm_id,
                'relation' AS osm_type,
                rt.building,
                CASE
                    WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                    ELSE o.outer_geom
                END AS geom
            FROM outer_polys o
            JOIN osm_relation_tags rt ON o.relation_id = rt.relation_id
            LEFT JOIN inner_polys i ON o.relation_id = i.relation_id
            WHERE rt.building IS NOT NULL AND o.outer_geom IS NOT NULL
            "
        ),
        [],
    )?;

    // Address geometry from relation
    conn.execute(
        &format!(
            "
            INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
            SELECT
                rt.relation_id AS osm_id,
                'relation' AS osm_type,
                rt.housenumber,
                rt.street,
                rt.city,
                rt.postcode,
                ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
            FROM osm_relation_tags rt
            JOIN osm_relations r ON rt.relation_id = r.relation_id
            JOIN osm_way_nodes wn ON r.member_id = wn.way_id AND r.member_type = 'way'
            JOIN osm_nodes n ON wn.node_id = n.node_id
            WHERE rt.relation_id = {relation_id}
              AND rt.housenumber IS NOT NULL
            GROUP BY rt.relation_id, rt.housenumber, rt.street, rt.city, rt.postcode
            "
        ),
        [],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;

    fn setup_test_db() -> Result<Connection> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS osm_way_tags (
                way_id BIGINT,
                tags MAP(VARCHAR, VARCHAR)
            );
            CREATE TABLE IF NOT EXISTS osm_relation_tags (
                relation_id BIGINT,
                building VARCHAR,
                housenumber VARCHAR,
                street VARCHAR,
                city VARCHAR,
                postcode VARCHAR
            );

            -- Seed with some initial data
            INSERT INTO osm_nodes VALUES (1, 20.0, 50.0);
            INSERT INTO osm_nodes VALUES (2, 20.001, 50.0);
            INSERT INTO osm_nodes VALUES (3, 20.001, 50.001);
            INSERT INTO osm_nodes VALUES (4, 20.0, 50.001);

            INSERT INTO osm_way_nodes VALUES (100, 1, 1);
            INSERT INTO osm_way_nodes VALUES (100, 2, 2);
            INSERT INTO osm_way_nodes VALUES (100, 3, 3);
            INSERT INTO osm_way_nodes VALUES (100, 4, 4);
            INSERT INTO osm_way_nodes VALUES (100, 1, 5);

            INSERT INTO osm_way_tags VALUES (100, MAP {'building': 'yes'});

            INSERT INTO osm_buildings VALUES (100, 'way', 'yes', ST_MakePolygon(ST_MakeLine(
                list_value(ST_Point(20.0, 50.0), ST_Point(20.001, 50.0),
                           ST_Point(20.001, 50.001), ST_Point(20.0, 50.001),
                           ST_Point(20.0, 50.0))
            )));

            INSERT INTO metadata VALUES ('osm_replication_sequence', '1000');
            ",
        )?;
        Ok(conn)
    }

    #[test]
    fn test_apply_node_create() -> Result<()> {
        let conn = setup_test_db()?;

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

        apply_changes(&conn, &changes)?;

        // Node should exist
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_nodes WHERE node_id = 10",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);

        // Address should exist
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
        let conn = setup_test_db()?;

        // First create a node with address
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
        apply_changes(&conn, &create)?;

        // Then delete it
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
        apply_changes(&conn, &delete)?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_nodes WHERE node_id = 20",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 0);

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
        let conn = setup_test_db()?;

        // Modify node 1 which is part of way 100
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

        apply_changes(&conn, &changes)?;

        // Node should be updated
        let (lon, lat): (f64, f64) = conn.query_row(
            "SELECT lon, lat FROM osm_nodes WHERE node_id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
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
        let conn = setup_test_db()?;

        let changes = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Delete,
                id: 100,
                node_refs: vec![],
                tags: vec![],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &changes)?;

        let building_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(building_count, 0);

        let wn_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_way_nodes WHERE way_id = 100",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(wn_count, 0);

        Ok(())
    }
}
