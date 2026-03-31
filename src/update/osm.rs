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
                // DELETE + INSERT (no PK for INSERT OR REPLACE)
                conn.execute("DELETE FROM osm_nodes WHERE node_id = ?", [node.id])?;
                conn.execute(
                    "INSERT INTO osm_nodes (node_id, lon, lat) VALUES (?, ?, ?)",
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
                    let city = tag_value(&node.tags, "addr:city")
                        .or_else(|| tag_value(&node.tags, "addr:place"));
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
                conn.execute("DELETE FROM osm_ways WHERE way_id = ?", [way.id])?;
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
                conn.execute("DELETE FROM osm_ways WHERE way_id = ?", [way.id])?;

                let node_ids_literal = format!(
                    "[{}]",
                    way.node_refs
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let tag_pairs: Vec<String> = way
                    .tags
                    .iter()
                    .map(|(k, v)| {
                        format!("'{}': '{}'", k.replace('\'', "''"), v.replace('\'', "''"))
                    })
                    .collect();
                let map_literal = if tag_pairs.is_empty() {
                    "MAP([]::VARCHAR[], []::VARCHAR[])".to_string()
                } else {
                    format!("MAP {{{}}}", tag_pairs.join(", "))
                };
                conn.execute_batch(&format!(
                    "INSERT INTO osm_ways (way_id, node_ids, tags) VALUES ({}, {}, {})",
                    way.id, node_ids_literal, map_literal
                ))?;

                // Clean old geometry entries
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;

                // Rebuild geometry for this way
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
            }
            ChangeAction::Create | ChangeAction::Modify => {
                conn.execute("DELETE FROM osm_relations WHERE relation_id = ?", [rel.id])?;

                let refs_literal = format!(
                    "[{}]",
                    rel.members
                        .iter()
                        .map(|m| m.member_ref.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let types_literal = format!(
                    "[{}]",
                    rel.members
                        .iter()
                        .map(|m| format!("'{}'", m.member_type))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let roles_literal = format!(
                    "[{}]",
                    rel.members
                        .iter()
                        .map(|m| format!("'{}'", m.role.replace('\'', "''")))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let tag_pairs: Vec<String> = rel
                    .tags
                    .iter()
                    .map(|(k, v)| {
                        format!("'{}': '{}'", k.replace('\'', "''"), v.replace('\'', "''"))
                    })
                    .collect();
                let map_literal = if tag_pairs.is_empty() {
                    "MAP([]::VARCHAR[], []::VARCHAR[])".to_string()
                } else {
                    format!("MAP {{{}}}", tag_pairs.join(", "))
                };

                conn.execute_batch(&format!(
                    "INSERT INTO osm_relations (relation_id, member_refs, member_types, member_roles, tags) VALUES ({}, {}, {}, {}, {})",
                    rel.id, refs_literal, types_literal, roles_literal, map_literal
                ))?;

                // Clean old geometry entries
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;

                // Rebuild geometry
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
    let mut stmt = conn.prepare("SELECT way_id FROM osm_ways WHERE list_contains(node_ids, ?)")?;
    let way_ids: Vec<i64> = stmt
        .query_map([node_id], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for way_id in way_ids {
        rebuild_way_geometry(conn, way_id)?;
    }

    Ok(())
}

fn rebuild_way_geometry(conn: &Connection, way_id: i64) -> Result<()> {
    let has_tags: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM osm_ways WHERE way_id = ?
             AND (element_at(tags, 'building')[1] IS NOT NULL
                  OR element_at(tags, 'addr:housenumber')[1] IS NOT NULL)",
            [way_id],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !has_tags {
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

    conn.execute(
        &format!(
            "
            INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
            WITH way_nodes AS (
                SELECT
                    w.way_id,
                    element_at(w.tags, 'building')[1] AS building,
                    UNNEST(w.node_ids) AS node_id,
                    UNNEST(generate_series(1, len(w.node_ids))) AS position
                FROM osm_ways w
                WHERE w.way_id = {way_id}
                  AND element_at(w.tags, 'building')[1] IS NOT NULL
            )
            SELECT
                wn.way_id AS osm_id,
                'way' AS osm_type,
                wn.building,
                ST_MakePolygon(
                    ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY wn.position))
                ) AS geom
            FROM way_nodes wn
            JOIN osm_nodes n ON wn.node_id = n.node_id
            GROUP BY wn.way_id, wn.building
            HAVING COUNT(*) >= 4
            "
        ),
        [],
    )?;

    conn.execute(
        &format!(
            "
            INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
            WITH way_nodes AS (
                SELECT
                    w.way_id,
                    element_at(w.tags, 'addr:housenumber')[1] AS housenumber,
                    element_at(w.tags, 'addr:street')[1] AS street,
                    COALESCE(element_at(w.tags, 'addr:city')[1], element_at(w.tags, 'addr:place')[1]) AS city,
                    element_at(w.tags, 'addr:postcode')[1] AS postcode,
                    UNNEST(w.node_ids) AS node_id
                FROM osm_ways w
                WHERE w.way_id = {way_id}
                  AND element_at(w.tags, 'addr:housenumber')[1] IS NOT NULL
            )
            SELECT
                wn.way_id AS osm_id,
                'way' AS osm_type,
                wn.housenumber,
                wn.street,
                wn.city,
                wn.postcode,
                ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
            FROM way_nodes wn
            JOIN osm_nodes n ON wn.node_id = n.node_id
            GROUP BY wn.way_id, wn.housenumber, wn.street, wn.city, wn.postcode
            "
        ),
        [],
    )?;

    Ok(())
}

fn rebuild_relation_geometry(conn: &Connection, relation_id: i64) -> Result<()> {
    let has_tags: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM osm_relations WHERE relation_id = ?
             AND (element_at(tags, 'building')[1] IS NOT NULL
                  OR element_at(tags, 'addr:housenumber')[1] IS NOT NULL)",
            [relation_id],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !has_tags {
        return Ok(());
    }

    conn.execute(
        &format!(
            "
            INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
            WITH rel_members AS (
                SELECT
                    r.relation_id,
                    UNNEST(r.member_refs) AS member_id,
                    UNNEST(r.member_types) AS member_type,
                    UNNEST(r.member_roles) AS member_role
                FROM osm_relations r
                WHERE r.relation_id = {relation_id}
            ),
            member_way_nodes AS (
                SELECT
                    rm.relation_id,
                    rm.member_id AS way_id,
                    rm.member_role,
                    UNNEST(w.node_ids) AS node_id,
                    UNNEST(generate_series(1, len(w.node_ids))) AS position
                FROM rel_members rm
                JOIN osm_ways w ON rm.member_id = w.way_id
                WHERE rm.member_type = 'way'
            ),
            rel_way_lines AS (
                SELECT
                    mwn.relation_id,
                    mwn.member_role,
                    ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY mwn.position)) AS line_geom
                FROM member_way_nodes mwn
                JOIN osm_nodes n ON mwn.node_id = n.node_id
                GROUP BY mwn.relation_id, mwn.way_id, mwn.member_role
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
                element_at(r.tags, 'building')[1] AS building,
                CASE
                    WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                    ELSE o.outer_geom
                END AS geom
            FROM outer_polys o
            JOIN osm_relations r ON o.relation_id = r.relation_id
            LEFT JOIN inner_polys i ON o.relation_id = i.relation_id
            WHERE element_at(r.tags, 'building')[1] IS NOT NULL AND o.outer_geom IS NOT NULL
            "
        ),
        [],
    )?;

    conn.execute(
        &format!(
            "
            INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
            WITH rel_members AS (
                SELECT
                    r.relation_id,
                    element_at(r.tags, 'addr:housenumber')[1] AS housenumber,
                    element_at(r.tags, 'addr:street')[1] AS street,
                    COALESCE(element_at(r.tags, 'addr:city')[1], element_at(r.tags, 'addr:place')[1]) AS city,
                    element_at(r.tags, 'addr:postcode')[1] AS postcode,
                    UNNEST(r.member_refs) AS member_id,
                    UNNEST(r.member_types) AS member_type
                FROM osm_relations r
                WHERE r.relation_id = {relation_id}
                  AND element_at(r.tags, 'addr:housenumber')[1] IS NOT NULL
            ),
            member_nodes AS (
                SELECT
                    rm.relation_id,
                    rm.housenumber,
                    rm.street,
                    rm.city,
                    rm.postcode,
                    UNNEST(w.node_ids) AS node_id
                FROM rel_members rm
                JOIN osm_ways w ON rm.member_id = w.way_id
                WHERE rm.member_type = 'way'
            )
            SELECT
                mn.relation_id AS osm_id,
                'relation' AS osm_type,
                mn.housenumber,
                mn.street,
                mn.city,
                mn.postcode,
                ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
            FROM member_nodes mn
            JOIN osm_nodes n ON mn.node_id = n.node_id
            GROUP BY mn.relation_id, mn.housenumber, mn.street, mn.city, mn.postcode
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
            -- Seed with some initial data
            INSERT INTO osm_nodes VALUES (1, 20.0, 50.0);
            INSERT INTO osm_nodes VALUES (2, 20.001, 50.0);
            INSERT INTO osm_nodes VALUES (3, 20.001, 50.001);
            INSERT INTO osm_nodes VALUES (4, 20.0, 50.001);

            INSERT INTO osm_ways VALUES (100, [1, 2, 3, 4, 1], MAP {'building': 'yes'});

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

        let way_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_ways WHERE way_id = 100",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(way_count, 0);

        Ok(())
    }
}
