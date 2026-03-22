use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::download::download_file;
use crate::osm::geometry;

const OSM_PBF_URL: &str = "https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf";

pub fn import(conn: &Connection, file: Option<&Path>) -> Result<()> {
    let pbf_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_file(OSM_PBF_URL, Path::new("./data"))?,
    };

    let pbf_str = pbf_path.to_str().context("PBF path is not valid UTF-8")?;

    info!(path = pbf_str, "Starting OSM import");

    import_nodes(conn, pbf_str)?;
    import_address_nodes(conn, pbf_str)?;
    import_ways(conn, pbf_str)?;
    geometry::build_way_geometries(conn)?;
    import_relations(conn, pbf_str)?;
    geometry::build_relation_geometries(conn)?;

    log_import_stats(conn)?;

    info!("OSM import complete");
    Ok(())
}

fn import_nodes(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Pass 1: Importing nodes");
    conn.execute_batch(&format!(
        "
        INSERT INTO osm_nodes (node_id, lon, lat)
        SELECT id, lon, lat
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'node' AND lon IS NOT NULL AND lat IS NOT NULL;
        "
    ))
    .context("Failed to import nodes")?;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM osm_nodes", [], |row| row.get(0))?;
    info!(count, "Nodes imported");

    Ok(())
}

fn import_address_nodes(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Pass 2: Importing address nodes");
    conn.execute_batch(&format!(
        "
        INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
        SELECT
            id AS osm_id,
            'node' AS osm_type,
            tags->>'addr:housenumber' AS housenumber,
            tags->>'addr:street' AS street,
            tags->>'addr:city' AS city,
            tags->>'addr:postcode' AS postcode,
            ST_Point(lon, lat) AS geom
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'node'
          AND tags->>'addr:housenumber' IS NOT NULL
          AND lon IS NOT NULL
          AND lat IS NOT NULL;
        "
    ))
    .context("Failed to import address nodes")?;

    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'node'",
        [],
        |row| row.get(0),
    )?;
    info!(count, "Address nodes imported");

    Ok(())
}

fn import_ways(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Pass 3: Importing ways and way-node mappings");

    // Import way-node mappings using unnest with generate_subscripts
    conn.execute_batch(&format!(
        "
        INSERT INTO osm_way_nodes (way_id, node_id, position)
        SELECT
            id AS way_id,
            UNNEST(refs) AS node_id,
            UNNEST(generate_series(1, len(refs))) AS position
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'way' AND refs IS NOT NULL AND len(refs) > 0;
        "
    ))
    .context("Failed to import way-node mappings")?;

    let wn_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_way_nodes", [], |row| row.get(0))?;
    info!(count = wn_count, "Way-node mappings imported");

    // Store way tags we care about in a temp table for geometry construction
    conn.execute_batch(&format!(
        "
        CREATE OR REPLACE TABLE osm_way_tags AS
        SELECT
            id AS way_id,
            tags
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'way'
          AND (tags->>'building' IS NOT NULL OR tags->>'addr:housenumber' IS NOT NULL);
        "
    ))
    .context("Failed to import way tags")?;

    let tag_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_way_tags", [], |row| row.get(0))?;
    info!(count = tag_count, "Ways with relevant tags stored");

    Ok(())
}

fn import_relations(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Pass 4: Importing relations");

    conn.execute_batch(&format!(
        "
        INSERT INTO osm_relations (relation_id, member_id, member_type, member_role, position)
        SELECT
            id AS relation_id,
            UNNEST(refs) AS member_id,
            UNNEST(ref_types::VARCHAR[]) AS member_type,
            UNNEST(ref_roles) AS member_role,
            UNNEST(generate_series(1, len(refs))) AS position
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'relation' AND refs IS NOT NULL AND len(refs) > 0;
        "
    ))
    .context("Failed to import relations")?;

    let rel_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT relation_id) FROM osm_relations",
        [],
        |row| row.get(0),
    )?;
    info!(count = rel_count, "Relations imported");

    // Store relation tags we care about
    conn.execute_batch(&format!(
        "
        CREATE OR REPLACE TABLE osm_relation_tags AS
        SELECT
            id AS relation_id,
            tags->>'building' AS building,
            tags->>'addr:housenumber' AS housenumber,
            tags->>'addr:street' AS street,
            tags->>'addr:city' AS city,
            tags->>'addr:postcode' AS postcode
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'relation'
          AND (tags->>'building' IS NOT NULL OR tags->>'addr:housenumber' IS NOT NULL);
        "
    ))
    .context("Failed to import relation tags")?;

    let tag_count: i64 = conn.query_row("SELECT COUNT(*) FROM osm_relation_tags", [], |row| {
        row.get(0)
    })?;
    info!(count = tag_count, "Relations with relevant tags stored");

    Ok(())
}

fn log_import_stats(conn: &Connection) -> Result<()> {
    let buildings: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_buildings", [], |row| row.get(0))?;
    let addresses: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_addresses", [], |row| row.get(0))?;
    info!(buildings, addresses, "OSM import totals");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;

    fn setup_test_db() -> Result<Connection> {
        let conn = init_db(Path::new(":memory:"))?;

        // Create the auxiliary tables that import creates
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
            ",
        )?;

        Ok(conn)
    }

    #[test]
    fn test_way_building_geometry() -> Result<()> {
        let conn = setup_test_db()?;

        // Create a square building from 4 nodes (closing the ring = 5 points)
        conn.execute_batch(
            "
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
            ",
        )?;

        geometry::build_way_geometries(&conn)?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1, "Should have 1 building");

        let geom_type: String = conn.query_row(
            "SELECT ST_GeometryType(geom) FROM osm_buildings WHERE osm_id = 100",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(geom_type, "POLYGON");

        Ok(())
    }

    #[test]
    fn test_way_address_geometry() -> Result<()> {
        let conn = setup_test_db()?;

        conn.execute_batch(
            "
            INSERT INTO osm_nodes VALUES (1, 20.0, 50.0);
            INSERT INTO osm_nodes VALUES (2, 20.002, 50.0);

            INSERT INTO osm_way_nodes VALUES (200, 1, 1);
            INSERT INTO osm_way_nodes VALUES (200, 2, 2);

            INSERT INTO osm_way_tags VALUES (200, MAP {
                'addr:housenumber': '42',
                'addr:street': 'ul. Testowa',
                'addr:city': 'Warszawa',
                'addr:postcode': '00-001'
            });
            ",
        )?;

        geometry::build_way_geometries(&conn)?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1, "Should have 1 address");

        // Check that the point is the average of the two nodes
        let (lon, lat): (f64, f64) = conn.query_row(
            "SELECT ST_X(geom), ST_Y(geom) FROM osm_addresses WHERE osm_id = 200",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!((lon - 20.001).abs() < 1e-6);
        assert!((lat - 50.0).abs() < 1e-6);

        Ok(())
    }

    #[test]
    fn test_relation_building_geometry() -> Result<()> {
        let conn = setup_test_db()?;

        // Outer ring: a square
        conn.execute_batch(
            "
            INSERT INTO osm_nodes VALUES (1, 20.0, 50.0);
            INSERT INTO osm_nodes VALUES (2, 20.01, 50.0);
            INSERT INTO osm_nodes VALUES (3, 20.01, 50.01);
            INSERT INTO osm_nodes VALUES (4, 20.0, 50.01);

            -- Inner ring (hole): a smaller square
            INSERT INTO osm_nodes VALUES (5, 20.003, 50.003);
            INSERT INTO osm_nodes VALUES (6, 20.007, 50.003);
            INSERT INTO osm_nodes VALUES (7, 20.007, 50.007);
            INSERT INTO osm_nodes VALUES (8, 20.003, 50.007);

            -- Outer way (way_id=10)
            INSERT INTO osm_way_nodes VALUES (10, 1, 1);
            INSERT INTO osm_way_nodes VALUES (10, 2, 2);
            INSERT INTO osm_way_nodes VALUES (10, 3, 3);
            INSERT INTO osm_way_nodes VALUES (10, 4, 4);
            INSERT INTO osm_way_nodes VALUES (10, 1, 5);

            -- Inner way (way_id=11)
            INSERT INTO osm_way_nodes VALUES (11, 5, 1);
            INSERT INTO osm_way_nodes VALUES (11, 6, 2);
            INSERT INTO osm_way_nodes VALUES (11, 7, 3);
            INSERT INTO osm_way_nodes VALUES (11, 8, 4);
            INSERT INTO osm_way_nodes VALUES (11, 5, 5);

            -- Relation 300 references both ways
            INSERT INTO osm_relations VALUES (300, 10, 'way', 'outer', 1);
            INSERT INTO osm_relations VALUES (300, 11, 'way', 'inner', 2);

            INSERT INTO osm_relation_tags VALUES (300, 'yes', NULL, NULL, NULL, NULL);
            ",
        )?;

        geometry::build_relation_geometries(&conn)?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_type = 'relation'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1, "Should have 1 relation building");

        // The area with hole should be less than the outer area alone
        let area: f64 = conn.query_row(
            "SELECT ST_Area(geom) FROM osm_buildings WHERE osm_id = 300",
            [],
            |row| row.get(0),
        )?;
        assert!(area > 0.0, "Building should have positive area");

        // Outer area = 0.01 * 0.01 = 0.0001
        // Inner area = 0.004 * 0.004 = 0.000016
        // Expected ≈ 0.000084
        assert!(area < 0.0001, "Area should be less than outer ring alone");
        assert!(area > 0.00005, "Area should still be substantial");

        Ok(())
    }
}
