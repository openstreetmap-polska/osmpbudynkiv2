use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::download::download_file;
use crate::osm::geometry;

pub fn import(conn: &Connection, file: Option<&Path>, url: &str) -> Result<()> {
    let pbf_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_file(url, Path::new("./data"))?,
    };

    let pbf_str = pbf_path.to_str().context("PBF path is not valid UTF-8")?;

    let has_data: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM osm_nodes LIMIT 1)",
        [],
        |row| row.get(0),
    )?;
    if has_data {
        anyhow::bail!("OSM data already imported. Drop the database and reimport if needed.");
    }

    info!(path = pbf_str, "Starting OSM import");

    import_nodes(conn, pbf_str)?;
    import_address_nodes(conn, pbf_str)?;
    import_ways(conn, pbf_str)?;
    geometry::build_way_geometries(conn)?;
    import_relations(conn, pbf_str)?;
    geometry::build_relation_geometries(conn)?;
    create_spatial_indexes(conn)?;

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
            element_at(tags, 'addr:housenumber')[1] AS housenumber,
            element_at(tags, 'addr:street')[1] AS street,
            element_at(tags, 'addr:city')[1] AS city,
            element_at(tags, 'addr:postcode')[1] AS postcode,
            ST_Point(lon, lat) AS geom
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'node'
          AND element_at(tags, 'addr:housenumber')[1] IS NOT NULL
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
    info!("Pass 3: Importing ways");

    conn.execute_batch(&format!(
        "
        INSERT INTO osm_ways (way_id, node_ids, tags)
        SELECT id, refs, tags
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'way' AND refs IS NOT NULL AND len(refs) > 0;
        "
    ))
    .context("Failed to import ways")?;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM osm_ways", [], |row| row.get(0))?;
    info!(count, "Ways imported");

    Ok(())
}

fn import_relations(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Pass 4: Importing relations");

    conn.execute_batch(&format!(
        "
        INSERT INTO osm_relations (relation_id, member_refs, member_types, member_roles, tags)
        SELECT id, refs, ref_types::VARCHAR[], ref_roles, tags
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'relation' AND refs IS NOT NULL AND len(refs) > 0;
        "
    ))
    .context("Failed to import relations")?;

    let count: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_relations", [], |row| row.get(0))?;
    info!(count, "Relations imported");

    Ok(())
}

fn create_spatial_indexes(conn: &Connection) -> Result<()> {
    info!("Creating spatial indexes");
    conn.execute_batch(
        "
        CREATE INDEX osm_buildings_geom_idx ON osm_buildings USING RTREE (geom);
        CREATE INDEX osm_addresses_geom_idx ON osm_addresses USING RTREE (geom);
        ",
    )
    .context("Failed to create spatial indexes")?;
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
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
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

            INSERT INTO osm_ways VALUES (100, [1, 2, 3, 4, 1], MAP {'building': 'yes'});
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

            INSERT INTO osm_ways VALUES (200, [1, 2], MAP {
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

            -- Outer way (way_id=10) and inner way (way_id=11)
            INSERT INTO osm_ways VALUES (10, [1, 2, 3, 4, 1], NULL);
            INSERT INTO osm_ways VALUES (11, [5, 6, 7, 8, 5], NULL);

            -- Relation 300 references both ways
            INSERT INTO osm_relations VALUES (300, [10, 11], ['way', 'way'], ['outer', 'inner'], MAP {'building': 'yes'});
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

    /// End-to-end test: import the fixture PBF and verify final counts.
    #[test]
    fn test_import_fixture_pbf() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
        import(&conn, Some(Path::new("fixtures/osm.pbf")), "")?;

        // 2 buildings: way 947235698 (apartments) + relation 1891415 (school)
        let buildings: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_buildings", [], |row| row.get(0))?;
        assert_eq!(buildings, 2, "Expected 2 buildings (1 way + 1 relation)");

        // 3 addresses: node 13200892212 + way 947235698 + relation 1891415
        let addresses: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_addresses", [], |row| row.get(0))?;
        assert_eq!(
            addresses, 3,
            "Expected 3 addresses (1 node + 1 way + 1 relation)"
        );

        Ok(())
    }

    /// Verify building types and tags after import.
    #[test]
    fn test_import_fixture_building_details() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
        import(&conn, Some(Path::new("fixtures/osm.pbf")), "")?;

        // Way building: apartments
        let building_tag: String = conn.query_row(
            "SELECT building FROM osm_buildings WHERE osm_id = 947235698 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(building_tag, "apartments");

        let geom_type: String = conn.query_row(
            "SELECT ST_GeometryType(geom) FROM osm_buildings WHERE osm_id = 947235698",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(geom_type, "POLYGON");

        // Relation building: school (multipolygon with inner hole)
        let building_tag: String = conn.query_row(
            "SELECT building FROM osm_buildings WHERE osm_id = 1891415 AND osm_type = 'relation'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(building_tag, "school");

        // School building should have smaller area than its outer ring (it has a hole)
        let area: f64 = conn.query_row(
            "SELECT ST_Area(geom) FROM osm_buildings WHERE osm_id = 1891415",
            [],
            |row| row.get(0),
        )?;
        assert!(area > 0.0, "School building should have positive area");

        Ok(())
    }

    /// Verify address details after import.
    #[test]
    fn test_import_fixture_address_details() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
        import(&conn, Some(Path::new("fixtures/osm.pbf")), "")?;

        // Node address: housenumber 32, Ludwika Narbutta
        let (hn, street): (String, String) = conn.query_row(
            "SELECT housenumber, street FROM osm_addresses WHERE osm_id = 13200892212 AND osm_type = 'node'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(hn, "32");
        assert_eq!(street, "Ludwika Narbutta");

        // Way address: housenumber 63, Kazimierzowska, Warszawa
        let (hn, street, city, postcode): (String, String, String, String) = conn.query_row(
            "SELECT housenumber, street, city, postcode FROM osm_addresses WHERE osm_id = 947235698 AND osm_type = 'way'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(hn, "63");
        assert_eq!(street, "Kazimierzowska");
        assert_eq!(city, "Warszawa");
        assert_eq!(postcode, "02-538");

        // Relation address: housenumber 60, Kazimierzowska, Warszawa
        let (hn, street, city, postcode): (String, String, String, String) = conn.query_row(
            "SELECT housenumber, street, city, postcode FROM osm_addresses WHERE osm_id = 1891415 AND osm_type = 'relation'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(hn, "60");
        assert_eq!(street, "Kazimierzowska");
        assert_eq!(city, "Warszawa");
        assert_eq!(postcode, "02-543");

        Ok(())
    }

    /// Verify address geometries are within expected bounding box (Warsaw area).
    #[test]
    fn test_import_fixture_address_geometries() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
        import(&conn, Some(Path::new("fixtures/osm.pbf")), "")?;

        // All addresses should have geometry in the Warsaw area (~21.01 lon, ~52.20 lat)
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_addresses
             WHERE ST_X(geom) BETWEEN 21.01 AND 21.02
               AND ST_Y(geom) BETWEEN 52.20 AND 52.21",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 3, "All 3 addresses should be in the Warsaw area");

        // Node address should be a point at its exact coordinates
        let (lon, lat): (f64, f64) = conn.query_row(
            "SELECT ST_X(geom), ST_Y(geom) FROM osm_addresses WHERE osm_id = 13200892212",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!((lon - 21.014861).abs() < 1e-5);
        assert!((lat - 52.206263).abs() < 1e-4);

        Ok(())
    }

    /// Verify raw node import counts from the fixture.
    #[test]
    fn test_import_fixture_node_counts() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
        import(&conn, Some(Path::new("fixtures/osm.pbf")), "")?;

        let node_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_nodes", [], |row| row.get(0))?;
        assert!(
            node_count >= 48,
            "Expected at least 48 nodes, got {node_count}"
        );

        // Ways: 3 ways in the fixture
        let way_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_ways", [], |row| row.get(0))?;
        assert_eq!(way_count, 3, "Expected 3 ways");

        // Relations: 1 relation in the fixture
        let rel_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_relations", [], |row| row.get(0))?;
        assert_eq!(rel_count, 1, "Expected 1 relation");

        Ok(())
    }
}
