use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use duckdb::vtab::arrow::ArrowVTab;
use duckdb::{Config, Connection};

use crate::osm::kvstore::RocksDB;
use crate::osm::udf;

pub fn init_db(
    path: &Path,
    init_commands: &[String],
    kv: Option<Arc<RocksDB>>,
) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        Config::default()
            .with("storage_compatibility_version", "latest")
            .unwrap(),
    )
    .with_context(|| format!("Failed to open database at {path:?}"))?;

    conn.register_table_function::<ArrowVTab>("arrow")
        .context("Failed to register arrow vtab")?;

    if let Some(kv) = kv {
        udf::register_udfs(&conn, kv)?;
    }

    for cmd in init_commands {
        conn.execute_batch(cmd)
            .with_context(|| format!("Failed to execute DuckDB init command: {cmd}"))?;
    }

    create_schema(&conn)?;

    Ok(conn)
}

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR,
            value VARCHAR
        );

        -- Processed OSM data with geometry
        CREATE TABLE IF NOT EXISTS osm_addresses (
            osm_id BIGINT,
            osm_type VARCHAR,
            housenumber VARCHAR,
            street VARCHAR,
            city VARCHAR,
            postcode VARCHAR,
            geom GEOMETRY
        );

        CREATE TABLE IF NOT EXISTS osm_buildings (
            osm_id BIGINT,
            osm_type VARCHAR,
            building VARCHAR,
            geom GEOMETRY
        );

        -- Export log for the /package endpoint (see GET /updates). Requires
        -- the spatial extension to already be loaded (via duckdb_init_commands)
        -- before this runs, since GEOMETRY('epsg:4326') needs spatial to
        -- resolve the CRS string -- unlike the bare GEOMETRY columns above.
        CREATE TABLE IF NOT EXISTS package_exports (
            exported_at TIMESTAMP WITH TIME ZONE,
            area GEOMETRY('epsg:4326'),
            datasets VARCHAR[],
            address_count INTEGER,
            building_count INTEGER
        );
        ",
    )
    .context("Failed to create schema")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_db_creates_tables() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        // Verify all tables exist by querying them
        let tables = [
            "metadata",
            "osm_addresses",
            "osm_buildings",
            "package_exports",
        ];
        for table in tables {
            let count: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0, "Table {table} should be empty initially");
        }

        Ok(())
    }

    #[test]
    fn test_init_db_is_idempotent() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        // Re-run schema creation — should not fail
        create_schema(&conn)?;
        Ok(())
    }

    #[test]
    fn test_package_exports_column_types_round_trip() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        conn.execute(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now(), ST_Point(21.0, 52.0), ['prg', 'bdot10k'], 3, 5)",
            [],
        )?;

        let (geojson, datasets_json, address_count, building_count): (String, String, i32, i32) =
            conn.query_row(
                "SELECT ST_AsGeoJSON(area), to_json(datasets), address_count, building_count
                 FROM package_exports",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;

        assert!(geojson.contains("\"Point\""));
        assert_eq!(datasets_json, r#"["prg","bdot10k"]"#);
        assert_eq!(address_count, 3);
        assert_eq!(building_count, 5);

        Ok(())
    }
}
