use std::path::Path;

use anyhow::{Context, Result};
use duckdb::Connection;

pub fn init_db(path: &Path) -> Result<Connection> {
    let conn =
        Connection::open(path).with_context(|| format!("Failed to open database at {path:?}"))?;

    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .context("Failed to install/load spatial extension")?;

    conn.execute_batch(
        "
        SET preserve_insertion_order = false;
        SET geometry_always_xy = true;
        ",
    )
    .context("Failed to configure DuckDB settings")?;

    create_schema(&conn)?;

    Ok(conn)
}

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR
        );

        -- OSM raw data (needed for geometry construction and replication updates)
        CREATE TABLE IF NOT EXISTS osm_nodes (
            node_id BIGINT PRIMARY KEY,
            lon DOUBLE,
            lat DOUBLE
        );

        CREATE TABLE IF NOT EXISTS osm_way_nodes (
            way_id BIGINT,
            node_id BIGINT,
            position INT
        );

        CREATE TABLE IF NOT EXISTS osm_relations (
            relation_id BIGINT,
            member_id BIGINT,
            member_type VARCHAR,
            member_role VARCHAR,
            position INT
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
        let conn = init_db(Path::new(":memory:"))?;

        // Verify all tables exist by querying them
        let tables = [
            "metadata",
            "osm_nodes",
            "osm_way_nodes",
            "osm_relations",
            "osm_addresses",
            "osm_buildings",
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
        let conn = init_db(Path::new(":memory:"))?;
        // Re-run schema creation — should not fail
        create_schema(&conn)?;
        Ok(())
    }
}
