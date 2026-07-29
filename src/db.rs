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

        -- One row per dataset refresh attempt, including no-ops. Owns snapshot_id,
        -- which is assigned inside the apply transaction as MAX(snapshot_id) + 1.
        CREATE TABLE IF NOT EXISTS dataset_refreshes (
            snapshot_id BIGINT PRIMARY KEY,
            source VARCHAR,
            started_at TIMESTAMP WITH TIME ZONE,
            finished_at TIMESTAMP WITH TIME ZONE,
            source_etag VARCHAR,
            added INTEGER,
            modified INTEGER,
            removed INTEGER
        );

        -- Aggregated change counts per XYZ tile (z = tile_math::CHANGE_CELL_ZOOM).
        -- Both the old and the new geometry of a changed object contribute, so an
        -- object that moves marks the cell it left and the cell it entered.
        CREATE TABLE IF NOT EXISTS dataset_change_areas (
            snapshot_id BIGINT,
            source VARCHAR,
            cell_z INTEGER,
            cell_x INTEGER,
            cell_y INTEGER,
            added INTEGER,
            modified INTEGER,
            removed INTEGER,
            detected_at TIMESTAMP WITH TIME ZONE
        );

        -- Precomputed unmatched government objects served by /tiles and /package.
        -- Only unmatched rows are stored, tagged with the z14 cell of their
        -- representative point and the time that cell was last recomputed.
        CREATE TABLE IF NOT EXISTS bdot10k_unmatched (
            LOKALNYID VARCHAR,
            geom GEOMETRY,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE
        );
        CREATE TABLE IF NOT EXISTS egib_unmatched (
            id_budynku VARCHAR,
            geom GEOMETRY,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE
        );
        CREATE TABLE IF NOT EXISTS prg_unmatched (
            geom GEOMETRY,
            lokalny_id VARCHAR,
            numer_porzadkowy VARCHAR,
            ulica VARCHAR,
            miejscowosc VARCHAR,
            kod_pocztowy VARCHAR,
            teryt_miejscowosc VARCHAR,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE
        );

        -- Dirty-cell queue drained by the match_refresh job. Duplicates allowed
        -- (deduped on drain). source is 'bdot10k'|'egib'|'prg'; an OSM building
        -- edit enqueues bdot10k+egib, an OSM address edit enqueues prg.
        -- cell_z is informational only: every producer writes CHANGE_CELL_ZOOM
        -- and the drain neither selects nor filters on it (recompute_cell_in_txn
        -- hardcodes CHANGE_CELL_ZOOM). If CHANGE_CELL_ZOOM ever changes, queue
        -- rows written at the old zoom are silently reinterpreted at the new
        -- one — drain the queue before changing it, then `compare reconcile`.
        CREATE TABLE IF NOT EXISTS match_dirty_cells (
            source VARCHAR,
            cell_z INTEGER,
            cell_x INTEGER,
            cell_y INTEGER,
            enqueued_at TIMESTAMP WITH TIME ZONE
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

    #[test]
    fn test_init_db_creates_changeset_tables() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        for table in ["dataset_refreshes", "dataset_change_areas"] {
            let count: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0, "Table {table} should be empty initially");
        }
        Ok(())
    }

    #[test]
    fn test_changeset_tables_round_trip() -> Result<()> {
        let init_commands = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        conn.execute_batch(
            "INSERT INTO dataset_refreshes
                 VALUES (1, 'bdot10k', now(), now(), 'etag-abc', 10, 20, 5);
             INSERT INTO dataset_change_areas
                 VALUES (1, 'bdot10k', 14, 9147, 5411, 10, 20, 5, now());",
        )?;

        let (source, added, modified, removed): (String, i32, i32, i32) = conn.query_row(
            "SELECT source, added, modified, removed FROM dataset_refreshes",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(
            (source.as_str(), added, modified, removed),
            ("bdot10k", 10, 20, 5)
        );

        let (z, x, y): (i32, i32, i32) = conn.query_row(
            "SELECT cell_z, cell_x, cell_y FROM dataset_change_areas",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!((z, x, y), (14, 9147, 5411));

        Ok(())
    }

    #[test]
    fn test_init_db_creates_serving_and_queue_tables() -> Result<()> {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None)?;
        for table in [
            "bdot10k_unmatched",
            "egib_unmatched",
            "prg_unmatched",
            "match_dirty_cells",
        ] {
            let n: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
            assert_eq!(n, 0, "table {table} should exist and be empty");
        }
        // prg_unmatched must carry the serving + cell columns.
        conn.execute_batch(
            "INSERT INTO prg_unmatched
             (geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
              teryt_miejscowosc, cell_x, cell_y, computed_at)
             VALUES (ST_Point(21.0,52.0),'id1','5','Main','Town','00-001','0918123',
                     9147, 5411, now());",
        )?;
        let (hn, cx): (String, i32) = conn.query_row(
            "SELECT numer_porzadkowy, cell_x FROM prg_unmatched",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!((hn.as_str(), cx), ("5", 9147));
        Ok(())
    }
}
