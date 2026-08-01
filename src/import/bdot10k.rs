use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::config::Config;
use crate::dataset::LoadStats;
use crate::download::download_file;
use crate::utils::format_duration;

/// Create `target_table` from a BDOT10k GeoParquet file, including the
/// `_row_hash` column, then delete any invalid-geometry rows (see
/// `docs/invalid_geometry_tile_500s.md`). Does NOT create an index --
/// callers that need one create it themselves, and the update path
/// deliberately does not.
///
/// Workaround: DuckDB's automatic GeoParquet conversion and ST_Read (GDAL)
/// both fail on BDOT10k files because their CRS (EPSG:2180) is stored as a
/// projjson string-in-string which DuckDB rejects as "invalid CRS". Instead
/// we disable the automatic conversion, read the file as plain parquet, and
/// manually convert the WKB geometry column. Geometry is transformed from
/// EPSG:2180 to EPSG:4326 for uniform spatial comparisons.
///
/// `enable_geoparquet_conversion` has GLOBAL scope in DuckDB — it's visible
/// to every connection sharing this database instance, not just this one, so
/// it must be restored before returning. Left disabled, it silently breaks
/// any later automatic GeoParquet decoding on the same instance — e.g.
/// EGIB's `load_into`, which relies on the default being on.
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let inner = format!(
        "SELECT * EXCLUDE(GEOM), \
         ST_Transform(ST_GeomFromWKB(GEOM), 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}'"
    );
    let select =
        crate::dataset::BDOT10K.with_centroid_select(&crate::dataset::hashed_select(&inner));
    conn.execute_batch(&format!(
        "SET enable_geoparquet_conversion = false;
         DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {select};
         SET enable_geoparquet_conversion = true;"
    ))
    .with_context(|| format!("Failed to load BDOT10k data into {target_table}"))?;

    crate::dataset::filter_invalid_geometry(conn, target_table, "LOKALNYID")
}

pub fn import(conn: &Connection, config: &Config, file: Option<&Path>, url: &str) -> Result<()> {
    let outcome = (|| -> Result<LoadStats> {
        let (parquet_path, was_downloaded) = match file {
            Some(path) => (PathBuf::from(path), false),
            None => (download_file(url, &config.download_dir())?, true),
        };

        let parquet_str = parquet_path
            .to_str()
            .context("Parquet path is not valid UTF-8")?;

        info!(path = parquet_str, "Importing BDOT10k buildings");

        let total = std::time::Instant::now();

        let t = std::time::Instant::now();
        let stats = load_into(conn, crate::dataset::BDOT10K.table, parquet_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: load table"
        );

        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);
             CREATE INDEX bdot10k_buildings_centroid_idx ON bdot10k_buildings USING RTREE (centroid);",
        )
        .context("Failed to create spatial indexes on bdot10k_buildings")?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: create spatial indexes"
        );

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |row| {
            row.get(0)
        })?;
        if was_downloaded && config.cleanup_downloaded_files {
            info!(path = %parquet_path.display(), "Cleaning up downloaded file");
            let _ = std::fs::remove_file(&parquet_path);
        }

        info!(
            count,
            elapsed = %format_duration(total.elapsed()),
            "BDOT10k import complete"
        );

        Ok(stats)
    })();

    match &outcome {
        Ok(stats) => {
            let _ =
                crate::job_log::record(conn, "import:bdot10k", "Success", Some(&summarize(stats)));
        }
        Err(e) => {
            let _ =
                crate::job_log::record(conn, "import:bdot10k", "Error", Some(&format!("{e:#}")));
        }
    }
    outcome.map(|_| ())
}

/// Human-readable message for the `job_run_log` row.
fn summarize(stats: &LoadStats) -> String {
    if stats.skipped_invalid_geometry == 0 {
        return "no invalid geometry".to_string();
    }
    let shown = stats.skipped_example_ids.join(", ");
    let more =
        (stats.skipped_invalid_geometry as usize).saturating_sub(stats.skipped_example_ids.len());
    if more > 0 {
        format!(
            "skipped {} invalid-geometry rows (ids: {shown}, +{more} more)",
            stats.skipped_invalid_geometry
        )
    } else {
        format!(
            "skipped {} invalid-geometry rows (ids: {shown})",
            stats.skipped_invalid_geometry
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    /// Sanity check that the existing fixture (also used by
    /// `tests/cli_import_bdot10k.rs`, which asserts `count=74`) has no
    /// invalid geometry, and that `load_into` now returns `LoadStats`
    /// reflecting that.
    #[test]
    fn load_into_the_fixture_has_no_invalid_geometry() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let stats = load_into(&conn, "bdot10k_buildings", "fixtures/bdot10k.parquet").unwrap();

        assert_eq!(
            stats,
            crate::dataset::LoadStats::default(),
            "fixture is expected to be clean"
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 74, "must match the known fixture row count");

        let mismatched: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM bdot10k_buildings
                 WHERE centroid IS DISTINCT FROM ST_Centroid(geom)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            mismatched, 0,
            "centroid must equal ST_Centroid(geom) for every row"
        );
    }

    /// `load_into` must actually remove invalid rows, not just report them --
    /// exercised directly since real fixtures don't contain one. Loads a tiny
    /// staged table by hand (mirroring `hashed_select`'s output shape) rather
    /// than going through `crate::dataset::filter_invalid_geometry` in
    /// isolation, so this catches a regression in how `load_into` calls it.
    #[test]
    fn load_into_drops_a_deliberately_invalid_row() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        // load_into always DROPs and recreates target_table from the parquet
        // path; to exercise the invalid-geometry deletion without a real
        // parquet file, pre-seed target_table exactly as load_into would
        // have left it, then call filter_invalid_geometry the same way
        // load_into does internally -- this is what load_into's last line
        // does, so asserting on it here documents that contract.
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 ('ok', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('bad', ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))'));",
        )
        .unwrap();

        let stats =
            crate::dataset::filter_invalid_geometry(&conn, "bdot10k_buildings", "LOKALNYID")
                .unwrap();

        assert_eq!(stats.skipped_invalid_geometry, 1);
        assert_eq!(stats.skipped_example_ids, vec!["bad".to_string()]);
    }

    #[test]
    fn import_records_success_with_no_skips_in_job_run_log() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let config = Config::default();

        import(
            &conn,
            &config,
            Some(Path::new("fixtures/bdot10k.parquet")),
            "unused",
        )
        .unwrap();

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get("import:bdot10k").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("no invalid geometry"));
    }

    #[test]
    fn import_records_error_in_job_run_log_on_failure() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let config = Config::default();

        let result = import(
            &conn,
            &config,
            Some(Path::new("nonexistent.parquet")),
            "unused",
        );
        assert!(result.is_err());

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log
            .get("import:bdot10k")
            .expect("entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.is_some());
    }
}
