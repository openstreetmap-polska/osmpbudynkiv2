use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::config::Config;
use crate::dataset::LoadStats;
use crate::download::download_file;
use crate::utils::format_duration;

/// Create `target_table` from an EGIB GeoParquet file, including the
/// `_row_hash` column, then delete any invalid-geometry rows (see
/// `docs/invalid_geometry_tile_500s.md`). Does NOT create an index.
///
/// Geometry is transformed from EPSG:2180 to EPSG:4326 for uniform spatial
/// comparisons.
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let inner = format!(
        "SELECT * EXCLUDE(geometry, geometry_bbox), \
         ST_Transform(geometry, 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}'"
    );
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {};",
        crate::dataset::hashed_select(&inner)
    ))
    .with_context(|| format!("Failed to load EGIB data into {target_table}"))?;

    crate::dataset::filter_invalid_geometry(conn, target_table, "id_budynku")
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

        info!(path = parquet_str, "Importing EGIB buildings");

        let total = std::time::Instant::now();

        let t = std::time::Instant::now();
        let stats = load_into(conn, crate::dataset::EGIB.table, parquet_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: load table"
        );

        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);",
        )
        .context("Failed to create spatial index on egib_buildings")?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: create spatial index"
        );

        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM egib_buildings", [], |row| row.get(0))?;
        if was_downloaded && config.cleanup_downloaded_files {
            info!(path = %parquet_path.display(), "Cleaning up downloaded file");
            let _ = std::fs::remove_file(&parquet_path);
        }

        info!(
            count,
            elapsed = %format_duration(total.elapsed()),
            "EGIB import complete"
        );

        Ok(stats)
    })();

    match &outcome {
        Ok(stats) => {
            let _ = crate::job_log::record(conn, "import:egib", "Success", Some(&summarize(stats)));
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, "import:egib", "Error", Some(&format!("{e:#}")));
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

    #[test]
    fn load_into_the_fixture_has_no_invalid_geometry() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let stats = load_into(&conn, "egib_buildings", "fixtures/egib.parquet").unwrap();

        assert_eq!(
            stats,
            crate::dataset::LoadStats::default(),
            "fixture is expected to be clean"
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM egib_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 74, "must match the known fixture row count");
    }

    #[test]
    fn load_into_drops_a_deliberately_invalid_row() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             INSERT INTO egib_buildings VALUES
                 ('ok', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('bad', ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))'));",
        )
        .unwrap();

        let stats =
            crate::dataset::filter_invalid_geometry(&conn, "egib_buildings", "id_budynku").unwrap();

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
            Some(Path::new("fixtures/egib.parquet")),
            "unused",
        )
        .unwrap();

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get("import:egib").expect("entry must be present");
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
            .get("import:egib")
            .expect("entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.is_some());
    }
}
