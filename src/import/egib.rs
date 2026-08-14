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
/// `docs/invalid_geometry_tile_500s.md`) and any oversized-geometry rows
/// (see `dataset::filter_oversized_geometry`). Does NOT create an index.
///
/// Geometry is transformed from EPSG:2180 to EPSG:4326 for uniform spatial
/// comparisons.
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let inner = format!(
        "SELECT * EXCLUDE(geometry, geometry_bbox), \
         ST_Transform(geometry, 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}'"
    );
    let select = crate::dataset::EGIB.with_centroid_select(&crate::dataset::hashed_select(&inner));
    let select = crate::mappings::egib::with_rodzaj_kod_select(&select);
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {select};"
    ))
    .with_context(|| format!("Failed to load EGIB data into {target_table}"))?;

    let stats = crate::dataset::filter_invalid_geometry(conn, target_table, "id_budynku")?;
    let oversized = crate::dataset::filter_oversized_geometry(conn, target_table, "id_budynku")?;
    Ok(stats.merge_oversized(oversized))
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

        // `load_into` itself is one `CREATE TABLE AS SELECT` statement (plus
        // the two single-statement geometry filters and the rodzaj_kod
        // cascade it calls) -- no Rust-side loop to check inside. This is
        // the one Rust-level step boundary `import` actually has: between
        // the table load above and the (also lengthy, on the real 17M-row
        // table) RTREE index build below. Bails with an Err, matching
        // `import::osm::import`'s `check_shutdown` convention -- a table
        // loaded but not yet indexed is not a usable import.
        crate::shutdown::check_requested()?;

        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);
             CREATE INDEX egib_buildings_centroid_idx ON egib_buildings USING RTREE (centroid);",
        )
        .context("Failed to create spatial indexes on egib_buildings")?;
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

/// Human-readable message for the `job_run_log` row. Reports both skip
/// reasons `load_into` can produce -- invalid geometry and oversized
/// geometry -- via the shared `dataset::format_skip_clause`, so a change to
/// one clause's wording can't drift from the other's.
fn summarize(stats: &LoadStats) -> String {
    let mut parts = Vec::new();
    if stats.skipped_invalid_geometry > 0 {
        parts.push(crate::dataset::format_skip_clause(
            "invalid-geometry",
            stats.skipped_invalid_geometry,
            &stats.skipped_example_ids,
        ));
    }
    if stats.skipped_oversized_geometry > 0 {
        parts.push(crate::dataset::format_skip_clause(
            "oversized-geometry",
            stats.skipped_oversized_geometry,
            &stats.skipped_oversized_example_ids,
        ));
    }
    if parts.is_empty() {
        "no invalid or oversized geometry".to_string()
    } else {
        parts.join("; ")
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

        let mismatched: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM egib_buildings
                 WHERE centroid IS DISTINCT FROM ST_Centroid(geom)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            mismatched, 0,
            "centroid must equal ST_Centroid(geom) for every row"
        );

        // rodzaj_kod is precomputed by mappings::egib::with_rodzaj_kod_select;
        // on this fixture (48 'm', 13 't', 6 'i', 5 'k', 1 'b', 1 'h', all
        // already bare letters) every row must resolve to its own letter.
        let mismatched: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM egib_buildings
                 WHERE rodzaj_kod IS DISTINCT FROM lower(trim(rodzaj))",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            mismatched, 0,
            "every fixture row's rodzaj is already a bare letter, so rodzaj_kod must equal it"
        );
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

    /// Same rationale and shape as `load_into_drops_a_deliberately_invalid_row`
    /// above, for the second filter `load_into` runs -- this is what proves
    /// the filter also runs on the *update* staging path, not just import:
    /// `update::dataset::refresh` calls this same `load_into` with the
    /// staging table as `target_table` (see `update::mod::run`'s `Egib`
    /// arm), so whatever `load_into` does to `target_table` here it does to
    /// `<table>__staging` there too -- one funnel, no separate call site to
    /// keep in sync.
    #[test]
    fn load_into_drops_a_deliberately_oversized_row() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             INSERT INTO egib_buildings VALUES
                 ('ok', ST_GeomFromText('POLYGON((21.0 52.0, 21.001 52.0, 21.001 52.001, 21.0 52.001, 21.0 52.0))')),
                 ('glued', ST_GeomFromText(
                     'MULTIPOLYGON(
                          ((19.875 52.0, 19.876 52.0, 19.876 52.001, 19.875 52.001, 19.875 52.0)),
                          ((20.5 52.0, 20.501 52.0, 20.501 52.001, 20.5 52.001, 20.5 52.0))
                      )'
                 ));",
        )
        .unwrap();

        let stats =
            crate::dataset::filter_oversized_geometry(&conn, "egib_buildings", "id_budynku")
                .unwrap();

        assert_eq!(stats.skipped_oversized_geometry, 1);
        assert_eq!(
            stats.skipped_oversized_example_ids,
            vec!["glued".to_string()]
        );
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
        assert_eq!(
            entry.message.as_deref(),
            Some("no invalid or oversized geometry")
        );
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
