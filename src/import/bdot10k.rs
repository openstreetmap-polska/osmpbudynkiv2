use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::config::Config;
use crate::dataset::LoadStats;
use crate::download::download_file;
use crate::utils::format_duration;

/// BDOT10k's record key -- composite, unlike EGIB's. One array feeds all
/// three sites that have to agree on it (the load select's `IS NOT NULL`
/// filter, the count query's `IS NULL` complement, and the dedup's
/// `PARTITION BY`), which matters more here than for a single-column key:
/// the complement of `a IS NOT NULL AND b IS NOT NULL` is `a IS NULL OR
/// b IS NULL`, and that asymmetry is easy to get wrong when spelled out by
/// hand. Plan 2 moves this onto `DatasetSpec::key_columns`.
const KEY_COLUMNS: &[&str] = &["PRZESTRZENNAZW", "LOKALNYID"];

/// Create `target_table` from a BDOT10k GeoParquet file, including the
/// `_row_hash` column, then delete any invalid-geometry rows (see
/// `docs/invalid_geometry_tile_500s.md`), any oversized-geometry rows (see
/// `dataset::filter_oversized_geometry`), drop any row with a NULL record
/// key (see `dataset::non_null_key_sql`), and finally collapse duplicate
/// keys down to one row each (see `dataset::deduplicate_by_key`). Does NOT
/// create an index -- callers that need one create it themselves, and the
/// update path deliberately does not.
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
/// it must be restored before returning -- on the success path AND the error
/// path, or a failed load leaks the disabled setting process-wide for as
/// long as the process runs. DuckDB rejects this file's CRS at bind time for
/// ANY query against it, regardless of which columns are projected, so the
/// null-key count query below has to share the same disabled window as the
/// `CREATE TABLE`, not run after the setting is restored -- both live inside
/// one closure, and the restoring `SET` runs unconditionally right after it,
/// before either the closure's error or its result is inspected. Left
/// disabled, it silently breaks any later automatic GeoParquet decoding on
/// the same instance — e.g. EGIB's `load_into`, which relies on the default
/// being on.
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let non_null = crate::dataset::non_null_key_sql(KEY_COLUMNS);
    let inner = format!(
        "SELECT * EXCLUDE(GEOM), \
         ST_Transform(ST_GeomFromWKB(GEOM), 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}' WHERE {non_null}"
    );
    let select =
        crate::dataset::BDOT10K.with_centroid_select(&crate::dataset::hashed_select(&inner));

    conn.execute_batch("SET enable_geoparquet_conversion = false;")
        .with_context(|| format!("Failed to disable GeoParquet conversion for {target_table}"))?;
    let loaded = (|| -> Result<i64> {
        // A filtered CTAS doesn't report how many rows its WHERE excluded, so
        // count them with a second, narrow query against the same parquet
        // file -- cheap, since parquet column pruning means it reads only
        // the key columns, not the whole file. Must run in this same
        // disabled-conversion window; see the doc comment above.
        let null_key_rows: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM '{parquet_path}' WHERE {}",
                    crate::dataset::null_key_sql(KEY_COLUMNS)
                ),
                [],
                |r| r.get(0),
            )
            .with_context(|| format!("Failed to count NULL-keyed rows in {parquet_path}"))?;

        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {target_table};
             CREATE TABLE {target_table} AS {select};"
        ))
        .with_context(|| format!("Failed to load BDOT10k data into {target_table}"))?;

        Ok(null_key_rows)
    })();
    // Restore before propagating: see the GLOBAL-scope note above -- an
    // early `?` on `loaded` here would leave conversion disabled for every
    // other connection sharing this instance, whether the closure above
    // succeeded or not.
    conn.execute_batch("SET enable_geoparquet_conversion = true;")
        .with_context(|| {
            format!("Failed to re-enable GeoParquet conversion after {target_table}")
        })?;
    let null_key_rows = loaded?;

    let stats = crate::dataset::filter_invalid_geometry(conn, target_table, "LOKALNYID")?;
    let oversized = crate::dataset::filter_oversized_geometry(conn, target_table, "LOKALNYID")?;
    // Must come after both geometry filters above: a duplicate pair whose
    // newest member has bad geometry must fall back to the older valid
    // member rather than being collapsed down to a row a geometry filter
    // then deletes, losing the object entirely. The NULL-key half of the
    // same "the key is usable" guarantee is enforced up in the load SELECT
    // instead, via `non_null_key_sql` above -- see that function's doc
    // comment for why the two run at different points rather than together
    // here.
    let mut unique = crate::dataset::deduplicate_by_key(
        conn,
        target_table,
        KEY_COLUMNS,
        "WERSJA DESC",
        "LOKALNYID",
    )?;
    unique.skipped_null_key = null_key_rows;
    Ok(stats.merge_oversized(oversized).merge_unique_key(unique))
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

        // `load_into` itself is a short, fixed sequence of single-statement
        // queries (the null-key count, the `CREATE TABLE AS SELECT`, the two
        // geometry filters, the dedup delete) -- no Rust-side loop to check
        // inside. This is the one Rust-level step boundary `import` actually
        // has: between the table load above
        // and the (also lengthy, on the real 16M-row table) RTREE index build
        // below. Bails with an Err, matching `import::osm::import`'s
        // `check_shutdown` convention -- a table loaded but not yet indexed
        // is not a usable import.
        crate::shutdown::check_requested()?;

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

/// Human-readable message for the `job_run_log` row. Reports all four skip
/// reasons `load_into` can produce -- invalid geometry, oversized geometry,
/// NULL record key, and duplicate record key -- via the shared
/// `dataset::format_skip_clause`, so a change to one clause's wording can't
/// drift from the others'. Ordered in the order `load_into` applies the
/// filters: the NULL-key `WHERE` runs first (inside the load SELECT), so it
/// leads; invalid- and oversized-geometry run next in that order; dedup runs
/// last, after both geometry filters (see `load_into`'s comment on why).
fn summarize(stats: &LoadStats) -> String {
    let mut parts = Vec::new();
    if stats.skipped_null_key > 0 {
        parts.push(crate::dataset::format_skip_clause(
            "null-key",
            stats.skipped_null_key,
            &[],
        ));
    }
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
    if stats.skipped_duplicate_key > 0 {
        parts.push(crate::dataset::format_skip_clause(
            "duplicate-key",
            stats.skipped_duplicate_key,
            &stats.skipped_duplicate_example_ids,
        ));
    }
    if parts.is_empty() {
        "no rows skipped".to_string()
    } else {
        parts.join("; ")
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

    /// Same rationale and shape as `load_into_drops_a_deliberately_invalid_row`
    /// above, for the second filter `load_into` runs -- this is what proves
    /// the filter also runs on the *update* staging path, not just import:
    /// `update::dataset::refresh` calls this same `load_into` with the
    /// staging table as `target_table` (see `update::mod::run`'s `Bdot10k`
    /// arm), so whatever `load_into` does to `target_table` here it does to
    /// `<table>__staging` there too -- one funnel, no separate call site to
    /// keep in sync.
    #[test]
    fn load_into_drops_a_deliberately_oversized_row() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
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
            crate::dataset::filter_oversized_geometry(&conn, "bdot10k_buildings", "LOKALNYID")
                .unwrap();

        assert_eq!(stats.skipped_oversized_geometry, 1);
        assert_eq!(
            stats.skipped_oversized_example_ids,
            vec!["glued".to_string()]
        );
    }

    /// Unlike the two filters above, the NULL-key filter now lives inside
    /// `load_into`'s load SELECT (`non_null_key_sql`), not in a standalone
    /// helper that can be seeded and called in isolation -- so this has to
    /// go through `load_into` with a real parquet path to exercise it at
    /// all. That's exactly why `fixtures/bdot10k_v2.parquet` gained a
    /// deliberate NULL-`LOKALNYID` row (see
    /// `fixtures/scripts/prepare_update_fixtures.sh`): the four committed v1
    /// fixtures have no NULL keys, so nothing else in the suite reaches this
    /// path.
    #[test]
    fn load_into_drops_null_keyed_rows() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let stats = load_into(&conn, "bdot10k_buildings", "fixtures/bdot10k_v2.parquet").unwrap();

        assert_eq!(stats.skipped_null_key, 1);
        let null_keyed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM bdot10k_buildings WHERE LOKALNYID IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(null_keyed, 0, "no NULL-keyed row may survive load_into");
    }

    /// Same "must go through `load_into`" rationale as
    /// `load_into_drops_null_keyed_rows` above -- the dedup runs on the table
    /// `load_into` just built, so a fixture with a real duplicate key is the
    /// only way to exercise it end to end. `bdot10k_v2.parquet`'s duplicate
    /// pair shares one `(PRZESTRZENNAZW, LOKALNYID)` key with a strictly
    /// OLDER `WERSJA` on the extra copy, so the part that actually matters
    /// here is confirming the *newer* row -- not just *a* row -- is the one
    /// that survives.
    #[test]
    fn load_into_collapses_duplicate_keys() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let stats = load_into(&conn, "bdot10k_buildings", "fixtures/bdot10k_v2.parquet").unwrap();

        assert_eq!(stats.skipped_duplicate_key, 1);
        assert!(
            !stats.skipped_duplicate_example_ids.is_empty(),
            "the deleted duplicate's id must be captured for the job log"
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 74, "duplicate collapses down to one row per key");

        // `prepare_update_fixtures.sh` set the duplicate's WERSJA one day
        // OLDER than the fixture's uniform WERSJA; if that older copy had
        // won the tie-break instead of the pre-existing row, this count
        // would be nonzero.
        let kept_old_version: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM bdot10k_buildings
                 WHERE WERSJA < TIMESTAMPTZ '2025-06-01 14:00:00+02'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            kept_old_version, 0,
            "the older duplicate must have lost the WERSJA DESC tie-break"
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
            Some(Path::new("fixtures/bdot10k.parquet")),
            "unused",
        )
        .unwrap();

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get("import:bdot10k").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("no rows skipped"));
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
