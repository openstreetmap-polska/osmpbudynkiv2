use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::config::Config;
use crate::dataset::LoadStats;
use crate::download::download_file;
use crate::utils::format_duration;

/// Create `target_table` from an EGIB GeoParquet file, then delete any
/// invalid-geometry rows (see `docs/invalid_geometry_tile_500s.md`), any
/// oversized-geometry rows (see `dataset::filter_oversized_geometry`), drop
/// any row with a NULL record key (see `dataset::non_null_key_sql`), and
/// finally collapse duplicate keys down to one row each (see
/// `dataset::deduplicate_by_key`). Does NOT create an index.
///
/// Geometry is transformed from EPSG:2180 to EPSG:4326 for uniform spatial
/// comparisons.
///
/// EGIB's record key -- `crate::dataset::EGIB.key_columns` -- feeds all three
/// sites that have to agree on it below (the load select's `IS NOT NULL`
/// filter, the count query's `IS NULL` complement, and the dedup's
/// `PARTITION BY`), so they cannot drift.
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let non_null = crate::dataset::non_null_key_sql(crate::dataset::EGIB.key_columns);
    // Explicit column list, not `SELECT * EXCLUDE(...)`: `pozostale_atrybuty`
    // is dropped outright (never needed), while `czas_pozyskania` stays in
    // the projection only because `deduplicate_by_key` below orders by it --
    // it is removed right after via `dataset::drop_ordering_column`, see
    // that function's doc comment ("the ordering-column problem" in
    // `docs/superpowers/plans/2026-08-14-column-trimming.md`).
    let inner = format!(
        "SELECT id_budynku, rodzaj, kondygnacje_nadziemne, kondygnacje_podziemne, \
                czas_pozyskania, \
                ST_Transform(geometry, 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}' WHERE {non_null}"
    );
    let select = crate::dataset::EGIB.with_centroid_select(&inner);
    let select = crate::mappings::egib::with_rodzaj_kod_select(&select);
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {select};"
    ))
    .with_context(|| format!("Failed to load EGIB data into {target_table}"))?;

    // A filtered CTAS doesn't report how many rows its WHERE excluded, so
    // count them with a second, narrow query against the same parquet file --
    // cheap, since parquet column pruning means it reads only the key
    // column, not the whole file.
    let null_key_rows: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM '{parquet_path}' WHERE {}",
                crate::dataset::null_key_sql(crate::dataset::EGIB.key_columns)
            ),
            [],
            |r| r.get(0),
        )
        .with_context(|| format!("Failed to count NULL-keyed rows in {parquet_path}"))?;

    let stats = crate::dataset::filter_invalid_geometry(conn, target_table, "id_budynku")?;
    let oversized = crate::dataset::filter_oversized_geometry(conn, target_table, "id_budynku")?;
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
        crate::dataset::EGIB.key_columns,
        "czas_pozyskania DESC",
        "id_budynku",
    )?;
    unique.skipped_null_key = null_key_rows;

    // `czas_pozyskania` has served its only purpose (ranking the dedup
    // above) -- nothing reads it downstream, so it does not survive into the
    // stored table. Must run after `deduplicate_by_key`, not before: see
    // `dataset::drop_ordering_column`'s doc comment.
    crate::dataset::drop_ordering_column(conn, target_table, "czas_pozyskania")?;

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

        info!(path = parquet_str, "Importing EGIB buildings");

        let total = std::time::Instant::now();

        let t = std::time::Instant::now();
        let stats = load_into(conn, crate::dataset::EGIB.table, parquet_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: load table"
        );

        // `load_into` itself is a short, fixed sequence of single-statement
        // queries (the `CREATE TABLE AS SELECT` with its rodzaj_kod cascade,
        // the null-key count, the two geometry filters, the dedup delete) --
        // no Rust-side loop to check inside. This is the one Rust-level step
        // boundary `import` actually has: between
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

    /// Unlike the two filters above, the NULL-key filter now lives inside
    /// `load_into`'s load SELECT (`non_null_key_sql`), not in a standalone
    /// helper that can be seeded and called in isolation -- so this has to
    /// go through `load_into` with a real parquet path to exercise it at
    /// all. That's exactly why `fixtures/egib_v2.parquet` gained a
    /// deliberate NULL-`id_budynku` row (see
    /// `fixtures/scripts/prepare_update_fixtures.sh`): the four committed v1
    /// fixtures have no NULL keys, so nothing else in the suite reaches this
    /// path.
    #[test]
    fn load_into_drops_null_keyed_rows() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let stats = load_into(&conn, "egib_buildings", "fixtures/egib_v2.parquet").unwrap();

        assert_eq!(stats.skipped_null_key, 1);
        let null_keyed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM egib_buildings WHERE id_budynku IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(null_keyed, 0, "no NULL-keyed row may survive load_into");
    }

    /// Same "must go through `load_into`" rationale as
    /// `load_into_drops_null_keyed_rows` above -- the dedup runs on the table
    /// `load_into` just built, so a fixture with a real duplicate key is the
    /// only way to exercise it end to end.
    ///
    /// This can no longer assert on the *winner* of the `czas_pozyskania
    /// DESC` tie-break: `czas_pozyskania` is the only column
    /// `egib_v2.parquet`'s duplicate pair differs on (see
    /// `fixtures/scripts/prepare_update_fixtures.sh`), and
    /// `dataset::drop_ordering_column` removes it from the table by the time
    /// `load_into` returns -- there is nothing left to distinguish which
    /// copy survived. That tie-break behaviour is pinned generically instead,
    /// against a synthetic table that keeps its ordering column, by
    /// `dataset::tests::deduplicate_by_key_keeps_the_newest_version`. This
    /// test's job is narrower: the duplicate is actually removed and its id
    /// reported.
    #[test]
    fn load_into_collapses_duplicate_keys() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let stats = load_into(&conn, "egib_buildings", "fixtures/egib_v2.parquet").unwrap();

        assert_eq!(stats.skipped_duplicate_key, 1);
        assert!(
            !stats.skipped_duplicate_example_ids.is_empty(),
            "the deleted duplicate's id must be captured for the job log"
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM egib_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 74, "duplicate collapses down to one row per key");
    }

    /// Part of `docs/superpowers/plans/2026-08-14-column-trimming.md`'s
    /// Testing requirement: the ordering column's removal is a separate
    /// statement from the projection (`dataset::drop_ordering_column`, run
    /// after the dedup), so it is exactly the kind of step that gets lost in
    /// a refactor while every other test still passes. `pozostale_atrybuty`
    /// is checked alongside it since it is EGIB's other dropped column --
    /// unlike `czas_pozyskania` it is never loaded at all, not merely
    /// dropped after the fact.
    #[test]
    fn load_into_drops_the_ordering_column_after_dedup() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        load_into(&conn, "egib_buildings", "fixtures/egib.parquet").unwrap();

        for dropped in ["czas_pozyskania", "pozostale_atrybuty"] {
            let present: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM duckdb_columns()
                     WHERE table_name = 'egib_buildings' AND column_name = ?",
                    duckdb::params![dropped],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(!present, "{dropped} must not survive into egib_buildings");
        }
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
            .get("import:egib")
            .expect("entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.is_some());
    }
}
