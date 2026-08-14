use anyhow::{Context, Result, bail};
use duckdb::Connection;
use tracing::{info, warn};

use crate::dataset::{DatasetSpec, ROW_HASH_VERSION, ROW_HASH_VERSION_KEY};
use crate::update::changeset::insert_change_areas;
use crate::update::diff::{self, DiffCounts};
use crate::utils::format_duration;

/// Fraction of the live table that may change before the refresh warns.
/// Measured normal churn for BDOT10k is ~2% over five weeks, so this only
/// fires on an upstream restructuring. It is a diagnostic, NOT a stop.
const IMPLAUSIBLE_CHURN_FRACTION: f64 = 0.5;

/// Drops every scratch table a refresh creates — the staging table and the
/// five `diff_*` temp tables — on every exit path, including early returns
/// and errors. DuckDB has no temp-table-per-transaction semantics here, so
/// this is the only thing standing between a failed refresh and a stale
/// staging table blocking the next one, and between a *successful* refresh
/// and its per-ID hash tables (~16M rows for PRG) sitting on the pooled
/// connection until the next refresh happens to drop them.
struct ScratchGuard<'a> {
    conn: &'a Connection,
    staging: String,
}

impl Drop for ScratchGuard<'_> {
    fn drop(&mut self) {
        let sql = format!(
            "DROP TABLE IF EXISTS {};
             DROP TABLE IF EXISTS diff_live_hashes;
             DROP TABLE IF EXISTS diff_new_hashes;
             DROP TABLE IF EXISTS diff_added;
             DROP TABLE IF EXISTS diff_removed;
             DROP TABLE IF EXISTS diff_modified;",
            self.staging
        );
        if let Err(e) = self.conn.execute_batch(&sql) {
            warn!(staging = %self.staging, error = %e, "failed to drop refresh scratch tables");
        }
    }
}

/// Stage a new snapshot, diff it against the live table, and apply the delta
/// in a single transaction together with the changeset.
///
/// `load` must create the staging table named by `spec.staging_table()`,
/// including a `_row_hash` column (use `crate::dataset::hashed_select`).
///
/// `is_cancelled` is polled at three points via [`check_cancelled`], all
/// outside the apply transaction -- see that helper's doc comment for both
/// why the checks stop there and why a cancelled `refresh` returns `Err`
/// rather than `Ok(())`.
pub fn refresh(
    conn: &Connection,
    spec: &DatasetSpec,
    load: impl FnOnce(&Connection, &str) -> Result<crate::dataset::LoadStats>,
    source_etag: Option<&str>,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<DiffCounts> {
    let total = std::time::Instant::now();
    let staging = spec.staging_table();

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging}"))
        .with_context(|| format!("Failed to clear stale staging table {staging}"))?;

    let _guard = ScratchGuard {
        conn,
        staging: staging.clone(),
    };

    let job_name = format!("update:{}", spec.name);
    let outcome = (|| -> Result<(DiffCounts, crate::dataset::LoadStats)> {
        // --- stage ---
        check_cancelled(spec.name, "staging", is_cancelled)?;
        let t = std::time::Instant::now();
        let load_stats = load(conn, &staging)
            .with_context(|| format!("Failed to stage {} snapshot", spec.name))?;
        let staged: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {staging}"), [], |row| {
                row.get(0)
            })
            .with_context(|| format!("Failed to count rows in {staging}"))?;
        info!(
            source = spec.name,
            rows = staged,
            elapsed = %format_duration(t.elapsed()),
            "Step done: stage snapshot"
        );

        // The load-bearing guard: an empty snapshot would delete the dataset.
        if staged == 0 {
            bail!(
                "Staged snapshot for {} has 0 rows — refusing to apply, \
                 which would delete the entire live dataset. The download is \
                 most likely empty or truncated.",
                spec.name
            );
        }

        let hash_version = check_row_hash_version(conn)?;

        // --- diff ---
        check_cancelled(spec.name, "diff", is_cancelled)?;
        let t = std::time::Instant::now();
        let counts = diff::compute(conn, spec)?;
        info!(
            source = spec.name,
            added = counts.added,
            modified = counts.modified,
            removed = counts.removed,
            elapsed = %format_duration(t.elapsed()),
            "Step done: diff snapshot"
        );

        let live_rows: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {}", spec.table), [], |row| {
                row.get(0)
            })
            .with_context(|| format!("Failed to count rows in {}", spec.table))?;
        let churn = counts.added + counts.modified + counts.removed;
        if live_rows > 0 && (churn as f64) > (live_rows as f64) * IMPLAUSIBLE_CHURN_FRACTION {
            warn!(
                source = spec.name,
                churn,
                live_rows,
                "implausibly large change set (>{:.0}% of rows) — proceeding, but this \
                 usually means the source was restructured rather than genuinely changed",
                IMPLAUSIBLE_CHURN_FRACTION * 100.0
            );
        }

        // --- apply ---
        // Last check before anything lands: everything from here on runs
        // inside the apply transaction, and check_cancelled must never be
        // called there (see its doc comment for why).
        check_cancelled(spec.name, "apply", is_cancelled)?;
        let t = std::time::Instant::now();
        let id = spec.id_column;
        let live = spec.table;

        conn.execute_batch("BEGIN TRANSACTION")
            .context("Failed to begin apply transaction")?;

        // snapshot_id is allocated inside the transaction: a concurrent refresh
        // that started BEGIN first will hold this SELECT until it commits or
        // rolls back, so two overlapping refreshes cannot allocate the same id.
        let applied = (|| -> Result<i64> {
            let snapshot_id: i64 = conn
                .query_row(
                    "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dataset_refreshes",
                    [],
                    |row| row.get(0),
                )
                .context("Failed to allocate snapshot_id")?;

            // Change areas are computed BEFORE the delta is applied: they read
            // the OLD geometry of removed/modified objects out of `live`. If
            // this ran after the DELETE+INSERT below, a removed object's row
            // would already be gone and a modified object's row would already
            // hold its NEW geometry — losing the "cell it left" signal entirely
            // (see the regression test `change_areas_capture_the_origin_cell_
            // before_the_delta_is_applied`).
            insert_change_areas(conn, spec, snapshot_id)?;
            crate::update::changeset::insert_dirty_cells(conn, spec)?;

            conn.execute_batch(&format!(
                "DELETE FROM {live} WHERE {id} IN (
                     SELECT id FROM diff_removed UNION ALL SELECT id FROM diff_modified);
                 INSERT INTO {live} SELECT * FROM {staging} WHERE {id} IN (
                     SELECT id FROM diff_added UNION ALL SELECT id FROM diff_modified);"
            ))
            .with_context(|| format!("Failed to apply delta to {live}"))?;

            // A stale stamp means every ID compared as modified, so the DELETE +
            // INSERT above just replaced the whole table with staging rows — whose
            // hashes come from the current expression. Re-stamping here is what
            // makes the mismatch warning fire once per bump instead of forever.
            // An unstamped database gets its first stamp the same way. Inside the
            // transaction, so a refresh that rolls back leaves the stamp alone.
            if hash_version != RowHashVersion::Current {
                crate::dataset::stamp_row_hash_version(conn)?;
            }

            // Unlike the stamp above, this fires on EVERY landed refresh, even a
            // 0/0/0 diff — a refresh can rewrite raw columns `/tiles` reads
            // (`centroid`, `rodzaj_kod`) that sit outside the diffed row set, so
            // "the delta was empty" does not mean "nothing /tiles reads changed".
            // Inside the transaction, so an aborted refresh (see the `?` above,
            // which returns before reaching here) cannot claim a bump that never
            // landed — see `serving_version`'s module doc.
            crate::serving_version::bump_serving_epoch(conn)?;

            conn.execute(
                "INSERT INTO dataset_refreshes
                 (snapshot_id, source, started_at, finished_at, source_etag,
                  added, modified, removed)
                 VALUES (?, ?, now(), now(), ?, ?, ?, ?)",
                duckdb::params![
                    snapshot_id,
                    spec.name,
                    source_etag,
                    counts.added,
                    counts.modified,
                    counts.removed,
                ],
            )
            .context("Failed to record refresh")?;

            Ok(snapshot_id)
        })();

        let snapshot_id = match applied {
            Ok(snapshot_id) => match conn.execute_batch("COMMIT") {
                Ok(()) => snapshot_id,
                Err(e) => {
                    if let Err(rb) = conn.execute_batch("ROLLBACK") {
                        warn!(error = %rb, "failed to roll back apply transaction after commit failure");
                    }
                    return Err(e).context("Failed to commit apply transaction");
                }
            },
            Err(e) => {
                if let Err(rb) = conn.execute_batch("ROLLBACK") {
                    warn!(error = %rb, "failed to roll back apply transaction");
                }
                return Err(e);
            }
        };

        info!(
            source = spec.name,
            snapshot_id,
            elapsed = %format_duration(t.elapsed()),
            "Step done: apply delta"
        );
        info!(
            source = spec.name,
            added = counts.added,
            modified = counts.modified,
            removed = counts.removed,
            elapsed = %format_duration(total.elapsed()),
            "Dataset refresh complete"
        );

        Ok((counts, load_stats))
    })();

    match &outcome {
        Ok((counts, stats)) => {
            let _ = crate::job_log::record(
                conn,
                &job_name,
                "Success",
                Some(&summarize_refresh(counts, stats)),
            );
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, &job_name, "Error", Some(&format!("{e:#}")));
        }
    }

    outcome.map(|(counts, _)| counts)
}

/// Human-readable message for the `job_run_log` row. Uses
/// `dataset::format_skip_clause` for both skip reasons -- see
/// `dataset::LoadStats` for why a loader can drop rows for either
/// invalid or oversized geometry.
fn summarize_refresh(counts: &DiffCounts, stats: &crate::dataset::LoadStats) -> String {
    let mut msg = format!(
        "added {} modified {} removed {}",
        counts.added, counts.modified, counts.removed
    );
    if stats.skipped_invalid_geometry > 0 {
        msg.push_str("; ");
        msg.push_str(&crate::dataset::format_skip_clause(
            "invalid-geometry",
            stats.skipped_invalid_geometry,
            &stats.skipped_example_ids,
        ));
    }
    if stats.skipped_oversized_geometry > 0 {
        msg.push_str("; ");
        msg.push_str(&crate::dataset::format_skip_clause(
            "oversized-geometry",
            stats.skipped_oversized_geometry,
            &stats.skipped_oversized_example_ids,
        ));
    }
    msg
}

/// Both cancellation signals `refresh` polls, mirroring the loop in
/// `update::osm::update` (`src/update/osm.rs:131-149`): the process-global
/// `crate::shutdown::is_requested()` (SIGINT/SIGTERM on the CLI path) and the
/// caller-injected `is_cancelled` (supervisor timeout or scheduler shutdown
/// on the server path). One function so `refresh`'s three call sites check
/// both signals identically.
///
/// Every call site in `refresh` sits OUTSIDE the apply transaction, and that
/// is deliberate: `readers_never_observe_a_partial_apply` and the
/// `insert_change_areas`-ordering test both depend on that transaction being
/// an atomic unit that either fully lands or fully doesn't. A check inside it
/// would buy nothing over checking before `BEGIN` -- the transaction would
/// just roll back either way -- while adding a rollback path that has to be
/// reasoned about for no benefit. Do not add a fourth call site inside it.
///
/// Returns `Err`, deliberately the opposite of `osm::update`'s `Ok(())` on
/// cancellation. `osm::update` commits one replication batch at a time and
/// resumes from a `metadata` stamp, so stopping early is real, durable
/// partial progress -- "less caught up, resume next run". `refresh` has no
/// such checkpoint: until the apply transaction commits, nothing has landed
/// -- no `dataset_refreshes` row, no delta, no dirty cells, no serving-epoch
/// bump. `Ok(())` here would make the `job_log::record` call at the bottom of
/// `refresh` write a `Success` row, and the job registry report a successful
/// run, for a refresh that did nothing at all.
fn check_cancelled(source: &str, stage: &str, is_cancelled: &dyn Fn() -> bool) -> Result<()> {
    let reason = if crate::shutdown::is_requested() {
        "shutdown requested"
    } else if is_cancelled() {
        "job cancelled"
    } else {
        return Ok(());
    };
    info!(
        source,
        stage, reason, "Dataset refresh cancelled before landing"
    );
    bail!("Cancelled before {stage} ({reason})")
}

/// What `metadata.row_hash_version` says about the live table's `_row_hash`
/// values, relative to the expression this build would produce.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RowHashVersion {
    /// Stamped, and the stamp matches. Nothing to do.
    Current,
    /// Stamped with a different version: `hashed_select` changed since this
    /// table was built, so every row will compare as modified.
    Stale(String),
    /// No stamp at all — a database imported before stamping existed.
    Unstamped,
}

/// Read the stamp and warn if it is stale. Pure read: re-stamping happens in
/// the apply transaction (see [`refresh`]), so a refresh that never lands
/// cannot leave a stamp claiming a rewrite that did not happen.
///
/// Deliberately narrow scope: [`ROW_HASH_VERSION`] is a constant we bump by
/// hand when we edit [`crate::dataset::hashed_select`]. It is not derived
/// from the DuckDB version, so an upgrade that silently changed `hash()`
/// output would NOT be caught here. That costs no correctness — the refresh
/// would simply be a full rewrite — only the explanatory warning.
fn check_row_hash_version(conn: &Connection) -> Result<RowHashVersion> {
    // `.optional()` turns `QueryReturnedNoRows` (nothing stored yet) into
    // `Ok(None)` while still propagating any genuine query error (e.g. the
    // `metadata` table itself is missing) — `.ok()` would have silently
    // swallowed both cases alike.
    use duckdb::OptionalExt;
    let stored: Option<String> = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = ?",
            duckdb::params![ROW_HASH_VERSION_KEY],
            |row| row.get(0),
        )
        .optional()
        .context("Failed to read row hash version from metadata")?;

    Ok(match stored {
        Some(v) if v == ROW_HASH_VERSION.to_string() => RowHashVersion::Current,
        Some(v) => {
            warn!(
                stored = %v,
                expected = ROW_HASH_VERSION,
                "row hash version mismatch — the row-hash expression changed since this \
                 table was built, so every row compares as modified and this refresh is \
                 effectively a full rewrite. It re-stamps the version on success, so this \
                 warning should not appear again."
            );
            RowHashVersion::Stale(v)
        }
        None => RowHashVersion::Unstamped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{DatasetSpec, GeomKind};
    use crate::db::init_db;
    use std::path::Path;

    const TEST_SPEC: DatasetSpec = DatasetSpec {
        name: "test",
        table: "live",
        id_column: "id",
        geom_kind: GeomKind::Point,
    };

    fn conn_with_live(rows: &str) -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
        conn.execute_batch(&format!(
            "CREATE TABLE live AS {};",
            crate::dataset::hashed_select(&inner)
        ))
        .unwrap();
        conn
    }

    /// Loader closure that fills staging from an inline VALUES list.
    fn loader(
        rows: &'static str,
    ) -> impl FnOnce(&Connection, &str) -> Result<crate::dataset::LoadStats> {
        move |conn: &Connection, target: &str| {
            let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
            conn.execute_batch(&format!(
                "CREATE TABLE {target} AS {};",
                crate::dataset::hashed_select(&inner)
            ))?;
            Ok(crate::dataset::LoadStats::default())
        }
    }

    const LIVE_ROWS: &str = "SELECT * FROM (VALUES
        ('keep','v1',21.0,52.0), ('mod','v1',21.0,52.0), ('del','v1',21.0,52.0)
      ) t(id,a,lon,lat)";
    const NEW_ROWS: &str = "SELECT * FROM (VALUES
        ('keep','v1',21.0,52.0), ('mod','CHANGED',21.0,52.0), ('add','v1',21.0,52.0)
      ) t(id,a,lon,lat)";

    #[test]
    fn refresh_records_success_in_job_run_log_including_skip_stats() {
        let conn = conn_with_live(LIVE_ROWS);
        let loader_with_stats = |rows: &'static str, stats: crate::dataset::LoadStats| {
            move |conn: &Connection, target: &str| -> Result<crate::dataset::LoadStats> {
                let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
                conn.execute_batch(&format!(
                    "CREATE TABLE {target} AS {};",
                    crate::dataset::hashed_select(&inner)
                ))?;
                Ok(stats)
            }
        };

        refresh(
            &conn,
            &TEST_SPEC,
            loader_with_stats(
                NEW_ROWS,
                crate::dataset::LoadStats {
                    skipped_invalid_geometry: 2,
                    skipped_example_ids: vec!["bad1".to_string(), "bad2".to_string()],
                    ..Default::default()
                },
            ),
            None,
            &|| false,
        )
        .unwrap();

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log
            .get("update:test")
            .expect("job_run_log entry must exist");
        assert_eq!(entry.outcome, "Success");
        let msg = entry.message.as_deref().unwrap();
        assert!(
            msg.contains("skipped 2 invalid-geometry rows"),
            "got: {msg}"
        );
        assert!(msg.contains("bad1") && msg.contains("bad2"), "got: {msg}");
    }

    /// Mirror of the test above for the other skip reason: an update refresh
    /// stages via `load_into`, which now also runs
    /// `dataset::filter_oversized_geometry` on the staging table, so its
    /// count and example ids must reach `update:<source>`'s job_run_log
    /// message the same way the invalid-geometry ones do.
    #[test]
    fn refresh_records_oversized_geometry_skips_in_job_run_log() {
        let conn = conn_with_live(LIVE_ROWS);
        let loader_with_stats = |rows: &'static str, stats: crate::dataset::LoadStats| {
            move |conn: &Connection, target: &str| -> Result<crate::dataset::LoadStats> {
                let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
                conn.execute_batch(&format!(
                    "CREATE TABLE {target} AS {};",
                    crate::dataset::hashed_select(&inner)
                ))?;
                Ok(stats)
            }
        };

        refresh(
            &conn,
            &TEST_SPEC,
            loader_with_stats(
                NEW_ROWS,
                crate::dataset::LoadStats {
                    skipped_oversized_geometry: 1,
                    skipped_oversized_example_ids: vec!["glued".to_string()],
                    ..Default::default()
                },
            ),
            None,
            &|| false,
        )
        .unwrap();

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log
            .get("update:test")
            .expect("job_run_log entry must exist");
        assert_eq!(entry.outcome, "Success");
        let msg = entry.message.as_deref().unwrap();
        assert!(
            msg.contains("skipped 1 oversized-geometry rows"),
            "got: {msg}"
        );
        assert!(msg.contains("glued"), "got: {msg}");
    }

    #[test]
    fn refresh_records_error_in_job_run_log_on_failure() {
        let conn = conn_with_live(LIVE_ROWS);
        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";

        let _ = refresh(&conn, &TEST_SPEC, loader(empty), None, &|| false);

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log
            .get("update:test")
            .expect("job_run_log entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.as_deref().unwrap().contains("0 rows"));
    }

    #[test]
    fn applies_delta_to_live_table() {
        let conn = conn_with_live(LIVE_ROWS);
        let counts = refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| false).unwrap();
        assert_eq!(
            counts,
            DiffCounts {
                added: 1,
                modified: 1,
                removed: 1
            }
        );

        let mut stmt = conn.prepare("SELECT id, a FROM live ORDER BY id").unwrap();
        let rows: Vec<(String, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![
                ("add".to_string(), "v1".to_string()),
                ("keep".to_string(), "v1".to_string()),
                ("mod".to_string(), "CHANGED".to_string()),
            ]
        );
    }

    #[test]
    fn writes_refresh_row_and_change_areas() {
        let conn = conn_with_live(LIVE_ROWS);
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), Some("etag-1"), &|| {
            false
        })
        .unwrap();

        let (snapshot_id, source, etag, added, modified, removed): (
            i64,
            String,
            String,
            i32,
            i32,
            i32,
        ) = conn
            .query_row(
                "SELECT snapshot_id, source, source_etag, added, modified, removed
                 FROM dataset_refreshes",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(snapshot_id, 1, "first refresh gets snapshot_id 1");
        assert_eq!((source.as_str(), etag.as_str()), ("test", "etag-1"));
        assert_eq!((added, modified, removed), (1, 1, 1));

        let cells: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM dataset_change_areas WHERE snapshot_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(cells > 0, "expected at least one change area row");

        let dirty: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'test'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(dirty > 0, "refresh must enqueue dirty cells");
    }

    /// The added/modified/removed columns in `dataset_refreshes` are written
    /// positionally. Every other test in this module uses counts that are
    /// tied (1,1,1 / 0,0,0 / 0,2,0), so none would catch e.g. `added` and
    /// `removed` being transposed in the INSERT. Pin them with three
    /// mutually distinct values instead, mirroring
    /// `counts_land_in_their_own_columns` in `changeset.rs`.
    #[test]
    fn refresh_row_pins_added_modified_removed_to_their_own_columns() {
        let live = "SELECT * FROM (VALUES
            ('r1','v1',21.0,52.0), ('r2','v1',21.0,52.0), ('r3','v1',21.0,52.0),
            ('m1','v1',21.0,52.0), ('m2','v1',21.0,52.0)
          ) t(id,a,lon,lat)";
        let conn = conn_with_live(live);
        const NEW: &str = "SELECT * FROM (VALUES
            ('m1','CHANGED',21.0,52.0), ('m2','CHANGED',21.0,52.0), ('a1','v1',21.0,52.0)
          ) t(id,a,lon,lat)";

        let counts = refresh(&conn, &TEST_SPEC, loader(NEW), None, &|| false).unwrap();
        assert_eq!(
            counts,
            DiffCounts {
                added: 1,
                modified: 2,
                removed: 3
            }
        );

        let (added, modified, removed): (i32, i32, i32) = conn
            .query_row(
                "SELECT added, modified, removed FROM dataset_refreshes",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!((added, modified, removed), (1, 2, 3));
    }

    /// snapshot_id is MAX + 1, so a second refresh does not collide.
    #[test]
    fn snapshot_ids_increment() {
        let conn = conn_with_live(LIVE_ROWS);
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| false).unwrap();
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| false).unwrap();

        let ids: Vec<i64> = {
            let mut stmt = conn
                .prepare("SELECT snapshot_id FROM dataset_refreshes ORDER BY snapshot_id")
                .unwrap();
            let r = stmt.query_map([], |r| r.get(0)).unwrap();
            r.map(|x| x.unwrap()).collect()
        };
        assert_eq!(ids, vec![1, 2]);
    }

    /// The load-bearing safety check: an empty staging table means a
    /// truncated or empty download, which would otherwise delete everything.
    #[test]
    fn empty_staging_aborts_and_leaves_live_untouched() {
        let conn = conn_with_live(LIVE_ROWS);
        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";
        let err = refresh(&conn, &TEST_SPEC, loader(empty), None, &|| false).unwrap_err();
        assert!(
            format!("{err:#}").contains("0 rows"),
            "error should name the empty staging table, got: {err:#}"
        );

        let live_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM live", [], |r| r.get(0))
            .unwrap();
        assert_eq!(live_rows, 3, "live table must be untouched");
        let refreshes: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_refreshes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(refreshes, 0, "aborted refresh must not be recorded");
    }

    /// Staging is dropped on both the success and the failure path.
    #[test]
    fn staging_table_is_always_cleaned_up() {
        let conn = conn_with_live(LIVE_ROWS);
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| false).unwrap();
        assert!(!staging_exists(&conn), "staging left behind after success");

        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";
        let _ = refresh(&conn, &TEST_SPEC, loader(empty), None, &|| false);
        assert!(!staging_exists(&conn), "staging left behind after failure");
    }

    /// The per-ID hash tables are the largest thing a refresh materializes
    /// (one row per ID, ~16M for PRG). They must not outlive the refresh on
    /// the pooled connection, waiting for the *next* refresh to drop them.
    #[test]
    fn diff_temp_tables_do_not_outlive_the_refresh() {
        let conn = conn_with_live(LIVE_ROWS);
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| false).unwrap();

        for t in [
            "diff_live_hashes",
            "diff_new_hashes",
            "diff_added",
            "diff_removed",
            "diff_modified",
        ] {
            assert!(!table_exists(&conn, t), "{t} left behind after success");
        }
    }

    fn staging_exists(conn: &Connection) -> bool {
        table_exists(conn, &TEST_SPEC.staging_table())
    }

    fn table_exists(conn: &Connection, name: &str) -> bool {
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?",
                duckdb::params![name],
                |r| r.get(0),
            )
            .unwrap();
        n > 0
    }

    /// An unchanged snapshot still records a refresh row, with zero counts
    /// and no change areas, so "ran and did nothing" is distinguishable from
    /// "never ran".
    #[test]
    fn unchanged_snapshot_records_a_noop_refresh() {
        let conn = conn_with_live(LIVE_ROWS);
        let counts = refresh(&conn, &TEST_SPEC, loader(LIVE_ROWS), None, &|| false).unwrap();
        assert_eq!(
            counts,
            DiffCounts {
                added: 0,
                modified: 0,
                removed: 0
            }
        );

        let refreshes: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_refreshes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(refreshes, 1);
        let cells: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(cells, 0);
    }

    /// Churn above IMPLAUSIBLE_CHURN_FRACTION warns but must NOT block —
    /// a genuinely restructured source should still land.
    #[test]
    fn implausible_churn_warns_but_still_applies() {
        let live = "SELECT * FROM (VALUES ('a','v1',21.0,52.0), ('b','v1',21.0,52.0))
                    t(id,a,lon,lat)";
        let conn = conn_with_live(live);
        const ALL_NEW: &str = "SELECT * FROM (VALUES ('a','X',21.0,52.0), ('b','X',21.0,52.0))
                               t(id,a,lon,lat)";

        let counts = refresh(&conn, &TEST_SPEC, loader(ALL_NEW), None, &|| false).unwrap();
        assert_eq!(
            counts,
            DiffCounts {
                added: 0,
                modified: 2,
                removed: 0
            }
        );

        let changed: i64 = conn
            .query_row("SELECT COUNT(*) FROM live WHERE a = 'X'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(changed, 2, "100% churn must still be applied");
    }

    /// `insert_change_areas` reads the OLD geometry of removed/modified
    /// objects out of the live table. If it runs AFTER the delta has already
    /// been applied, the removed object's row is gone and the modified
    /// object's row has already been overwritten with the NEW geometry — so
    /// the cell it left never gets marked, and the destination cell gets
    /// double-counted instead. This must go through `refresh()` end-to-end,
    /// not `insert_change_areas` in isolation, because the isolated tests
    /// build `live`/`staging` by hand and never see the reorder bug.
    #[test]
    fn change_areas_capture_the_origin_cell_before_the_delta_is_applied() {
        use crate::tile_math::{CHANGE_CELL_ZOOM, lonlat_to_tile};

        const BEFORE: &str = "SELECT * FROM (VALUES
            ('del','v1',21.0,52.0), ('mov','v1',21.0,52.0)
          ) t(id,a,lon,lat)";
        // 'del' is gone; 'mov' moved to a different cell.
        const AFTER: &str = "SELECT * FROM (VALUES
            ('mov','v1',19.0,50.0)
          ) t(id,a,lon,lat)";

        let conn = conn_with_live(BEFORE);
        refresh(&conn, &TEST_SPEC, loader(AFTER), None, &|| false).unwrap();

        let (origin_x, origin_y) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
        let (dest_x, dest_y) = lonlat_to_tile(19.0, 50.0, CHANGE_CELL_ZOOM);

        let (origin_modified, origin_removed): (i32, i32) = conn
            .query_row(
                "SELECT modified, removed FROM dataset_change_areas
                 WHERE cell_x = ? AND cell_y = ?",
                duckdb::params![origin_x, origin_y],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            origin_removed, 1,
            "origin cell must record the removed object"
        );
        assert_eq!(
            origin_modified, 1,
            "origin cell must record the moved object leaving"
        );

        let dest_modified: i32 = conn
            .query_row(
                "SELECT modified FROM dataset_change_areas WHERE cell_x = ? AND cell_y = ?",
                duckdb::params![dest_x, dest_y],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            dest_modified, 1,
            "destination cell must record the moved object arriving, not double-counted"
        );
    }

    /// The whole point of the design: a concurrent reader must see the old
    /// snapshot or the new one, never a half-applied state. The delta below
    /// changes the row count (3 -> 4), so an intermediate would be visible
    /// as any count outside {3, 4}.
    #[test]
    fn readers_never_observe_a_partial_apply() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        const BEFORE: &str = "SELECT * FROM (VALUES
            ('a','v1',21.0,52.0), ('b','v1',21.0,52.0), ('c','v1',21.0,52.0)
          ) t(id,a,lon,lat)";
        // Same three rows, all modified, plus a fourth: a large delete+insert
        // with a net row-count change.
        const AFTER: &str = "SELECT * FROM (VALUES
            ('a','X',21.0,52.0), ('b','X',21.0,52.0), ('c','X',21.0,52.0),
            ('d','X',21.0,52.0)
          ) t(id,a,lon,lat)";

        let conn = conn_with_live(BEFORE);
        let reader = conn.try_clone().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_reader = stop.clone();

        let handle = std::thread::spawn(move || {
            let mut seen = Vec::new();
            while !stop_reader.load(Ordering::SeqCst) {
                if let Ok(n) =
                    reader.query_row("SELECT COUNT(*) FROM live", [], |r| r.get::<_, i64>(0))
                {
                    seen.push(n);
                }
            }
            seen
        });

        refresh(&conn, &TEST_SPEC, loader(AFTER), None, &|| false).unwrap();
        stop.store(true, Ordering::SeqCst);
        let seen = handle.join().unwrap();

        assert!(!seen.is_empty(), "reader thread observed nothing");
        for n in &seen {
            assert!(
                *n == 3 || *n == 4,
                "reader saw a partially-applied state: {n} rows (expected 3 or 4). \
                 Observed sequence: {seen:?}"
            );
        }
    }

    /// A bare connection with the schema (including `metadata`) but no
    /// `live`/`staging` tables — enough for `check_row_hash_version` alone.
    fn bare_conn() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    #[test]
    fn check_row_hash_version_matches_is_a_noop() {
        let conn = bare_conn();
        conn.execute(
            "INSERT INTO metadata VALUES (?, ?)",
            duckdb::params![ROW_HASH_VERSION_KEY, ROW_HASH_VERSION.to_string()],
        )
        .unwrap();

        assert_eq!(
            check_row_hash_version(&conn).unwrap(),
            RowHashVersion::Current
        );

        let value: String = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = ?",
                duckdb::params![ROW_HASH_VERSION_KEY],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(value, ROW_HASH_VERSION.to_string());
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM metadata", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1, "must not insert a duplicate row");
    }

    /// A version mismatch must warn, not error, and must not re-stamp on its
    /// own — the stamp is only earned by an apply that actually lands.
    #[test]
    fn check_row_hash_version_mismatch_warns_but_does_not_block() {
        let conn = bare_conn();
        conn.execute(
            "INSERT INTO metadata VALUES (?, ?)",
            duckdb::params![ROW_HASH_VERSION_KEY, "999"],
        )
        .unwrap();

        assert_eq!(
            check_row_hash_version(&conn).unwrap(),
            RowHashVersion::Stale("999".to_string())
        );

        let value: String = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = ?",
                duckdb::params![ROW_HASH_VERSION_KEY],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            value, "999",
            "the check itself must not overwrite the stored version"
        );
    }

    #[test]
    fn check_row_hash_version_reports_unstamped_without_writing() {
        let conn = bare_conn();

        assert_eq!(
            check_row_hash_version(&conn).unwrap(),
            RowHashVersion::Unstamped
        );

        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM metadata WHERE key = ?",
                duckdb::params![ROW_HASH_VERSION_KEY],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            rows, 0,
            "the check must not stamp; only a landed apply does"
        );
    }

    /// The self-heal: a stale stamp means the refresh rewrote every row with
    /// the current expression, so it must re-stamp. Otherwise the warning
    /// fires forever and the advice it prints ("re-run the import") cannot
    /// clear it either.
    #[test]
    fn stale_version_is_restamped_after_a_successful_refresh() {
        let conn = conn_with_live(LIVE_ROWS);
        conn.execute(
            "INSERT INTO metadata VALUES (?, ?)",
            duckdb::params![ROW_HASH_VERSION_KEY, "999"],
        )
        .unwrap();

        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| false).unwrap();

        assert_eq!(
            check_row_hash_version(&conn).unwrap(),
            RowHashVersion::Current,
            "a second refresh must not warn again"
        );
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM metadata WHERE key = ?",
                duckdb::params![ROW_HASH_VERSION_KEY],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1, "re-stamping must replace, not duplicate");
    }

    #[test]
    fn unstamped_database_is_stamped_after_a_successful_refresh() {
        let conn = conn_with_live(LIVE_ROWS);
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| false).unwrap();

        assert_eq!(
            check_row_hash_version(&conn).unwrap(),
            RowHashVersion::Current
        );
    }

    /// The stamp claims "the live table was rebuilt with this expression". A
    /// refresh that aborts rebuilds nothing, so it must leave the old stamp
    /// in place — otherwise the next run would silently skip the warning for
    /// a table whose hashes are still stale.
    #[test]
    fn aborted_refresh_does_not_restamp() {
        let conn = conn_with_live(LIVE_ROWS);
        conn.execute(
            "INSERT INTO metadata VALUES (?, ?)",
            duckdb::params![ROW_HASH_VERSION_KEY, "999"],
        )
        .unwrap();

        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";
        refresh(&conn, &TEST_SPEC, loader(empty), None, &|| false).unwrap_err();

        assert_eq!(
            check_row_hash_version(&conn).unwrap(),
            RowHashVersion::Stale("999".to_string()),
            "an aborted refresh must not claim the rewrite happened"
        );
    }

    // --- serving_version bump -------------------------------------------
    //
    // Mirrors the `stamp_row_hash_version` twins above, but for
    // `serving_version::bump_serving_epoch`: unlike the stamp, the bump is
    // unconditional on every LANDED refresh (see the comment beside its call
    // site in `refresh`, above) -- these three tests pin "landed" (bumps),
    // "aborted" (does not) and "never attempted" (does not) as the three
    // cases that matter.

    /// The 0/0/0-diff case (`LIVE_ROWS` staged against itself, so nothing
    /// changed) still counts as a landed refresh and must still bump --
    /// raw columns `/tiles` reads (`centroid`, `rodzaj_kod`) can move even
    /// when the diffed row set didn't.
    #[test]
    fn successful_refresh_bumps_the_serving_epoch() {
        let conn = conn_with_live(LIVE_ROWS);
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            0
        );
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| false).unwrap();
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            1
        );
    }

    /// Pins the in-transaction placement: an aborted refresh applied nothing,
    /// so it must not claim the serving state moved either.
    #[test]
    fn aborted_refresh_does_not_bump_the_serving_epoch() {
        let conn = conn_with_live(LIVE_ROWS);
        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";
        refresh(&conn, &TEST_SPEC, loader(empty), None, &|| false).unwrap_err();
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            0
        );
    }

    /// `update::record_noop_refresh` (the ETag-unchanged skip; see
    /// `serving_version`'s module-doc "Must NOT bump" list) never calls
    /// `refresh` at all, so it must not move the epoch either -- otherwise a
    /// daily poll against an unchanged government source would flush every
    /// cached tile in the country for a change that never happened.
    /// `record_noop_refresh` is private to `update`, but reachable here
    /// since `update::dataset` is a descendant module of `update`.
    #[test]
    fn noop_refresh_does_not_bump_the_serving_epoch() {
        let conn = bare_conn();
        crate::update::record_noop_refresh(&conn, "test", Some("etag-1")).unwrap();
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            0
        );
    }

    /// `.ok()` would have silently swallowed a genuine query failure (not
    /// just "no row yet") as if the version had never been recorded. Drop
    /// `metadata` entirely so the query fails for a real reason, and confirm
    /// the error propagates instead of being treated as first-run.
    #[test]
    fn check_row_hash_version_propagates_genuine_query_errors() {
        let conn = bare_conn();
        conn.execute_batch("DROP TABLE metadata").unwrap();

        let err = check_row_hash_version(&conn).unwrap_err();
        assert!(
            format!("{err:#}").contains("row hash version"),
            "error should mention row hash version, got: {err:#}"
        );
    }

    // --- cancellation ------------------------------------------------------
    //
    // `check_cancelled` is polled at three points, all before the apply
    // transaction begins (see its doc comment for why). These tests drive it
    // with an injected closure over an `AtomicUsize` -- the same shape
    // `compare::drain::drain_batch`'s `cancellation_stops_the_batch_between_
    // cells` test and `compare::buildings`'s `cancelled_compare_leaves_the_
    // previous_contents_intact` test use -- and never by flipping the
    // process-global `crate::shutdown` flag, which has no public setter for
    // exactly this reason: a test cannot flip it without leaking `true` into
    // every other test sharing this binary's process.

    /// Cancelled at the very first check point, before `load` ever runs:
    /// nothing has happened at all, so this pins the cleanest possible bail.
    #[test]
    fn cancelled_before_staging_leaves_everything_untouched() {
        let conn = conn_with_live(LIVE_ROWS);
        let err = refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| true).unwrap_err();
        assert!(
            format!("{err:#}").contains("Cancelled before staging"),
            "got: {err:#}"
        );

        let live_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM live", [], |r| r.get(0))
            .unwrap();
        assert_eq!(live_rows, 3, "live table must be untouched");
        let refreshes: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_refreshes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(refreshes, 0, "a cancelled refresh must not be recorded");
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            0
        );
        assert!(
            !staging_exists(&conn),
            "ScratchGuard must clean up the staging table even though load() never ran"
        );
    }

    /// Cancelled AFTER staging has already landed a full snapshot, but
    /// before the apply transaction begins. This is the case that actually
    /// proves a staged-but-not-applied refresh discards cleanly: a bug that
    /// only shows up once something has genuinely been staged (staging left
    /// behind past `ScratchGuard`, or dirty cells enqueued before `BEGIN`)
    /// would not be caught by the "before staging" test above.
    #[test]
    fn cancelled_after_staging_discards_the_staged_snapshot_cleanly() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let conn = conn_with_live(LIVE_ROWS);
        let calls = AtomicUsize::new(0);
        // false on the first poll (the "staging" check, letting `load` run
        // and create the staging table), true from the second call onward --
        // so this bails at the "diff" check point, after staging succeeded.
        let cancel = || calls.fetch_add(1, Ordering::SeqCst) >= 1;

        let err = refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &cancel).unwrap_err();
        // Assert the exact stage, not just "Cancelled before": reaching the
        // "diff" check point is the only proof this test staged anything at
        // all. A substring match on "Cancelled before" would also accept a
        // bail at "staging", which is the *previous* test's case -- so a
        // future refactor that moved or lost the post-staging check point
        // would leave this test passing while it stopped covering the thing
        // its name and doc comment promise.
        assert!(
            format!("{err:#}").contains("Cancelled before diff"),
            "got: {err:#}"
        );

        let live_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM live", [], |r| r.get(0))
            .unwrap();
        assert_eq!(live_rows, 3, "live table must be untouched");
        let refreshes: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_refreshes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(refreshes, 0, "a cancelled refresh must not be recorded");
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            0
        );
        assert!(
            !staging_exists(&conn),
            "staging left behind after a cancelled refresh"
        );
        let dirty: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'test'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            dirty, 0,
            "dirty cells are only enqueued inside the apply transaction, \
             which a cancelled-before-apply refresh never reaches"
        );
    }

    /// A cancelled refresh is a real failure, not a quiet no-op: it must
    /// land in `job_run_log` as `Error`, the same as any other aborted
    /// refresh, so an operator (or `/status`) can tell "cancelled" apart
    /// from "never ran".
    #[test]
    fn cancellation_is_recorded_in_job_run_log_as_an_error() {
        let conn = conn_with_live(LIVE_ROWS);
        let _ = refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None, &|| true);

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log
            .get("update:test")
            .expect("job_run_log entry must exist even for a cancelled refresh");
        assert_eq!(entry.outcome, "Error");
        assert!(
            entry.message.as_deref().unwrap().contains("Cancelled"),
            "got: {:?}",
            entry.message
        );
    }
}
