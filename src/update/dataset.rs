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
#[allow(dead_code)]
const IMPLAUSIBLE_CHURN_FRACTION: f64 = 0.5;

/// Drops the staging table on every exit path, including early returns and
/// errors. DuckDB has no temp-table-per-transaction semantics here, so this
/// is the only thing standing between a failed refresh and a stale staging
/// table blocking the next one.
#[allow(dead_code)]
struct StagingGuard<'a> {
    conn: &'a Connection,
    table: String,
}

impl Drop for StagingGuard<'_> {
    fn drop(&mut self) {
        if let Err(e) = self
            .conn
            .execute_batch(&format!("DROP TABLE IF EXISTS {}", self.table))
        {
            warn!(table = %self.table, error = %e, "failed to drop staging table");
        }
    }
}

/// Stage a new snapshot, diff it against the live table, and apply the delta
/// in a single transaction together with the changeset.
///
/// `load` must create the staging table named by `spec.staging_table()`,
/// including a `_row_hash` column (use `crate::dataset::hashed_select`).
#[allow(dead_code)]
pub fn refresh(
    conn: &Connection,
    spec: &DatasetSpec,
    load: impl FnOnce(&Connection, &str) -> Result<()>,
    source_etag: Option<&str>,
) -> Result<DiffCounts> {
    let total = std::time::Instant::now();
    let staging = spec.staging_table();

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging}"))
        .with_context(|| format!("Failed to clear stale staging table {staging}"))?;

    let _guard = StagingGuard {
        conn,
        table: staging.clone(),
    };

    // --- stage ---
    let t = std::time::Instant::now();
    load(conn, &staging).with_context(|| format!("Failed to stage {} snapshot", spec.name))?;
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

    check_row_hash_version(conn)?;

    // --- diff ---
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

        conn.execute_batch(&format!(
            "DELETE FROM {live} WHERE {id} IN (
                 SELECT id FROM diff_removed UNION ALL SELECT id FROM diff_modified);
             INSERT INTO {live} SELECT * FROM {staging} WHERE {id} IN (
                 SELECT id FROM diff_added UNION ALL SELECT id FROM diff_modified);"
        ))
        .with_context(|| format!("Failed to apply delta to {live}"))?;

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

    Ok(counts)
}

/// A DuckDB upgrade can change `hash()` output, which makes every row compare
/// as modified. That is correct but slow and produces a misleadingly large
/// changeset, so warn loudly and explain the cause — but do not block.
#[allow(dead_code)]
fn check_row_hash_version(conn: &Connection) -> Result<()> {
    // `.optional()` turns `QueryReturnedNoRows` (first run, nothing stored
    // yet) into `Ok(None)` while still propagating any genuine query error
    // (e.g. the `metadata` table itself is missing) — `.ok()` would have
    // silently swallowed both cases alike.
    use duckdb::OptionalExt;
    let stored: Option<String> = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = ?",
            duckdb::params![ROW_HASH_VERSION_KEY],
            |row| row.get(0),
        )
        .optional()
        .context("Failed to read row hash version from metadata")?;

    match stored {
        Some(v) if v == ROW_HASH_VERSION.to_string() => {}
        Some(v) => warn!(
            stored = %v,
            expected = ROW_HASH_VERSION,
            "row hash version mismatch — every row will compare as modified. \
             This refresh is effectively a full rewrite. Re-run the full import \
             to resync."
        ),
        None => {
            conn.execute(
                "INSERT INTO metadata VALUES (?, ?)",
                duckdb::params![ROW_HASH_VERSION_KEY, ROW_HASH_VERSION.to_string()],
            )
            .context("Failed to record row hash version")?;
        }
    }
    Ok(())
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
    fn loader(rows: &'static str) -> impl FnOnce(&Connection, &str) -> Result<()> {
        move |conn: &Connection, target: &str| {
            let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
            conn.execute_batch(&format!(
                "CREATE TABLE {target} AS {};",
                crate::dataset::hashed_select(&inner)
            ))?;
            Ok(())
        }
    }

    const LIVE_ROWS: &str = "SELECT * FROM (VALUES
        ('keep','v1',21.0,52.0), ('mod','v1',21.0,52.0), ('del','v1',21.0,52.0)
      ) t(id,a,lon,lat)";
    const NEW_ROWS: &str = "SELECT * FROM (VALUES
        ('keep','v1',21.0,52.0), ('mod','CHANGED',21.0,52.0), ('add','v1',21.0,52.0)
      ) t(id,a,lon,lat)";

    #[test]
    fn applies_delta_to_live_table() {
        let conn = conn_with_live(LIVE_ROWS);
        let counts = refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None).unwrap();
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
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), Some("etag-1")).unwrap();

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

        let counts = refresh(&conn, &TEST_SPEC, loader(NEW), None).unwrap();
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
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None).unwrap();
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None).unwrap();

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
        let err = refresh(&conn, &TEST_SPEC, loader(empty), None).unwrap_err();
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
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None).unwrap();
        assert!(!staging_exists(&conn), "staging left behind after success");

        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";
        let _ = refresh(&conn, &TEST_SPEC, loader(empty), None);
        assert!(!staging_exists(&conn), "staging left behind after failure");
    }

    fn staging_exists(conn: &Connection) -> bool {
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                duckdb::params![TEST_SPEC.staging_table()],
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
        let counts = refresh(&conn, &TEST_SPEC, loader(LIVE_ROWS), None).unwrap();
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

        let counts = refresh(&conn, &TEST_SPEC, loader(ALL_NEW), None).unwrap();
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
        refresh(&conn, &TEST_SPEC, loader(AFTER), None).unwrap();

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

        refresh(&conn, &TEST_SPEC, loader(AFTER), None).unwrap();
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

        check_row_hash_version(&conn).unwrap();

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

    /// A version mismatch must warn, not error, and must not overwrite the
    /// stored value — the whole point is to keep running with a full-rewrite
    /// diagnostic, not to "fix" the discrepancy on its own.
    #[test]
    fn check_row_hash_version_mismatch_warns_but_does_not_block() {
        let conn = bare_conn();
        conn.execute(
            "INSERT INTO metadata VALUES (?, ?)",
            duckdb::params![ROW_HASH_VERSION_KEY, "999"],
        )
        .unwrap();

        check_row_hash_version(&conn).unwrap();

        let value: String = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = ?",
                duckdb::params![ROW_HASH_VERSION_KEY],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            value, "999",
            "mismatch must not silently overwrite the stored version"
        );
    }

    #[test]
    fn check_row_hash_version_first_run_inserts() {
        let conn = bare_conn();

        check_row_hash_version(&conn).unwrap();

        let value: String = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = ?",
                duckdb::params![ROW_HASH_VERSION_KEY],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(value, ROW_HASH_VERSION.to_string());
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
}
