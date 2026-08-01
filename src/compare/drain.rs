use anyhow::{Context, Result};
use duckdb::Connection;

use crate::compare::incremental::recompute_cell_in_txn;

pub struct DrainStats {
    pub cells: u64,
    /// Cells whose recompute errored (e.g. an unknown `source` string). Each
    /// is rolled back, logged, and left in the queue for retry; it does not
    /// abort the rest of the batch.
    pub failed: u64,
}

/// Drain up to `batch_size` distinct (source, cell) whose enqueued_at is at or
/// before the batch start, oldest-enqueued first. Each cell: recompute +
/// delete its queue rows under the same cutoff, in one transaction. A cell
/// re-dirtied after batch_start keeps a surviving queue row for the next
/// tick. `is_cancelled` is polled between cells (never mid-transaction, since
/// each cell is already its own atomic unit) so a timeout or shutdown can
/// stop the batch early without abandoning an in-flight recompute.
pub fn drain_batch(
    conn: &Connection,
    batch_size: usize,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<DrainStats> {
    // A single wall-clock cutoff for the whole batch.
    let batch_start: String = conn
        .query_row("SELECT now()::VARCHAR", [], |r| r.get(0))
        .context("drain: read batch_start")?;

    let cells: Vec<(String, i32, i32)> = {
        // GROUP BY + ORDER BY MIN(enqueued_at) (equivalent to the DISTINCT it
        // replaces, since it also collapses to one row per (source, cell_x,
        // cell_y)) drains oldest-enqueued first. Alphabetical source ordering
        // would starve later sources indefinitely under a sustained backlog.
        let mut stmt = conn.prepare(
            "SELECT source, cell_x, cell_y FROM match_dirty_cells
             WHERE enqueued_at <= ?::TIMESTAMPTZ
             GROUP BY source, cell_x, cell_y
             ORDER BY MIN(enqueued_at)
             LIMIT ?",
        )?;
        let rows = stmt.query_map(duckdb::params![batch_start, batch_size as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i32>(1)?,
                r.get::<_, i32>(2)?,
            ))
        })?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        v
    };

    let mut drained = 0u64;
    let mut failed = 0u64;
    for (source, cx, cy) in &cells {
        if is_cancelled() {
            break;
        }
        conn.execute_batch("BEGIN TRANSACTION")?;
        let res = (|| -> Result<()> {
            recompute_cell_in_txn(conn, source, *cx, *cy)?;
            conn.execute(
                "DELETE FROM match_dirty_cells
                 WHERE source = ? AND cell_x = ? AND cell_y = ? AND enqueued_at <= ?::TIMESTAMPTZ",
                duckdb::params![source, cx, cy, batch_start],
            )?;
            Ok(())
        })();
        match res {
            Ok(()) => {
                conn.execute_batch("COMMIT")?;
                drained += 1;
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                tracing::warn!(
                    source = %source,
                    cell_x = cx,
                    cell_y = cy,
                    error = %e,
                    "match_refresh: cell recompute failed, leaving it queued for retry"
                );
                failed += 1;
            }
        }
    }
    Ok(DrainStats {
        cells: drained,
        failed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);",
        )
        .unwrap();
        c
    }

    #[test]
    fn drains_up_to_batch_size_and_clears_queue() {
        let c = conn();
        // Enqueue three distinct bdot10k cells.
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,100,100,now()),
                 ('bdot10k',14,101,100,now()),
                 ('bdot10k',14,102,100,now());",
        )
        .unwrap();
        let s = drain_batch(&c, 2, &|| false).unwrap();
        assert_eq!(s.cells, 2, "batch_size caps the drain");
        let left: i64 = c
            .query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 1, "two of three cells drained");
        drain_batch(&c, 10, &|| false).unwrap();
        let left: i64 = c
            .query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0);
    }

    #[test]
    fn cell_reenqueued_after_batch_start_survives() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES ('bdot10k',14,100,100, TIMESTAMPTZ '2000-01-01');",
        )
        .unwrap();
        // A newer enqueue of the same cell, timestamped in the future relative to any batch_start now.
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES ('bdot10k',14,100,100, TIMESTAMPTZ '2999-01-01');",
        )
        .unwrap();
        drain_batch(&c, 10, &|| false).unwrap();
        // The future-timestamped duplicate must remain (its edit is not yet processed).
        let left: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE enqueued_at = TIMESTAMPTZ '2999-01-01'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 1, "a re-dirty after batch_start must not be deleted");
        // The past-dated row that WAS seen by this tick's recompute must be
        // gone -- pins the other half of the cutoff invariant.
        let past: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE enqueued_at = TIMESTAMPTZ '2000-01-01'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(past, 0, "the batch-start-visible row must be deleted");
    }

    /// The design specifies `ORDER BY enqueued_at` (oldest first), not
    /// alphabetical-by-source: under a sustained backlog, alphabetical
    /// ordering would starve every source after the first indefinitely.
    #[test]
    fn drains_oldest_enqueued_cell_first() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,102,100, TIMESTAMPTZ '2020-01-01'),
                 ('bdot10k',14,100,100, TIMESTAMPTZ '2020-01-02');",
        )
        .unwrap();
        // batch_size 1 admits only one cell -- the older enqueue, even though
        // its cell_x (102) sorts after the newer one's (100) alphabetically.
        let s = drain_batch(&c, 1, &|| false).unwrap();
        assert_eq!(s.cells, 1);
        let remaining: Vec<(i32, i32)> = {
            let mut stmt = c
                .prepare("SELECT cell_x, cell_y FROM match_dirty_cells")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            remaining,
            vec![(100, 100)],
            "the earlier-enqueued cell (102,100) must drain first"
        );
    }

    /// A cell whose recompute fails (here: an unknown source string, which
    /// `recompute_cell_in_txn` rejects outright) must not abort the rest of
    /// the batch, and must remain queued for retry rather than being deleted.
    #[test]
    fn per_cell_failure_does_not_abort_the_batch() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('unknown',14,100,100,now()),
                 ('bdot10k',14,101,100,now());",
        )
        .unwrap();
        let s = drain_batch(&c, 10, &|| false).unwrap();
        assert_eq!(s.cells, 1, "the valid cell still drains");
        assert_eq!(s.failed, 1, "the unknown-source cell is counted as failed");

        let bdot10k_left: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'bdot10k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(bdot10k_left, 0, "the drained cell's queue row is removed");

        let unknown_left: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'unknown'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            unknown_left, 1,
            "the failing cell's queue row survives for retry"
        );
    }

    /// `is_cancelled` is polled between cells: once it starts returning true,
    /// the batch stops before starting the next cell, leaving the rest queued.
    #[test]
    fn cancellation_stops_the_batch_between_cells() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,100,100,now()),
                 ('bdot10k',14,101,100,now()),
                 ('bdot10k',14,102,100,now());",
        )
        .unwrap();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        // false on the first poll (let one cell run), true from then on.
        let cancel = || calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 1;

        let s = drain_batch(&c, 10, &cancel).unwrap();
        assert_eq!(
            s.cells, 1,
            "drain stops after the first cell once cancelled"
        );
        let left: i64 = c
            .query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 2, "the remaining cells stay queued for the next tick");
    }
}
