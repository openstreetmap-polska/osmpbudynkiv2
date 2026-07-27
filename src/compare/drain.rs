use anyhow::{Context, Result};
use duckdb::Connection;

use crate::compare::incremental::recompute_cell_in_txn;

pub struct DrainStats {
    #[allow(dead_code)] // only read from #[cfg(test)] until Task 11 wires drain_batch's caller
    pub cells: u64,
}

/// Drain up to `batch_size` distinct (source, cell) whose enqueued_at is at or
/// before the batch start. Each cell: recompute + delete its queue rows under
/// the same cutoff, in one transaction. A cell re-dirtied after batch_start
/// keeps a surviving queue row for the next tick.
#[allow(dead_code)] // not yet consumed: wired up by Task 11 (match_refresh background job)
pub fn drain_batch(conn: &Connection, batch_size: usize) -> Result<DrainStats> {
    // A single wall-clock cutoff for the whole batch.
    let batch_start: String = conn
        .query_row("SELECT now()::VARCHAR", [], |r| r.get(0))
        .context("drain: read batch_start")?;

    let cells: Vec<(String, i32, i32)> = {
        let mut stmt = conn.prepare(
            "SELECT DISTINCT source, cell_x, cell_y FROM match_dirty_cells
             WHERE enqueued_at <= ?::TIMESTAMPTZ
             ORDER BY source, cell_x, cell_y
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
    for (source, cx, cy) in &cells {
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
                return Err(e);
            }
        }
    }
    Ok(DrainStats { cells: drained })
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
        c.execute_batch("CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);")
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
        let s = drain_batch(&c, 2).unwrap();
        assert_eq!(s.cells, 2, "batch_size caps the drain");
        let left: i64 = c
            .query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 1, "two of three cells drained");
        drain_batch(&c, 10).unwrap();
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
        drain_batch(&c, 10).unwrap();
        // The future-timestamped duplicate must remain (its edit is not yet processed).
        let left: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE enqueued_at = TIMESTAMPTZ '2999-01-01'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 1, "a re-dirty after batch_start must not be deleted");
    }
}
