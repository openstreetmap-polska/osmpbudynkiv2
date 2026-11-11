use anyhow::{Context, Result};
use duckdb::Connection;

use crate::compare::incremental::{recompute_cell_in_txn, recompute_cells_in_txn};
use crate::dataset::ALL_SPECS;
use crate::server::tile_dirty;
use crate::shutdown;

pub struct DrainStats {
    pub cells: u64,
    /// Cells whose recompute errored. Each is rolled back, logged, and left in
    /// the queue for retry; it does not abort the rest of the batch -- see
    /// [`drain_batch`]'s replay path.
    pub failed: u64,
    /// Queue rows discarded because their `source` is not a real dataset.
    /// Should always be zero: this codebase is the only writer of the queue.
    pub purged: u64,
}

/// Drain up to `batch_size` distinct (source, cell) whose enqueued_at is at or
/// before the batch start, oldest-enqueued first.
///
/// # A transaction per [`CELLS_PER_COMMIT`] cells, not per cell or per batch
///
/// One `BEGIN`/`COMMIT` per group of cells, not one per cell: a commit is a
/// WAL append and flush, against ~0.02s of actual work per cell or less,
/// which is the wrong ratio on the slow production disk. Neither constraint
/// that would block it holds: the only table shared with a concurrent dataset
/// refresh is
/// `match_dirty_cells` (`tile_dirty_cells` is append-vs-append), and
/// append-vs-delete-of-different-rows is not a conflict for DuckDB's
/// optimistic CC no matter how many cells the transaction spans -- see
/// `compare::drain_refresh_concurrency`, which drives this function directly
/// and is the standing evidence.
///
/// It used to be one transaction for the whole batch. That held ~123-171 MiB
/// of transaction-local memory until COMMIT on DuckDB 1.5.x (two INSERTs per
/// cell, each holding ~18-29 KiB per column; see [`CELLS_PER_COMMIT`]).
///
/// # One statement for a run of neighbouring cells
///
/// The read hands cells out a render block at a time, so a group is mostly
/// runs of neighbours. A run close enough together ([`reads_compactly`]) is
/// recomputed by one statement per table ([`drain_cells`]) instead of one per
/// cell; a lone or scattered cell keeps the per-cell statement and its cached
/// plan. Both leave the same rows, cell for cell (see
/// `incremental::build_cells_sql` for why that holds on a cell edge).
/// Measured draining one refresh-like queue of 18,065 cells on the national
/// database under production's limits: 6m 14s -> 2m 28s, same output.
///
/// Two things the per-cell transaction used to buy are kept, more cheaply:
///
/// - **Cancellation commits rather than rolling back.** Each unit of work --
///   a cell, or a run recomputed together -- is paired with its own queue
///   delete, so a transaction is a grouping of independently valid units and
///   committing at any unit boundary is correct. That is also what makes
///   committing every [`CELLS_PER_COMMIT`] cells correct. `is_cancelled` is
///   polled between units; on a stop, what is finished is committed. That is
///   strictly better than abandoning the in-flight unit.
/// - **A poison cell is isolated by replay.** If a group's transaction fails,
///   it rolls back and that group's cells are re-run one at a time, so exactly
///   the failing cell is warned about and left queued while the rest still
///   drain. Groups already committed are not touched again, and the groups
///   after it run normally. The cost is a second pass over one group, paid
///   only when something actually failed.
///
/// # The cutoff
///
/// `batch_start` is used for *both* the read and the paired queue-delete, and
/// both sides must use that same stored value rather than `now()`: a cell
/// re-dirtied after `batch_start` must survive the delete (its edit was not
/// seen by this tick) and be picked up by the next one.
pub fn drain_batch(
    conn: &Connection,
    batch_size: usize,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<DrainStats> {
    // A single wall-clock cutoff for the whole batch.
    let batch_start: String = conn
        .query_row("SELECT now()::VARCHAR", [], |r| r.get(0))
        .context("drain: read batch_start")?;

    // Validity is `dataset::spec_by_name`'s question, so the list comes from
    // `ALL_SPECS` rather than three strings typed here.
    let source_list = ALL_SPECS
        .iter()
        .map(|spec| format!("'{}'", spec.name))
        .collect::<Vec<_>>()
        .join(", ");

    // A row naming a source that does not exist can never be recomputed, so
    // filtering it out of the read is not enough -- it would sit in the table
    // forever, inflating /status's queue depth with work nothing will ever do.
    // Purge it under the same cutoff instead. This codebase is the only writer
    // of the queue, so this is a backstop, not a live failure mode; a non-zero
    // count means something is wrong upstream and the warning says so.
    let purged = conn
        .execute(
            &format!(
                "DELETE FROM match_dirty_cells
                 WHERE enqueued_at <= ?::TIMESTAMPTZ AND source NOT IN ({source_list})"
            ),
            duckdb::params![batch_start],
        )
        .context("drain: purge unknown-source queue rows")? as u64;
    if purged > 0 {
        tracing::warn!(
            rows = purged,
            "match_refresh: discarded queue rows naming an unknown source -- \
             this codebase is the only writer, so something wrote them wrong"
        );
    }

    let cells: Vec<(String, i32, i32)> = {
        // GROUP BY + ORDER BY MIN(enqueued_at) (equivalent to the DISTINCT it
        // replaces, since it also collapses to one row per (source, cell_x,
        // cell_y)) drains oldest-enqueued first. Alphabetical source ordering
        // would starve later sources indefinitely under a sustained backlog,
        // which is why `source` only breaks ties.
        //
        // Within one timestamp -- every cell of a dataset refresh has the
        // same one -- cells are taken a render block at a time. Each group of
        // `CELLS_PER_COMMIT` enqueues its tile cells under its own timestamp,
        // so this is what makes `tile_refresh` see them as compact runs it
        // can dedup and batch; see `tile_dirty::block_order_sql`.
        let mut stmt = conn.prepare(&format!(
            "SELECT source, cell_x, cell_y FROM match_dirty_cells
             WHERE enqueued_at <= ?::TIMESTAMPTZ AND source IN ({source_list})
             GROUP BY source, cell_x, cell_y
             ORDER BY MIN(enqueued_at), source, {block_order}
             LIMIT ?",
            block_order = tile_dirty::block_order_sql("cell_x", "cell_y"),
        ))?;
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
    for group in cells.chunks(CELLS_PER_COMMIT) {
        let outcome = drain_group(conn, group, &batch_start, is_cancelled)?;
        drained += outcome.drained;
        failed += outcome.failed;
        if outcome.cancelled {
            break;
        }
    }
    Ok(DrainStats {
        cells: drained,
        failed,
        purged,
    })
}

/// Cells per transaction in [`drain_batch`].
///
/// Each cell's recompute is two INSERT statements (`<source>_unmatched`, 9-15
/// columns, and `cell_totals`, 4), and on DuckDB 1.5.x every INSERT inside a
/// transaction holds ~18-29 KiB per column until COMMIT
/// (`docs/duckdb_per_statement_insert_memory.md`). Measured on a copy of the
/// national database on 1.5.x: 512 cells in one transaction held 171 MiB
/// (bdot10k), 123 MiB (egib) and 143 MiB (prg) of `IN_MEMORY_TABLE`; 64 cells
/// hold an eighth of that. DuckDB 2.0 no longer holds that memory, so the
/// bound now matters for a different reason: a failed group replays cell by
/// cell, and 64 caps the work one failure rolls back and redoes. It is also
/// one render block's worth of cells, so a fully dirty block is usually one
/// multi-cell statement and one commit.
const CELLS_PER_COMMIT: usize = 64;

struct GroupOutcome {
    drained: u64,
    failed: u64,
    cancelled: bool,
}

/// One group of [`drain_batch`]: one transaction, with the tile enqueue for
/// exactly the cells it recomputed, so a committed recompute can never lose
/// its tile enqueue. On failure, replays just this group one cell at a time.
fn drain_group(
    conn: &Connection,
    group: &[(String, i32, i32)],
    batch_start: &str,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<GroupOutcome> {
    conn.execute_batch("BEGIN TRANSACTION")?;
    let mut cancelled = false;
    let result = (|| -> Result<Vec<(i32, i32)>> {
        let mut done: Vec<(i32, i32)> = Vec::with_capacity(group.len());
        'runs: for run in group.chunk_by(|a, b| a.0 == b.0 && render_block(a) == render_block(b)) {
            if reads_compactly(run) {
                if is_cancelled() {
                    cancelled = true;
                    break;
                }
                let cells: Vec<(i32, i32)> = run.iter().map(|(_, cx, cy)| (*cx, *cy)).collect();
                drain_cells(conn, &run[0].0, &cells, batch_start)?;
                done.extend(cells);
                continue;
            }
            for (source, cx, cy) in run {
                if is_cancelled() {
                    cancelled = true;
                    break 'runs;
                }
                drain_one_cell(conn, source, *cx, *cy, batch_start)?;
                done.push((*cx, *cy));
            }
        }
        enqueue_tiles_for_cells(conn, &done)?;
        Ok(done)
    })();

    match result {
        Ok(done) => {
            conn.execute_batch("COMMIT")?;
            Ok(GroupOutcome {
                drained: done.len() as u64,
                failed: 0,
                cancelled,
            })
        }
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                let rb = rb.to_string();
                // Not a poison cell: every later statement fails the same way,
                // so a replay isolates nothing. The cells never committed and
                // are still queued; the error carries them so a crash can be
                // reproduced, and the marker so the server restarts.
                if shutdown::is_database_invalidated(&rb) {
                    return Err(e.context(format!(
                        "match_refresh: a batch of {} cells left DuckDB invalidated \
                         (rollback: {rb}); cells: {}",
                        group.len(),
                        cell_list(group)
                    )));
                }
                tracing::warn!(error = %rb, "match_refresh: failed to roll back failed batch");
            }
            tracing::warn!(
                error = %e,
                cell_count = group.len(),
                cells = %cell_list(group),
                "match_refresh: batch failed, replaying it one cell at a time to isolate the cause"
            );
            let (drained, failed) = replay_one_at_a_time(conn, group, batch_start, is_cancelled);
            Ok(GroupOutcome {
                drained,
                failed,
                cancelled: is_cancelled(),
            })
        }
    }
}

/// The render block a queued cell lies in. The queue read hands cells out a
/// block at a time (`tile_dirty::block_order_sql`), so cells sharing one are
/// adjacent in a group.
fn render_block(cell: &(String, i32, i32)) -> (i32, i32) {
    let shift = crate::server::tiles::RENDER_BLOCK_SHIFT;
    (cell.1 >> shift, cell.2 >> shift)
}

/// How many cells of bounding box a run may read per cell it recomputes and
/// still go through [`drain_cells`]. One statement reads the whole bounding
/// box of its cells, at ~2.3 ms a cell on the national tables, where a cell
/// on its own costs ~14 ms; that breaks even near six, and four keeps a
/// margin for dense areas.
const MAX_CELLS_READ_PER_CELL: usize = 4;

/// Whether a run of cells in one render block is worth one statement: more
/// than one cell, and close enough together that reading their bounding box
/// is cheaper than recomputing them one at a time. Two cells in opposite
/// corners of a block are not.
fn reads_compactly(run: &[(String, i32, i32)]) -> bool {
    if run.len() < 2 {
        return false;
    }
    let span = |pick: fn(&(String, i32, i32)) -> i32| {
        let min = run.iter().map(pick).min().unwrap_or(0);
        let max = run.iter().map(pick).max().unwrap_or(0);
        (max - min + 1) as usize
    };
    span(|c| c.1) * span(|c| c.2) <= run.len() * MAX_CELLS_READ_PER_CELL
}

/// Recompute a run of one source's cells with one statement per table, then
/// delete their queue rows under the batch cutoff: [`drain_one_cell`] for
/// several cells at once, leaving the same rows. Assumes an open transaction.
fn drain_cells(
    conn: &Connection,
    source: &str,
    cells: &[(i32, i32)],
    batch_start: &str,
) -> Result<()> {
    recompute_cells_in_txn(conn, source, cells)?;
    // One constant text for any number of cells, so the plan is cached like
    // every other statement of a drain: the cells arrive as two lists.
    let list = |pick: fn(&(i32, i32)) -> i32| {
        let items: Vec<String> = cells.iter().map(|c| pick(c).to_string()).collect();
        format!("[{}]", items.join(","))
    };
    conn.prepare_cached(
        "DELETE FROM match_dirty_cells
         USING (SELECT unnest($3::INTEGER[]) AS cx, unnest($4::INTEGER[]) AS cy) d
         WHERE source = $1 AND cell_x = d.cx AND cell_y = d.cy
           AND enqueued_at <= $2::TIMESTAMPTZ",
    )?
    .execute(duckdb::params![
        source,
        batch_start,
        list(|c| c.0),
        list(|c| c.1)
    ])?;
    Ok(())
}

/// `source/cell_x/cell_y` per cell, space-separated: enough to re-queue and
/// replay a failed group by hand.
fn cell_list(group: &[(String, i32, i32)]) -> String {
    group
        .iter()
        .map(|(source, cx, cy)| format!("{source}/{cx}/{cy}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Recompute one cell, delete its queue rows under the batch cutoff. Assumes
/// an open transaction; the tile enqueue is the caller's, so both the batch
/// and the replay path spell it exactly once (see [`enqueue_tiles_for_cells`]).
fn drain_one_cell(
    conn: &Connection,
    source: &str,
    cell_x: i32,
    cell_y: i32,
    batch_start: &str,
) -> Result<()> {
    recompute_cell_in_txn(conn, source, cell_x, cell_y)?;
    conn.prepare_cached(
        "DELETE FROM match_dirty_cells
         WHERE source = ? AND cell_x = ? AND cell_y = ? AND enqueued_at <= ?::TIMESTAMPTZ",
    )?
    .execute(duckdb::params![source, cell_x, cell_y, batch_start])?;
    Ok(())
}

/// The tile half of a drain, in the same transaction as the recompute that
/// earned it.
///
/// Same transaction is not optional: a committed recompute whose tile enqueue
/// was lost is a permanently stale tile, and no read-time check remains to
/// catch it. One statement for the whole set rather than one per cell, and one
/// home for both the batch and replay paths.
fn enqueue_tiles_for_cells(conn: &Connection, cells: &[(i32, i32)]) -> Result<()> {
    if cells.is_empty() {
        return Ok(());
    }
    // Literals rather than bound parameters: these are `i32`s read out of the
    // queue moments ago, and 512 cells would otherwise mean 1024 binds.
    let values = cells
        .iter()
        .map(|(x, y)| format!("({x},{y})"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&tile_dirty::enqueue_from_select_sql(
        "cx",
        "cy",
        &format!("FROM (VALUES {values}) t(cx, cy)"),
    ))
    .context("drain: enqueue dirty tiles")
}

/// The failure path: re-run a failed group's cells one transaction at a time,
/// so exactly the cell that cannot be recomputed is left queued and warned
/// about while every other cell still drains.
fn replay_one_at_a_time(
    conn: &Connection,
    cells: &[(String, i32, i32)],
    batch_start: &str,
    is_cancelled: &dyn Fn() -> bool,
) -> (u64, u64) {
    let mut drained = 0u64;
    let mut failed = 0u64;
    for (source, cx, cy) in cells {
        if is_cancelled() {
            break;
        }
        if conn.execute_batch("BEGIN TRANSACTION").is_err() {
            break;
        }
        let res = drain_one_cell(conn, source, *cx, *cy, batch_start)
            .and_then(|()| enqueue_tiles_for_cells(conn, &[(*cx, *cy)]));
        match res {
            Ok(()) => match conn.execute_batch("COMMIT") {
                Ok(()) => drained += 1,
                Err(e) => {
                    tracing::warn!(
                        source = %source,
                        cell_x = cx,
                        cell_y = cy,
                        error = %e,
                        "match_refresh: cell commit failed"
                    );
                    failed += 1;
                }
            },
            Err(e) => {
                if let Err(rb) = conn.execute_batch("ROLLBACK") {
                    tracing::warn!(
                        source = %source,
                        cell_x = cx,
                        cell_y = cy,
                        error = %rb,
                        "match_refresh: failed to roll back failed cell"
                    );
                }
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
    (drained, failed)
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
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);",
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

    /// Within one timestamp the drain takes cells a render block at a time,
    /// which is what makes the tile cells each commit enqueues a compact run
    /// `tile_refresh` can dedup and batch. Two z14 blocks interleaved in the
    /// INSERT; a batch sized to one block must drain exactly one of them.
    #[test]
    fn cells_sharing_a_timestamp_drain_one_render_block_at_a_time() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells
             SELECT 'bdot10k', 14, CASE WHEN b = 0 THEN 4096 ELSE 7200 END + x, 8192 + y,
                    TIMESTAMPTZ '2020-01-01'
             FROM range(8) t(x), range(8) u(y), range(2) v(b)
             ORDER BY hash(x, y, b);",
        )
        .unwrap();

        let s = drain_batch(&c, 64, &|| false).unwrap();
        assert_eq!(s.cells, 64);

        let left: Vec<i32> = {
            let mut stmt = c
                .prepare("SELECT DISTINCT cell_x >> 3 FROM match_dirty_cells")
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            left,
            vec![900],
            "the batch must drain the whole first block and leave the other whole"
        );
    }

    /// An unknown source can never be recomputed, so it is discarded rather
    /// than retried forever -- filtering it out of the read alone would leave
    /// it inflating the queue depth with work nothing will ever do.
    #[test]
    fn a_queue_row_with_an_unknown_source_is_purged_rather_than_retried_forever() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('unknown',14,100,100,now()),
                 ('bdot10k',14,101,100,now());",
        )
        .unwrap();
        let s = drain_batch(&c, 10, &|| false).unwrap();
        assert_eq!(s.cells, 1, "the valid cell still drains");
        assert_eq!(
            s.failed, 0,
            "an unrecomputable row is not a failure to retry"
        );
        assert_eq!(s.purged, 1, "it is discarded instead");

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
            unknown_left, 0,
            "the unknown-source row is gone, not left to be retried forever"
        );
    }

    /// The tile queue is fed from the same transaction as the recompute: a
    /// committed recompute whose tile enqueue was lost would be a permanently
    /// stale tile, with no read-time check left to catch it.
    #[test]
    fn a_drained_cell_enqueues_its_tile_cell_in_the_same_transaction() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,100,100,now()),
                 ('bdot10k',14,101,100,now());",
        )
        .unwrap();
        drain_batch(&c, 10, &|| false).unwrap();

        let mut stmt = c
            .prepare("SELECT cell_x, cell_y FROM tile_dirty_cells ORDER BY cell_x")
            .unwrap();
        let rows: Vec<(i32, i32)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![(100, 100), (101, 100)],
            "each drained cell must land in the tile queue exactly once"
        );
    }

    /// A batch that hits a genuinely unrecomputable cell must not strand the
    /// rest of the batch behind it. The replay path exists for exactly this,
    /// and it is what makes the batch-wide transaction safe.
    #[test]
    fn a_failing_cell_is_isolated_by_replaying_the_batch_one_cell_at_a_time() {
        let c = conn();
        // egib_buildings is absent from this fixture, so any egib cell fails
        // to recompute while bdot10k cells succeed.
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,100,100,now()),
                 ('egib',14,200,200,now()),
                 ('bdot10k',14,101,100,now());",
        )
        .unwrap();

        let s = drain_batch(&c, 10, &|| false).unwrap();
        assert_eq!(s.cells, 2, "both healthy cells still drain");
        assert_eq!(s.failed, 1, "exactly the poison cell is counted failed");

        let egib_left: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'egib'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(egib_left, 1, "the failing cell stays queued for retry");
        let bdot_left: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'bdot10k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(bdot_left, 0, "the healthy cells drained despite it");
    }

    /// Cancellation **commits** what it finished rather than rolling it back:
    /// each cell carries its own paired queue delete, so the transaction is a
    /// grouping of independently valid units and stopping at a cell boundary
    /// is correct. A rollback here would redo that work on the next tick, and
    /// on every shutdown. The cells are in three render blocks, so each is a
    /// unit of its own; neighbours would be one statement and one unit.
    #[test]
    fn a_cancelled_batch_commits_the_cells_it_already_finished() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,100,100,now()),
                 ('bdot10k',14,200,100,now()),
                 ('bdot10k',14,300,100,now());",
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
        let tiles: i64 = c
            .query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            tiles, 1,
            "the finished cell's tile enqueue must have committed with it"
        );
    }

    /// A batch spanning several transactions: a poison cell in the *second*
    /// group is isolated by replaying that group alone. The first group was
    /// already committed and must be neither replayed nor counted twice, which
    /// would show as a second tile enqueue per cell; and the batch does not
    /// stop at the failure.
    #[test]
    fn a_failing_cell_in_a_later_group_replays_only_that_group() {
        let c = conn();
        let healthy = CELLS_PER_COMMIT as i32 + 2;
        c.execute_batch(&format!(
            "INSERT INTO match_dirty_cells
                 SELECT 'bdot10k', 14, 100 + i, 100, now() - INTERVAL 1 HOUR
                 FROM range({healthy}) t(i);
             INSERT INTO match_dirty_cells VALUES
                 ('egib', 14, 200, 200, now() - INTERVAL 1 MINUTE);"
        ))
        .unwrap();

        let s = drain_batch(&c, 1000, &|| false).unwrap();
        assert_eq!((s.cells, s.failed), (healthy as u64, 1));
        let left: Vec<String> = c
            .prepare("SELECT source FROM match_dirty_cells")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<duckdb::Result<_>>()
            .unwrap();
        assert_eq!(
            left,
            vec!["egib".to_string()],
            "only the poison cell stays queued"
        );
        let tiles: i64 = c
            .query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tiles, healthy as i64, "one tile enqueue per drained cell");
    }

    fn cell(x: i32, y: i32) -> (String, i32, i32) {
        ("bdot10k".to_string(), x, y)
    }

    /// One statement reads the bounding box of its cells, so it only pays for
    /// cells that are close together.
    #[test]
    fn only_cells_close_together_share_a_statement() {
        assert!(
            !reads_compactly(&[cell(96, 96)]),
            "one cell has its own cached statement"
        );
        assert!(reads_compactly(&[cell(96, 96), cell(97, 96)]));
        let row: Vec<_> = (96..104).map(|x| cell(x, 96)).collect();
        assert!(reads_compactly(&row));
        let block: Vec<_> = (96..104)
            .flat_map(|y| (96..104).map(move |x| cell(x, y)))
            .collect();
        assert!(reads_compactly(&block));
        assert!(
            !reads_compactly(&[cell(96, 96), cell(103, 103)]),
            "two corners of a block would read all 64 cells between them"
        );
    }

    /// Two unmatched buildings in neighbouring cells of one render block, one
    /// matched building beside them, and one building in a far cell.
    fn seed_neighbours(c: &Connection) -> [(i32, i32); 3] {
        use crate::tile_math::{CHANGE_CELL_ZOOM, tile_to_bbox};
        let (cx, cy) = (9144u32, 5408u32);
        let (west, south, _, north) = tile_to_bbox(CHANGE_CELL_ZOOM, cx, cy);
        let (far_west, ..) = tile_to_bbox(CHANGE_CELL_ZOOM, cx + 40, cy);
        let mid = (south + north) / 2.0;
        let step = 360.0 / 16384.0;
        c.execute_batch(&format!(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('a',       ST_MakeEnvelope({west} + 0.001, {mid}, {west} + 0.0012, {mid} + 0.0002)),
                 ('matched', ST_MakeEnvelope({west} + 0.003, {mid}, {west} + 0.0032, {mid} + 0.0002)),
                 ('b',       ST_MakeEnvelope({west} + {step} + 0.001, {mid}, {west} + {step} + 0.0012, {mid} + 0.0002)),
                 ('far',     ST_MakeEnvelope({far_west} + 0.001, {mid}, {far_west} + 0.0012, {mid} + 0.0002));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope({west} + 0.0029, {mid}, {west} + 0.0033, {mid} + 0.0002));"
        ))
        .unwrap();
        [
            (cx as i32, cy as i32),
            (cx as i32 + 1, cy as i32),
            (cx as i32 + 40, cy as i32),
        ]
    }

    fn served(c: &Connection) -> Vec<String> {
        let mut out = Vec::new();
        for sql in [
            "SELECT x::VARCHAR FROM (SELECT * EXCLUDE (computed_at) FROM bdot10k_unmatched) x ORDER BY 1",
            "SELECT t::VARCHAR FROM cell_totals t ORDER BY 1",
        ] {
            let mut stmt = c.prepare(sql).unwrap();
            out.extend(
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .map(|r| r.unwrap()),
            );
        }
        out
    }

    /// Neighbouring cells are recomputed by one statement, and the drain must
    /// leave what it left when it took every cell on its own: the same rows,
    /// an empty queue, and one tile enqueue per cell.
    #[test]
    fn neighbouring_cells_drain_to_the_same_rows_as_one_cell_at_a_time() {
        let one_at_a_time = conn();
        let cells = seed_neighbours(&one_at_a_time);
        one_at_a_time.execute_batch("BEGIN TRANSACTION").unwrap();
        for (x, y) in cells {
            recompute_cell_in_txn(&one_at_a_time, "bdot10k", x, y).unwrap();
        }
        one_at_a_time.execute_batch("COMMIT").unwrap();

        let c = conn();
        seed_neighbours(&c);
        for (x, y) in cells {
            c.execute_batch(&format!(
                "INSERT INTO match_dirty_cells VALUES ('bdot10k', 14, {x}, {y}, now());"
            ))
            .unwrap();
        }
        let queued: Vec<_> = cells.iter().map(|&(x, y)| cell(x, y)).collect();
        assert!(
            reads_compactly(&queued[..2]) && render_block(&queued[0]) == render_block(&queued[1]),
            "the fixture's first two cells must take the one-statement path"
        );

        let s = drain_batch(&c, 10, &|| false).unwrap();
        assert_eq!((s.cells, s.failed), (3, 0));
        assert_eq!(served(&c), served(&one_at_a_time));
        assert_eq!(served(&c).len(), 6, "three unmatched rows and three totals");
        let (left, tiles): (i64, i64) = c
            .query_row(
                "SELECT (SELECT COUNT(*) FROM match_dirty_cells),
                        (SELECT COUNT(*) FROM tile_dirty_cells)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((left, tiles), (0, 3));
    }

    /// A run that fails as one statement names no cell, so the replay has to
    /// take it apart: each cell of it is retried alone, counted and left
    /// queued, and the healthy run beside it still drains.
    #[test]
    fn a_failing_run_of_neighbours_is_replayed_one_cell_at_a_time() {
        let c = conn();
        // egib_buildings is absent from this fixture, so every egib cell fails.
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,96,96,now()),
                 ('bdot10k',14,97,96,now()),
                 ('egib',14,96,96,now()),
                 ('egib',14,97,96,now());",
        )
        .unwrap();

        let s = drain_batch(&c, 10, &|| false).unwrap();
        assert_eq!((s.cells, s.failed), (2, 2));
        let left: Vec<String> = c
            .prepare("SELECT DISTINCT source FROM match_dirty_cells")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<duckdb::Result<_>>()
            .unwrap();
        assert_eq!(left, vec!["egib".to_string()]);
    }
}
