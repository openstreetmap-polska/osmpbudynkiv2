//! The tile-invalidation queue, and the one home for turning a changed z14
//! cell into the tiles that render it.
//!
//! # The rule
//!
//! > A site enqueues `tile_dirty_cells` iff it changes **what a tile
//! > renders**. It enqueues `match_dirty_cells` iff it changes **which objects
//! > are unmatched**.
//!
//! Most producers do both. Two do exactly one, and they are the test of the
//! rule: `mappings::building_types` is serving-time only, so it dirties tiles
//! without dirtying matches; `update::dirty_cells` (OSM) reaches a tile only
//! through the drain, so it dirties matches without dirtying tiles. See
//! CLAUDE.md for the full producer table.
//!
//! Producers enqueue the **bare changed cell** -- the same `(cell_x, cell_y)`
//! they already write to `match_dirty_cells`. Expanding a cell into tile keys
//! is [`tiles_for_cell`]'s job and nothing else's, so no producer has to know
//! about rings or parents, and the queue stays an eleventh the size a
//! pre-expanded one would be.
//!
//! # Why the expansion is per-tier
//!
//! The two persisted tiers select their rows in fundamentally different ways,
//! which is easy to miss because they read the same tables:
//!
//! - **z14 selects by geometry** (`ST_Intersects(geom, envelope)` in
//!   `tiles`' four layer queries), while rows are *tagged* with the cell of
//!   their representative point. So a z14 tile renders rows its neighbours
//!   own, and a changed cell dirties the **3x3 ring** of z14 tiles.
//! - **z12..=z13 selects by cell range** (`cell_x BETWEEN ? AND ?` in
//!   `points_mvt_sql`). A tile there is an *exact* function of the z14 cells
//!   beneath it, so a changed cell dirties exactly **one tile per zoom** --
//!   its parent, a pure bit shift.
//!
//! Getting this backwards in either direction is silent: a ring at z12 would
//! be wasted work, and a parent-only expansion at z14 would leave the eight
//! neighbours permanently stale.
//!
//! z5..=z11 appears nowhere here. That tier's content moves with the wall
//! clock (`dataset_change_areas` is read through a `now() - max_age_days`
//! bound), so there is no invalidation signal to push and it gets a TTL cache
//! instead -- see `server::tile_cache`.

#[cfg(test)]
use anyhow::{Context, Result};
#[cfg(test)]
use duckdb::Connection;

use crate::server::tile_store::TileKey;
use crate::tile_math::CHANGE_CELL_ZOOM;

/// A z14 tile renders rows tagged to the cells one step away, so a change
/// dirties the 3x3 ring centred on it.
///
/// Radius 1 is **exact, not a safety margin**: `dataset::filter_oversized_geometry`
/// drops any row whose bbox spans a full cell in either axis, so a surviving
/// row's reach from its own representative point's cell is `<= 1` by
/// construction. This is the ONE home for that number -- narrowing it breaks
/// the invariant, and widening it is pure waste.
const RING_RADIUS: i32 = 1;

/// Lowest zoom served from the persistent store. Below this, tiles are
/// RAM-cached with a TTL and are not invalidated at all.
pub const MIN_PERSISTED_ZOOM: u32 = 12;

/// Every tile key a change to this z14 cell can affect: the 3x3 ring at
/// [`CHANGE_CELL_ZOOM`], then one parent per zoom down to
/// [`MIN_PERSISTED_ZOOM`].
///
/// Negative or out-of-range ring members (a cell on the antimeridian or at a
/// pole) are dropped rather than wrapped: there is no tile there to
/// invalidate.
pub fn tiles_for_cell(cell_x: i32, cell_y: i32) -> Vec<TileKey> {
    let mut keys = Vec::with_capacity(9 + (CHANGE_CELL_ZOOM - MIN_PERSISTED_ZOOM) as usize);
    let max = 1i64 << CHANGE_CELL_ZOOM;
    for dy in -RING_RADIUS..=RING_RADIUS {
        for dx in -RING_RADIUS..=RING_RADIUS {
            let x = cell_x as i64 + dx as i64;
            let y = cell_y as i64 + dy as i64;
            if x < 0 || y < 0 || x >= max || y >= max {
                continue;
            }
            keys.push((CHANGE_CELL_ZOOM, x as u32, y as u32));
        }
    }
    // Parents of the cell itself, NOT of the ring: z12/z13 filter by cell
    // range, so only the tile actually containing this cell can change.
    if cell_x >= 0 && cell_y >= 0 && (cell_x as i64) < max && (cell_y as i64) < max {
        for z in MIN_PERSISTED_ZOOM..CHANGE_CELL_ZOOM {
            let shift = CHANGE_CELL_ZOOM - z;
            keys.push((z, cell_x as u32 >> shift, cell_y as u32 >> shift));
        }
    }
    keys
}

/// Enqueue one changed cell.
///
/// Test-only: every real producer already holds a *set* of changed cells (a
/// diff, a mapping delta, a drained batch) and goes through
/// [`enqueue_from_select_sql`], which is one statement rather than one per
/// cell. Kept because it is what lets a test seed the queue without building a
/// source table to select from.
#[cfg(test)]
pub fn enqueue(conn: &Connection, cell_x: i32, cell_y: i32) -> Result<()> {
    conn.execute(
        "INSERT INTO tile_dirty_cells (cell_x, cell_y, enqueued_at) VALUES (?, ?, now())",
        duckdb::params![cell_x, cell_y],
    )
    .context("enqueue tile dirty cell")?;
    Ok(())
}

/// The same insert as a SQL fragment, for set-based producers (the dataset
/// refresh's delta, both mapping deltas, the drain's batch).
///
/// `from_sql` is a complete `FROM ...` clause (optionally with `WHERE`), and
/// the two expressions project the cell out of it. `DISTINCT` here rather than
/// at read time keeps the queue small at the source -- a refresh touching one
/// building enqueues one row, not one per changed column.
pub fn enqueue_from_select_sql(cell_x_expr: &str, cell_y_expr: &str, from_sql: &str) -> String {
    format!(
        "INSERT INTO tile_dirty_cells (cell_x, cell_y, enqueued_at)
         SELECT DISTINCT {cell_x_expr}, {cell_y_expr}, now() {from_sql}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn conn() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        crate::db::init_db(std::path::Path::new(":memory:"), &init, None).unwrap()
    }

    fn keys(cell_x: i32, cell_y: i32) -> BTreeSet<TileKey> {
        tiles_for_cell(cell_x, cell_y).into_iter().collect()
    }

    #[test]
    fn a_changed_cell_dirties_the_nine_z14_tiles_around_it_including_the_corners() {
        let k = keys(100, 200);
        for dx in -1..=1 {
            for dy in -1..=1 {
                assert!(
                    k.contains(&(14, (100 + dx) as u32, (200 + dy) as u32)),
                    "missing z14 ring member ({dx}, {dy})"
                );
            }
        }
        assert_eq!(
            k.iter().filter(|(z, _, _)| *z == 14).count(),
            9,
            "the ring is exactly 3x3 -- no more, no less"
        );
    }

    /// The asymmetry that makes the per-tier expansion necessary: z12/z13
    /// filter by cell range, so only the tile actually containing the changed
    /// cell can change. A ring there would be wasted work.
    #[test]
    fn a_z12_or_z13_tile_gets_only_its_parent_never_a_ring() {
        let k = keys(100, 200);
        assert_eq!(
            k.iter().filter(|(z, _, _)| *z == 13).count(),
            1,
            "z13 gets exactly one tile"
        );
        assert_eq!(
            k.iter().filter(|(z, _, _)| *z == 12).count(),
            1,
            "z12 gets exactly one tile"
        );
        assert!(k.contains(&(13, 50, 100)), "100 >> 1 = 50, 200 >> 1 = 100");
        assert!(k.contains(&(12, 25, 50)), "100 >> 2 = 25, 200 >> 2 = 50");
    }

    /// The parent shift here must be the exact inverse of the cell range
    /// `tiles::serve_tile_agg`/`serve_tile_points` derive from a tile, or a
    /// tile would be invalidated that does not contain the cell (or worse,
    /// one that does would not be).
    #[test]
    fn the_parent_shift_inverts_the_cell_range_the_serving_path_derives() {
        for (cell_x, cell_y) in [(0, 0), (1, 1), (100, 200), (16383, 16383), (8191, 4096)] {
            for (z, px, py) in tiles_for_cell(cell_x, cell_y)
                .into_iter()
                .filter(|(z, _, _)| *z < CHANGE_CELL_ZOOM)
            {
                // serve_tile_points' arithmetic, verbatim.
                let cell_shift = CHANGE_CELL_ZOOM - z;
                let lo_x = (px << cell_shift) as i32;
                let hi_x = (((px + 1) << cell_shift) - 1) as i32;
                let lo_y = (py << cell_shift) as i32;
                let hi_y = (((py + 1) << cell_shift) - 1) as i32;
                assert!(
                    (lo_x..=hi_x).contains(&cell_x) && (lo_y..=hi_y).contains(&cell_y),
                    "z{z} tile ({px},{py}) covers {lo_x}..={hi_x} x {lo_y}..={hi_y}, \
                     which does not contain the cell ({cell_x},{cell_y}) that produced it"
                );
            }
        }
    }

    #[test]
    fn a_cell_at_the_edge_of_the_world_drops_its_out_of_range_neighbours() {
        let k = keys(0, 0);
        assert!(k.contains(&(14, 0, 0)));
        assert!(k.contains(&(14, 1, 1)));
        assert_eq!(
            k.iter().filter(|(z, _, _)| *z == 14).count(),
            4,
            "only the in-range quadrant of the ring exists at the origin"
        );
    }

    #[test]
    fn enqueue_writes_one_row_per_call() {
        let conn = conn();
        enqueue(&conn, 100, 200).unwrap();
        enqueue(&conn, 100, 200).unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2, "dedup is the reader's job, not the writer's");
        let (x, y): (i32, i32) = conn
            .query_row(
                "SELECT cell_x, cell_y FROM tile_dirty_cells LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((x, y), (100, 200));
    }

    /// The SQL and Rust enqueue paths must land the same cells -- the
    /// `matched_key_sql_agrees_with_key_of` pattern.
    #[test]
    fn the_set_based_enqueue_writes_the_same_cells_as_the_row_based_one() {
        let conn = conn();
        conn.execute_batch(
            "CREATE TABLE src (cx INTEGER, cy INTEGER);
             INSERT INTO src VALUES (100, 200), (100, 200), (101, 201);",
        )
        .unwrap();
        conn.execute_batch(&enqueue_from_select_sql("cx", "cy", "FROM src"))
            .unwrap();

        let mut stmt = conn
            .prepare("SELECT cell_x, cell_y FROM tile_dirty_cells ORDER BY cell_x, cell_y")
            .unwrap();
        let rows: Vec<(i32, i32)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![(100, 200), (101, 201)],
            "the set-based path must DISTINCT its input"
        );
    }
}
