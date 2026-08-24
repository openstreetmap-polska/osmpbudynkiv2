//! A cheap, legible version string for a served z14 tile, plus the global
//! "serving epoch" counter it is built from.
//!
//! A later phase turns [`z14_tile_version`]'s output into an `ETag` on z14
//! `/tiles` responses -- this module only builds and tests the primitive.
//! **No axum types here and no HTTP wiring**; see `src/server/` for that.
//!
//! # The global epoch is doing real work, not belt-and-braces
//!
//! [`z14_tile_version`] combines two kinds of coverage:
//!
//! - **Per-cell**, from the `*_unmatched` serving tables' own `computed_at`
//!   (see [`z14_tile_version`]'s doc for why that has to be a per-table
//!   `(count, max)` pair, not one merged max).
//! - **Global**, from [`SERVING_EPOCH_KEY`] in `metadata`.
//!
//! The global half is load-bearing, not a safety margin, because several
//! things that change what a tile renders touch *no* dirty cell and *no*
//! `*_unmatched` row at all:
//!
//! - The two `*_building_types` tables
//!   (`bdot10k_building_types`/`egib_building_types`) change tile `tags`
//!   purely at serve time -- see the CLAUDE.md gotcha on building-type
//!   mapping. Loading a new building-type file recomputes nothing.
//! - `street_name_mappings` is a **partial** case, and the distinction
//!   matters. It used to belong in the bullet above, but the address match
//!   rule now resolves PRG's street name through it (`compare::rule`'s rule
//!   B), so a mapping edit *can* change which addresses are unmatched. Its
//!   loader therefore enqueues dirty cells for the addresses it can flip, and
//!   the drain moves their `computed_at` — per-cell coverage, eventually. The
//!   epoch bump in that loader is still required rather than superseded: it
//!   covers the window before those cells drain (an undrained cell serves the
//!   old match decision with the new serve-time `addr:street`), plus the
//!   `addresses_all` layer and the z5-z13 tiers, which no per-cell version
//!   reaches at all.
//! - The `addresses_all`/`buildings_all` legend layers read the raw
//!   `prg_addresses`/`bdot10k_buildings`/`egib_buildings` tables directly,
//!   never the `*_unmatched` serving tables, so per-cell `computed_at` never
//!   moves for them either.
//! - The adjacency `nb` CTEs (`server/package.rs`, `server/tiles.rs`) read
//!   raw buildings up to `ADJACENCY_READ_BUFFER_DEG` (~35-55 m) *outside*
//!   the tile/cell being served. A per-cell version, however finely grained,
//!   could never cover a write to a neighbouring cell's raw row even in
//!   principle -- there is no cell-local signal to attach it to.
//!
//! The only writers of those raw tables are `import` and
//! `update::dataset::refresh` -- both already bump sites below -- which is
//! what makes epoch-only coverage sound for them: every write that matters
//! for tiles and isn't covered by a per-cell version goes through one of
//! those two paths, and both bump the epoch.
//!
//! # Bump sites
//!
//! Module-doc rule: **bump wherever a table `/tiles` reads and no per-cell
//! version tracks is rewritten.**
//!
//! - `import` dispatch, the `bdot10k` / `egib` / `prg` / `full` arms
//!   (`src/import/mod.rs`, immediately after each arm's own
//!   `<source>::import(...)` call).
//! - `update::dataset::refresh`'s apply transaction (`src/update/dataset.rs`)
//!   -- inside the transaction, so a rollback can't leave a bumped epoch
//!   describing a delta that never landed, and unconditional: every refresh
//!   that lands changes what `/tiles` reads, even a "zero
//!   added/modified/removed" diff-of-nothing, because raw tables outside the
//!   diffed live table -- e.g. `centroid`, `rodzaj_kod` -- aren't part of the
//!   diff either.
//! - `mappings::street_names::load_from_path`, inside the loader itself
//!   (in the same swap transaction as the `DELETE`+`INSERT`), not at its two
//!   call sites (`src/import/mod.rs`, `src/server/jobs/street_mappings_update.rs`)
//!   -- the loader is the one home, same reasoning as everywhere else in
//!   this codebase that a transformation gets one funnel.
//! - `mappings::building_types::load_from_path` -- same funnel, same
//!   reasoning, one bump per source (bdot10k and egib are independent
//!   files with independent swap transactions).
//! - `compare::run` for the `Full` target (`src/compare/mod.rs`) -- pure
//!   insurance so a client's cached `ETag` can't survive an offline rebuild
//!   that never touched a dirty cell (e.g. restoring the DB from a snapshot
//!   and re-running `compare full` by hand).
//!
//! # Must NOT bump
//!
//! This list matters more than the one above -- bumping here would be
//! silently correct (tiles would just look "changed" more often than they
//! are) but would defeat the whole point of a later phase's `ETag`, which is
//! to let a client skip re-fetching a tile that didn't change:
//!
//! - `update::record_noop_refresh` (`src/update/mod.rs`, three call sites).
//!   An ETag-unchanged poll against the government source rewrote nothing --
//!   bumping there would flush the world's cached tiles daily, three times
//!   over (once per source), for changing nothing at all.
//! - The mapping jobs' own ETag-match early returns
//!   (`server::jobs::street_mappings_update`, `server::jobs::building_types_update`).
//!   Both return *before* calling `load_from_path`, so putting the bump
//!   inside the loader (not at the call sites) gets this right for free --
//!   verified by reading both jobs: each does `if previous == current {
//!   return Ok(()); }` strictly above its call into the loader.
//! - `queue reconcile`. It only re-enqueues dirty cells for the drain to
//!   pick up later; the drain's own per-cell recompute is what actually
//!   changes `computed_at`, and that's already covered per-cell.
//! - `import osm` / `update osm`. `/tiles` reads no `osm_*` table directly --
//!   an OSM edit reaches tiles exclusively via dirty cells -> the
//!   `match_refresh` drain -> the per-cell version. Bumping here would be
//!   redundant with per-cell coverage, not wrong, but it would flush every
//!   tile in the country on every minutely OSM update, which defeats the
//!   `ETag`'s purpose just as thoroughly as a genuine bug would.
//! - Single-source `compare buildings` / `compare addresses`. Unlike `compare
//!   full`, these are not the "offline rebuild that skipped the dirty-cell
//!   path" scenario the `Full`-target bump exists for -- they're the normal
//!   CLI-driven path for one source, and its `*_unmatched` rewrite already
//!   moves that source's per-cell `computed_at`. Bumping here would flush
//!   every tile in the country for a change local to one source.
//! - `POST /report`, and every other writer of `object_reports`
//!   (`reports::insert`, `reports::revoke`, `reports::reconcile_source`).
//!   `/tiles` never reads `object_reports`; a report reaches a tile only by
//!   changing what a recompute writes into `<source>_unmatched`, and each of
//!   those writers enqueues the affected z14 cell so the drain moves that
//!   cell's `computed_at`. This is the same argument as `import osm`/`update
//!   osm` above, and the stakes are higher: reports arrive one user action at
//!   a time, so bumping would flush every cached tile in the country per
//!   click. Note this is exactly why `object_reports` is deliberately *not* a
//!   `/tiles` input -- rendering a "reported" layer would make the epoch bump
//!   unavoidable.

use anyhow::{Context, Result};
use duckdb::Connection;

/// `metadata` key for the global serving-epoch counter. See the module doc
/// for what it covers and why it exists alongside the per-cell version.
pub const SERVING_EPOCH_KEY: &str = "serving_epoch";

/// Bumped by hand whenever the MVT SQL changes shape -- a new attribute
/// column, a renamed one, a different geometry simplification. A constant
/// bumped by hand is the only way to catch a shape change like this: without
/// it, a deployed binary that changes what a tile *contains* would still
/// produce the same version string for the same underlying rows (nothing in
/// the data changed, only how it's rendered), so a client's cached `ETag`
/// would keep matching and every existing client would serve the stale shape
/// forever, with no way to self-heal short of a manual cache-buster.
pub const TILE_FORMAT_VERSION: u32 = 1;

/// Record that the live serving state moved in a way no per-cell version
/// tracks. A **counter**, not a timestamp -- deliberately, so a backwards
/// NTP step can't produce a value already seen and hand out a version that
/// looks unchanged when it isn't. Delete-then-insert, the same convention
/// `job_log::record` uses: `metadata` has no primary key (see `src/db.rs`),
/// so replacing the row this way rather than `UPDATE`-ing it is what keeps
/// it from silently becoming two rows.
///
/// Does not open its own transaction: every call site either already has an
/// ambient one open (e.g. `update::dataset::refresh`'s apply transaction,
/// where this must land or roll back together with the delta it describes)
/// or is a standalone statement outside any transaction (the `import`
/// dispatch), and wrapping a `BEGIN`/`COMMIT` around a call inside an
/// existing transaction would be a nested-transaction error.
pub fn bump_serving_epoch(conn: &Connection) -> Result<()> {
    let next = read_serving_epoch(conn)? + 1;
    conn.execute_batch(&format!(
        "DELETE FROM metadata WHERE key = '{SERVING_EPOCH_KEY}';
         INSERT INTO metadata VALUES ('{SERVING_EPOCH_KEY}', '{next}');"
    ))
    .context("Failed to bump serving epoch")
}

/// Read the current serving epoch, degrading to `0` rather than erroring.
///
/// `MAX(TRY_CAST(value AS BIGINT))` over a `WHERE key = ...` filter, not a
/// plain point lookup, handles three ways the stored row can be unusable
/// without ever failing a tile request over it: absent entirely (a fresh or
/// pre-this-feature database -- `COALESCE` catches the `NULL` from an empty
/// aggregate), garbage (`TRY_CAST` turns a non-numeric `value` into `NULL`,
/// which `MAX` then ignores rather than erroring), or duplicated (`metadata`
/// has no primary key and every writer is delete-then-insert, so a crash
/// between the two can leave zero *or* two rows for the same key -- `MAX`
/// picks the higher one, which is never wrong for a monotonic counter).
pub fn read_serving_epoch(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "SELECT COALESCE(MAX(TRY_CAST(value AS BIGINT)), 0) FROM metadata WHERE key = ?",
        duckdb::params![SERVING_EPOCH_KEY],
        |row| row.get(0),
    )
    .context("Failed to read serving epoch")
}

/// A short, legible version string for the z14 tile at `(x, y)`, folding
/// together the global [`SERVING_EPOCH_KEY`] and the `*_unmatched` state
/// actually visible from that tile. Deliberately not hashed -- there is no
/// size pressure (the whole string is ~60 chars) and a legible version is
/// worth a lot when diagnosing a stale tile by eye: `fmt-epoch-bc.bt-ec.et-pc.pt`
/// tells you at a glance whether the epoch moved, or which source's count or
/// timestamp did.
///
/// Four choices below are each a correction to the obvious approach:
///
/// 1. **Per-table `(count, max(computed_at))`, never one merged `max`
///    across all three sources.** With a single merged max: a cell holding
///    3 bdot10k rows at T0 and 5 prg rows at T1 (T1 > T0) reads version
///    `...T1...`. The drain then recomputes bdot10k down to *zero* rows in
///    that cell -- a real, common mutation (a building gets matched to OSM,
///    or suppressed by the former-building veto) -- and the merged max is
///    STILL `T1`, because prg's `computed_at` didn't move. Same version,
///    three buildings gone. The per-table pair is faithful by construction
///    instead: a recompute either inserts >=1 row for that table (its
///    `max(computed_at)` becomes the transaction's `now()`) or inserts 0
///    (its `count` drops to reflect that) -- one of the two always changes.
///
/// 2. **The 3x3 z14-cell ring around `(x, y)`, not just the tile's own
///    cell.** `*_unmatched` rows are *selected* for a tile by
///    `ST_Intersects` against the tile envelope but *tagged* with the cell
///    of their representative point, so a tile can render rows owned by a
///    neighbouring cell. `dataset::filter_oversized_geometry` makes a
///    surviving building's bbox strictly narrower than one z14 cell (see
///    its doc comment), so it touches at most a 2x2 block of cells, one of
///    which is its own centroid's cell -- its reach FROM its own cell is
///    therefore `<= 1` by construction, an enforced invariant rather than
///    an empirical guess. A 3x3 ring (radius 1) is exactly enough to cover
///    that reach in every direction.
///    `x`/`y` are cast to `i32` before the arithmetic and the bind, so
///    `x = 0` yields a low bound of `-1` (matching no real cell, since
///    `cell_x`/`cell_y` are never negative) rather than wrapping around to
///    `u32::MAX` the way unsigned subtraction would.
///
/// 3. **`epoch_us(...)`, not `(...)::VARCHAR`.** Casting a
///    `TIMESTAMP WITH TIME ZONE` to text renders it through the session's
///    `TimeZone` setting, so the same instant could produce two different
///    version strings under two different session configs. An integer
///    microsecond count is timezone-independent by construction.
///
/// 4. **`MAX(TRY_CAST(value AS BIGINT))` on the epoch read, inlined as a
///    subquery here rather than a separate [`read_serving_epoch`] call.**
///    Same reasoning as that function's doc: `metadata` has no primary key
///    and every writer is delete-then-insert, so a crash between the two
///    could transiently leave zero or two rows. Degrading to `0` keeps a
///    tile request serving instead of 500ing over a metadata hiccup that
///    has nothing to do with the tile's actual content.
pub fn z14_tile_version(conn: &Connection, x: u32, y: u32) -> Result<String> {
    // i32, not u32: see point 2 above. z14 coordinates fit comfortably in an
    // i32 (max ~16384), so this cannot itself overflow.
    let x = x as i32;
    let y = y as i32;
    let (x_lo, x_hi, y_lo, y_hi) = (x - 1, x + 1, y - 1, y + 1);

    const SQL: &str = "
        WITH b AS (
            SELECT count(*) AS c, epoch_us(max(computed_at)) AS t
            FROM bdot10k_unmatched
            WHERE cell_x BETWEEN ? AND ? AND cell_y BETWEEN ? AND ?
        ), e AS (
            SELECT count(*) AS c, epoch_us(max(computed_at)) AS t
            FROM egib_unmatched
            WHERE cell_x BETWEEN ? AND ? AND cell_y BETWEEN ? AND ?
        ), p AS (
            SELECT count(*) AS c, epoch_us(max(computed_at)) AS t
            FROM prg_unmatched
            WHERE cell_x BETWEEN ? AND ? AND cell_y BETWEEN ? AND ?
        )
        SELECT (SELECT COALESCE(MAX(TRY_CAST(value AS BIGINT)), 0) FROM metadata WHERE key = 'serving_epoch'),
               b.c, COALESCE(b.t,0), e.c, COALESCE(e.t,0), p.c, COALESCE(p.t,0)
        FROM b, e, p";

    let (epoch, bc, bt, ec, et, pc, pt): (i64, i64, i64, i64, i64, i64, i64) = conn
        .query_row(
            SQL,
            duckdb::params![
                x_lo, x_hi, y_lo, y_hi, x_lo, x_hi, y_lo, y_hi, x_lo, x_hi, y_lo, y_hi
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .context("Failed to compute z14 tile version")?;

    Ok(format!(
        "{TILE_FORMAT_VERSION}-{epoch}-{bc}.{bt}-{ec}.{et}-{pc}.{pt}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    fn insert_unmatched(conn: &Connection, table: &str, cell_x: i32, cell_y: i32) {
        let cols = match table {
            "bdot10k_unmatched" => "LOKALNYID, geom, cell_x, cell_y, computed_at",
            "egib_unmatched" => "id_budynku, geom, cell_x, cell_y, computed_at",
            "prg_unmatched" => "geom, lokalny_id, numer_porzadkowy, cell_x, cell_y, computed_at",
            _ => unreachable!(),
        };
        let sql = if table == "prg_unmatched" {
            format!(
                "INSERT INTO {table} ({cols})
                 VALUES (ST_Point(21.0, 52.0), 'id', '1', {cell_x}, {cell_y}, now())"
            )
        } else {
            format!(
                "INSERT INTO {table} ({cols})
                 VALUES ('id', ST_Point(21.0, 52.0), {cell_x}, {cell_y}, now())"
            )
        };
        conn.execute_batch(&sql).unwrap();
    }

    // --- bump_serving_epoch / read_serving_epoch -----------------------

    #[test]
    fn epoch_starts_at_one_after_the_first_bump() {
        let conn = conn();
        assert_eq!(read_serving_epoch(&conn).unwrap(), 0, "absent reads as 0");
        bump_serving_epoch(&conn).unwrap();
        assert_eq!(read_serving_epoch(&conn).unwrap(), 1);
    }

    #[test]
    fn epoch_is_monotonic_across_repeated_bumps() {
        let conn = conn();
        for expected in 1..=5 {
            bump_serving_epoch(&conn).unwrap();
            assert_eq!(read_serving_epoch(&conn).unwrap(), expected);
        }
    }

    #[test]
    fn bump_replaces_rather_than_appends_a_metadata_row() {
        let conn = conn();
        bump_serving_epoch(&conn).unwrap();
        bump_serving_epoch(&conn).unwrap();
        bump_serving_epoch(&conn).unwrap();
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM metadata WHERE key = ?",
                duckdb::params![SERVING_EPOCH_KEY],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1, "must not accumulate one row per bump");
    }

    #[test]
    fn read_recovers_from_a_garbage_value_instead_of_erroring() {
        let conn = conn();
        conn.execute(
            "INSERT INTO metadata VALUES (?, 'not-a-number')",
            duckdb::params![SERVING_EPOCH_KEY],
        )
        .unwrap();
        assert_eq!(read_serving_epoch(&conn).unwrap(), 0);
    }

    #[test]
    fn read_recovers_from_a_duplicated_row_by_taking_the_max() {
        let conn = conn();
        conn.execute_batch(&format!(
            "INSERT INTO metadata VALUES ('{SERVING_EPOCH_KEY}', '3');
             INSERT INTO metadata VALUES ('{SERVING_EPOCH_KEY}', '7');"
        ))
        .unwrap();
        assert_eq!(read_serving_epoch(&conn).unwrap(), 7);
    }

    // --- z14_tile_version ------------------------------------------------

    #[test]
    fn version_changes_on_a_cell_recompute() {
        let conn = conn();
        let before = z14_tile_version(&conn, 9147, 5411).unwrap();
        insert_unmatched(&conn, "bdot10k_unmatched", 9147, 5411);
        let after = z14_tile_version(&conn, 9147, 5411).unwrap();
        assert_ne!(before, after);
    }

    /// The test that fails under a merged `max(computed_at)` across sources:
    /// prg's row is inserted (and its `max` becomes the newer of the two)
    /// AFTER bdot10k's, so a merged max would report the same timestamp
    /// whether or not bdot10k's row is still present. Deleting bdot10k's row
    /// afterwards changes bdot10k's own `count` to 0 -- which the per-table
    /// pair must reflect in the version string -- while a merged max stays
    /// pinned to prg's (newer, untouched) timestamp and would see no change.
    #[test]
    fn version_changes_when_a_cell_empties_out() {
        let conn = conn();
        insert_unmatched(&conn, "bdot10k_unmatched", 9147, 5411);
        insert_unmatched(&conn, "prg_unmatched", 9147, 5411);
        let with_building = z14_tile_version(&conn, 9147, 5411).unwrap();

        conn.execute_batch("DELETE FROM bdot10k_unmatched WHERE LOKALNYID = 'id'")
            .unwrap();
        let emptied = z14_tile_version(&conn, 9147, 5411).unwrap();

        assert_ne!(
            with_building, emptied,
            "emptying one source's rows in a cell must change the version even \
             though another source's newer computed_at is untouched -- a merged \
             max() across sources would fail this"
        );
    }

    #[test]
    fn version_changes_across_an_add_then_remove_walk() {
        let conn = conn();
        let a = z14_tile_version(&conn, 9147, 5411).unwrap();

        insert_unmatched(&conn, "egib_unmatched", 9147, 5411);
        let b = z14_tile_version(&conn, 9147, 5411).unwrap();
        assert_ne!(a, b, "A -> B: add must change the version");

        conn.execute_batch("DELETE FROM egib_unmatched").unwrap();
        let a_prime = z14_tile_version(&conn, 9147, 5411).unwrap();
        assert_ne!(b, a_prime, "B -> A': remove must change the version");
    }

    #[test]
    fn version_changes_on_an_epoch_bump() {
        let conn = conn();
        let before = z14_tile_version(&conn, 9147, 5411).unwrap();
        bump_serving_epoch(&conn).unwrap();
        let after = z14_tile_version(&conn, 9147, 5411).unwrap();
        assert_ne!(before, after);
    }

    /// Pins the 3x3 ring: a neighbouring cell one step away in both x and y
    /// is still inside the ring, so its recompute must move THIS tile's
    /// version even though the tile's own cell never changed.
    #[test]
    fn version_changes_when_a_neighbouring_cell_is_recomputed() {
        let conn = conn();
        let before = z14_tile_version(&conn, 9147, 5411).unwrap();
        insert_unmatched(&conn, "bdot10k_unmatched", 9148, 5412);
        let after = z14_tile_version(&conn, 9147, 5411).unwrap();
        assert_ne!(
            before, after,
            "a neighbouring cell inside the 3x3 ring must affect this tile's version"
        );
    }

    /// A cell TWO steps away is outside the ring and must not affect the
    /// version -- otherwise the ring would effectively be unbounded and the
    /// version would churn on unrelated, distant recomputes.
    #[test]
    fn version_is_unaffected_by_a_cell_outside_the_ring() {
        let conn = conn();
        let before = z14_tile_version(&conn, 9147, 5411).unwrap();
        insert_unmatched(&conn, "bdot10k_unmatched", 9150, 5411);
        let after = z14_tile_version(&conn, 9147, 5411).unwrap();
        assert_eq!(
            before, after,
            "a cell outside the 3x3 ring must not affect this tile's version"
        );
    }

    #[test]
    fn version_is_stable_across_repeated_calls_with_no_writes_in_between() {
        let conn = conn();
        insert_unmatched(&conn, "bdot10k_unmatched", 9147, 5411);
        let a = z14_tile_version(&conn, 9147, 5411).unwrap();
        let b = z14_tile_version(&conn, 9147, 5411).unwrap();
        assert_eq!(
            a, b,
            "no writes happened between calls; a stray now() would fail this"
        );
    }

    #[test]
    fn version_embeds_the_tile_format_version() {
        let conn = conn();
        let v = z14_tile_version(&conn, 9147, 5411).unwrap();
        assert!(
            v.starts_with(&format!("{TILE_FORMAT_VERSION}-")),
            "got: {v}"
        );
    }

    /// A bundled-DuckDB upgrade that silently dropped or renamed `epoch_us`
    /// would otherwise only surface the first time a real z14 request hit
    /// it in production. Pin the function's availability directly.
    #[test]
    fn epoch_us_is_available() {
        let conn = conn();
        let v: i64 = conn
            .query_row("SELECT epoch_us(now())", [], |r| r.get(0))
            .unwrap();
        assert!(v > 0);
    }

    #[test]
    fn x_zero_and_y_zero_do_not_panic_or_wrap() {
        let conn = conn();
        // Must simply run to completion without panicking (an unsigned
        // underflow in a debug build would panic) and without matching the
        // wrapped-around cells a naive u32 subtraction would produce.
        let v = z14_tile_version(&conn, 0, 0).unwrap();
        assert!(v.starts_with(&format!("{TILE_FORMAT_VERSION}-0-0.0-0.0-0.0")));
    }
}
