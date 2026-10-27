//! `tile_refresh` background job: drains `tile_dirty_cells` (populated
//! wherever something changes what a tile renders -- see
//! `server::tile_dirty`'s module doc for the exact producer rule), keeping
//! `server::tile_store::TileStore` in step with what `/tiles` would render
//! fresh.
//!
//! # Overwrite in place, and a queue row goes only once its tiles have
//!
//! A tick re-renders each resident stale tile and writes it over the old
//! value. It never deletes a tile first, and it deletes a cell's queue row
//! only after every resident tile that cell expands to has been dealt with.
//! So a tick that is cut short -- the supervisor's timeout, a shutdown -- leaves
//! the cells it did not finish queued, with their old tiles still resident,
//! and the next tick picks them up as if nothing had happened.
//!
//! **This replaced a delete-everything-first pass, which lost tiles.** That
//! design evicted every resident key and the queue rows up front, then
//! re-rendered, on the grounds that deleting is microseconds and rendering
//! milliseconds, so staleness would be bounded by the delete lag. But a tick
//! cut short in the render pass left the rest evicted *and* unqueued, and
//! since residency is the only route back into this job, those tiles stayed
//! cold until someone requested them. Blocks render z12 -> z13 -> z14, so it
//! was always the expensive z14 tier that was lost, and a dataset window --
//! a national refresh dirtying cells twice over, rendering against the drain
//! with DuckDB at its memory limit -- is exactly when ticks overran. Over many
//! refreshes a warmed store wore away. What overwriting costs is a stale tile
//! served until its re-render lands rather than until its delete does: at
//! most one tick's render time. That was never the bound that mattered
//! anyway, since a tile whose cell is still waiting further back in the
//! queue is stale for the whole backlog under either design.
//!
//! **Only re-rendering store-resident keys is what keeps background load
//! proportional to what has actually been rendered.** A dirty cell can expand
//! (via [`tiles_for_cell`]) into z12..=z14 tiles nobody has ever requested;
//! rendering all of them regardless of residency would turn every edit into a
//! whole-pyramid render, working areas nobody has looked at yet for free.
//! Skipping non-resident keys means a cold region churns this job for nothing
//! and a warm one gets kept warm.
//!
//! # `may_exist` is a probe, not a read, and false positives are expected
//!
//! [`TileStore::may_exist`] is a bloom-filter check with no disk I/O -- see
//! its own doc. A false positive costs one extra tile rendered and stored
//! that was not actually resident, which merely warms it early. Do not "fix" this by reading for real
//! (e.g. `TileStore::get`/`multi_get_cf`): that would materialize thousands
//! of tile *values* -- some several hundred KB -- to answer what is
//! structurally a yes/no residency question.
//!
//! # A render failure evicts, and is not retried
//!
//! A block that fails to render (a transient DB hiccup, an out-of-memory
//! error) is logged and its tiles are **deleted**, and their cells' queue rows
//! go as normal. Leaving them queued would retry them every tick, and a poison
//! block would hold its cells at the head of the oldest-first queue forever.
//! Leaving them resident would serve the stale bytes indefinitely, since
//! nothing would ever dirty them again. Evicted, each falls back to a cold
//! render on its next request, which also writes it back to the store. This
//! is the one path where a resident tile still becomes non-resident. It
//! differs from a cut-short tick on purpose: a tick that overran will finish
//! next time, but a block that failed may fail again.
//!
//! # Dedup across the whole tick, not per cell
//!
//! Neighbouring z14 cells share z12/z13 parents and their 3x3 z14 rings
//! overlap, so expanding each dirty cell independently and processing every
//! result would re-render the same key more than once per tick. [`tick`]
//! expands every cell first and collects the result into one
//! `BTreeSet<TileKey>` before doing anything else, so a shared parent is
//! re-rendered once regardless of how many dirty cells produced it.

use std::collections::{BTreeSet, HashSet};

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::server::jobs::{Job, JobContext};
use crate::server::tile_dirty::tiles_for_cell;
use crate::server::tile_store::{self, TileKey, TileStore};
use crate::server::tiles;

/// `job_run_log` key this job reports under (see `Job::log_keys`).
const JOB_LOG_KEY: &str = "tile_refresh";

/// One tick's outcome.
pub struct TileRefreshStats {
    /// Distinct `(cell_x, cell_y)` rows read from the queue this tick.
    pub cells: u64,
    /// Of `cells`, how many were left queued because the tick was cut short
    /// before all their resident tiles were re-rendered. Zero unless
    /// cancelled.
    pub deferred: u64,
    /// Expanded tile keys `TileStore::may_exist` reported resident, and so
    /// were due a re-render. Includes any bloom filter false positives -- see
    /// the module doc.
    pub resident: u64,
    /// Of `resident`, how many were re-rendered and written back.
    pub rendered: u64,
    /// Of `resident`, how many failed to re-render and were evicted instead
    /// (logged, not fatal).
    pub failed: u64,
}

pub struct TileRefreshJob {
    batch_size: usize,
}

impl TileRefreshJob {
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }
}

impl Job for TileRefreshJob {
    fn name(&self) -> &'static str {
        "tile_refresh"
    }

    fn log_keys(&self) -> &'static [&'static str] {
        &[JOB_LOG_KEY]
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        // Built fresh per run from the injected kv handle and the live
        // config, rather than stored on the job or added to `JobContext`'s
        // shape (every other job would have to ignore it). `TileStore` is
        // cheap to construct -- an `Arc` clone plus a handful of atomics --
        // and reads `config.cache.persist_tiles` at run time, so flipping
        // that off between ticks takes effect immediately: `TileStore::new`
        // becomes a genuine no-op store, `may_exist` always reports nothing
        // resident, and this job harmlessly drains the queue without
        // rendering anything.
        let store = TileStore::new(
            ctx.kv.clone(),
            tiles::TILE_FORMAT_VERSION,
            ctx.config.cache.persist_tiles,
        );

        let outcome = tick(&conn, &store, self.batch_size, &|| ctx.is_cancelled());

        match &outcome {
            Ok(stats) => {
                if stats.cells > 0 {
                    tracing::info!(
                        cells = stats.cells,
                        deferred = stats.deferred,
                        resident = stats.resident,
                        rendered = stats.rendered,
                        "tile_refresh drained dirty cells"
                    );
                }
                if stats.failed > 0 {
                    tracing::warn!(
                        failed = stats.failed,
                        "tile_refresh: some tiles failed to re-render and were \
                         evicted; each serves a cold render on its next request"
                    );
                }
                let mut msg = format!(
                    "drained {} cells, {} resident tiles re-rendered",
                    stats.cells - stats.deferred,
                    stats.rendered
                );
                if stats.failed > 0 {
                    msg.push_str(&format!(", {} failed and evicted", stats.failed));
                }
                if stats.deferred > 0 {
                    msg.push_str(&format!(
                        ", cut short with {} cells left queued",
                        stats.deferred
                    ));
                }
                let _ = crate::job_log::record(&conn, JOB_LOG_KEY, "Success", Some(&msg));
            }
            Err(e) => {
                let _ =
                    crate::job_log::record(&conn, JOB_LOG_KEY, "Error", Some(&format!("{e:#}")));
            }
        }

        outcome.map(|_| ())
    }
}

/// One drain tick. See the module doc for why tiles are overwritten in place
/// and queue rows deleted last.
///
/// Reads up to `batch_size` distinct `(cell_x, cell_y)` from
/// `tile_dirty_cells` whose `enqueued_at` is at or before a single
/// `batch_start` taken via `SELECT now()::VARCHAR` -- the exact cutoff
/// convention `compare::drain::drain_batch` uses, and for the same reason: a
/// cell re-dirtied *after* `batch_start` (a producer enqueueing it while this
/// tick is running) must survive the paired delete and be picked up by the
/// next tick, because this tick's render cannot have seen that edit. Both the
/// read and the delete use the same stored `batch_start` string, never
/// `now()` a second time.
///
/// Unlike `drain_batch`, there is no per-cell transaction here: writing a
/// cell's tiles and deleting its queue row carry no atomicity requirement
/// between them. The tiles are written first, so if this process dies (or a
/// write fails) before the DELETE, the cell simply stays queued and is
/// re-rendered next tick -- wasted work, never a lost tile.
pub fn tick(
    conn: &Connection,
    store: &TileStore,
    batch_size: usize,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<TileRefreshStats> {
    // A single wall-clock cutoff for the whole tick.
    let batch_start: String = conn
        .query_row("SELECT now()::VARCHAR", [], |r| r.get(0))
        .context("tile_refresh: read batch_start")?;

    // Oldest-enqueued-first, mirroring drain_batch's GROUP BY + ORDER BY
    // MIN(enqueued_at) shape (equivalent to DISTINCT, since it also collapses
    // to one row per cell) so a sustained backlog doesn't starve later cells.
    let cells: Vec<(i32, i32)> = {
        let mut stmt = conn.prepare(
            "SELECT cell_x, cell_y FROM tile_dirty_cells
             WHERE enqueued_at <= ?::TIMESTAMPTZ
             GROUP BY cell_x, cell_y
             ORDER BY MIN(enqueued_at)
             LIMIT ?",
        )?;
        let rows = stmt.query_map(duckdb::params![batch_start, batch_size as i64], |r| {
            Ok((r.get::<_, i32>(0)?, r.get::<_, i32>(1)?))
        })?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        v
    };

    // A `BTreeSet` so neighbouring cells' overlapping rings and shared
    // z12/z13 parents collapse to one entry each -- see the module doc.
    let mut keys: BTreeSet<TileKey> = BTreeSet::new();
    for (cx, cy) in &cells {
        keys.extend(tiles_for_cell(*cx, *cy));
    }

    // `may_exist` decides residency for every key up front, and only the
    // probable hits are re-rendered. False positives are expected and
    // harmless -- see the module doc.
    let resident: Vec<TileKey> = keys.into_iter().filter(|k| store.may_exist(*k)).collect();

    // Every tier renders in blocks -- `tiles::render_blocks` groups the
    // resident keys by zoom and common ancestor, and `render_block` picks the
    // tier's batched query. A tick's cells collapse to a handful of blocks, so
    // this is where the batching pays: one query per layer per block instead of
    // one per layer per tile.
    //
    // A key leaves `unsettled` once its stored value reflects this tick:
    // overwritten by a fresh render, or evicted after a failed one. Whatever
    // is still in it after the loop decides which queue rows survive.
    let mut unsettled: HashSet<TileKey> = resident.iter().copied().collect();
    let mut rendered = 0u64;
    let mut failed = 0u64;
    let mut batch = store.batch();
    for block in tiles::render_blocks(&resident) {
        // Renders are milliseconds each, so this is where a timeout or
        // shutdown needs to be able to cut a tick short. Nothing is lost by
        // stopping: the unrendered tiles are still resident and their cells
        // still queued.
        if is_cancelled() {
            break;
        }
        // A block fails as a unit and is evicted rather than retried -- see
        // the module doc.
        let out = match tiles::render_block(conn, &block) {
            Ok(out) => out,
            Err(e) => {
                let n = block.tiles.len();
                tracing::warn!(z = block.z, tiles = n, error = %e, "tile_refresh: failed to re-render a block of tiles, evicting them");
                Vec::new()
            }
        };
        for ((x, y), raw) in out {
            let key: TileKey = (block.z, x, y);
            match tile_store::prepare(&raw) {
                Ok((etag, body)) => {
                    store.batch_put(&mut batch, key, &etag, &body);
                    unsettled.remove(&key);
                    rendered += 1;
                    store.flush_if_full(&mut batch)?;
                }
                Err(e) => {
                    tracing::warn!(z = block.z, x, y, error = %e, "tile_refresh: failed to prepare a re-rendered tile, evicting it");
                }
            }
        }
        // Whatever this block did not write -- the whole block on a render
        // error, a tile `prepare` rejected, or one the query failed to answer
        // for -- is evicted, never left serving the stale bytes.
        for &(x, y) in &block.tiles {
            let key: TileKey = (block.z, x, y);
            if unsettled.remove(&key) {
                store.batch_delete(&mut batch, key);
                failed += 1;
            }
        }
    }
    store.write_batch(batch)?;

    // Only now, with this tick's writes durable, delete the queue rows -- and
    // only for cells none of whose resident tiles is still unsettled, so a
    // tick cut short leaves the rest for the next one. Same batch_start cutoff
    // as the read; see this function's doc.
    let mut deferred = 0u64;
    for (cx, cy) in &cells {
        if tiles_for_cell(*cx, *cy)
            .iter()
            .any(|k| unsettled.contains(k))
        {
            deferred += 1;
            continue;
        }
        conn.execute(
            "DELETE FROM tile_dirty_cells
             WHERE cell_x = ? AND cell_y = ? AND enqueued_at <= ?::TIMESTAMPTZ",
            duckdb::params![cx, cy, batch_start],
        )?;
    }

    Ok(TileRefreshStats {
        cells: cells.len() as u64,
        deferred,
        resident: resident.len() as u64,
        rendered,
        failed,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::config::Config as AppConfig;
    use crate::db::init_db;
    use crate::tile_math::tile_to_bbox;

    /// In-memory DB with the raw government tables `render_tile` reads that
    /// `init_db`'s real schema doesn't own (mirrors `tiles::tests::make_state`,
    /// minus the RTREE indexes -- these tests aren't exercising index usage).
    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR,
                 geom GEOMETRY);
             CREATE TABLE bdot10k_buildings (
                 PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR,
                 LICZBAKONDYGNACJI SMALLINT, KATEGORIAISTNIENIA VARCHAR, NAZWA VARCHAR,
                 FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (
                 id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR,
                 kondygnacje_nadziemne INTEGER, kondygnacje_podziemne INTEGER, rodzaj VARCHAR);",
        )
        .unwrap();
        c
    }

    fn store(dir: &tempfile::TempDir) -> TileStore {
        let kv = Arc::new(crate::osm::kvstore::open(dir.path(), 8, 4, 8).unwrap());
        TileStore::new(kv, tiles::TILE_FORMAT_VERSION, true)
    }

    fn seed_placeholder(store: &TileStore, key: TileKey, raw: &[u8]) {
        let (etag, body) = tile_store::prepare(raw).unwrap();
        store.put(key, &etag, &body);
    }

    #[test]
    fn name_is_tile_refresh() {
        assert_eq!(TileRefreshJob::new(100).name(), "tile_refresh");
    }

    /// The centerpiece behavior: a resident z14 tile gets re-rendered in
    /// place to reflect current DB state, while never-requested ring
    /// members that were never in the store stay absent rather than being
    /// created from nothing.
    #[test]
    fn tile_refresh_re_renders_only_tiles_already_in_the_store() {
        let c = conn();
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);

        // A z14 cell with an actual unmatched building inside its bbox, so a
        // real render produces non-trivial bytes distinguishable from a
        // placeholder.
        let (cx, cy) = crate::tile_math::lonlat_to_tile(21.0, 52.0, 14);
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, cx, cy);
        let cx_lon = (min_lon + max_lon) / 2.0;
        let cy_lat = (min_lat + max_lat) / 2.0;
        c.execute(
            "INSERT INTO bdot10k_unmatched
                 (LOKALNYID, PRZESTRZENNAZW, geom, cell_x, cell_y, computed_at)
             VALUES ('b1', 'X0001', ST_MakeEnvelope(?, ?, ? + 0.0001, ? + 0.0001), ?, ?, now())",
            duckdb::params![cx_lon, cy_lat, cx_lon, cy_lat, cx as i32, cy as i32],
        )
        .unwrap();

        let key: TileKey = (14, cx, cy);
        seed_placeholder(&s, key, b"a stale placeholder, not a real mvt tile");

        // A ring neighbour that was never requested/rendered, so it must
        // never appear in the store even after the tick.
        let never_resident: TileKey = (14, cx + 1, cy);

        crate::server::tile_dirty::enqueue(&c, cx as i32, cy as i32).unwrap();

        let stats = tick(&c, &s, 256, &|| false).unwrap();
        assert_eq!(stats.cells, 1);
        assert_eq!(
            stats.rendered, 1,
            "the one resident key must be re-rendered"
        );
        assert_eq!(stats.failed, 0);

        let expected = tiles::render_tile(&c, 14, cx, cy).unwrap();
        let got = s.get(key).expect("the resident tile must still be present");
        assert_eq!(
            got.body.decompress().unwrap(),
            expected,
            "the stored tile must be the fresh render, not the stale placeholder"
        );
        assert_ne!(
            got.body.decompress().unwrap(),
            b"a stale placeholder, not a real mvt tile".to_vec(),
        );

        assert!(
            s.get(never_resident).is_none(),
            "a ring member that was never resident must not be created by the refresh"
        );

        let left: i64 = c
            .query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0, "the drained cell's queue row must be gone");
    }

    /// One dirty cell with a placeholder seeded at its z14 tile and both its
    /// z12/z13 parents, so every tier has something resident to re-render.
    fn seed_cell_pyramid(c: &Connection, s: &TileStore) -> (u32, u32) {
        let (cx, cy) = crate::tile_math::lonlat_to_tile(21.0, 52.0, 14);
        for key in [(14, cx, cy), (13, cx >> 1, cy >> 1), (12, cx >> 2, cy >> 2)] {
            seed_placeholder(s, key, PLACEHOLDER);
        }
        crate::server::tile_dirty::enqueue(c, cx as i32, cy as i32).unwrap();
        (cx, cy)
    }

    const PLACEHOLDER: &[u8] = b"a stale placeholder, not a real mvt tile";

    fn queued(c: &Connection) -> i64 {
        c.query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |r| r.get(0))
            .unwrap()
    }

    /// The regression this job's design exists to prevent. Blocks render z12
    /// -> z13 -> z14, so a tick cut short after the cheap tiers used to have
    /// already evicted the z14 tile and deleted its queue row, leaving it
    /// cold for good. Now the stale tile stays resident and the cell stays
    /// queued, and the next tick finishes the job.
    #[test]
    fn a_tick_cut_short_before_z14_keeps_the_tile_and_its_cell_for_the_next_tick() {
        let c = conn();
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let (cx, cy) = seed_cell_pyramid(&c, &s);

        // Cancelled at the third block check: z12 and z13 render, z14 never.
        let checks = std::cell::Cell::new(0);
        let stats = tick(&c, &s, 256, &|| {
            checks.set(checks.get() + 1);
            checks.get() > 2
        })
        .unwrap();
        assert_eq!(stats.resident, 3);
        assert_eq!(stats.rendered, 2, "z12 and z13 rendered before the cut");
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.deferred, 1);

        let z14 = s
            .get((14, cx, cy))
            .expect("the unrendered z14 tile must still be resident");
        assert_eq!(z14.body.decompress().unwrap(), PLACEHOLDER.to_vec());
        assert_eq!(queued(&c), 1, "the unfinished cell must stay queued");

        let stats = tick(&c, &s, 256, &|| false).unwrap();
        assert_eq!(stats.deferred, 0);
        assert_eq!(
            s.get((14, cx, cy)).unwrap().body.decompress().unwrap(),
            tiles::render_tile(&c, 14, cx, cy).unwrap(),
            "the next tick must re-render the tile the cut-short one left"
        );
        assert_eq!(queued(&c), 0);
    }

    /// A cut-short tick defers only the cells that still have work: one with
    /// nothing resident has nothing to wait for and drains regardless.
    #[test]
    fn a_tick_cut_short_still_drains_cells_with_nothing_resident() {
        let c = conn();
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        seed_cell_pyramid(&c, &s);
        crate::server::tile_dirty::enqueue(&c, 100, 200).unwrap();

        let stats = tick(&c, &s, 256, &|| true).unwrap();
        assert_eq!(stats.cells, 2);
        assert_eq!(stats.deferred, 1);
        assert_eq!(stats.rendered, 0);

        let left: Vec<(i32, i32)> = {
            let mut stmt = c
                .prepare("SELECT DISTINCT cell_x, cell_y FROM tile_dirty_cells")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        assert!(
            !left.contains(&(100, 200)),
            "a cell with nothing resident must drain even when cut short"
        );
        assert_eq!(left.len(), 1);
    }

    /// A failed render evicts rather than leaving the stale bytes served, and
    /// the cell drains rather than being retried every tick -- see the module
    /// doc for why a failure and a cut-short tick are treated differently.
    #[test]
    fn a_failed_render_evicts_its_tiles_and_drains_the_cell() {
        let c = conn();
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let (cx, cy) = seed_cell_pyramid(&c, &s);
        // Every tier reads this table, so every block fails.
        c.execute_batch("DROP TABLE bdot10k_unmatched").unwrap();

        let stats = tick(&c, &s, 256, &|| false).unwrap();
        assert_eq!(stats.resident, 3);
        assert_eq!(stats.rendered, 0);
        assert_eq!(stats.failed, 3);
        assert_eq!(stats.deferred, 0);

        for key in [(14, cx, cy), (13, cx >> 1, cy >> 1), (12, cx >> 2, cy >> 2)] {
            assert!(
                s.get(key).is_none(),
                "{key:?} must be evicted, not left stale"
            );
        }
        assert_eq!(queued(&c), 0, "a failure must not stay queued for retry");
    }

    /// The `batch_start` cutoff, mirroring
    /// `compare::drain::tests::cell_reenqueued_after_batch_start_survives`:
    /// a re-dirty landing after this tick's cutoff must not be deleted, since
    /// this tick's (nonexistent, here) render cannot have seen it.
    #[test]
    fn a_cell_re_dirtied_mid_tick_survives_for_the_next_tick() {
        let c = conn();
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);

        c.execute_batch(
            "INSERT INTO tile_dirty_cells VALUES (100, 200, TIMESTAMPTZ '2000-01-01');
             INSERT INTO tile_dirty_cells VALUES (100, 200, TIMESTAMPTZ '2999-01-01');",
        )
        .unwrap();

        let stats = tick(&c, &s, 256, &|| false).unwrap();
        assert_eq!(stats.cells, 1, "one distinct cell, drained once");

        let future_left: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM tile_dirty_cells WHERE enqueued_at = TIMESTAMPTZ '2999-01-01'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            future_left, 1,
            "a re-dirty after batch_start must not be deleted"
        );

        let past_left: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM tile_dirty_cells WHERE enqueued_at = TIMESTAMPTZ '2000-01-01'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(past_left, 0, "the batch-start-visible row must be deleted");
    }

    /// Two adjacent z14 cells share a z13 parent (both `>> 1`) and a z12
    /// parent (both `>> 2`); the whole-tick dedup must collapse each to one
    /// entry rather than processing it twice.
    #[test]
    fn a_shared_parent_is_rendered_once_per_tick() {
        let mut keys: BTreeSet<TileKey> = BTreeSet::new();
        keys.extend(tiles_for_cell(100, 200));
        keys.extend(tiles_for_cell(101, 200));

        // z13/z12 parents of both cells coincide (100>>1 == 101>>1 == 50,
        // 200>>1 == 100; 100>>2 == 101>>2 == 25, 200>>2 == 50), so each must
        // appear exactly once in the deduplicated set.
        assert_eq!(keys.iter().filter(|(z, _, _)| *z == 13).count(), 1);
        assert!(keys.contains(&(13, 50, 100)));
        assert_eq!(keys.iter().filter(|(z, _, _)| *z == 12).count(), 1);
        assert!(keys.contains(&(12, 25, 50)));

        // The two 3x3 z14 rings (x in 99..=101 and 100..=102, both y in
        // 199..=201) overlap in a 2x3 strip (x in 100..=101), so the union is
        // 9 + 9 - 6 = 12 distinct z14 keys, not 18.
        assert_eq!(keys.iter().filter(|(z, _, _)| *z == 14).count(), 12);

        // 12 (z14) + 1 (z13) + 1 (z12) = 14 total, well under 2x either
        // cell's own 11-key expansion (9 ring + 1 z13 + 1 z12).
        assert_eq!(keys.len(), 14);
    }

    /// A dirty cell whose expanded tile keys are all absent from the store
    /// still has its queue row cleared -- it has nothing left to wait for.
    #[test]
    fn the_queue_drains_to_zero_even_when_nothing_is_resident() {
        let c = conn();
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);

        crate::server::tile_dirty::enqueue(&c, 100, 200).unwrap();
        crate::server::tile_dirty::enqueue(&c, 5000, 6000).unwrap();

        let stats = tick(&c, &s, 256, &|| false).unwrap();
        assert_eq!(stats.cells, 2);
        assert_eq!(stats.resident, 0, "nothing was ever put in the store");
        assert_eq!(stats.rendered, 0);

        let left: i64 = c
            .query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0, "both cells' queue rows must be gone");
    }

    // An empty queue is still a run worth showing in /status, not silence.
    #[test]
    fn logs_zero_cells_when_the_queue_is_empty() {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR,
                 geom GEOMETRY);
             CREATE TABLE bdot10k_buildings (
                 PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR,
                 LICZBAKONDYGNACJI SMALLINT, KATEGORIAISTNIENIA VARCHAR, NAZWA VARCHAR,
                 FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (
                 id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR,
                 kondygnacje_nadziemne INTEGER, kondygnacje_podziemne INTEGER, rodzaj VARCHAR);",
        )
        .unwrap();
        let pool = crate::server::build_pool(conn, 2).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(crate::osm::kvstore::open(dir.path(), 8, 4, 8).unwrap());
        let ctx = JobContext {
            pool,
            kv,
            config: Arc::new(AppConfig::default()),
            cancel: Arc::new(AtomicBool::new(false)),
        };

        TileRefreshJob::new(100).run(&ctx).unwrap();

        let conn = ctx.pool.get().unwrap();
        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(log[JOB_LOG_KEY].outcome, "Success");
        assert_eq!(
            log[JOB_LOG_KEY].message.as_deref(),
            Some("drained 0 cells, 0 resident tiles re-rendered")
        );
    }
}
