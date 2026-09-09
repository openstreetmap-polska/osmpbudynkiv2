//! `tile_refresh` background job: drains `tile_dirty_cells` (populated
//! wherever something changes what a tile renders -- see
//! `server::tile_dirty`'s module doc for the exact producer rule), keeping
//! `server::tile_store::TileStore` in step with what `/tiles` would render
//! fresh.
//!
//! # Two passes, and the split is load-bearing
//!
//! A tick does not delete-then-immediately-re-render each tile key in turn.
//! It deletes every resident stale key first, across the whole tick, and only
//! afterwards re-renders them:
//!
//! - **Deleting is microseconds per key; re-rendering is milliseconds**
//!   (z14 costs a median 49 ms, up to 709 ms in dense cities -- see
//!   `tile_store`'s module doc). In a single interleaved pass, a tile near
//!   the back of a large invalidation would keep serving stale bytes for the
//!   whole time it took to work through everything ahead of it. Splitting
//!   the passes bounds staleness by the delete lag alone: once pass 1
//!   finishes, every stale tile this tick found is gone from the store, and
//!   a request landing in the gap before pass 2 gets an ordinary cold
//!   render -- correct, just not pre-warmed.
//! - **Only re-rendering store-resident keys is what keeps background load
//!   proportional to what has actually been rendered.** A dirty cell can
//!   expand (via [`tiles_for_cell`]) into z12..=z14 tiles nobody has ever
//!   requested; rendering all of them regardless of residency would turn
//!   every edit into a whole-pyramid render, working areas nobody has looked
//!   at yet for free. Skipping non-resident keys means a cold region churns
//!   this job for nothing and a warm one gets kept warm.
//!
//! # `may_exist` is a probe, not a read, and false positives are expected
//!
//! [`TileStore::may_exist`] is a bloom-filter check with no disk I/O -- see
//! its own doc. A false positive costs a tombstone for a key that was never
//! there (harmless: `batch_delete` on an absent key is a no-op) plus one
//! extra tile rendered that was not actually resident, which merely warms it
//! early. Do not "fix" this by reading for real
//! (e.g. `TileStore::get`/`multi_get_cf`): that would materialize thousands
//! of tile *values* -- some several hundred KB -- to answer what is
//! structurally a yes/no residency question.
//!
//! # A render failure must not abort the tick
//!
//! By the time pass 2 runs, pass 1 has already deleted the queue rows for
//! this tick and evicted the stale tile from the store. If `render_tile`
//! then errors for one key (a bad request, a transient DB hiccup), that one
//! tile is logged and skipped rather than aborting the batch -- the
//! remaining keys still get re-rendered and their queue rows still cleared.
//! The tradeoff, stated plainly: that tile now serves as a cold render on
//! its next request (fine) until its cell is next dirtied for real (its
//! only path back into this job, since the queue row is already gone) --
//! never automatically retried. Given failures here are expected to be rare
//! and each is logged, this is the same choice `compare::drain::drain_batch`
//! makes for a failed cell, just with a different fallback (there: stays
//! queued for retry; here: falls back to on-demand cold render).
//!
//! # Dedup across the whole tick, not per cell
//!
//! Neighbouring z14 cells share z12/z13 parents and their 3x3 z14 rings
//! overlap, so expanding each dirty cell independently and processing every
//! result would delete (and potentially re-render) the same key more than
//! once per tick. [`tick`] expands every cell first and collects the result
//! into one `BTreeSet<TileKey>` before doing anything else, so a shared
//! parent is deleted once and re-rendered once regardless of how many dirty
//! cells produced it.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::server::jobs::{Job, JobContext};
use crate::server::tile_dirty::tiles_for_cell;
use crate::server::tile_store::{self, TileKey, TileStore};
use crate::server::tiles;

/// `job_run_log` key this job reports under (see `Job::log_keys`).
const JOB_LOG_KEY: &str = "tile_refresh";

/// One tick's outcome. `cells` and `resident` describe pass 1's work,
/// `rendered`/`failed` pass 2's.
pub struct TileRefreshStats {
    /// Distinct `(cell_x, cell_y)` rows drained this tick.
    pub cells: u64,
    /// Expanded tile keys `TileStore::may_exist` reported resident, and so
    /// were deleted in pass 1 and attempted in pass 2. Includes any bloom
    /// filter false positives -- see the module doc.
    pub resident: u64,
    /// Of `resident`, how many pass 2 successfully re-rendered and wrote
    /// back.
    pub rendered: u64,
    /// Of `resident`, how many failed to re-render (logged, not fatal).
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
                        resident = stats.resident,
                        rendered = stats.rendered,
                        "tile_refresh drained dirty cells"
                    );
                }
                if stats.failed > 0 {
                    tracing::warn!(
                        failed = stats.failed,
                        "tile_refresh: some tiles failed to re-render and will \
                         serve a cold render until their cell is next dirtied"
                    );
                }
                let msg = if stats.failed > 0 {
                    format!(
                        "drained {} cells, {} resident tiles deleted, {} re-rendered, {} failed",
                        stats.cells, stats.resident, stats.rendered, stats.failed
                    )
                } else {
                    format!(
                        "drained {} cells, {} resident tiles deleted and re-rendered",
                        stats.cells, stats.resident
                    )
                };
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

/// One drain tick. See the module doc for the two-pass rationale.
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
/// Unlike `drain_batch`, there is no per-cell transaction here: deleting a
/// queue row and evicting/re-rendering its tiles carry no atomicity
/// requirement between them (there is no recompute step that must land
/// atomically with the delete). If this process dies between the SELECT and
/// the DELETE, the cell simply stays queued and is retried next tick -- the
/// same safe fallback a transaction would have produced, without needing
/// one.
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

    // ---- Pass 1: expand + dedup + delete promptly ----
    //
    // A `BTreeSet` so neighbouring cells' overlapping rings and shared
    // z12/z13 parents collapse to one entry each -- see the module doc.
    let mut keys: BTreeSet<TileKey> = BTreeSet::new();
    for (cx, cy) in &cells {
        keys.extend(tiles_for_cell(*cx, *cy));
    }

    // `may_exist` decides residency for every key up front; only the
    // probable hits are deleted and carried into pass 2. False positives are
    // expected and harmless -- see the module doc.
    let mut resident: Vec<TileKey> = Vec::new();
    let mut delete_batch = store.batch();
    for key in &keys {
        if store.may_exist(*key) {
            resident.push(*key);
            store.batch_delete(&mut delete_batch, *key);
        }
    }
    store.write_batch(delete_batch)?;

    // Delete the queue rows under the same batch_start cutoff used to read
    // them -- see this function's doc for why a cell re-dirtied after
    // batch_start must survive this.
    for (cx, cy) in &cells {
        conn.execute(
            "DELETE FROM tile_dirty_cells
             WHERE cell_x = ? AND cell_y = ? AND enqueued_at <= ?::TIMESTAMPTZ",
            duckdb::params![cx, cy, batch_start],
        )?;
    }

    // ---- Pass 2: re-render lazily, only what pass 1 found resident ----
    //
    // Every tier renders in blocks -- `tiles::render_blocks` groups the
    // resident keys by zoom and common ancestor, and `render_block` picks the
    // tier's batched query. A tick's cells collapse to a handful of blocks, so
    // this is where the batching pays: one query per layer per block instead of
    // one per layer per tile.
    let mut rendered = 0u64;
    let mut failed = 0u64;
    let mut render_batch = store.batch();
    for block in tiles::render_blocks(&resident) {
        // Deletes are microseconds each and already done; renders are
        // milliseconds each, so this is where a timeout or shutdown actually
        // needs to be able to cut a tick short.
        if is_cancelled() {
            break;
        }
        // A block fails as a unit. The queue rows are already gone, so those
        // tiles stay stale (served as a cold render) until their cells are next
        // dirtied for real -- the same outcome a per-tile failure had, only
        // coarser. See the module doc's tradeoff.
        let out = match tiles::render_block(conn, &block) {
            Ok(out) => out,
            Err(e) => {
                let n = block.tiles.len();
                tracing::warn!(z = block.z, tiles = n, error = %e, "tile_refresh: failed to re-render a block of tiles, leaving them stale");
                failed += n as u64;
                continue;
            }
        };
        for ((x, y), raw) in out {
            let key: TileKey = (block.z, x, y);
            match tile_store::prepare(&raw) {
                Ok((etag, body)) => {
                    store.batch_put(&mut render_batch, key, &etag, &body);
                    rendered += 1;
                    store.flush_if_full(&mut render_batch)?;
                }
                Err(e) => {
                    tracing::warn!(z = block.z, x, y, error = %e, "tile_refresh: failed to prepare a re-rendered tile, leaving it stale");
                    failed += 1;
                }
            }
        }
    }
    store.write_batch(render_batch)?;

    Ok(TileRefreshStats {
        cells: cells.len() as u64,
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

    /// The centerpiece behavior: a resident z14 tile gets deleted and
    /// re-rendered to reflect current DB state, while never-requested ring
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
    /// still has its queue row cleared -- pass 1's delete is unconditional
    /// once a cell is read, independent of what pass 2 finds to do.
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
            Some("drained 0 cells, 0 resident tiles deleted and re-rendered")
        );
    }
}
