//! The `tiles warm` / `tiles clear` CLI verbs.
//!
//! Both need exclusive access to the database, like `queue drain` -- they are
//! offline maintenance, not something to run against a database a `run` server
//! also has open.
//!
//! # Why warming is worth a whole command
//!
//! The store is populated lazily by ordinary requests, so without this the
//! first visitor to every part of the country pays the cold render (a median
//! 49 ms at z14, up to 709 ms in dense cities, worse on the production disk).
//! Warming moves that cost to a maintenance window, once.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use anyhow::{Context, Result};
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::info;

use crate::cli::TilesAction;
use crate::config::Config;
use crate::osm::kvstore::RocksDB;
use crate::server::tile_dirty::tiles_for_cell;
use crate::server::tile_store::{TileKey, TileStore};
use crate::server::tiles::{TILE_FORMAT_VERSION, render_points_tiles, render_tile};
use crate::tile_math::lonlat_to_tile;

pub fn run(
    conn: &Connection,
    kv: Arc<RocksDB>,
    action: TilesAction,
    config: &Config,
) -> Result<()> {
    let store = TileStore::new(kv, TILE_FORMAT_VERSION, true);
    match action {
        TilesAction::Clear => {
            store.clear()?;
            info!("tile store cleared");
            Ok(())
        }
        TilesAction::Warm { bbox, jobs } => warm(conn, &store, bbox.as_deref(), jobs, config),
    }
}

/// Every tile the data can render, derived from the data rather than from a
/// bounding box.
///
/// `cell_totals` carries one row per z14 cell holding government objects (it is
/// the denominator the low-zoom ratio divides by), so expanding it through
/// `tiles_for_cell` yields exactly the tiles that can draw anything -- plus the
/// ring members that draw a neighbour's rows. Poland's land area is ~65% of its
/// bounding box, so this skips roughly a third of the tiles a bbox sweep would
/// render, all of them empty sea and border.
fn covered_tiles(conn: &Connection, bbox: Option<&str>) -> Result<Vec<TileKey>> {
    let range = bbox.map(parse_bbox_cells).transpose()?;
    let mut stmt = conn
        .prepare("SELECT DISTINCT cell_x, cell_y FROM cell_totals ORDER BY cell_x, cell_y")
        .context("tiles warm: read covered cells")?;
    let cells = stmt
        .query_map([], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i32>(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut keys = BTreeSet::new();
    for (cell_x, cell_y) in cells {
        // Both axes, not either: a cell inside the longitude span but far
        // north of the bbox is outside it. `&&` between the two negations
        // would keep that cell and quietly warm most of the country.
        if let Some((lo_x, lo_y, hi_x, hi_y)) = range
            && !((lo_x..=hi_x).contains(&cell_x) && (lo_y..=hi_y).contains(&cell_y))
        {
            continue;
        }
        keys.extend(tiles_for_cell(cell_x, cell_y));
    }
    Ok(keys.into_iter().collect())
}

/// `min_lon,min_lat,max_lon,max_lat` -> an inclusive z14 cell range.
///
/// Note the Y inversion: higher latitude means a *smaller* `cell_y`, so
/// `max_lat` maps to the minimum index. Getting this backwards yields an empty
/// range and silently warms nothing.
fn parse_bbox_cells(bbox: &str) -> Result<(i32, i32, i32, i32)> {
    let parts: Vec<f64> = bbox
        .split(',')
        .map(|p| p.trim().parse::<f64>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("tiles warm: --bbox must be four comma-separated numbers")?;
    let [min_lon, min_lat, max_lon, max_lat] = parts.as_slice() else {
        anyhow::bail!("tiles warm: --bbox must be min_lon,min_lat,max_lon,max_lat");
    };
    let (lo_x, lo_y) = lonlat_to_tile(*min_lon, *max_lat, crate::tile_math::CHANGE_CELL_ZOOM);
    let (hi_x, hi_y) = lonlat_to_tile(*max_lon, *min_lat, crate::tile_math::CHANGE_CELL_ZOOM);
    Ok((lo_x as i32, lo_y as i32, hi_x as i32, hi_y as i32))
}

/// One unit of warming work: a single z14 tile, or a run of same-zoom z12/z13
/// tiles rendered by one query.
///
/// The split is the tiers' own. z14 is four queries per tile with no batched
/// form; z12--z13 render a whole run in one query, worth 27x
/// (`tiles::render_points_tiles`). Runs are cut out of the *sorted* key list
/// so each is spatially coherent, which is not cosmetic: the batched query
/// bounds its table scan by the run's cell range before joining to the exact
/// tile list, so a scattered run would read a bounding box nobody asked for.
enum WarmUnit {
    Z14(TileKey),
    Points { z: u32, tiles: Vec<(u32, u32)> },
}

impl WarmUnit {
    fn tiles(&self) -> usize {
        match self {
            WarmUnit::Z14(_) => 1,
            WarmUnit::Points { tiles, .. } => tiles.len(),
        }
    }
}

/// Tiles per batched points query. Large enough that the per-query cost
/// disappears (0.16 ms/tile at 253 tiles against 4.43 ms at one), small enough
/// that a unit's rendered bytes stay bounded -- the query materialises every
/// tile in the run at once, and a dense z12 tile is ~70 KB.
const POINTS_BATCH_TILES: usize = 256;

/// Group a sorted key list into work units, preserving order so the runs stay
/// spatially coherent.
fn warm_units(todo: &[TileKey]) -> Vec<WarmUnit> {
    let mut units = Vec::new();
    let mut run: Vec<(u32, u32)> = Vec::new();
    let mut run_z = 0u32;
    for &(z, x, y) in todo {
        if !run.is_empty() && (z == crate::tile_math::CHANGE_CELL_ZOOM || z != run_z) {
            units.push(WarmUnit::Points {
                z: run_z,
                tiles: std::mem::take(&mut run),
            });
        }
        if z == crate::tile_math::CHANGE_CELL_ZOOM {
            units.push(WarmUnit::Z14((z, x, y)));
            continue;
        }
        run_z = z;
        run.push((x, y));
        if run.len() >= POINTS_BATCH_TILES {
            units.push(WarmUnit::Points {
                z: run_z,
                tiles: std::mem::take(&mut run),
            });
        }
    }
    if !run.is_empty() {
        units.push(WarmUnit::Points {
            z: run_z,
            tiles: run,
        });
    }
    units
}

/// Render in parallel, write in batches.
///
/// **`import osm`'s deliberate refusal to parallelize does not apply here**,
/// and the difference is the point: that pass measured as bound by RocksDB
/// write throughput and the sequential blob read, so 12 cores bought 41s on a
/// 5-minute run. Warming is bound by DuckDB query CPU, which is exactly what
/// scales -- and the server's own pool already runs these same queries
/// concurrently, so nothing here is new load. Measured shape: ~2.7 CPU-hours
/// serial for all of Poland, ~15 minutes on 12 cores.
///
/// One writer thread rather than N: `WriteBatch` is not shared, and batching
/// is the other half of the win (one write per 32 MB instead of one per tile).
/// The channel between them is **bounded** -- a 463 KB tile per slot, so an
/// unbounded queue would be a memory spike waiting to happen.
fn warm(
    conn: &Connection,
    store: &TileStore,
    bbox: Option<&str>,
    jobs: Option<usize>,
    _config: &Config,
) -> Result<()> {
    let all = covered_tiles(conn, bbox)?;
    // Skipping what is already stored is what makes an interrupted warm
    // resumable rather than a restart. `may_exist` may say yes for a key that
    // is absent; the cost is one tile not warmed, which the next request
    // renders anyway.
    let todo: Vec<TileKey> = all.into_iter().filter(|k| !store.may_exist(*k)).collect();
    if todo.is_empty() {
        info!("tile store already warm, nothing to do");
        return Ok(());
    }

    let workers = jobs.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });
    info!(tiles = todo.len(), workers, "warming tile store");

    let pb = ProgressBar::new(todo.len() as u64);
    pb.set_style(
        ProgressStyle::with_template("{bar:40} {pos}/{len} tiles ({eta} left)")
            .unwrap_or_else(|_| ProgressStyle::default_bar()),
    );

    let next = Arc::new(AtomicUsize::new(0));
    let units = Arc::new(warm_units(&todo));
    let failed = Arc::new(AtomicU64::new(0));
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<(TileKey, String, crate::server::tile_store::TileBody)>(
            workers * 4,
        );

    let written = std::thread::scope(|scope| -> Result<u64> {
        for _ in 0..workers {
            // Each worker gets its own connection: `try_clone` opens a new one
            // rather than sharing, which is what lets these run concurrently
            // at all. Note the cancellation limit that comes with it --
            // `shutdown::register_interrupt_handle` covers only the CLI's
            // original connection, so a Ctrl+C lets each worker finish its
            // in-flight tile (seconds) before the poll below stops it.
            let worker_conn = conn.try_clone().context("tiles warm: clone connection")?;
            let units = units.clone();
            let next = next.clone();
            let failed = failed.clone();
            let tx = tx.clone();
            scope.spawn(move || {
                loop {
                    if crate::shutdown::is_requested() {
                        return;
                    }
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(unit) = units.get(i) else {
                        return;
                    };
                    // A whole unit fails together: one batched query either
                    // produces every tile in its run or none of them. That is
                    // coarser than the per-tile failure this replaced, and
                    // acceptable for the same reason a failed render is
                    // acceptable at all -- the tile is simply not warmed, and
                    // the next request renders it.
                    let rendered = match unit {
                        WarmUnit::Z14(key) => render_tile(&worker_conn, key.0, key.1, key.2)
                            .map(|raw| vec![(*key, raw)]),
                        WarmUnit::Points { z, tiles } => {
                            render_points_tiles(&worker_conn, *z, tiles).map(|out| {
                                out.into_iter()
                                    .map(|((x, y), raw)| ((*z, x, y), raw))
                                    .collect()
                            })
                        }
                    };
                    let rendered: Vec<(TileKey, Vec<u8>)> = match rendered {
                        Ok(r) => r,
                        Err(e) => {
                            failed.fetch_add(unit.tiles() as u64, Ordering::Relaxed);
                            tracing::warn!(error = %e, tiles = unit.tiles(), "tiles warm: render failed");
                            continue;
                        }
                    };
                    for (key, raw) in rendered {
                        match crate::server::tile_store::prepare(&raw) {
                            Ok((etag, body)) => {
                                // A closed receiver means the writer died; stop.
                                if tx.send((key, etag, body)).is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                failed.fetch_add(1, Ordering::Relaxed);
                                let (z, x, y) = key;
                                tracing::warn!(error = %e, z, x, y, "tiles warm: prepare failed");
                            }
                        }
                    }
                }
            });
        }
        // Dropped so the writer's loop ends once every worker has finished.
        drop(tx);

        let mut batch = store.batch();
        let mut written = 0u64;
        for (key, etag, body) in rx {
            store.batch_put(&mut batch, key, &etag, &body);
            store.flush_if_full(&mut batch)?;
            written += 1;
            pb.inc(1);
        }
        store.write_batch(batch)?;
        Ok(written)
    })?;

    pb.finish_and_clear();
    let failed = failed.load(Ordering::Relaxed);
    if failed > 0 {
        tracing::warn!(written, failed, "tile warm complete with failures");
    } else {
        info!(written, "tile warm complete");
    }
    crate::shutdown::check_requested()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Y is inverted in Web Mercator: higher latitude is a *smaller* `cell_y`,
    /// so `max_lat` must map to the minimum index. Backwards, this yields an
    /// empty range and silently warms nothing.
    #[test]
    fn a_bbox_maps_to_a_cell_range_with_y_inverted() {
        let (lo_x, lo_y, hi_x, hi_y) = parse_bbox_cells("21.0,52.0,21.2,52.2").unwrap();
        assert!(lo_x < hi_x, "x grows eastward");
        assert!(
            lo_y < hi_y,
            "y grows southward, so the north edge is the min"
        );
        // The northwest corner really is (lo_x, lo_y).
        let (nw_x, nw_y) = lonlat_to_tile(21.0, 52.2, crate::tile_math::CHANGE_CELL_ZOOM);
        assert_eq!((nw_x as i32, nw_y as i32), (lo_x, lo_y));
    }

    /// z14 keys stay one unit each; z12/z13 keys collapse into batched runs
    /// that never straddle a zoom.
    ///
    /// The zoom boundary matters more than the size cap: `render_points_tiles`
    /// takes a single `z` and derives every tile's envelope and cell range
    /// from it, so a run mixing z12 and z13 would render half its tiles at the
    /// wrong zoom rather than fail.
    #[test]
    fn warm_units_batch_the_points_tiers_per_zoom_and_leave_z14_alone() {
        let mut todo: Vec<TileKey> = Vec::new();
        todo.extend((0..POINTS_BATCH_TILES as u32 + 3).map(|i| (12, 100, i)));
        todo.extend((0..5u32).map(|i| (13, 200, i)));
        todo.extend((0..2u32).map(|i| (14, 300, i)));

        let units = warm_units(&todo);
        assert_eq!(
            units.iter().map(WarmUnit::tiles).sum::<usize>(),
            todo.len(),
            "every requested tile must land in exactly one unit"
        );
        let z14: Vec<_> = units
            .iter()
            .filter(|u| matches!(u, WarmUnit::Z14(_)))
            .collect();
        assert_eq!(z14.len(), 2, "z14 keys are never batched");
        let runs: Vec<(u32, usize)> = units
            .iter()
            .filter_map(|u| match u {
                WarmUnit::Points { z, tiles } => Some((*z, tiles.len())),
                WarmUnit::Z14(_) => None,
            })
            .collect();
        assert_eq!(
            runs,
            vec![(12, POINTS_BATCH_TILES), (12, 3), (13, 5)],
            "runs split on the size cap and on the zoom change, never across one"
        );
    }

    /// A `--bbox` has to exclude a cell that is outside it on *either* axis.
    /// Requiring both put every cell sharing a longitude band with Warsaw into
    /// a Warsaw warm, which reads as "the bbox is being ignored" only if you
    /// happen to count the tiles.
    #[test]
    fn a_bbox_excludes_a_cell_outside_it_on_either_axis_alone() {
        let (lo_x, lo_y, hi_x, hi_y) = parse_bbox_cells("21.0,52.0,21.2,52.2").unwrap();
        let inside = |cell_x: i32, cell_y: i32| {
            (lo_x..=hi_x).contains(&cell_x) && (lo_y..=hi_y).contains(&cell_y)
        };
        assert!(inside(lo_x, lo_y), "the northwest corner is inside");
        assert!(inside(hi_x, hi_y), "the southeast corner is inside");
        assert!(!inside(lo_x, hi_y + 500), "same longitude, far south");
        assert!(!inside(hi_x + 500, lo_y), "same latitude, far east");
    }

    #[test]
    fn a_malformed_bbox_is_rejected_rather_than_silently_warming_everything() {
        assert!(parse_bbox_cells("21.0,52.0,21.2").is_err());
        assert!(parse_bbox_cells("not,a,bbox,at all").is_err());
    }
}
