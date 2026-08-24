use std::collections::HashSet;

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::compare::rule::OSM_MATCH_BUFFER_DEG;
use crate::tile_math::{CHANGE_CELL_ZOOM, cell_x_frac_sql, cell_y_frac_sql, lonlat_to_tile};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Buildings,
    Addresses,
}

/// Degrees of read buffer around a layer's recorded cell(s) that a caller
/// must widen its enqueue reach by, to match what that layer's match rule
/// actually reads from OSM at recompute time. This is the one place that
/// reach is decided, so a future change to either rule's OSM read only needs
/// updating here.
///
/// **Buildings** (0.0, unbuffered): `rule::unmatched_buildings_sql`, via the
/// shared `buildings_predicate` builder, scopes both the `osm_buildings` and
/// `osm_former_buildings` `ST_Intersects` tests to the exact same `envelope`
/// used to scope the government row itself -- no `buffer()` call anywhere in
/// that path (verified by reading `rule.rs`). So a changed OSM building or
/// former-building polygon can only affect the cell(s) its own bbox touches.
///
/// **Addresses** (`OSM_MATCH_BUFFER_DEG`): `rule::unmatched_addresses_in_cell_sql`
/// takes separate `write`/`read` bounds and reads `osm_addresses` from
/// `read`; every caller (`compare::incremental::build_sql`'s prg branch)
/// passes `rule::buffer(write, OSM_MATCH_BUFFER_DEG)` for it. So a changed
/// OSM address point can affect its own cell plus any neighbour whose
/// buffered envelope reaches it.
///
/// If the buildings rule ever grows a read buffer of its own, this constant
/// must grow to match -- otherwise the enqueue reach silently falls short of
/// what a recompute reads, the same "read wide, write narrow" trap
/// `OSM_MATCH_BUFFER_DEG`'s own doc comment (`compare::rule`) warns about for
/// `MATCH_DISTANCE_METERS`. The blast radius of getting this wrong is
/// bounded, not silent forever, though: `queue reconcile` (the daily
/// `match_reconcile` job) re-enqueues every live cell regardless, so a
/// subtly-too-narrow reach degrades to "stale until the next sweep", not
/// "wrong forever".
fn layer_buffer_deg(layer: Layer) -> f64 {
    match layer {
        Layer::Buildings => 0.0,
        Layer::Addresses => OSM_MATCH_BUFFER_DEG,
    }
}

/// Ceiling on how many z14 cells one OSM row may enqueue in
/// [`DirtyCells::note_existing`]. A row whose bbox demands more is skipped
/// with a warning rather than expanded.
///
/// The reach itself is exact and must stay that way (see `note_existing`'s
/// doc and the CLAUDE.md gotcha on the 5.6x amplification the old fixed 3x3
/// caused), so this is not a tuning knob for normal data -- it is a guard
/// against one input class that government exports do not have and OSM does:
/// a node dragged to (0, 0). Measured over the 2026-08 Poland extract, the
/// widest real `osm_buildings` row spans 0.7306 z14 cells, so every genuine
/// building touches at most 2x2 = 4 cells; a Polish building way with one
/// Null Island node spans lon 21deg->0 and lat 52deg->0, which is 955 x 2781
/// = **2,659,592** cells inserted into the HashSet and then into
/// `match_dirty_cells`, inside the update transaction. At the ~0.098 s per
/// z14 recompute measured in docs/per_cell_recompute_cell_guard_scan.md that
/// is days of drain backlog produced by a single way edit.
///
/// 1024 (a 32x32 cell block, roughly 48 km square at Polish latitudes) sits
/// 256x above the widest real row and far below the corrupt case, so no
/// plausible building is affected either way. Skipping rather than clamping
/// is safe because `queue reconcile` re-enqueues every live cell on its
/// daily sweep: a skipped row degrades to "stale until the next sweep", the
/// same failure bound `layer_buffer_deg`'s doc describes for a too-narrow
/// reach -- whereas enqueueing the millions would take the drain out for days.
const MAX_ENQUEUE_CELLS_PER_ROW: i64 = 1024;

#[derive(Default)]
pub struct DirtyCells {
    buildings: HashSet<(i32, i32)>,
    addresses: HashSet<(i32, i32)>,
}

impl DirtyCells {
    pub fn new() -> Self {
        Self::default()
    }

    fn set(&mut self, layer: Layer) -> &mut HashSet<(i32, i32)> {
        match layer {
            Layer::Buildings => &mut self.buildings,
            Layer::Addresses => &mut self.addresses,
        }
    }

    /// Record every cell touched by the point's neighbourhood, buffered per
    /// [`layer_buffer_deg`] (node fast path -- no query).
    ///
    /// Needs no [`MAX_ENQUEUE_CELLS_PER_ROW`] guard, unlike
    /// [`Self::note_existing`]: the range here spans a fixed buffer around a
    /// single point rather than an arbitrary bbox, so it is at most 2x2 cells
    /// whatever the coordinates are. A node dragged to (0, 0) enqueues that
    /// one wrong cell, not a rectangle stretching back to Poland.
    pub fn note_point(&mut self, layer: Layer, lon: f64, lat: f64) {
        if lon.is_finite() && lat.is_finite() {
            let buf = layer_buffer_deg(layer);
            // Y is inverted relative to X: higher latitude means a SMALLER
            // tile y, so the north corner (lat + buf) gives the low y index
            // and the south corner (lat - buf) gives the high one.
            let (x_lo, y_lo) = lonlat_to_tile(lon - buf, lat + buf, CHANGE_CELL_ZOOM);
            let (x_hi, y_hi) = lonlat_to_tile(lon + buf, lat - buf, CHANGE_CELL_ZOOM);
            for x in x_lo..=x_hi {
                for y in y_lo..=y_hi {
                    self.set(layer).insert((x as i32, y as i32));
                }
            }
        }
    }

    /// Record every cell touched by the bbox of a row currently in `table`
    /// for (osm_id, osm_type), buffered per [`layer_buffer_deg`]. A no-op
    /// when the row is absent (nothing to leave from).
    ///
    /// Uses `ST_XMin`/`ST_XMax`/`ST_YMin`/`ST_YMax` rather than a single
    /// representative point, so a building or address whose (buffered) bbox
    /// straddles a cell boundary enqueues every cell it actually touches --
    /// the old fixed 3x3 expansion this replaces existed only to paper over
    /// reading just one cell. `cell_x_frac_sql`/`cell_y_frac_sql` take a bare
    /// scalar expression rather than a point, precisely so a bbox edge can be
    /// passed directly -- this also drops the old `ST_Centroid` routing,
    /// which existed only because `ST_X`/`ST_Y` require a POINT geometry.
    pub fn note_existing(
        &mut self,
        conn: &Connection,
        layer: Layer,
        table: &str,
        osm_id: i64,
        osm_type: &str,
    ) -> Result<()> {
        let buf = layer_buffer_deg(layer);
        let x_lo = cell_x_frac_sql(&format!("ST_XMin(geom) - {buf}"));
        let x_hi = cell_x_frac_sql(&format!("ST_XMax(geom) + {buf}"));
        // Same Y inversion as note_point above: the row's north edge
        // (ST_YMax) gives the low cell_y index, its south edge (ST_YMin)
        // gives the high one.
        let y_lo = cell_y_frac_sql(&format!("ST_YMax(geom) + {buf}"));
        let y_hi = cell_y_frac_sql(&format!("ST_YMin(geom) - {buf}"));
        let sql = format!(
            "SELECT floor({x_lo})::INTEGER, floor({x_hi})::INTEGER,
                    floor({y_lo})::INTEGER, floor({y_hi})::INTEGER
             FROM {table}
             WHERE osm_id = ? AND osm_type = ? AND geom IS NOT NULL"
        );
        let mut stmt = conn
            .prepare(&sql)
            .with_context(|| format!("note_existing prepare {table}"))?;
        let rows = stmt.query_map(duckdb::params![osm_id, osm_type], |r| {
            Ok((
                r.get::<_, i32>(0)?,
                r.get::<_, i32>(1)?,
                r.get::<_, i32>(2)?,
                r.get::<_, i32>(3)?,
            ))
        })?;
        for row in rows {
            let (x_lo, x_hi, y_lo, y_hi) = row?;
            // Widths as i64 before multiplying: the corrupt case this guards
            // against reaches ~2.7M cells, and a row spanning the whole world
            // would overflow an i32 product.
            let cells = (x_hi as i64 - x_lo as i64 + 1) * (y_hi as i64 - y_lo as i64 + 1);
            if cells > MAX_ENQUEUE_CELLS_PER_ROW {
                tracing::warn!(
                    table,
                    osm_id,
                    osm_type,
                    cells,
                    max = MAX_ENQUEUE_CELLS_PER_ROW,
                    "Skipping dirty-cell enqueue for an implausibly large OSM row \
                     (likely a node at 0,0) — reconcile will pick these cells up"
                );
                continue;
            }
            for x in x_lo..=x_hi {
                for y in y_lo..=y_hi {
                    self.set(layer).insert((x, y));
                }
            }
        }
        Ok(())
    }

    /// Insert every recorded cell into match_dirty_cells: buildings ->
    /// bdot10k+egib, addresses -> prg.
    ///
    /// `note_existing`/`note_point` already record the exact cell reach each
    /// source's match rule can read from (see [`layer_buffer_deg`]), so
    /// nothing expands here anymore. The old fixed 3x3 neighbourhood measured
    /// 5.6x amplification on the live queue (~229 genuinely-edited cells ->
    /// 1277 enqueued) for a margin 98.08% of real building footprints never
    /// needed -- they touch exactly one z14 cell.
    pub fn flush(&self, conn: &Connection) -> Result<()> {
        let z = CHANGE_CELL_ZOOM as i32;
        Self::insert_cells(conn, "bdot10k", z, &self.buildings)?;
        Self::insert_cells(conn, "egib", z, &self.buildings)?;
        Self::insert_cells(conn, "prg", z, &self.addresses)?;
        Ok(())
    }

    /// One multi-row INSERT for `source`, skipped entirely when `cells` is
    /// empty. One `stmt.execute` per cell instead would be thousands of
    /// statements per flush on a real queue. `now()`
    /// is written as literal SQL text, not a bound parameter, so every row in
    /// the statement gets the identical transaction-start timestamp -- see
    /// CLAUDE.md's "`now()` is transaction-start-scoped" gotcha. An
    /// `Appender` would be the wrong tool here for the same reason: it binds
    /// literal values, not SQL expressions, so `now()` would need a separate
    /// round trip. `source` is always one of the three literal strings
    /// `flush` passes in, never caller-controlled input.
    fn insert_cells(
        conn: &Connection,
        source: &str,
        z: i32,
        cells: &HashSet<(i32, i32)>,
    ) -> Result<()> {
        if cells.is_empty() {
            return Ok(());
        }
        let values = cells
            .iter()
            .map(|(x, y)| format!("('{source}', {z}, {x}, {y}, now())"))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!("INSERT INTO match_dirty_cells VALUES {values}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use crate::tile_math::tile_to_bbox;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    #[test]
    fn flush_inserts_the_recorded_cell_and_fans_out_by_layer() {
        let c = conn();
        let mut d = DirtyCells::default();
        d.note_point(Layer::Buildings, 21.0, 52.0);
        d.flush(&c).unwrap();

        // Buildings fan out to bdot10k + egib; a single point (buf 0) is
        // exactly 1 cell each now, not a 3x3 neighbourhood.
        let (bx, by) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
        let bdot: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let egib: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='egib'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!((bdot, egib), (1, 1));
        // The recorded cell itself is present.
        let center: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k' AND cell_x=? AND cell_y=?",
                duckdb::params![bx as i32, by as i32],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(center, 1);
        // Addresses untouched.
        let prg: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='prg'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(prg, 0);
    }

    /// The Null Island case, at full scale: a Polish building way with one
    /// node dragged to (0, 0). Without the cap this enqueues 955 x 2781 =
    /// 2,659,592 cells from a single edit; with it, nothing is enqueued and
    /// the daily reconcile sweep is what picks the real cells up.
    #[test]
    fn note_existing_skips_a_row_whose_bbox_spans_an_implausible_number_of_cells() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES (9,'way',NULL, ST_MakeEnvelope(0.0,0.0,21.0,52.0));",
        )
        .unwrap();
        let mut d = DirtyCells::default();
        d.note_existing(&c, Layer::Buildings, "osm_buildings", 9, "way")
            .unwrap();
        d.flush(&c).unwrap();

        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "an implausibly large row must enqueue nothing");
    }

    /// The other side of the same guard: a row right at the cap's scale still
    /// enqueues normally, so the cap cannot quietly swallow ordinary edits.
    /// 0.02 degrees of longitude is a little under one z14 cell at this
    /// latitude, i.e. far larger than any real building and still nowhere
    /// near 1024 cells.
    #[test]
    fn note_existing_still_enqueues_a_merely_large_row() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES (10,'way',NULL, ST_MakeEnvelope(21.0,52.0,21.02,52.02));",
        )
        .unwrap();
        let mut d = DirtyCells::default();
        d.note_existing(&c, Layer::Buildings, "osm_buildings", 10, "way")
            .unwrap();
        d.flush(&c).unwrap();

        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            n > 1 && n <= MAX_ENQUEUE_CELLS_PER_ROW,
            "a large-but-plausible row must still enqueue its cells, got {n}"
        );
    }

    #[test]
    fn note_existing_reads_geom_cell_from_table() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES (5,'way',NULL, ST_MakeEnvelope(21.0,52.0,21.001,52.001));",
        )
        .unwrap();
        let mut d = DirtyCells::default();
        d.note_existing(&c, Layer::Buildings, "osm_buildings", 5, "way")
            .unwrap();
        d.flush(&c).unwrap();
        let (bx, by) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);
        let center: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k' AND cell_x=? AND cell_y=?",
                duckdb::params![bx as i32, by as i32],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(center, 1);
    }

    /// A building whose bbox straddles a real z14 cell boundary must enqueue
    /// both cells it touches, not just the one its centroid lands in.
    /// Boundary coordinates come straight from `tile_to_bbox`'s own computed
    /// f64 output rather than hand-typed decimals, and the offset past it is
    /// a dyadic fraction (exact in f64) -- see the CLAUDE.md gotcha on
    /// hand-written geometry fixtures needing exact binary fractions, and
    /// `tile_math::cell_frac_is_unfloored_across_a_cell_boundary`, which uses
    /// the same technique.
    #[test]
    fn note_existing_records_both_cells_a_straddling_bbox_touches() {
        let c = conn();
        let (min_lon, min_lat, _, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, 9147, 5411);
        let mid_lat = (min_lat + max_lat) / 2.0;
        let shift = 1.0 / 8192.0; // ~13.7m at this latitude; exact in f64.

        // Straddles the west edge of cell (9147, 5411) -- half in (9146,
        // 5411), half in (9147, 5411) -- while staying well clear of any y
        // boundary.
        c.execute_batch(&format!(
            "INSERT INTO osm_buildings VALUES (7, 'way', NULL, ST_MakeEnvelope(
                 {}, {}, {}, {}));",
            min_lon - shift,
            mid_lat - shift,
            min_lon + shift,
            mid_lat + shift
        ))
        .unwrap();

        let mut d = DirtyCells::default();
        d.note_existing(&c, Layer::Buildings, "osm_buildings", 7, "way")
            .unwrap();
        d.flush(&c).unwrap();

        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 2,
            "a bbox straddling one boundary must enqueue exactly 2 cells"
        );
        for (cx, cy) in [(9146, 5411), (9147, 5411)] {
            let hit: i64 = c
                .query_row(
                    "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k' AND cell_x=? AND cell_y=?",
                    duckdb::params![cx, cy],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(hit, 1, "expected cell ({cx},{cy}) to be enqueued");
        }
    }

    /// The corner case: a bbox straddling the shared corner of four cells
    /// must enqueue all four. `tile_to_bbox(z, 9147, 5411)`'s own
    /// (min_lon, min_lat) corner is simultaneously the SE corner of
    /// (9146, 5411), the SW corner of (9147, 5411), the NE corner of
    /// (9146, 5412) and the NW corner of (9147, 5412) -- verified by reading
    /// `tile_to_bbox`: its `min_lat` for tile y uses the same formula as its
    /// `max_lat` for tile y+1, so the two are bit-for-bit equal.
    #[test]
    fn note_existing_records_all_four_cells_at_a_shared_corner() {
        let c = conn();
        let (min_lon, min_lat, _, _) = tile_to_bbox(CHANGE_CELL_ZOOM, 9147, 5411);
        let shift = 1.0 / 8192.0; // ~13.7m at this latitude; exact in f64.

        c.execute_batch(&format!(
            "INSERT INTO osm_buildings VALUES (8, 'way', NULL, ST_MakeEnvelope(
                 {}, {}, {}, {}));",
            min_lon - shift,
            min_lat - shift,
            min_lon + shift,
            min_lat + shift
        ))
        .unwrap();

        let mut d = DirtyCells::default();
        d.note_existing(&c, Layer::Buildings, "osm_buildings", 8, "way")
            .unwrap();
        d.flush(&c).unwrap();

        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 4,
            "a bbox straddling a shared corner must enqueue exactly 4 cells"
        );
        for (cx, cy) in [(9146, 5411), (9147, 5411), (9146, 5412), (9147, 5412)] {
            let hit: i64 = c
                .query_row(
                    "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k' AND cell_x=? AND cell_y=?",
                    duckdb::params![cx, cy],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(hit, 1, "expected cell ({cx},{cy}) to be enqueued");
        }
    }

    /// An address point close enough to a cell edge that
    /// `OSM_MATCH_BUFFER_DEG` reaches past it must enqueue both cells; one
    /// safely inside must enqueue only its own.
    ///
    /// Correct by construction for any `OSM_MATCH_BUFFER_DEG` smaller than
    /// half a z14 cell in both axes (see the sibling
    /// `note_point_address_at_cell_centre_stays_in_one_cell` test, which
    /// pins that precondition) -- the offset below is derived from the
    /// constant itself rather than a hardcoded distance, so it stays a valid
    /// "closer to the edge than one buffer-width" probe whatever the
    /// constant's current value is.
    #[test]
    fn note_point_address_near_a_cell_edge_reaches_the_neighbour() {
        let c = conn();
        let (min_lon, min_lat, _, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, 9147, 5411);
        let mid_lat = (min_lat + max_lat) / 2.0;
        // Half a buffer-width inside the west edge -- closer to the edge
        // than OSM_MATCH_BUFFER_DEG itself, so the buffered read reaches
        // past it regardless of the constant's value.
        let lon = min_lon + OSM_MATCH_BUFFER_DEG / 2.0;

        let mut d = DirtyCells::default();
        d.note_point(Layer::Addresses, lon, mid_lat);
        d.flush(&c).unwrap();

        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='prg'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 2,
            "a point within the match buffer of a cell edge must enqueue both neighbours"
        );
    }

    /// The property this pins: `OSM_MATCH_BUFFER_DEG` is smaller than half a
    /// z14 cell in both axes, so a point at the cell's centre -- the point
    /// furthest from every edge -- cannot have its buffered envelope reach a
    /// neighbour. No offset is derived from the constant here (the centre
    /// needs none), but the assertion only holds under that precondition; if
    /// `OSM_MATCH_BUFFER_DEG` ever grows past half a cell width at Polish
    /// latitudes, this test would start failing, which is the correct signal
    /// to revisit the reach rather than a false alarm.
    #[test]
    fn note_point_address_at_cell_centre_stays_in_one_cell() {
        let c = conn();
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, 9147, 5411);
        let mid_lon = (min_lon + max_lon) / 2.0;
        let mid_lat = (min_lat + max_lat) / 2.0;

        let mut d = DirtyCells::default();
        d.note_point(Layer::Addresses, mid_lon, mid_lat);
        d.flush(&c).unwrap();

        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='prg'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "a point in the interior of a cell, further from every edge than \
             OSM_MATCH_BUFFER_DEG, must enqueue only its own cell"
        );
    }
}
