use std::f64::consts::PI;

/// Zoom level at which dataset change areas are aggregated. Matches the
/// highest zoom `/tiles` serves, so a change cell maps 1:1 onto a served
/// tile for cache invalidation.
pub const CHANGE_CELL_ZOOM: u32 = 14;

/// Bounding box of an XYZ tile as (min_lon, min_lat, max_lon, max_lat).
pub fn tile_to_bbox(z: u32, x: u32, y: u32) -> (f64, f64, f64, f64) {
    let n = 2f64.powi(z as i32);
    let min_lon = x as f64 / n * 360.0 - 180.0;
    let max_lon = (x + 1) as f64 / n * 360.0 - 180.0;
    let max_lat = (PI * (1.0 - 2.0 * y as f64 / n)).sinh().atan() * 180.0 / PI;
    let min_lat = (PI * (1.0 - 2.0 * (y + 1) as f64 / n)).sinh().atan() * 180.0 / PI;
    (min_lon, min_lat, max_lon, max_lat)
}

/// XYZ tile containing a lon/lat point. Inverse of [`tile_to_bbox`].
///
/// Test-only in the binary: the production change-area path computes tiles in
/// SQL (see `update::changeset`), and this is the Rust side that keeps those
/// two projections honest via round-trip assertions.
#[allow(dead_code)]
pub fn lonlat_to_tile(lon: f64, lat: f64, z: u32) -> (u32, u32) {
    let n = 2f64.powi(z as i32);
    let x = ((lon + 180.0) / 360.0 * n).floor();
    let lat_rad = lat.to_radians();
    let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * n).floor();
    (x as u32, y as u32)
}

// --- The SQL projection ---------------------------------------------------
//
// `cell_x_sql` / `cell_y_sql` and their unfloored siblings `cell_x_frac_sql`
// / `cell_y_frac_sql` below are the ONLY home for the Web-Mercator XYZ
// projection expressed in SQL. Every producer of `match_dirty_cells` and
// every writer of a `cell_x`/`cell_y` column goes through the floored pair,
// so the cell a row is tagged with and the cell a query looks for cannot
// drift apart. Their Rust inverse is [`lonlat_to_tile`], and
// `cell_sql_matches_lonlat_to_tile` pins the two together over a spread of
// coordinates.
//
// (This note used to sit on `cell_x_sql` alone, which made it look like a
// property of the X projection rather than of the pair.)
//
// The frac pair exists for `dataset::filter_oversized_geometry`, which needs
// the *unfloored* fractional cell coordinate of a bbox edge (`ST_XMin`,
// `ST_XMax`, ...), not the cell index a point falls in -- flooring first and
// then subtracting indices would compare which cell each edge's index lands
// in rather than how far apart the edges actually are. `cell_x_sql` /
// `cell_y_sql` are now defined as `floor(...)` of the frac functions, so
// there is still exactly one place the arithmetic is written.

/// The `2^CHANGE_CELL_ZOOM` factor shared by both cell projections.
fn cell_zoom_factor_sql() -> String {
    format!("pow(2, {CHANGE_CELL_ZOOM})")
}

/// SQL for the unfloored fractional Web-Mercator cell X of `lon_expr`, a
/// longitude scalar in degrees, at [`CHANGE_CELL_ZOOM`]. Takes a scalar
/// rather than a point expression so a caller can pass `ST_XMin(geom)` /
/// `ST_XMax(geom)` directly, without wrapping the bbox edge back into
/// `ST_Point`. [`cell_x_sql`] is `floor(...)::INTEGER` of this, applied to
/// `ST_X(point_expr)`.
pub fn cell_x_frac_sql(lon_expr: &str) -> String {
    let n = cell_zoom_factor_sql();
    format!("(({lon_expr}) + 180) / 360 * {n}")
}

/// SQL for the unfloored fractional Web-Mercator cell Y of `lat_expr`, a
/// latitude scalar in degrees, at [`CHANGE_CELL_ZOOM`]. Mirror of
/// [`cell_x_frac_sql`] for the Y axis; see [`cell_y_sql`].
pub fn cell_y_frac_sql(lat_expr: &str) -> String {
    let n = cell_zoom_factor_sql();
    format!("(1 - ln(tan(radians({lat_expr})) + 1 / cos(radians({lat_expr}))) / pi()) / 2 * {n}")
}

/// SQL for the Web-Mercator XYZ tile X of `point_expr` at [`CHANGE_CELL_ZOOM`].
pub fn cell_x_sql(point_expr: &str) -> String {
    let frac = cell_x_frac_sql(&format!("ST_X({point_expr})"));
    format!("floor({frac})::INTEGER")
}

/// SQL for the Web-Mercator XYZ tile Y of `point_expr` at [`CHANGE_CELL_ZOOM`].
pub fn cell_y_sql(point_expr: &str) -> String {
    let frac = cell_y_frac_sql(&format!("ST_Y({point_expr})"));
    format!("floor({frac})::INTEGER")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verified by hand against the SQL form used in the changeset builder:
    /// lon=21.0, lat=52.0 at z14 lands in tile (9147, 5411).
    #[test]
    fn lonlat_to_tile_known_point() {
        assert_eq!(lonlat_to_tile(21.0, 52.0, 14), (9147, 5411));
    }

    /// The tile a point maps to must be the tile whose bbox contains it.
    /// This is the property that keeps the Rust and SQL forms honest.
    #[test]
    fn tile_contains_the_point_that_produced_it() {
        for (lon, lat) in [(21.0, 52.0), (14.5, 49.35), (23.88, 54.54), (19.94, 50.06)] {
            let (x, y) = lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM);
            let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, x, y);
            assert!(
                min_lon <= lon && lon <= max_lon && min_lat <= lat && lat <= max_lat,
                "tile ({x},{y}) bbox ({min_lon},{min_lat},{max_lon},{max_lat}) \
                 does not contain ({lon},{lat})"
            );
        }
    }

    /// Known bbox for the tile above, to catch a silently changed formula.
    #[test]
    fn tile_to_bbox_known_tile() {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 9147, 5411);
        assert!((min_lon - 20.983887).abs() < 1e-5, "min_lon was {min_lon}");
        assert!((min_lat - 51.998410).abs() < 1e-5, "min_lat was {min_lat}");
        assert!((max_lon - 21.005859).abs() < 1e-5, "max_lon was {max_lon}");
        assert!((max_lat - 52.011937).abs() < 1e-5, "max_lat was {max_lat}");
    }

    #[test]
    fn cell_sql_matches_lonlat_to_tile() {
        use crate::db::init_db;
        use std::path::Path;
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        for (lon, lat) in [(21.0, 52.0), (14.5, 49.35), (23.88, 54.54), (19.94, 50.06)] {
            let (rx, ry) = lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM);
            let sql = format!(
                "SELECT {}, {}",
                cell_x_sql(&format!("ST_Point({lon}, {lat})")),
                cell_y_sql(&format!("ST_Point({lon}, {lat})")),
            );
            let (sx, sy): (i32, i32) = conn
                .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap();
            assert_eq!(
                (sx as u32, sy as u32),
                (rx, ry),
                "mismatch at ({lon},{lat})"
            );
        }
    }

    /// Pins the reason `dataset::filter_oversized_geometry` must be built
    /// from the unfloored frac functions and not from `cell_x_sql` itself:
    /// two points ~27m apart (well under a single ~1.5km z14 cell) that
    /// happen to straddle a real cell boundary floor to DIFFERENT cell
    /// indices. If the extent test naively subtracted floored indices
    /// (`cell_x_sql(XMax) - cell_x_sql(XMin)`), it would read as "spans 1
    /// full cell" and delete a legitimate small building purely because of
    /// where it sits relative to a boundary -- position-dependent behaviour
    /// the design deliberately avoids (see the doc comment on
    /// `filter_oversized_geometry`). The unfloored fractional difference
    /// must stay small regardless.
    ///
    /// lon_min/lon_max are dyadic fractions (exact in `f64`, see the
    /// CLAUDE.md gotcha on hand-written geometry fixtures) straddling the
    /// real z14 cell boundary at x=9147 (20.98388671875, from
    /// `tile_to_bbox_known_tile` above) by 1/8192 degree (~13.7m at this
    /// latitude) on each side.
    #[test]
    fn cell_frac_is_unfloored_across_a_cell_boundary() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let lon_min = 20.9837646484375_f64;
        let lon_max = 20.9840087890625_f64;

        let (floor_min, floor_max): (i32, i32) = conn
            .query_row(
                &format!(
                    "SELECT {}, {}",
                    cell_x_sql(&format!("ST_Point({lon_min}, 52.0)")),
                    cell_x_sql(&format!("ST_Point({lon_max}, 52.0)")),
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            (floor_min, floor_max),
            (9146, 9147),
            "the two points must straddle a real cell boundary for this test \
             to be meaningful"
        );

        let frac_diff: f64 = conn
            .query_row(
                &format!(
                    "SELECT ({}) - ({})",
                    cell_x_frac_sql(&lon_max.to_string()),
                    cell_x_frac_sql(&lon_min.to_string()),
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            frac_diff < 1.0,
            "unfloored extent must be a small fraction of a cell, not >= 1 \
             like the floored indices suggest; got {frac_diff}"
        );
    }
}
