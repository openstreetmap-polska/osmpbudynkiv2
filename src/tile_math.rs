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
// `cell_x_sql` and `cell_y_sql` below are the ONLY home for the Web-Mercator
// XYZ projection expressed in SQL. Every producer of `match_dirty_cells` and
// every writer of a `cell_x`/`cell_y` column goes through them, so the cell a
// row is tagged with and the cell a query looks for cannot drift apart. Their
// Rust inverse is [`lonlat_to_tile`], and `cell_sql_matches_lonlat_to_tile`
// pins the two together over a spread of coordinates.
//
// (This note used to sit on `cell_x_sql` alone, which made it look like a
// property of the X projection rather than of the pair.)

/// The `2^CHANGE_CELL_ZOOM` factor shared by both cell projections.
fn cell_zoom_factor_sql() -> String {
    format!("pow(2, {CHANGE_CELL_ZOOM})")
}

/// SQL for the Web-Mercator XYZ tile X of `point_expr` at [`CHANGE_CELL_ZOOM`].
pub fn cell_x_sql(point_expr: &str) -> String {
    let n = cell_zoom_factor_sql();
    format!("floor((ST_X({point_expr}) + 180) / 360 * {n})::INTEGER")
}

/// SQL for the Web-Mercator XYZ tile Y of `point_expr` at [`CHANGE_CELL_ZOOM`].
pub fn cell_y_sql(point_expr: &str) -> String {
    let n = cell_zoom_factor_sql();
    format!(
        "floor((1 - ln(tan(radians(ST_Y({point_expr}))) + 1 / cos(radians(ST_Y({point_expr})))) \
         / pi()) / 2 * {n})::INTEGER"
    )
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
}
