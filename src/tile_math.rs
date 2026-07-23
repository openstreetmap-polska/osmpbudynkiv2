use std::f64::consts::PI;

/// Zoom level at which dataset change areas are aggregated. Matches the
/// highest zoom `/tiles` serves, so a change cell maps 1:1 onto a served
/// tile for cache invalidation.
#[allow(dead_code)]
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
#[allow(dead_code)]
pub fn lonlat_to_tile(lon: f64, lat: f64, z: u32) -> (u32, u32) {
    let n = 2f64.powi(z as i32);
    let x = ((lon + 180.0) / 360.0 * n).floor();
    let lat_rad = lat.to_radians();
    let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * n).floor();
    (x as u32, y as u32)
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
}
