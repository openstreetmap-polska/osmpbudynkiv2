//! The single home for "which government object is unmatched against OSM".
//! Both `compare` (full recompute) and `match_refresh` (incremental per-cell)
//! resolve to this rule; the equivalence test in `compare` pins the address
//! grid-key fast path to it.

/// (min_lon, min_lat, max_lon, max_lat).
pub type Bounds = (f64, f64, f64, f64);

pub const MATCH_DISTANCE_METERS: f64 = 50.0;
/// OSM read buffer around a cell for address matching. Matches /package.
pub const OSM_MATCH_BUFFER_DEG: f64 = 0.001;

pub fn buffer(b: Bounds, deg: f64) -> Bounds {
    (b.0 - deg, b.1 - deg, b.2 + deg, b.3 + deg)
}

/// Unmatched building rows: government centroid within `area` and NOT contained
/// by any osm_buildings polygon (osm filtered to `area` for the R-tree scan —
/// no buffer needed: any polygon containing an in-`area` point has a bbox that
/// intersects `area`).
pub fn unmatched_buildings_sql(source_table: &str, select_list: &str, area: Bounds) -> String {
    let (x1, y1, x2, y2) = area;
    format!(
        "SELECT {select_list}
         FROM {source_table} b
         WHERE ST_Intersects(ST_Centroid(b.geom), ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           AND NOT EXISTS (
               SELECT 1 FROM osm_buildings osm
               WHERE ST_Intersects(osm.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
                 AND ST_Contains(osm.geom, ST_Centroid(b.geom))
           )"
    )
}

/// Unmatched address rows: government point within `write` and no osm_addresses
/// point (read from `read`) with equal normalized housenumber within 50 m.
/// NULL housenumber never matches (SQL `= NULL` is never true).
pub fn unmatched_addresses_in_cell_sql(
    source_table: &str,
    select_list: &str,
    write: Bounds,
    read: Bounds,
) -> String {
    let (wx1, wy1, wx2, wy2) = write;
    let (rx1, ry1, rx2, ry2) = read;
    let dist = MATCH_DISTANCE_METERS;
    format!(
        "SELECT {select_list}
         FROM {source_table} a
         WHERE ST_Intersects(a.geom, ST_MakeEnvelope({wx1}, {wy1}, {wx2}, {wy2}))
           AND NOT EXISTS (
               SELECT 1 FROM osm_addresses o
               WHERE ST_Intersects(o.geom, ST_MakeEnvelope({rx1}, {ry1}, {rx2}, {ry2}))
                 AND UPPER(TRIM(o.housenumber)) = UPPER(TRIM(a.numer_porzadkowy))
                 AND ST_Distance_Sphere(o.geom, a.geom) <= {dist}
           )"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> duckdb::Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bsrc (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE asrc (lokalny_id VARCHAR, numer_porzadkowy VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        c
    }

    #[test]
    fn building_contained_by_osm_is_not_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope(20.0,52.0,20.002,52.002));
             INSERT INTO bsrc VALUES ('in', ST_MakeEnvelope(20.0005,52.0005,20.0007,52.0007));
             INSERT INTO bsrc VALUES ('out', ST_MakeEnvelope(21.0,52.0,21.001,52.001));",
        )
        .unwrap();
        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (14.0, 49.0, 25.0, 55.0));
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["out".to_string()],
            "only the uncontained building is unmatched"
        );
    }

    #[test]
    fn address_within_50m_same_hn_is_not_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_addresses VALUES (1,'node','12',NULL,NULL,NULL, ST_Point(21.01,52.2102));
             INSERT INTO asrc VALUES ('match','12', ST_Point(21.01,52.21));
             INSERT INTO asrc VALUES ('far','12', ST_Point(21.01,52.212));",
        )
        .unwrap();
        let area = (21.0, 52.2, 21.02, 52.22);
        let sql = unmatched_addresses_in_cell_sql(
            "asrc",
            "a.lokalny_id",
            area,
            buffer(area, OSM_MATCH_BUFFER_DEG),
        );
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["far".to_string()],
            "the ~22m match drops out, the ~220m one stays"
        );
    }
}
