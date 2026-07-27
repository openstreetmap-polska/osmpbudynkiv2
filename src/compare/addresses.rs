use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::utils::format_duration;

/// Grid cell size in degrees for the spatial grid-key matching strategy.
/// 0.005° ≈ 340 m east-west at 52 °N and ≈ 556 m north-south — both well above
/// the 50 m match distance. Any two addresses within 50 m therefore fall in the
/// same or adjacent grid cells, so a ±1 cell neighbourhood is always sufficient.
const GRID_KEY_DEG: f64 = 0.005;

/// Two address points are considered matching when their (trimmed, uppercased)
/// housenumbers are equal AND they are within this distance in meters.
const MATCH_DISTANCE_METERS: f64 = 50.0;

pub fn compare_prg(conn: &Connection) -> Result<()> {
    info!("Comparing PRG addresses against OSM");
    let t = std::time::Instant::now();

    compare_addresses(conn).context("Failed to compare PRG addresses against OSM")?;

    let total: i64 = conn.query_row("SELECT COUNT(*) FROM prg_addresses", [], |row| row.get(0))?;
    let candidates: i64 =
        conn.query_row("SELECT COUNT(*) FROM prg_unmatched", [], |row| row.get(0))?;

    info!(
        total,
        candidates,
        matched = total - candidates,
        elapsed = %format_duration(t.elapsed()),
        "PRG comparison complete"
    );

    Ok(())
}

/// Compare addresses in a single parallel pass using a spatial grid-key strategy.
///
/// The naive grid-chunked approach suffers from an O(n²) fan-out: common short
/// housenumbers (1, 2, 3 …) appear thousands of times within a 0.5° cell, producing
/// hundreds of millions of distance calculations per cell. The Warsaw cell alone had
/// ~591 million intermediate pairs, taking ~52 s; the full dataset of 264 cells
/// took several hours.
///
/// This function avoids that by assigning each address an integer (gx, gy) key
/// derived from `floor(coord / GRID_KEY_DEG)`. Each source address is expanded to
/// its 3×3 neighbourhood (9 rows) and equality-joined against OSM on
/// (normalised_housenumber, gx, gy). Because GRID_KEY_DEG ≈ 340 m >> 50 m match
/// distance, any genuine within-50-m match is guaranteed to land in the same or an
/// adjacent cell — no valid match is missed. DuckDB parallelises the hash join
/// across all available threads in a single pass; no Rust-level loop is needed.
///
/// Writes only unmatched rows into `prg_unmatched`, tagged with the z14 cell of
/// their point and `computed_at`. `compare` runs offline (no concurrent
/// readers), so this clears the table then inserts.
fn compare_addresses(conn: &Connection) -> Result<()> {
    conn.execute_batch("DELETE FROM prg_unmatched;")?;
    let cx = crate::tile_math::cell_x_sql("s.geom");
    let cy = crate::tile_math::cell_y_sql("s.geom");
    conn.execute_batch(&format!(
        "INSERT INTO prg_unmatched
         (geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
          teryt_miejscowosc, cell_x, cell_y, computed_at)
         WITH
         neighbor_offsets(dx, dy) AS (
             VALUES (-1,-1),(-1,0),(-1,1),(0,-1),(0,0),(0,1),(1,-1),(1,0),(1,1)
         ),
         src_norm AS (
             SELECT lokalny_id,
                    UPPER(TRIM(numer_porzadkowy)) AS _hn,
                    FLOOR(ST_X(geom) / {GRID_KEY_DEG})::BIGINT AS _gx,
                    FLOOR(ST_Y(geom) / {GRID_KEY_DEG})::BIGINT AS _gy,
                    geom
             FROM prg_addresses
         ),
         osm_norm AS (
             SELECT UPPER(TRIM(housenumber)) AS _hn,
                    FLOOR(ST_X(geom) / {GRID_KEY_DEG})::BIGINT AS _gx,
                    FLOOR(ST_Y(geom) / {GRID_KEY_DEG})::BIGINT AS _gy,
                    geom
             FROM osm_addresses
         ),
         src_expanded AS (
             SELECT s.lokalny_id, s._hn, s.geom, s._gx + o.dx AS _sgx, s._gy + o.dy AS _sgy
             FROM src_norm s CROSS JOIN neighbor_offsets o
         ),
         matched_ids AS (
             SELECT DISTINCT s.lokalny_id
             FROM src_expanded s
             JOIN osm_norm o
               ON  s._hn = o._hn AND s._sgx = o._gx AND s._sgy = o._gy
               AND ST_Distance_Sphere(o.geom, s.geom) <= {MATCH_DISTANCE_METERS}
         )
         SELECT s.geom, s.lokalny_id, s.numer_porzadkowy, s.ulica, s.miejscowosc,
                s.kod_pocztowy, s.teryt_miejscowosc, {cx}, {cy}, now()
         FROM prg_addresses s
         WHERE NOT EXISTS (SELECT 1 FROM matched_ids m WHERE m.lokalny_id = s.lokalny_id);"
    ))
    .context("Failed to run address comparison query")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::db::init_db;

    fn setup() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR,
                 numer_porzadkowy VARCHAR,
                 ulica VARCHAR,
                 miejscowosc VARCHAR,
                 kod_pocztowy VARCHAR,
                 teryt_miejscowosc VARCHAR,
                 geom GEOMETRY
             );",
        )
        .unwrap();
        conn
    }

    /// Insert a `prg_addresses` row with only the id/housenumber/geom populated
    /// — the other serving columns are irrelevant to the matching logic.
    fn insert_prg(conn: &Connection, id: &str, hn: &str, point_sql: &str) {
        conn.execute_batch(&format!(
            "INSERT INTO prg_addresses VALUES ('{id}', '{hn}', NULL, NULL, NULL, NULL, {point_sql});"
        ))
        .unwrap();
    }

    fn unmatched_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM prg_unmatched", [], |row| row.get(0))
            .unwrap()
    }

    fn unmatched_ids(conn: &Connection) -> Vec<String> {
        let mut s = conn
            .prepare("SELECT lokalny_id FROM prg_unmatched ORDER BY lokalny_id")
            .unwrap();
        s.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    fn run(conn: &Connection) {
        compare_addresses(conn).unwrap();
    }

    /// Same housenumber, ~22 m apart → match → excluded from prg_unmatched.
    #[test]
    fn matched_within_50m_excluded_from_unmatched() {
        let conn = setup();
        insert_prg(&conn, "p1", "12", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '12', NULL, NULL, NULL, ST_Point(21.01, 52.2102));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 0);
        assert!(
            unmatched_ids(&conn).is_empty(),
            "the matched address must be absent from prg_unmatched"
        );
    }

    /// Same housenumber, ~200 m apart → too far → 1 unmatched row.
    #[test]
    fn same_number_but_too_far_is_unmatched() {
        let conn = setup();
        insert_prg(&conn, "p1", "12", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '12', NULL, NULL, NULL, ST_Point(21.01, 52.212));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 1);
        assert_eq!(unmatched_ids(&conn), vec!["p1".to_string()]);
    }

    /// Different housenumbers at the same point → no match → 1 unmatched row.
    #[test]
    fn different_numbers_within_50m_is_unmatched() {
        let conn = setup();
        insert_prg(&conn, "p1", "12", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '14', NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 1);
    }

    /// TRIM + UPPER normalization: ' 12a ' vs '12A' → match.
    #[test]
    fn trim_and_upper_normalization() {
        let conn = setup();
        insert_prg(&conn, "p1", " 12a ", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '12A', NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 0);
    }

    /// NULL housenumbers on both sides → SQL NULL ≠ NULL in joins → no match → unmatched.
    #[test]
    fn null_housenumbers_dont_match() {
        let conn = setup();
        conn.execute_batch(
            "INSERT INTO prg_addresses VALUES ('p1', NULL, NULL, NULL, NULL, NULL, ST_Point(21.01, 52.21));
             INSERT INTO osm_addresses VALUES (1, 'node', NULL, NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 1);
    }

    /// PRG and OSM points straddle a grid-cell boundary but are within 50 m.
    /// PRG at lon=14.4997 → gx=floor(14.4997/0.005)=2899
    /// OSM at lon=14.5003 → gx=floor(14.5003/0.005)=2900  (adjacent cell)
    /// Separation ≈ 41 m — within the 50 m threshold → should match → 0 unmatched.
    #[test]
    fn adjacent_grid_cells_within_50m_match() {
        let conn = setup();
        insert_prg(&conn, "p1", "5", "ST_Point(14.4997, 52.25)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '5', NULL, NULL, NULL, ST_Point(14.5003, 52.25));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(
            unmatched_count(&conn),
            0,
            "addresses in adjacent grid cells within 50 m should match"
        );
    }

    /// An unmatched source address produces exactly one row, tagged with its
    /// z14 cell. The single-pass approach never duplicates rows, even when an
    /// address falls exactly on a grid-cell boundary (unlike the old chunked
    /// approach which emitted boundary rows twice).
    #[test]
    fn unmatched_address_produces_exactly_one_row_with_cell_tags() {
        let conn = setup();
        // lon=14.5 lands exactly on a grid boundary (14.5/0.005=2900.0).
        insert_prg(&conn, "boundary", "99", "ST_Point(14.5, 52.25)");

        run(&conn);
        assert_eq!(
            unmatched_count(&conn),
            1,
            "each source address should appear at most once in prg_unmatched"
        );
        let (cx, cy): (i32, i32) = conn
            .query_row("SELECT cell_x, cell_y FROM prg_unmatched", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        let (ex, ey) =
            crate::tile_math::lonlat_to_tile(14.5, 52.25, crate::tile_math::CHANGE_CELL_ZOOM);
        assert_eq!((cx as u32, cy as u32), (ex, ey));
    }

    /// Running the comparison twice must not duplicate rows in prg_unmatched.
    #[test]
    fn compare_addresses_is_idempotent() {
        let conn = setup();
        insert_prg(&conn, "p1", "12", "ST_Point(21.01, 52.21)");

        run(&conn);
        run(&conn);
        assert_eq!(
            unmatched_count(&conn),
            1,
            "re-running must not duplicate rows"
        );
    }
}
