use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::utils::format_duration;

/// Grid cell size in degrees for chunked spatial comparison.
/// Same as buildings — 0.5 degrees is roughly 35x55 km, yielding ~264 cells
/// over Poland.
const GRID_STEP: f64 = 0.5;

/// Buffer added to each side of the OSM-side bounding box to catch
/// cross-cell-boundary matches. A PRG address inside cell C can match an OSM
/// address up to 50 m outside C. 0.001° is ≥ 64 m at all Polish latitudes,
/// so no real match is lost.
const OSM_BBOX_BUFFER: f64 = 0.001;

/// Two address points are considered matching when their (trimmed, uppercased)
/// housenumbers are equal AND they are within this distance in meters.
const MATCH_DISTANCE_METERS: f64 = 50.0;

pub fn compare_prg(conn: &Connection) -> Result<()> {
    info!("Comparing PRG addresses against OSM");
    let t = std::time::Instant::now();

    compare_addresses_chunked(
        conn,
        "prg_addresses",
        "numer_porzadkowy",
        "prg_import_candidates",
    )
    .context("Failed to compare PRG addresses against OSM")?;

    let total: i64 = conn.query_row("SELECT COUNT(*) FROM prg_addresses", [], |row| row.get(0))?;
    let candidates: i64 =
        conn.query_row("SELECT COUNT(*) FROM prg_import_candidates", [], |row| {
            row.get(0)
        })?;

    info!(
        total,
        candidates,
        matched = total - candidates,
        elapsed = %format_duration(t.elapsed()),
        "PRG comparison complete"
    );

    Ok(())
}

/// Compare addresses using a spatial grid to keep memory usage bounded.
///
/// Unlike the buildings comparison (which stores every source row with a
/// `matched` flag), this stores only the **unmatched** source rows — i.e. the
/// import candidates. Two addresses match when:
///
/// 1. `UPPER(TRIM(housenumber))` is equal on both sides, AND
/// 2. `ST_Distance_Sphere(osm.geom, prg.geom) <= 50 m`.
///
/// The grid + buffer strategy is the same as `compare_chunked` in buildings:
/// the PRG side is filtered by the exact cell bbox, and the OSM side is
/// filtered by a slightly larger bbox (cell + BUFFER) so cross-boundary
/// matches within 50 m are not missed.
fn compare_addresses_chunked(
    conn: &Connection,
    source_table: &str,
    housenumber_col: &str,
    candidates_table: &str,
) -> Result<()> {
    // Create an empty candidates table with the same schema as the source
    // (including the geom column added during PRG import).
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {candidates_table};
         CREATE TABLE {candidates_table} AS SELECT * FROM {source_table} LIMIT 0;"
    ))?;

    // Fixed grid covering Poland's extent (EPSG:4326).
    let (min_x, min_y, max_x, max_y) = (14.0, 49.0, 25.0, 55.0);

    let cols_count = ((max_x - min_x) / GRID_STEP).ceil() as u32;
    let rows_count = ((max_y - min_y) / GRID_STEP).ceil() as u32;
    let total_cells = cols_count * rows_count;
    info!(
        grid_step = GRID_STEP,
        cells = total_cells,
        "Processing comparison in grid cells"
    );

    let mut cell = 0u32;
    let mut y = min_y;
    while y < max_y {
        let mut x = min_x;
        while x < max_x {
            cell += 1;
            let x2 = x + GRID_STEP;
            let y2 = y + GRID_STEP;

            // OSM-side bbox is expanded by BUFFER so addresses up to 50 m
            // outside the cell boundary can still match a PRG row inside it.
            let bx = x - OSM_BBOX_BUFFER;
            let by = y - OSM_BBOX_BUFFER;
            let bx2 = x2 + OSM_BBOX_BUFFER;
            let by2 = y2 + OSM_BBOX_BUFFER;

            conn.execute_batch(&format!(
                "INSERT INTO {candidates_table}
                 WITH cell_src AS (
                     SELECT * FROM {source_table}
                     WHERE ST_Intersects(geom, ST_MakeEnvelope({x}, {y}, {x2}, {y2}))
                 )
                 SELECT p.*
                 FROM cell_src p
                 WHERE NOT EXISTS (
                     SELECT 1 FROM osm_addresses osm
                     WHERE ST_Intersects(osm.geom, ST_MakeEnvelope({bx}, {by}, {bx2}, {by2}))
                       AND UPPER(TRIM(osm.housenumber)) = UPPER(TRIM(p.{housenumber_col}))
                       AND ST_Distance_Sphere(osm.geom, p.geom) <= {MATCH_DISTANCE_METERS}
                 );"
            ))
            .with_context(|| format!("Failed at grid cell {cell}/{total_cells}"))?;

            x += GRID_STEP;
        }
        y += GRID_STEP;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::db::init_db;

    /// Set up an in-memory DuckDB with spatial loaded, `osm_addresses`
    /// (created by init_db), and a minimal `test_src` table.
    fn setup() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE test_src (
                 lokalny_id VARCHAR,
                 numer_porzadkowy VARCHAR,
                 geom GEOMETRY
             );",
        )
        .unwrap();
        conn
    }

    fn candidate_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM test_candidates", [], |row| row.get(0))
            .unwrap()
    }

    fn run(conn: &Connection) {
        compare_addresses_chunked(conn, "test_src", "numer_porzadkowy", "test_candidates").unwrap();
    }

    /// Same housenumber, ~22 m apart → match → 0 candidates.
    #[test]
    fn matched_within_50m_excluded_from_candidates() {
        let conn = setup();
        // 21.01 and 52.21 are safely inside a cell (not on a boundary).
        conn.execute_batch(
            "INSERT INTO test_src VALUES ('p1', '12', ST_Point(21.01, 52.21));
             INSERT INTO osm_addresses VALUES (1, 'node', '12', NULL, NULL, NULL, ST_Point(21.01, 52.2102));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(candidate_count(&conn), 0);
    }

    /// Same housenumber, ~200 m apart → too far → 1 candidate.
    #[test]
    fn same_number_but_too_far_is_candidate() {
        let conn = setup();
        conn.execute_batch(
            "INSERT INTO test_src VALUES ('p1', '12', ST_Point(21.01, 52.21));
             INSERT INTO osm_addresses VALUES (1, 'node', '12', NULL, NULL, NULL, ST_Point(21.01, 52.212));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(candidate_count(&conn), 1);
    }

    /// Different housenumbers at the same point → no match → 1 candidate.
    #[test]
    fn different_numbers_within_50m_is_candidate() {
        let conn = setup();
        conn.execute_batch(
            "INSERT INTO test_src VALUES ('p1', '12', ST_Point(21.01, 52.21));
             INSERT INTO osm_addresses VALUES (1, 'node', '14', NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(candidate_count(&conn), 1);
    }

    /// TRIM + UPPER normalization: ' 12a ' vs '12A' → match.
    #[test]
    fn trim_and_upper_normalization() {
        let conn = setup();
        conn.execute_batch(
            "INSERT INTO test_src VALUES ('p1', ' 12a ', ST_Point(21.01, 52.21));
             INSERT INTO osm_addresses VALUES (1, 'node', '12A', NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(candidate_count(&conn), 0);
    }

    /// NULL housenumbers on both sides → SQL NULL = NULL is NULL, not TRUE →
    /// no match → candidate.
    #[test]
    fn null_housenumbers_dont_match() {
        let conn = setup();
        conn.execute_batch(
            "INSERT INTO test_src VALUES ('p1', NULL, ST_Point(21.01, 52.21));
             INSERT INTO osm_addresses VALUES (1, 'node', NULL, NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(candidate_count(&conn), 1);
    }

    /// PRG just inside cell boundary, OSM just outside → the OSM-side buffer
    /// of 0.001° allows the match to succeed across the cell boundary.
    #[test]
    fn osm_buffer_catches_cross_boundary_match() {
        let conn = setup();
        // PRG at (14.4997, 52.25) — just west of the x=14.5 cell boundary
        // (falls in cell [14.0, 14.5]).
        // OSM at (14.5003, 52.25) — just east (in cell [14.5, 15.0]).
        // Separation: 0.0006° lon at lat 52.25° ≈ 0.0006 * 111320 * cos(52.25°)
        //           ≈ 0.0006 * 68027 ≈ 41 m — within 50 m threshold.
        // Without the OSM bbox buffer, the OSM point wouldn't be in the
        // PRG cell's bbox and the match would be missed.
        conn.execute_batch(
            "INSERT INTO test_src VALUES ('p1', '5', ST_Point(14.4997, 52.25));
             INSERT INTO osm_addresses VALUES (1, 'node', '5', NULL, NULL, NULL, ST_Point(14.5003, 52.25));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(
            candidate_count(&conn),
            0,
            "OSM buffer should allow cross-boundary match within 50 m"
        );
    }

    /// Known edge case: a source point exactly on a cell boundary is picked
    /// up by both adjacent cells, producing a duplicate in the candidates
    /// table when unmatched. Mirrors the buildings test at
    /// `compare_chunked_duplicates_source_on_cell_boundary`.
    #[test]
    fn boundary_duplication_documented() {
        let conn = setup();
        // x = 14.5 is the boundary between the first and second grid columns.
        // No matching OSM address → both cells emit the row.
        conn.execute_batch(
            "INSERT INTO test_src VALUES ('boundary', '99', ST_Point(14.5, 52.25));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(
            candidate_count(&conn),
            2,
            "source on cell boundary is processed by both adjacent cells"
        );
    }
}
