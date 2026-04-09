use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::utils::format_duration;

/// Grid cell size in degrees for chunked spatial comparison.
/// 0.5 degrees is roughly 35x55 km, yielding ~250 cells over Poland.
const GRID_STEP: f64 = 0.5;

pub fn compare_bdot10k(conn: &Connection) -> Result<()> {
    info!("Comparing BDOT10k buildings against OSM");
    let t = std::time::Instant::now();

    compare_chunked(
        conn,
        "bdot10k_buildings",
        "LOKALNYID",
        "lokalnyid",
        "bdot10k_comparison",
    )
    .context("Failed to compare BDOT10k buildings against OSM")?;

    let (total, matched): (i64, i64) = conn.query_row(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE matched) FROM bdot10k_comparison",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    info!(
        total,
        matched,
        unmatched = total - matched,
        elapsed = %format_duration(t.elapsed()),
        "BDOT10k comparison complete"
    );

    Ok(())
}

pub fn compare_egib(conn: &Connection) -> Result<()> {
    info!("Comparing EGIB buildings against OSM");
    let t = std::time::Instant::now();

    compare_chunked(
        conn,
        "egib_buildings",
        "id_budynku",
        "id_budynku",
        "egib_comparison",
    )
    .context("Failed to compare EGIB buildings against OSM")?;

    let (total, matched): (i64, i64) = conn.query_row(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE matched) FROM egib_comparison",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    info!(
        total,
        matched,
        unmatched = total - matched,
        elapsed = %format_duration(t.elapsed()),
        "EGIB comparison complete"
    );

    Ok(())
}

/// Compare buildings using a spatial grid to keep memory usage bounded.
///
/// DuckDB's R-tree indexes only accelerate queries where one argument to the
/// spatial predicate is a constant known at planning time. A lateral join between
/// two table columns cannot use the index, causing a full scan of osm_buildings
/// per source row and OOM on large datasets.
///
/// This function divides the data extent into grid cells and processes each cell
/// independently. Within each cell, `ST_Intersects(geom, constant_bbox)` enables
/// R-tree index scans on both tables, reducing each chunk to a small subset.
fn compare_chunked(
    conn: &Connection,
    source_table: &str,
    source_id_col: &str,
    result_id_col: &str,
    result_table: &str,
) -> Result<()> {
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {result_table};
         CREATE TABLE {result_table} (
             {result_id_col} VARCHAR,
             matched_osm_id BIGINT,
             matched_osm_type VARCHAR,
             matched BOOLEAN
         );"
    ))?;

    // Fixed grid covering Poland's extent (EPSG:4326).
    // All government datasets (PRG, BDOT10k, EGIB) fall within these bounds.
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

            // Filter source buildings into this cell via CTE, then lateral-join
            // against OSM buildings filtered by constant bbox (enables R-tree
            // index scan on osm_buildings).
            conn.execute_batch(&format!(
                "INSERT INTO {result_table}
                 WITH cell_source AS (
                     SELECT * FROM {source_table}
                     WHERE ST_Intersects(ST_Centroid(geom), ST_MakeEnvelope({x}, {y}, {x2}, {y2}))
                 )
                 SELECT
                     b.{source_id_col},
                     m.osm_id,
                     m.osm_type,
                     m.osm_id IS NOT NULL
                 FROM cell_source b
                 LEFT JOIN LATERAL (
                     SELECT osm.osm_id, osm.osm_type
                     FROM osm_buildings osm
                     WHERE ST_Contains(osm.geom, ST_Centroid(b.geom))
                       AND ST_Intersects(osm.geom, ST_MakeEnvelope({x}, {y}, {x2}, {y2}))
                     LIMIT 1
                 ) m ON TRUE;"
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

    /// Spin up an in-memory DuckDB with spatial loaded and a custom
    /// `test_source` table. `osm_buildings` is created by `init_db` and
    /// is seeded with a single polygon unless the caller overrides.
    fn setup() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE test_source (src_id VARCHAR, geom GEOMETRY);
             INSERT INTO osm_buildings VALUES
                 (1, 'way', NULL, ST_MakeEnvelope(20.0, 52.0, 20.001, 52.001));",
        )
        .unwrap();
        conn
    }

    fn counts(conn: &Connection, table: &str) -> (i64, i64) {
        conn.query_row(
            &format!("SELECT COUNT(*), COUNT(*) FILTER (WHERE matched) FROM {table}"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    /// A source centroid that lies inside an OSM polygon must be matched,
    /// and the result row must carry the matching osm_id/osm_type.
    #[test]
    fn compare_chunked_matches_contained_centroid() {
        let conn = setup();
        // Source envelope wholly inside the seeded osm_buildings polygon.
        conn.execute_batch(
            "INSERT INTO test_source VALUES
                 ('inside', ST_MakeEnvelope(20.0002, 52.0002, 20.0008, 52.0008));",
        )
        .unwrap();

        compare_chunked(&conn, "test_source", "src_id", "src_id", "test_result").unwrap();

        assert_eq!(counts(&conn, "test_result"), (1, 1));

        let (src_id, osm_id, osm_type): (String, i64, String) = conn
            .query_row(
                "SELECT src_id, matched_osm_id, matched_osm_type
                 FROM test_result WHERE matched",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(src_id, "inside");
        assert_eq!(osm_id, 1);
        assert_eq!(osm_type, "way");
    }

    /// A source centroid with no containing OSM building produces an
    /// unmatched row with NULL osm_id/osm_type — the source row is NOT
    /// dropped from the result table.
    #[test]
    fn compare_chunked_emits_null_row_for_unmatched_source() {
        let conn = setup();
        // Source somewhere else in Poland, well away from the seeded polygon.
        conn.execute_batch(
            "INSERT INTO test_source VALUES
                 ('lonely', ST_MakeEnvelope(21.0, 52.2, 21.001, 52.201));",
        )
        .unwrap();

        compare_chunked(&conn, "test_source", "src_id", "src_id", "test_result").unwrap();

        assert_eq!(counts(&conn, "test_result"), (1, 0));

        let (osm_id, osm_type): (Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT matched_osm_id, matched_osm_type FROM test_result",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(osm_id.is_none());
        assert!(osm_type.is_none());
    }

    /// `compare_chunked` iterates a fixed grid over Poland's bounding box
    /// (14..25 E, 49..55 N). Source buildings whose centroids fall outside
    /// that extent are silently dropped from the result table. This test
    /// documents that behavior; if the grid coverage changes (or becomes
    /// data-driven) this test must change too.
    #[test]
    fn compare_chunked_silently_drops_source_outside_poland_extent() {
        let conn = setup();
        // Longitude 30°E — outside the 14..25 grid.
        conn.execute_batch(
            "INSERT INTO test_source VALUES
                 ('far_east', ST_MakeEnvelope(30.0, 52.0, 30.001, 52.001));",
        )
        .unwrap();

        compare_chunked(&conn, "test_source", "src_id", "src_id", "test_result").unwrap();

        assert_eq!(counts(&conn, "test_result"), (0, 0));
    }

    /// Known edge case: a source centroid that lands *exactly* on a cell
    /// boundary is processed by both adjacent cells, because ST_Intersects
    /// returns true on touch. The source row ends up duplicated in the
    /// result table. This is vanishingly rare in practice (centroids are
    /// float-valued) but this test locks the behavior in so future changes
    /// are deliberate. If you ever switch to a half-open cell convention
    /// (e.g. `ST_Within` with lower-closed / upper-open ranges), this test
    /// will need updating.
    #[test]
    fn compare_chunked_duplicates_source_on_cell_boundary() {
        let conn = setup();
        // x = 14.5 is the boundary between the first and second grid
        // columns (GRID_STEP = 0.5 starting at 14.0). y = 52.25 is safely
        // inside one row.
        conn.execute_batch(
            "INSERT INTO test_source VALUES
                 ('boundary', ST_Point(14.5, 52.25));",
        )
        .unwrap();

        compare_chunked(&conn, "test_source", "src_id", "src_id", "test_result").unwrap();

        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM test_result", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            total, 2,
            "source centroid on a cell boundary is processed by both adjacent cells"
        );
    }
}
