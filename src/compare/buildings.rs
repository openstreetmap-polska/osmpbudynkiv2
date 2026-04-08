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
