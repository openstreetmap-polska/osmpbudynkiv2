use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::utils::format_duration;

pub fn compare_bdot10k(conn: &Connection) -> Result<()> {
    info!("Comparing BDOT10k buildings against OSM");
    let t = std::time::Instant::now();

    conn.execute_batch(
        "
        DROP TABLE IF EXISTS bdot10k_comparison;
        CREATE TABLE bdot10k_comparison AS
        SELECT
            b.LOKALNYID AS lokalnyid,
            m.osm_id AS matched_osm_id,
            m.osm_type AS matched_osm_type,
            m.osm_id IS NOT NULL AS matched
        FROM bdot10k_buildings b
        LEFT JOIN LATERAL (
            SELECT osm.osm_id, osm.osm_type
            FROM osm_buildings osm
            WHERE ST_Contains(osm.geom, ST_Centroid(b.geom))
            LIMIT 1
        ) m ON TRUE;
        ",
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

    conn.execute_batch(
        "
        DROP TABLE IF EXISTS egib_comparison;
        CREATE TABLE egib_comparison AS
        SELECT
            b.id_budynku,
            m.osm_id AS matched_osm_id,
            m.osm_type AS matched_osm_type,
            m.osm_id IS NOT NULL AS matched
        FROM egib_buildings b
        LEFT JOIN LATERAL (
            SELECT osm.osm_id, osm.osm_type
            FROM osm_buildings osm
            WHERE ST_Contains(osm.geom, ST_Centroid(b.geom))
            LIMIT 1
        ) m ON TRUE;
        ",
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
