use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::download::download_file;
use crate::utils::format_duration;

pub fn import(conn: &Connection, file: Option<&Path>, url: &str) -> Result<()> {
    let parquet_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_file(url, Path::new("./data"))?,
    };

    let parquet_str = parquet_path
        .to_str()
        .context("Parquet path is not valid UTF-8")?;

    info!(path = parquet_str, "Importing EGIB buildings");

    let total = std::time::Instant::now();

    // Geometry is transformed from EPSG:2180 to EPSG:4326 for uniform spatial comparisons.
    let t = std::time::Instant::now();
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS egib_buildings;
        CREATE TABLE egib_buildings AS
        SELECT * EXCLUDE(geometry, geometry_bbox),
               ST_Transform(geometry, 'EPSG:2180', 'EPSG:4326') AS geom
        FROM '{parquet_str}';
        "
    ))
    .context("Failed to import EGIB data from GeoParquet")?;
    info!(
        elapsed = %format_duration(t.elapsed()),
        "Step done: load table"
    );

    let t = std::time::Instant::now();
    conn.execute_batch("CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);")
        .context("Failed to create spatial index on egib_buildings")?;
    info!(
        elapsed = %format_duration(t.elapsed()),
        "Step done: create spatial index"
    );

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM egib_buildings", [], |row| row.get(0))?;
    info!(
        count,
        elapsed = %format_duration(total.elapsed()),
        "EGIB import complete"
    );

    Ok(())
}
