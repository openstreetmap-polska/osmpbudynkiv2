use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::config::Config;
use crate::download::download_file;
use crate::utils::format_duration;

pub fn import(conn: &Connection, config: &Config, file: Option<&Path>, url: &str) -> Result<()> {
    let (parquet_path, was_downloaded) = match file {
        Some(path) => (PathBuf::from(path), false),
        None => (download_file(url, &config.download_dir())?, true),
    };

    let parquet_str = parquet_path
        .to_str()
        .context("Parquet path is not valid UTF-8")?;

    info!(path = parquet_str, "Importing BDOT10k buildings");

    let total = std::time::Instant::now();

    // Workaround: DuckDB's automatic GeoParquet conversion and ST_Read (GDAL) both fail on
    // BDOT10k files because their CRS (EPSG:2180) is stored as a projjson string-in-string
    // which DuckDB rejects as "invalid CRS". Instead we disable the automatic conversion,
    // read the file as plain parquet, and manually convert the WKB geometry column.
    // Geometry is transformed from EPSG:2180 to EPSG:4326 for uniform spatial comparisons.
    let t = std::time::Instant::now();
    conn.execute_batch(&format!(
        "
        SET enable_geoparquet_conversion = false;
        DROP TABLE IF EXISTS bdot10k_buildings;
        CREATE TABLE bdot10k_buildings AS
        SELECT * EXCLUDE(GEOM),
               ST_Transform(ST_GeomFromWKB(GEOM), 'EPSG:2180', 'EPSG:4326') AS geom
        FROM '{parquet_str}';
        "
    ))
    .context("Failed to import BDOT10k data from GeoParquet")?;
    info!(
        elapsed = %format_duration(t.elapsed()),
        "Step done: load table"
    );

    let t = std::time::Instant::now();
    conn.execute_batch(
        "CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);",
    )
    .context("Failed to create spatial index on bdot10k_buildings")?;
    info!(
        elapsed = %format_duration(t.elapsed()),
        "Step done: create spatial index"
    );

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |row| {
        row.get(0)
    })?;
    if was_downloaded {
        info!(path = %parquet_path.display(), "Cleaning up downloaded file");
        let _ = std::fs::remove_file(&parquet_path);
    }

    info!(
        count,
        elapsed = %format_duration(total.elapsed()),
        "BDOT10k import complete"
    );

    Ok(())
}
