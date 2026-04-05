use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::download::download_file;
use crate::utils::format_duration;

const PARQUET_ENTRY: &str = "OT_BUBD_A.parquet";

pub fn import(conn: &Connection, file: Option<&Path>, url: &str) -> Result<()> {
    let parquet_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_and_extract(url)?,
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
    conn.execute_batch("CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);")
        .context("Failed to create spatial index on bdot10k_buildings")?;
    info!(
        elapsed = %format_duration(t.elapsed()),
        "Step done: create spatial index"
    );

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |row| {
        row.get(0)
    })?;
    info!(
        count,
        elapsed = %format_duration(total.elapsed()),
        "BDOT10k import complete"
    );

    Ok(())
}

fn download_and_extract(url: &str) -> Result<PathBuf> {
    let zip_path = download_file(url, Path::new("./data"))?;
    let extract_dir = Path::new("./data/bdot10k");
    std::fs::create_dir_all(extract_dir)?;

    let target_path = extract_dir.join(PARQUET_ENTRY);
    if target_path.exists() {
        info!(path = %target_path.display(), "Parquet file already extracted");
        return Ok(target_path);
    }

    info!("Extracting {} from ZIP", PARQUET_ENTRY);
    let file = std::fs::File::open(&zip_path)?;
    let mut archive = zip::ZipArchive::new(file).context("Failed to open ZIP archive")?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if name.ends_with(PARQUET_ENTRY) {
            let mut outfile = std::fs::File::create(&target_path)?;
            std::io::copy(&mut entry, &mut outfile)?;
            info!(path = %target_path.display(), "Extracted parquet file");
            return Ok(target_path);
        }
    }

    anyhow::bail!("Could not find {PARQUET_ENTRY} in ZIP archive")
}
