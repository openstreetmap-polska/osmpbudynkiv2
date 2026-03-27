use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::download::download_file;

const EGIB_URL: &str = "https://opendata.geoportal.gov.pl/InneDane/latest_exports/eziudp_wfs/PARQUET/0_budynki.parquet";

pub fn import(conn: &Connection, file: Option<&Path>) -> Result<()> {
    let parquet_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_file(EGIB_URL, Path::new("./data"))?,
    };

    let parquet_str = parquet_path
        .to_str()
        .context("Parquet path is not valid UTF-8")?;

    info!(path = parquet_str, "Importing EGIB buildings");

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS egib_buildings;
        CREATE TABLE egib_buildings AS
        SELECT * EXCLUDE(geometry_bbox)
        FROM '{parquet_str}';
        CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);
        "
    ))
    .context("Failed to import EGIB data from GeoParquet")?;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM egib_buildings", [], |row| row.get(0))?;
    info!(count, "EGIB buildings imported");

    Ok(())
}
