use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use prg_convert::common::SCHEMA_CSV;
use prg_convert::{CRS, OutputFormat, get_address_parser_2021_zip, get_teryt_mapping};
use tracing::info;
use zip::ZipArchive;

use crate::config::Config;
use crate::download::download_file_as;
use crate::utils::format_duration;

const PRG_DOWNLOAD_FILENAME: &str = "PRG-punkty_adresowe.zip";

/// Import PRG addresses (2021 GML schema) from a local zip file into the
/// `prg_addresses` DuckDB table.
///
/// The current GUGiK distribution (`PRG-punkty_adresowe_YYYY-MM-DD.zip`) is a
/// single zip containing one `NOWE_*.gml` per voivodeship plus the legacy
/// `*.xml` 2012-schema files. We process every entry whose name ends in `.gml`
/// (case-insensitive) and ignore the rest.
///
/// A TERC mapping is required to resolve voivodeship/county/municipality names
/// from the TERYT codes embedded in the 2021 GML. Resolution priority:
/// `--terc-file` CLI flag > `teryt.file_path` in config > TERYT API download.
pub fn import(
    conn: &Connection,
    config: &Config,
    file: Option<&Path>,
    terc_file: Option<&Path>,
    url: &str,
) -> Result<()> {
    let zip_path = match file {
        Some(p) => PathBuf::from(p),
        None => {
            info!(url, "Downloading PRG data");
            download_file_as(url, &config.download_dir(), PRG_DOWNLOAD_FILENAME)
                .context("Failed to download PRG data")?
        }
    };

    let zip_str = zip_path
        .to_str()
        .context("PRG zip path is not valid UTF-8")?;

    // Resolve TERYT: CLI flag takes priority, then config file_path, then API download
    let terc_file_path = terc_file
        .map(PathBuf::from)
        .or_else(|| config.teryt.file_path.as_ref().map(PathBuf::from));

    info!(
        path = zip_str,
        teryt_source = if terc_file_path.is_some() {
            "file"
        } else {
            "api"
        },
        "Importing PRG addresses (2021 schema)"
    );

    let total = std::time::Instant::now();

    // Build TERC mapping (small, ~3000 entries — fits comfortably in memory).
    let t = std::time::Instant::now();
    let terc = if let Some(ref path) = terc_file_path {
        let terc_str = path.to_str().context("TERC path is not valid UTF-8")?;
        info!(path = terc_str, "Loading TERC mapping from file");
        get_teryt_mapping(false, &None, &None, &Some(path.clone()))
            .with_context(|| format!("Failed to load TERC mapping from {terc_str}"))?
    } else {
        // No file path provided — check if download is enabled
        if !config.teryt.download {
            bail!(
                "PRG 2021 import requires a TERYT dictionary; \
                 pass --terc-file <PATH>, set teryt.file_path in config, \
                 or set teryt.download = true to fetch from the API"
            );
        }
        // Auto-download from TERYT API
        let username = config
            .teryt
            .api_username
            .clone()
            .or_else(|| std::env::var("TERYT_API_USERNAME").ok())
            .context(
                "TERYT API username required: set teryt.api_username in config \
                 or TERYT_API_USERNAME env var",
            )?;
        let password = config
            .teryt
            .api_password
            .clone()
            .or_else(|| std::env::var("TERYT_API_PASSWORD").ok())
            .context(
                "TERYT API password required: set teryt.api_password in config \
                 or TERYT_API_PASSWORD env var",
            )?;
        info!("Downloading TERC mapping from TERYT API");
        get_teryt_mapping(true, &Some(username), &Some(password), &None)
            .context("Failed to download TERC mapping from TERYT API")?
    };
    info!(
        entries = terc.len(),
        elapsed = %format_duration(t.elapsed()),
        "Step done: load TERC mapping"
    );

    // List the zip once and pick the indices of all 2021 GML entries.
    let mut archive = ZipArchive::new(
        File::open(&zip_path).with_context(|| format!("Failed to open {zip_str}"))?,
    )
    .with_context(|| format!("Failed to read PRG zip archive {zip_str}"))?;

    let gml_indices = collect_gml_indices(&mut archive)
        .with_context(|| format!("Failed to enumerate entries in {zip_str}"))?;
    if gml_indices.is_empty() {
        bail!("No .gml entries found in PRG zip {zip_str}");
    }
    info!(
        entries = gml_indices.len(),
        "Found PRG 2021 GML entries in archive"
    );

    // Drop any leftover staging table from a previous run; we (re)create it
    // lazily from the first arrow batch's schema.
    conn.execute_batch("DROP TABLE IF EXISTS prg_addresses_raw")
        .context("Failed to drop existing prg_addresses_raw")?;

    // The PointType arg is only consumed by the GeoParquet output writer.
    // We use OutputFormat::CSV so it never gets read, but the API still
    // requires us to pass one.
    let dummy_point_type = make_dummy_point_type();

    let t = std::time::Instant::now();
    let mut table_created = false;
    let mut total_rows: usize = 0;
    for (n, &idx) in gml_indices.iter().enumerate() {
        info!(
            entry = n + 1,
            of = gml_indices.len(),
            zip_index = idx,
            "Streaming PRG GML entry"
        );
        let parser = get_address_parser_2021_zip(
            &mut archive,
            &2048, // STANDARD_VECTOR_SIZE; arrow vtab panics on larger batches
            &OutputFormat::CSV,
            &terc,
            idx,
            &CRS::Epsg4326,
            SCHEMA_CSV.clone(),
            &dummy_point_type,
        )
        .with_context(|| format!("Failed to build PRG 2021 parser for zip entry {idx}"))?;

        for batch in parser {
            if batch.num_rows() == 0 {
                continue;
            }
            total_rows += batch.num_rows();
            let params = arrow_recordbatch_to_query_params(batch);
            if !table_created {
                conn.execute(
                    "CREATE TABLE prg_addresses_raw AS SELECT * FROM arrow(?, ?)",
                    params,
                )
                .context("Failed to create prg_addresses_raw from first arrow batch")?;
                table_created = true;
            } else {
                conn.execute(
                    "INSERT INTO prg_addresses_raw SELECT * FROM arrow(?, ?)",
                    params,
                )
                .context("Failed to insert PRG batch into prg_addresses_raw")?;
            }
        }
    }
    if !table_created {
        bail!("PRG parser yielded no rows");
    }
    info!(
        rows = total_rows,
        elapsed = %format_duration(t.elapsed()),
        "Step done: stream PRG batches into staging table"
    );

    // Materialize the final table with a geometry column built from
    // EPSG:4326 lon/lat (the parser already reprojected from EPSG:2180).
    let t = std::time::Instant::now();
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS prg_addresses;
        CREATE TABLE prg_addresses AS
        SELECT *,
               ST_Point(dlugosc_geograficzna, szerokosc_geograficzna) AS geom
        FROM prg_addresses_raw
        WHERE dlugosc_geograficzna IS NOT NULL
          AND szerokosc_geograficzna IS NOT NULL;
        DROP TABLE prg_addresses_raw;
        ",
    )
    .context("Failed to materialize prg_addresses table")?;
    info!(
        elapsed = %format_duration(t.elapsed()),
        "Step done: build prg_addresses with geom column"
    );

    let t = std::time::Instant::now();
    conn.execute_batch("CREATE INDEX prg_addresses_geom_idx ON prg_addresses USING RTREE (geom);")
        .context("Failed to create spatial index on prg_addresses")?;
    info!(
        elapsed = %format_duration(t.elapsed()),
        "Step done: create spatial index"
    );

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM prg_addresses", [], |row| row.get(0))?;

    info!(
        count,
        elapsed = %format_duration(total.elapsed()),
        "PRG import complete"
    );

    Ok(())
}

/// Walk the archive once and collect indices of entries whose name ends in
/// `.gml` (case-insensitive). The 2021 schema lives in those files; the
/// 2012-schema `.xml` entries are ignored.
fn collect_gml_indices(archive: &mut ZipArchive<File>) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .with_context(|| format!("Failed to read zip entry {i}"))?;
        let is_gml = entry
            .enclosed_name()
            .and_then(|n| {
                n.extension()
                    .map(|e| e.to_ascii_lowercase() == OsStr::new("gml"))
            })
            .unwrap_or(false);
        if is_gml {
            indices.push(i);
        }
    }
    Ok(indices)
}

/// Construct a dummy PointType for the `geoarrow_geom_type` argument.
/// Only the GeoParquet writer uses this; with `OutputFormat::CSV` it is
/// unused, but the parser's constructor requires it.
fn make_dummy_point_type() -> geoarrow::datatypes::PointType {
    use geoarrow::datatypes::{Crs, Dimension, Metadata, PointType};
    PointType::new(
        Dimension::XY,
        Arc::new(Metadata::new(Crs::from_srid("4326".to_string()), None)),
    )
}
