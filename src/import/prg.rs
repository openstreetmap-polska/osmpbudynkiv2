use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use prg_convert::terc::Terc;
use prg_convert::{get_address_parser_2021_zip, get_teryt_mapping};
use tracing::info;
use zip::ZipArchive;

use crate::config::Config;
use crate::download::download_file_as;
use crate::utils::format_duration;

const PRG_DOWNLOAD_FILENAME: &str = "PRG-punkty_adresowe.zip";

/// Resolved PRG input: the zip path, whether it was downloaded (vs.
/// user-supplied), and the TERC mapping needed to parse it.
type PreparedSource = (PathBuf, bool, Arc<HashMap<String, Terc>>);

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
    let total = std::time::Instant::now();

    let (zip_path, was_downloaded, terc) = prepare_source(config, file, terc_file, url)?;

    let raw_table = "prg_addresses_raw";
    stream_gml_into(conn, &zip_path, &terc, raw_table)?;
    cleanup_if_downloaded(&zip_path, was_downloaded && config.cleanup_downloaded_files);

    // Materialize the final table with a geometry column built from
    // EPSG:4326 lon/lat (the parser already reprojected from EPSG:2180).
    let t = std::time::Instant::now();
    materialize_into(conn, crate::dataset::PRG.table, raw_table)?;
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

/// Refresh `prg_addresses` from a fresh snapshot, reusing the import
/// streaming path to build the staging table.
///
/// `source_etag` is the validator observed by the caller's HEAD check (see
/// `update::source_unchanged`), if any; it is threaded through unchanged to
/// `dataset::refresh` so it lands in `dataset_refreshes.source_etag`.
pub fn update_prg(
    conn: &Connection,
    config: &Config,
    file: Option<&Path>,
    terc_file: Option<&Path>,
    url: &str,
    source_etag: Option<&str>,
) -> Result<()> {
    let (zip_path, was_downloaded, terc) = prepare_source(config, file, terc_file, url)?;
    let result = crate::update::dataset::refresh(
        conn,
        &crate::dataset::PRG,
        |c, target| {
            let raw = format!("{target}_raw");
            stream_gml_into(c, &zip_path, &terc, &raw)?;
            materialize_into(c, target, &raw)
        },
        source_etag,
    );
    cleanup_if_downloaded(&zip_path, was_downloaded && config.cleanup_downloaded_files);
    result.map(|_| ())
}

/// Resolve the PRG zip (local file or download) and build the TERC mapping
/// needed to parse it. Resolution priority for TERC: `--terc-file` CLI flag >
/// `teryt.file_path` in config > TERYT API download.
///
/// Does not touch the database — this is pure "get the inputs ready" work
/// shared by both `import` and `update_prg`.
///
/// Returns whether the zip was downloaded (vs. a user-supplied `--file`), so
/// callers know whether it's theirs to delete once consumed.
fn prepare_source(
    config: &Config,
    file: Option<&Path>,
    terc_file: Option<&Path>,
    url: &str,
) -> Result<PreparedSource> {
    let (zip_path, was_downloaded) = match file {
        Some(p) => (PathBuf::from(p), false),
        None => {
            info!(url, "Downloading PRG data");
            let path = download_file_as(url, &config.download_dir(), PRG_DOWNLOAD_FILENAME)
                .context("Failed to download PRG data")?;
            (path, true)
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
        "Preparing PRG addresses source (2021 schema)"
    );

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
    let terc = Arc::new(terc);
    info!(
        entries = terc.len(),
        elapsed = %format_duration(t.elapsed()),
        "Step done: load TERC mapping"
    );

    Ok((zip_path, was_downloaded, terc))
}

/// Remove the zip once it's been fully consumed. `should_delete` must already
/// fold in both "we downloaded it ourselves" (a user-supplied `--file` is
/// never deleted) and `config.cleanup_downloaded_files`.
fn cleanup_if_downloaded(zip_path: &Path, should_delete: bool) {
    if should_delete {
        info!(path = %zip_path.display(), "Cleaning up downloaded file");
        let _ = std::fs::remove_file(zip_path);
    }
}

/// Enumerate every `.gml` entry in the PRG zip and stream its parsed arrow
/// batches into `raw_table`, creating it from the first batch's schema and
/// appending the rest. Drops any leftover `raw_table` from a previous run
/// first.
fn stream_gml_into(
    conn: &Connection,
    zip_path: &Path,
    terc: &Arc<HashMap<String, Terc>>,
    raw_table: &str,
) -> Result<()> {
    let zip_str = zip_path
        .to_str()
        .context("PRG zip path is not valid UTF-8")?;

    // List the zip once and pick the indices of all 2021 GML entries.
    let mut archive =
        ZipArchive::new(File::open(zip_path).with_context(|| format!("Failed to open {zip_str}"))?)
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
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {raw_table}"))
        .with_context(|| format!("Failed to drop existing {raw_table}"))?;

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
            terc,
            idx,
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
                    &format!("CREATE TABLE {raw_table} AS SELECT * FROM arrow(?, ?)"),
                    params,
                )
                .with_context(|| format!("Failed to create {raw_table} from first arrow batch"))?;
                table_created = true;
            } else {
                conn.execute(
                    &format!("INSERT INTO {raw_table} SELECT * FROM arrow(?, ?)"),
                    params,
                )
                .with_context(|| format!("Failed to insert PRG batch into {raw_table}"))?;
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

    Ok(())
}

/// Build `target_table` from the streamed `raw_table`, adding a geometry
/// column built from EPSG:4326 lon/lat (the parser already reprojected from
/// EPSG:2180) and the `_row_hash` column. Drops `raw_table` afterwards.
/// Does NOT create an index.
pub fn materialize_into(conn: &Connection, target_table: &str, raw_table: &str) -> Result<()> {
    let inner = format!(
        "SELECT *, ST_Point(dlugosc_geograficzna, szerokosc_geograficzna) AS geom \
         FROM {raw_table} \
         WHERE dlugosc_geograficzna IS NOT NULL \
           AND szerokosc_geograficzna IS NOT NULL"
    );
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {};
         DROP TABLE {raw_table};",
        crate::dataset::hashed_select(&inner)
    ))
    .with_context(|| format!("Failed to materialize {target_table}"))
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
