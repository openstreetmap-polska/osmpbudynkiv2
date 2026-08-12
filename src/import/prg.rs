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
use crate::download::{download_file_as, download_file_as_quiet};
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

    let (zip_path, was_downloaded, terc) = prepare_source(config, file, terc_file, url, true)?;

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
#[allow(clippy::too_many_arguments)]
pub fn update_prg(
    conn: &Connection,
    config: &Config,
    file: Option<&Path>,
    terc_file: Option<&Path>,
    url: &str,
    source_etag: Option<&str>,
    show_progress: bool,
) -> Result<()> {
    let (zip_path, was_downloaded, terc) =
        prepare_source(config, file, terc_file, url, show_progress)?;
    let result = crate::update::dataset::refresh(
        conn,
        &crate::dataset::PRG,
        |c, target| {
            let raw = format!("{target}_raw");
            stream_gml_into(c, &zip_path, &terc, &raw)?;
            // PRG does not filter invalid geometry (that's bdot10k/egib-only,
            // see `crate::dataset::filter_invalid_geometry`), so there is
            // nothing to report beyond the default (all-zero) stats.
            materialize_into(c, target, &raw).map(|()| crate::dataset::LoadStats::default())
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
    show_progress: bool,
) -> Result<PreparedSource> {
    let (zip_path, was_downloaded) = match file {
        Some(p) => (PathBuf::from(p), false),
        None => {
            info!(url, "Downloading PRG data");
            let path = if show_progress {
                download_file_as(url, &config.download_dir(), PRG_DOWNLOAD_FILENAME)
            } else {
                download_file_as_quiet(url, &config.download_dir(), PRG_DOWNLOAD_FILENAME)
            }
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

/// Strip the redundant street-type prefix PRG embeds in `ulica`.
///
/// Warsaw's EMUiA operator writes `TERYTNazwa1` as `ulica Wał Miedzeszyński`
/// while *also* declaring `<prgad:rodzaj>1</prgad:rodzaj>` (= ulica), so the
/// type word arrives duplicated inside the name. `prg_convert` only ever
/// prepends a *missing* type word, never removes an embedded one, so it
/// faithfully passes the duplicate through and we strip it here. See
/// `docs/prg_ulica_prefix.md`.
///
/// Deliberately narrow: only `ulica` / `ul.` are removed. The other cecha
/// words Warsaw spells out — `Aleja`, `Aleje`, `Trakt`, `Osiedle` — are part
/// of the correct name and are used verbatim by OSM.
///
/// The trailing `\S` keeps a bare `"ulica"` (nothing after the prefix) intact
/// rather than turning it into an empty string.
const ULICA_PREFIX_STRIP_SQL: &str = r"CASE
        WHEN regexp_matches(ulica, '^(?i)(ulica|ul\.)\s+\S')
            THEN regexp_replace(ulica, '^(?i)(ulica|ul\.)\s+', '')
        ELSE ulica
     END";

/// Build `target_table` from the streamed `raw_table`, adding a geometry
/// column built from EPSG:4326 lon/lat (the parser already reprojected from
/// EPSG:2180) and the `_row_hash` column. Drops `raw_table` afterwards.
/// Does NOT create an index.
///
/// Also normalizes `ulica` via [`ULICA_PREFIX_STRIP_SQL`]. This is the one
/// funnel both `import` and `update`'s staging load pass through, so the
/// normalization lands on both paths from a single site. It sits INSIDE
/// `hashed_select`'s projection — unlike `DatasetSpec::with_centroid_select`
/// and `mappings::egib::with_rodzaj_kod_select`, which wrap it from outside —
/// because it rewrites a stored value rather than deriving an extra column:
/// hashing the value as stored is what lets `ROW_HASH_VERSION` catch a future
/// edit to the expression and self-heal on the next refresh. **Editing
/// `ULICA_PREFIX_STRIP_SQL` therefore requires bumping `ROW_HASH_VERSION`.**
pub fn materialize_into(conn: &Connection, target_table: &str, raw_table: &str) -> Result<()> {
    let inner = format!(
        "SELECT * REPLACE (({ULICA_PREFIX_STRIP_SQL}) AS ulica), \
                ST_Point(dlugosc_geograficzna, szerokosc_geograficzna) AS geom \
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;

    /// Materialize `inputs` (raw `ulica` values) through the real
    /// `materialize_into` and read the resulting column back, so the tests
    /// exercise the actual import SQL rather than a copy of the expression.
    fn materialized_ulica(inputs: &[Option<&str>]) -> Vec<Option<String>> {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_raw (
                 ord INTEGER,
                 ulica VARCHAR,
                 dlugosc_geograficzna DOUBLE,
                 szerokosc_geograficzna DOUBLE
             );",
        )
        .unwrap();
        for (i, value) in inputs.iter().enumerate() {
            conn.execute(
                "INSERT INTO prg_raw VALUES (?, ?, 21.0, 52.0)",
                duckdb::params![i as i32, value],
            )
            .unwrap();
        }

        materialize_into(&conn, "prg_out", "prg_raw").unwrap();

        let mut stmt = conn
            .prepare("SELECT ulica FROM prg_out ORDER BY ord")
            .unwrap();
        let rows: Vec<Option<String>> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        rows
    }

    #[test]
    fn materialize_strips_the_redundant_ulica_prefix() {
        assert_eq!(
            materialized_ulica(&[
                Some("ulica Wał Miedzeszyński"),
                Some("Ulica Szkółka Brzeźnica"),
                Some("ul. Szkolna"),
                Some("UL. Szkolna"),
            ]),
            vec![
                Some("Wał Miedzeszyński".to_string()),
                Some("Szkółka Brzeźnica".to_string()),
                Some("Szkolna".to_string()),
                Some("Szkolna".to_string()),
            ]
        );
    }

    /// The other cecha words Warsaw spells out are part of the correct name —
    /// OSM uses "Aleja Krakowska" and "Aleje Jerozolimskie" itself — so the
    /// strip must not generalize to them.
    #[test]
    fn materialize_keeps_other_street_type_words() {
        let names = ["Aleja Krakowska", "Aleje Jerozolimskie", "Trakt Lubelski"];
        assert_eq!(
            materialized_ulica(&names.map(Some)),
            names.map(|n| Some(n.to_string())).to_vec()
        );
    }

    /// `Ulanów` shares a prefix with `ulica` but is a whole different word;
    /// the `\s+` in the pattern is what keeps it intact.
    #[test]
    fn materialize_does_not_strip_a_word_merely_starting_with_ul() {
        assert_eq!(
            materialized_ulica(&[Some("Ulanów"), Some("Ulica Ulanowska")]),
            vec![Some("Ulanów".to_string()), Some("Ulanowska".to_string()),]
        );
    }

    /// A bare `"ulica"` has nothing to strip down to, and a NULL street has
    /// nothing to strip at all — neither may become an empty string.
    #[test]
    fn materialize_leaves_degenerate_street_names_alone() {
        assert_eq!(
            materialized_ulica(&[Some("ulica"), Some("ul."), None]),
            vec![Some("ulica".to_string()), Some("ul.".to_string()), None]
        );
    }

    /// The `* REPLACE` rewrite must not disturb the rest of the projection:
    /// every row still needs a hash, and the geometry column still has to be
    /// built from the lon/lat pair.
    #[test]
    fn materialize_still_writes_row_hash_and_geometry() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_raw (
                 ulica VARCHAR,
                 dlugosc_geograficzna DOUBLE,
                 szerokosc_geograficzna DOUBLE
             );
             INSERT INTO prg_raw VALUES
                 ('ulica Herbaciana', 21.0, 52.0),
                 ('Aleja Krakowska', 21.5, 52.5),
                 -- filtered out: no coordinates
                 ('ulica Bezdomna', NULL, NULL);",
        )
        .unwrap();

        materialize_into(&conn, "prg_out", "prg_raw").unwrap();

        let (rows, null_hashes, null_geoms): (i64, i64, i64) = conn
            .query_row(
                "SELECT COUNT(*),
                        COUNT(*) FILTER (WHERE _row_hash IS NULL),
                        COUNT(*) FILTER (WHERE geom IS NULL)
                 FROM prg_out",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            rows, 2,
            "the coordinate-less row must still be filtered out"
        );
        assert_eq!(null_hashes, 0, "every row must carry a hash");
        assert_eq!(null_geoms, 0, "every row must carry a geometry");
    }
}
