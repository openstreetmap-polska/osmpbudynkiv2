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
    let stats = materialize_into(conn, crate::dataset::PRG.table, raw_table)?;
    // `import prg` has no `job_run_log` integration the way `update prg`'s
    // `refresh` -> `summarize_refresh` does, so this `info!` is the only
    // operator-visible signal that `materialize_into` silently dropped a row
    // for having a NULL or duplicate `lokalny_id` -- surface both counts
    // rather than discarding the returned `LoadStats`.
    info!(
        elapsed = %format_duration(t.elapsed()),
        skipped_null_key = stats.skipped_null_key,
        skipped_duplicate_key = stats.skipped_duplicate_key,
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
///
/// `is_cancelled` is threaded through unchanged to `dataset::refresh`, which
/// polls it (alongside `crate::shutdown::is_requested()`) at three points
/// before its apply transaction begins -- see `dataset::refresh`'s and
/// `check_cancelled`'s doc comments for why cancellation is checked only
/// there and not, e.g., mid-stream through `stream_gml_into`.
#[allow(clippy::too_many_arguments)]
pub fn update_prg(
    conn: &Connection,
    config: &Config,
    file: Option<&Path>,
    terc_file: Option<&Path>,
    url: &str,
    source_etag: Option<&str>,
    show_progress: bool,
    is_cancelled: &dyn Fn() -> bool,
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
            // see `crate::dataset::filter_invalid_geometry`), so
            // `materialize_into`'s `LoadStats` carries only the null-key and
            // duplicate-key counts.
            materialize_into(c, target, &raw)
        },
        source_etag,
        is_cancelled,
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
        // Checked once per voivodeship entry as well as once per batch below
        // -- a full PRG zip has ~16 entries, each itself taking a while to
        // stream, so this catches a shutdown request sitting between entries
        // without waiting for the next batch-loop check to happen to fire.
        // This function is shared by both `import prg` and `update prg`'s
        // staging load (see `update_prg`'s call above in this file), so a
        // single check here covers both paths. Bails with an `Err`, matching
        // `import::osm::import`'s `check_shutdown` convention: a partially
        // streamed staging table must be reported as a failure, not silently
        // accepted as if the whole zip had loaded -- `materialize_into`
        // would otherwise build the final table from a truncated
        // `raw_table` with no indication anything was cut short.
        crate::shutdown::check_requested()?;
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
            // Same rationale as the entry-loop check above, at finer grain:
            // a single GML entry (one voivodeship) can itself take a while
            // to stream through its ~2048-row batches, so this is the seam
            // that actually bounds how long a shutdown request waits before
            // being noticed.
            crate::shutdown::check_requested()?;
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
/// EPSG:2180), then drop any row with a NULL record key (see
/// `dataset::non_null_key_sql`) and collapse duplicate keys down to one row
/// each (see `dataset::deduplicate_by_key`). Drops `raw_table` afterwards.
/// Does NOT create an index.
///
/// PRG's record key -- `crate::dataset::PRG.key_columns` -- feeds all three
/// sites below that have to agree on it (the load select's `IS NOT NULL`
/// filter, the count query's `IS NULL` complement, and the dedup's
/// `PARTITION BY`), so they cannot drift.
///
/// Projects an explicit column list rather than `SELECT *`/`SELECT *
/// REPLACE(...)` -- see
/// `docs/superpowers/plans/2026-08-14-column-trimming.md` for the audit
/// behind which of PRG's 24 raw columns are kept (the dropped ones and their
/// restore cost are catalogued in `docs/prg_dropped_columns.md`).
/// `teryt_gmina`/`gmina` are kept despite having no current reader, the same
/// "kept but not compared" shape as `wazny_od_lub_data_nadania` below.
/// `wersja_id` is a special case: it stays in the projection only long
/// enough to feed the dedup's `ORDER BY` below, then is removed via
/// `dataset::drop_ordering_column` -- see that function's doc comment.
///
/// Also normalizes `ulica` via [`ULICA_PREFIX_STRIP_SQL`]. This is the one
/// funnel both `import` and `update`'s staging load pass through, so the
/// normalization lands on both paths from a single site. It is safe to edit
/// freely, unlike the columns discussed next: `ulica` is one of
/// `PRG.compared_columns`, so a change to this expression surfaces as an
/// ordinary "modified" record on the next refresh and self-heals per record
/// with no version bump to remember.
///
/// Contrast that with values derived *outside* `compared_columns` --
/// `centroid` (`DatasetSpec::with_centroid_select`) and EGIB's `rodzaj_kod`
/// (`mappings::egib::RODZAJ_KOD_CASE_SQL`). Those are recomputed for every
/// *staged* row on every refresh, so a record that happens to be modified for
/// some unrelated reason picks up the new expression -- but a record that
/// stays unmodified keeps its old, stale value forever. Editing one of those
/// expressions requires a re-import, not merely a refresh.
pub fn materialize_into(
    conn: &Connection,
    target_table: &str,
    raw_table: &str,
) -> Result<crate::dataset::LoadStats> {
    let non_null = crate::dataset::non_null_key_sql(crate::dataset::PRG.key_columns);
    let inner = format!(
        "SELECT lokalny_id, numer_porzadkowy, \
                ({ULICA_PREFIX_STRIP_SQL}) AS ulica, \
                miejscowosc, kod_pocztowy, teryt_miejscowosc, teryt_gmina, gmina, \
                wazny_od_lub_data_nadania, wersja_id, \
                ST_Point(dlugosc_geograficzna, szerokosc_geograficzna) AS geom \
         FROM {raw_table} \
         WHERE dlugosc_geograficzna IS NOT NULL \
           AND szerokosc_geograficzna IS NOT NULL \
           AND {non_null}"
    );

    // A filtered CTAS doesn't report how many rows its WHERE excluded, so
    // count them with a second, narrow query -- unlike BDOT10k/EGIB, which
    // count against the source parquet file on disk (and so can run the
    // count at any point relative to their CREATE TABLE), PRG's source here
    // is the in-database `raw_table`, and the CREATE TABLE statement below
    // drops `raw_table` as the last step of its own `execute_batch`. This
    // count MUST run before that batch executes, or it fails with "table
    // does not exist".
    let null_key_rows: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM {raw_table} WHERE {}",
                crate::dataset::null_key_sql(crate::dataset::PRG.key_columns)
            ),
            [],
            |r| r.get(0),
        )
        .with_context(|| format!("Failed to count NULL-keyed rows in {raw_table}"))?;

    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {inner};
         DROP TABLE {raw_table};"
    ))
    .with_context(|| format!("Failed to materialize {target_table}"))?;

    // Unlike BDOT10k/EGIB, PRG runs no geometry filters at all (there is no
    // `filter_invalid_geometry`/`filter_oversized_geometry` call for PRG
    // points), so their "must come after both geometry filters" ordering
    // constraint on the dedup does not apply here. It still has to come
    // after the table is built, to match the other two loaders.
    //
    // `wersja_id DESC` is deliberate: measured on the real 2026-01-10
    // snapshot, lokalny_id 49ba6299-04f9-48c1-8c50-56737d64927e appears
    // twice, differing only in wersja_id and in a field the newer version
    // corrects (`wazny_od_lub_data_nadania` -- one copy carries the typo
    // `0200-07-15`, the other the correction `2003-07-15` at a later
    // wersja_id). Newest wersja_id wins so the correction survives, not the
    // stale typo.
    let mut unique = crate::dataset::deduplicate_by_key(
        conn,
        target_table,
        crate::dataset::PRG.key_columns,
        "wersja_id DESC",
        "lokalny_id",
    )?;
    unique.skipped_null_key = null_key_rows;

    // `wersja_id` has served its only purpose (ranking the dedup above) --
    // nothing reads it downstream, so it does not survive into the stored
    // table. Must run after `deduplicate_by_key`, not before: see
    // `dataset::drop_ordering_column`'s doc comment.
    crate::dataset::drop_ordering_column(conn, target_table, "wersja_id")?;

    Ok(unique)
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
    /// Each row gets its own `lokalny_id` (derived from `ord`, so distinct)
    /// and a constant `wersja_id` -- these tests care about `ulica`
    /// stripping, not the key/dedup machinery, so the key columns are just
    /// populated well enough to stay out of the way. The remaining columns
    /// (`numer_porzadkowy` through `wazny_od_lub_data_nadania`) exist purely
    /// because `materialize_into`'s explicit projection now names them --
    /// see `docs/superpowers/plans/2026-08-14-column-trimming.md` -- and are
    /// filled with placeholder constants nothing in these tests inspects.
    fn materialized_ulica(inputs: &[Option<&str>]) -> Vec<Option<String>> {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_raw (
                 ord INTEGER,
                 ulica VARCHAR,
                 dlugosc_geograficzna DOUBLE,
                 szerokosc_geograficzna DOUBLE,
                 lokalny_id VARCHAR,
                 wersja_id INTEGER,
                 numer_porzadkowy VARCHAR,
                 miejscowosc VARCHAR,
                 kod_pocztowy VARCHAR,
                 teryt_miejscowosc VARCHAR,
                 teryt_gmina VARCHAR,
                 gmina VARCHAR,
                 wazny_od_lub_data_nadania DATE
             );",
        )
        .unwrap();
        for (i, value) in inputs.iter().enumerate() {
            conn.execute(
                "INSERT INTO prg_raw VALUES
                     (?, ?, 21.0, 52.0, ?, 1, '1', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL)",
                duckdb::params![i as i32, value, format!("key-{i}")],
            )
            .unwrap();
        }

        materialize_into(&conn, "prg_out", "prg_raw").unwrap();

        // `ord` is a test-only helper column, not part of `materialize_into`'s
        // explicit projection (see its doc comment), so it does not survive
        // into `prg_out`. `lokalny_id` ("key-{i}") sorts the same as `ord`
        // for the single-digit input counts every caller here uses.
        let mut stmt = conn
            .prepare("SELECT ulica FROM prg_out ORDER BY lokalny_id")
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

    /// The explicit projection must not disturb the rest of the SELECT: the
    /// geometry column still has to be built from the lon/lat pair, and the
    /// coordinate-less filter still applies.
    #[test]
    fn materialize_still_writes_geometry() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_raw (
                 ulica VARCHAR,
                 dlugosc_geograficzna DOUBLE,
                 szerokosc_geograficzna DOUBLE,
                 lokalny_id VARCHAR,
                 wersja_id INTEGER,
                 numer_porzadkowy VARCHAR,
                 miejscowosc VARCHAR,
                 kod_pocztowy VARCHAR,
                 teryt_miejscowosc VARCHAR,
                 teryt_gmina VARCHAR,
                 gmina VARCHAR,
                 wazny_od_lub_data_nadania DATE
             );
             INSERT INTO prg_raw VALUES
                 ('ulica Herbaciana', 21.0, 52.0, 'a', 1, '1', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL),
                 ('Aleja Krakowska', 21.5, 52.5, 'b', 1, '2', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL),
                 -- filtered out: no coordinates
                 ('ulica Bezdomna', NULL, NULL, 'c', 1, '3', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL);",
        )
        .unwrap();

        materialize_into(&conn, "prg_out", "prg_raw").unwrap();

        let (rows, null_geoms): (i64, i64) = conn
            .query_row(
                "SELECT COUNT(*),
                        COUNT(*) FILTER (WHERE geom IS NULL)
                 FROM prg_out",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            rows, 2,
            "the coordinate-less row must still be filtered out"
        );
        assert_eq!(null_geoms, 0, "every row must carry a geometry");
    }

    /// A duplicate `lokalny_id` collapses to the row carrying the higher
    /// `wersja_id` -- the real case measured on the 2026-01-10 snapshot (see
    /// `materialize_into`'s doc comment): two rows share one `lokalny_id`,
    /// differing only in `wersja_id` and in a field the newer version
    /// corrects. Driven through the real `materialize_into`, not a copy of
    /// its SQL.
    #[test]
    fn materialize_collapses_duplicate_lokalny_id_to_the_newer_wersja_id() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_raw (
                 lokalny_id VARCHAR,
                 wersja_id INTEGER,
                 ulica VARCHAR,
                 dlugosc_geograficzna DOUBLE,
                 szerokosc_geograficzna DOUBLE,
                 numer_porzadkowy VARCHAR,
                 miejscowosc VARCHAR,
                 kod_pocztowy VARCHAR,
                 teryt_miejscowosc VARCHAR,
                 teryt_gmina VARCHAR,
                 gmina VARCHAR,
                 wazny_od_lub_data_nadania DATE
             );
             INSERT INTO prg_raw VALUES
                 ('dup-key', 1, 'stale', 21.0, 52.0, '1', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL),
                 ('dup-key', 2, 'fresh', 21.0, 52.0, '1', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL),
                 ('unique-key', 1, 'other', 22.0, 53.0, '2', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL);",
        )
        .unwrap();

        let stats = materialize_into(&conn, "prg_out", "prg_raw").unwrap();

        assert_eq!(stats.skipped_duplicate_key, 1);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM prg_out", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2, "duplicate collapses down to one row per key");

        let surviving_ulica: String = conn
            .query_row(
                "SELECT ulica FROM prg_out WHERE lokalny_id = 'dup-key'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            surviving_ulica, "fresh",
            "the higher wersja_id must win the tie-break, not just any surviving row"
        );
    }

    /// A NULL `lokalny_id` row has no usable identity and cannot be diffed
    /// or deduplicated, so it must not survive into the target table --
    /// mirroring BDOT10k/EGIB's `load_into_drops_null_keyed_rows`.
    #[test]
    fn materialize_drops_null_lokalny_id_rows() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_raw (
                 lokalny_id VARCHAR,
                 wersja_id INTEGER,
                 ulica VARCHAR,
                 dlugosc_geograficzna DOUBLE,
                 szerokosc_geograficzna DOUBLE,
                 numer_porzadkowy VARCHAR,
                 miejscowosc VARCHAR,
                 kod_pocztowy VARCHAR,
                 teryt_miejscowosc VARCHAR,
                 teryt_gmina VARCHAR,
                 gmina VARCHAR,
                 wazny_od_lub_data_nadania DATE
             );
             INSERT INTO prg_raw VALUES
                 ('has-key', 1, 'ok', 21.0, 52.0, '1', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL),
                 (NULL, 1, 'no-key', 22.0, 53.0, '2', 'Test', '00-000', '1465011', '1465011', 'Test Gmina', NULL);",
        )
        .unwrap();

        let stats = materialize_into(&conn, "prg_out", "prg_raw").unwrap();

        assert_eq!(stats.skipped_null_key, 1);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM prg_out", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "the NULL-keyed row must not survive");
    }

    /// Part of `docs/superpowers/plans/2026-08-14-column-trimming.md`'s
    /// Testing requirement: `wersja_id`'s removal
    /// (`dataset::drop_ordering_column`, run after the dedup) is a separate
    /// statement from the projection, so it is exactly the kind of step that
    /// gets lost in a refactor while every other test still passes. Also
    /// confirms `teryt_gmina`/`gmina` -- kept despite having no current
    /// reader, see `materialize_into`'s doc comment -- actually survive.
    #[test]
    fn materialize_drops_wersja_id_and_keeps_teryt_gmina_and_gmina() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_raw (
                 lokalny_id VARCHAR,
                 wersja_id INTEGER,
                 ulica VARCHAR,
                 dlugosc_geograficzna DOUBLE,
                 szerokosc_geograficzna DOUBLE,
                 numer_porzadkowy VARCHAR,
                 miejscowosc VARCHAR,
                 kod_pocztowy VARCHAR,
                 teryt_miejscowosc VARCHAR,
                 teryt_gmina VARCHAR,
                 gmina VARCHAR,
                 wazny_od_lub_data_nadania DATE
             );
             INSERT INTO prg_raw VALUES
                 ('a', 1, 'Testowa', 21.0, 52.0, '1', 'Test', '00-000', '1465011', '0805011', 'Kolsko', NULL);",
        )
        .unwrap();

        materialize_into(&conn, "prg_out", "prg_raw").unwrap();

        let has_wersja_id: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM duckdb_columns()
                 WHERE table_name = 'prg_out' AND column_name = 'wersja_id'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            !has_wersja_id,
            "wersja_id must not survive into the stored table -- \
             nothing reads it once the dedup it orders has run"
        );

        let (teryt_gmina, gmina): (String, String) = conn
            .query_row(
                "SELECT teryt_gmina, gmina FROM prg_out WHERE lokalny_id = 'a'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            (teryt_gmina.as_str(), gmina.as_str()),
            ("0805011", "Kolsko")
        );
    }

    /// Asserts against the raw table `stream_gml_into` produces, deliberately,
    /// rather than against `prg_addresses`: the obvious check
    /// (`SELECT DISTINCT wojewodztwo FROM prg_addresses`) is impossible because
    /// `wojewodztwo` is one of the columns `materialize_into` drops (see
    /// `docs/superpowers/plans/2026-08-14-column-trimming.md`,
    /// recommendation (1) under "One real coverage loss to resolve"). TERC is
    /// consumed inside `prg_convert`'s parser regardless of what this loader
    /// stores afterward, so `--terc-file` stays required either way -- this
    /// test pins that the mapping actually landed, not that the flag is merely
    /// accepted.
    #[test]
    fn stream_gml_into_applies_the_terc_mapping_before_materialize_projects_it_away() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let terc = get_teryt_mapping(
            false,
            &None,
            &None,
            &Some(PathBuf::from("fixtures/teryt.zip")),
        )
        .unwrap();
        let terc = Arc::new(terc);

        stream_gml_into(&conn, Path::new("fixtures/prg.zip"), &terc, "prg_raw").unwrap();

        let woj: String = conn
            .query_row("SELECT DISTINCT wojewodztwo FROM prg_raw", [], |r| r.get(0))
            .unwrap();
        assert_eq!(woj, "lubuskie");
    }
}
