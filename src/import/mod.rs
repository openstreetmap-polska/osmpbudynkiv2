pub mod bdot10k;
pub mod egib;
pub mod osm;
pub mod prg;

use anyhow::Result;
use duckdb::Connection;
use tracing::{info, warn};

use crate::cli::ImportSource;
use crate::config::{Config, DownloadUrls};
use crate::osm::kvstore::RocksDB;

pub fn run(
    conn: &Connection,
    kv: &RocksDB,
    source: ImportSource,
    config: &Config,
    urls: &DownloadUrls,
) -> Result<()> {
    match source {
        ImportSource::Osm { file } => osm::import(conn, kv, config, file.as_deref(), &urls.osm_pbf),
        ImportSource::Bdot10k { file } => {
            bdot10k::import(conn, config, file.as_deref(), &urls.bdot10k)?;
            stamp_row_hash_version(conn)?;
            bump_serving_epoch(conn)
        }
        ImportSource::Egib { file } => {
            egib::import(conn, config, file.as_deref(), &urls.egib)?;
            stamp_row_hash_version(conn)?;
            bump_serving_epoch(conn)
        }
        ImportSource::Prg { file, terc_file } => {
            prg::import(
                conn,
                config,
                file.as_deref(),
                terc_file.as_deref(),
                &urls.prg,
            )?;
            stamp_row_hash_version(conn)?;
            bump_serving_epoch(conn)
        }
        ImportSource::StreetMappings { file, url } => {
            let outcome = (|| -> Result<crate::mappings::LoadStats> {
                let (path, was_downloaded) = match file {
                    Some(p) => (p, false),
                    None => {
                        let src = url.as_deref().unwrap_or(&urls.street_mappings);
                        let downloaded = crate::download::download_file_as(
                            src,
                            &config.download_dir(),
                            "street_names_mappings.csv",
                        )?;
                        (downloaded, true)
                    }
                };
                let stats = crate::mappings::load_from_path(conn, &path)?;

                if was_downloaded {
                    if config.cleanup_downloaded_files {
                        info!(path = %path.display(), "Cleaning up downloaded file");
                        let _ = std::fs::remove_file(&path);
                    } else {
                        warn!(
                            path = %path.display(),
                            "cleanup_downloaded_files is false; leaving downloaded file in place \
                             (it will be reused on the next run since download_file_as skips \
                             re-downloading an existing destination)"
                        );
                    }
                }

                Ok(stats)
            })();
            match &outcome {
                Ok(stats) => {
                    let msg = format!(
                        "loaded {} mapping rows ({} not present in current PRG data)",
                        stats.rows_loaded, stats.rows_absent_from_prg
                    );
                    let _ = crate::job_log::record(
                        conn,
                        "import:street-mappings",
                        "Success",
                        Some(&msg),
                    );
                }
                Err(e) => {
                    let _ = crate::job_log::record(
                        conn,
                        "import:street-mappings",
                        "Error",
                        Some(&format!("{e:#}")),
                    );
                }
            }
            outcome.map(|_| ())
        }
        ImportSource::BuildingTypes {
            bdot10k_file,
            egib_file,
            bdot10k_url,
            egib_url,
        } => {
            use crate::mappings::building_types::{BDOT10K, BuildingTypeStats, EGIB};

            let outcome = (|| -> Result<(BuildingTypeStats, BuildingTypeStats)> {
                let bdot10k_stats = load_building_type_file(
                    conn,
                    config,
                    &BDOT10K,
                    bdot10k_file,
                    bdot10k_url
                        .as_deref()
                        .unwrap_or(&urls.bdot10k_building_types),
                    "bdot10k_building_types.csv",
                )?;
                let egib_stats = load_building_type_file(
                    conn,
                    config,
                    &EGIB,
                    egib_file,
                    egib_url.as_deref().unwrap_or(&urls.egib_building_types),
                    "egib_building_types.csv",
                )?;
                Ok((bdot10k_stats, egib_stats))
            })();

            match &outcome {
                Ok((b, e)) => {
                    let msg = format!(
                        "bdot10k: loaded {} rows ({} keys absent from source, {} source keys \
                         / {} source rows uncovered); egib: loaded {} rows ({} keys absent \
                         from source, {} source keys / {} source rows uncovered)",
                        b.rows_loaded,
                        b.keys_absent_from_source,
                        b.source_keys_uncovered,
                        b.source_rows_uncovered,
                        e.rows_loaded,
                        e.keys_absent_from_source,
                        e.source_keys_uncovered,
                        e.source_rows_uncovered,
                    );
                    let _ = crate::job_log::record(
                        conn,
                        "import:building-types",
                        "Success",
                        Some(&msg),
                    );
                }
                Err(e) => {
                    let _ = crate::job_log::record(
                        conn,
                        "import:building-types",
                        "Error",
                        Some(&format!("{e:#}")),
                    );
                }
            }
            outcome.map(|_| ())
        }
        ImportSource::Full {
            osm_file,
            bdot10k_file,
            egib_file,
            prg_file,
            terc_file,
        } => {
            osm::import(conn, kv, config, osm_file.as_deref(), &urls.osm_pbf)?;
            crate::shutdown::check_requested()?;
            bdot10k::import(conn, config, bdot10k_file.as_deref(), &urls.bdot10k)?;
            crate::shutdown::check_requested()?;
            egib::import(conn, config, egib_file.as_deref(), &urls.egib)?;
            crate::shutdown::check_requested()?;
            prg::import(
                conn,
                config,
                prg_file.as_deref(),
                terc_file.as_deref(),
                &urls.prg,
            )?;
            stamp_row_hash_version(conn)?;
            bump_serving_epoch(conn)
        }
    }
}

/// Resolve `file` vs. downloading from `url` (mirrors the `StreetMappings`
/// arm above), load it through `source`, then clean up a downloaded file per
/// `config.cleanup_downloaded_files` -- a user-supplied `--*-file` is never
/// deleted regardless of that setting.
fn load_building_type_file(
    conn: &Connection,
    config: &Config,
    source: &crate::mappings::building_types::BuildingTypeSource,
    file: Option<std::path::PathBuf>,
    url: &str,
    download_filename: &str,
) -> Result<crate::mappings::building_types::BuildingTypeStats> {
    let (path, was_downloaded) = match file {
        Some(p) => (p, false),
        None => {
            let downloaded =
                crate::download::download_file_as(url, &config.download_dir(), download_filename)?;
            (downloaded, true)
        }
    };
    let stats = crate::mappings::building_types::load_from_path(conn, source, &path)?;

    if was_downloaded {
        if config.cleanup_downloaded_files {
            info!(path = %path.display(), "Cleaning up downloaded file");
            let _ = std::fs::remove_file(&path);
        } else {
            warn!(
                path = %path.display(),
                "cleanup_downloaded_files is false; leaving downloaded file in place \
                 (it will be reused on the next run since download_file_as skips \
                 re-downloading an existing destination)"
            );
        }
    }
    Ok(stats)
}

/// Stamp the row-hash version after an import rebuilds a dataset table.
///
/// An import writes `_row_hash` with the current expression, so the stamp is
/// what lets a later `update` tell "these hashes are comparable" from "the
/// expression changed underneath them". OSM is exempt — it has no `_row_hash`.
fn stamp_row_hash_version(conn: &Connection) -> Result<()> {
    crate::dataset::stamp_row_hash_version(conn)
}

/// Bump the serving epoch after an import rebuilds a dataset table.
///
/// `/tiles` reads bdot10k/egib/prg through the `*_unmatched` serving tables
/// (per-cell versioned, see `serving_version`) but also reads the raw
/// `bdot10k_buildings`/`egib_buildings`/`prg_addresses` tables directly for
/// the `*_all` legend layers and the adjacency CTEs — neither of which any
/// per-cell version can cover. An `import` rewrites those raw tables
/// wholesale, so it must bump. OSM is exempt — `/tiles` reads no `osm_*`
/// table (see `serving_version`'s module doc, "Must NOT bump").
fn bump_serving_epoch(conn: &Connection) -> Result<()> {
    crate::serving_version::bump_serving_epoch(conn)
}
