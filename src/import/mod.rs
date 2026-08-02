pub mod bdot10k;
pub mod egib;
pub mod osm;
pub mod prg;

use anyhow::Result;
use duckdb::Connection;

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
            stamp_row_hash_version(conn)
        }
        ImportSource::Egib { file } => {
            egib::import(conn, config, file.as_deref(), &urls.egib)?;
            stamp_row_hash_version(conn)
        }
        ImportSource::Prg { file, terc_file } => {
            prg::import(
                conn,
                config,
                file.as_deref(),
                terc_file.as_deref(),
                &urls.prg,
            )?;
            stamp_row_hash_version(conn)
        }
        ImportSource::StreetMappings { file } => {
            let path =
                file.ok_or_else(|| anyhow::anyhow!("import street-mappings requires --file"))?;
            let outcome = crate::mappings::load_from_path(conn, &path);
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
        ImportSource::Full {
            osm_file,
            bdot10k_file,
            egib_file,
            prg_file,
            terc_file,
        } => {
            osm::import(conn, kv, config, osm_file.as_deref(), &urls.osm_pbf)?;
            bdot10k::import(conn, config, bdot10k_file.as_deref(), &urls.bdot10k)?;
            egib::import(conn, config, egib_file.as_deref(), &urls.egib)?;
            prg::import(
                conn,
                config,
                prg_file.as_deref(),
                terc_file.as_deref(),
                &urls.prg,
            )?;
            stamp_row_hash_version(conn)
        }
    }
}

/// Stamp the row-hash version after an import rebuilds a dataset table.
///
/// An import writes `_row_hash` with the current expression, so the stamp is
/// what lets a later `update` tell "these hashes are comparable" from "the
/// expression changed underneath them". OSM is exempt — it has no `_row_hash`.
fn stamp_row_hash_version(conn: &Connection) -> Result<()> {
    crate::dataset::stamp_row_hash_version(conn)
}
