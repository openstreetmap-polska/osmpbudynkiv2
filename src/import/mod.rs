pub mod bdot10k;
pub mod egib;
pub mod osm;
pub mod prg;

use anyhow::{Result, bail};
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
            bdot10k::import(conn, config, file.as_deref(), &urls.bdot10k)
        }
        ImportSource::Egib { file } => egib::import(conn, config, file.as_deref(), &urls.egib),
        ImportSource::Prg { file, terc_file } => prg::import(
            conn,
            config,
            file.as_deref(),
            terc_file.as_deref(),
            &urls.prg,
        ),
        ImportSource::Full => {
            bail!("Full import is not yet implemented");
        }
    }
}
