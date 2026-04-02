pub mod bdot10k;
pub mod egib;
pub mod osm;

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
        ImportSource::Bdot10k { file } => bdot10k::import(conn, file.as_deref(), &urls.bdot10k),
        ImportSource::Egib { file } => egib::import(conn, file.as_deref(), &urls.egib),
        ImportSource::Prg { .. } => bail!("PRG import is not yet implemented"),
        ImportSource::Full => {
            bail!("Full import is not yet implemented");
        }
    }
}
