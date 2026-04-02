pub mod osm;

use anyhow::{Result, bail};
use duckdb::Connection;

use crate::cli::UpdateSource;
use crate::config::{Config, DownloadUrls};
use crate::osm::kvstore::RocksDB;

pub fn run(
    conn: &Connection,
    kv: &RocksDB,
    source: UpdateSource,
    _config: &Config,
    urls: &DownloadUrls,
) -> Result<()> {
    match source {
        UpdateSource::Osm => osm::update(conn, kv, _config, &urls.osm_replication),
        UpdateSource::Bdot10k => bail!("BDOT10k update is not yet implemented"),
        UpdateSource::Egib => bail!("EGIB update is not yet implemented"),
        UpdateSource::Prg => bail!("PRG update is not yet implemented"),
    }
}
