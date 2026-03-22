pub mod bdot10k;
pub mod egib;
pub mod osm;

use anyhow::{Result, bail};
use duckdb::Connection;

use crate::cli::ImportSource;

pub fn run(conn: &Connection, source: ImportSource) -> Result<()> {
    match source {
        ImportSource::Osm { file } => osm::import(conn, file.as_deref()),
        ImportSource::Bdot10k { file } => bdot10k::import(conn, file.as_deref()),
        ImportSource::Egib { file } => egib::import(conn, file.as_deref()),
        ImportSource::Prg { .. } => bail!("PRG import is not yet implemented"),
        ImportSource::Full => {
            bail!("Full import is not yet implemented");
        }
    }
}
