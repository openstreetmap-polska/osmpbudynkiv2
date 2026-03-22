pub mod osm;

use anyhow::{Result, bail};
use duckdb::Connection;

use crate::cli::UpdateSource;

pub fn run(conn: &Connection, source: UpdateSource) -> Result<()> {
    match source {
        UpdateSource::Osm => osm::update(conn),
        UpdateSource::Bdot10k => bail!("BDOT10k update is not yet implemented"),
        UpdateSource::Egib => bail!("EGIB update is not yet implemented"),
        UpdateSource::Prg => bail!("PRG update is not yet implemented"),
    }
}
