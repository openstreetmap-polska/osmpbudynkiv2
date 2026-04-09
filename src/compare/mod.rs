pub mod buildings;

use anyhow::Result;
use duckdb::Connection;

use crate::cli::{BuildingsSource, CompareTarget};

pub fn run(conn: &Connection, target: CompareTarget) -> Result<()> {
    match target {
        CompareTarget::Buildings { source } => match source {
            None | Some(BuildingsSource::All) => {
                buildings::compare_bdot10k(conn)?;
                buildings::compare_egib(conn)?;
            }
            Some(BuildingsSource::Bdot10k) => buildings::compare_bdot10k(conn)?,
            Some(BuildingsSource::Egib) => buildings::compare_egib(conn)?,
        },
        // When new comparison targets are added, fan out to them here.
        CompareTarget::Full => {
            buildings::compare_bdot10k(conn)?;
            buildings::compare_egib(conn)?;
        }
    }
    Ok(())
}
