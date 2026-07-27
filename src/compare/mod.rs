pub mod addresses;
pub mod buildings;
pub mod drain;
pub mod incremental;
pub mod reconcile;
pub mod rule;

use anyhow::Result;
use duckdb::Connection;
use tracing::info;

use crate::cli::{AddressesSource, BuildingsSource, CompareTarget};

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
        CompareTarget::Addresses { source } => match source {
            None | Some(AddressesSource::All) => addresses::compare_prg(conn)?,
            Some(AddressesSource::Prg) => addresses::compare_prg(conn)?,
        },
        CompareTarget::Full => {
            buildings::compare_bdot10k(conn)?;
            buildings::compare_egib(conn)?;
            addresses::compare_prg(conn)?;
        }
        CompareTarget::Reconcile => {
            let enqueued = reconcile::enqueue_all(conn)?;
            info!(enqueued, "reconcile sweep complete");
        }
    }
    Ok(())
}
