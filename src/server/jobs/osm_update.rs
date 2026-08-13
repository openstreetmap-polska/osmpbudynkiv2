use anyhow::{Context, Result};

use crate::server::jobs::{Job, JobContext};

/// Background job that applies OSM minutely replication diffs.
///
/// Delegates to [`crate::update::osm::update`]. That function polls both the
/// global shutdown flag AND `ctx.is_cancelled()` between sequences (never
/// mid-sequence -- a sequence's DuckDB transaction is the atomic unit), so a
/// SIGINT exits gracefully and a supervisor timeout on a long catch-up run
/// (e.g. importing a day-old PBF and replaying ~1440 minutely sequences)
/// actually shortens the run instead of only being recorded after the fact
/// once the whole backlog has drained.
pub struct OsmUpdateJob;

impl Job for OsmUpdateJob {
    fn name(&self) -> &'static str {
        "osm_update"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        crate::update::osm::update(
            &conn,
            &ctx.kv,
            &ctx.config,
            &ctx.config.download_urls.osm_replication,
            false,
            &|| ctx.is_cancelled(),
        )
    }
}
