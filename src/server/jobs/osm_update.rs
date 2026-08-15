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

    /// See `update::osm::OSM_UPDATE_JOB_LOG_KEY` -- `update::osm::update`
    /// self-reports under this key rather than the wrapper doing it, since
    /// that function is also reachable straight from the CLI.
    fn log_keys(&self) -> &'static [&'static str] {
        &[crate::update::osm::OSM_UPDATE_JOB_LOG_KEY]
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
