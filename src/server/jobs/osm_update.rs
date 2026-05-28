use anyhow::Result;

use crate::server::jobs::{Job, JobContext};

/// Background job that applies OSM minutely replication diffs.
///
/// Delegates to [`crate::update::osm::update`]. That function already
/// polls the global shutdown flag between sequences, so a SIGINT during
/// a long run exits gracefully without needing the per-job cancel flag.
/// Per-job timeouts may still expire mid-sequence; the supervisor records
/// `TimedOut` and the inner update returns at its next natural exit point.
pub struct OsmUpdateJob;

impl Job for OsmUpdateJob {
    fn name(&self) -> &'static str {
        "osm_update"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx.write.lock().expect("write mutex poisoned");
        crate::update::osm::update(
            &conn,
            &ctx.kv,
            &ctx.config,
            &ctx.config.download_urls.osm_replication,
        )
    }
}
