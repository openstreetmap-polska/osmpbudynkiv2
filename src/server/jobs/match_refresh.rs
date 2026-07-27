//! `match_refresh` background job: drains the dirty-cell queue populated by
//! the government/OSM producers (see `compare::drain`), keeping the
//! `*_unmatched` serving tables fresh.

use anyhow::{Context, Result};

use crate::server::jobs::{Job, JobContext};

pub struct MatchRefreshJob {
    batch_size: usize,
}

impl MatchRefreshJob {
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }
}

impl Job for MatchRefreshJob {
    fn name(&self) -> &'static str {
        "match_refresh"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        let stats = crate::compare::drain::drain_batch(&conn, self.batch_size)?;
        if stats.cells > 0 {
            tracing::info!(cells = stats.cells, "match_refresh drained dirty cells");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_is_match_refresh() {
        assert_eq!(MatchRefreshJob::new(100).name(), "match_refresh");
    }
}
