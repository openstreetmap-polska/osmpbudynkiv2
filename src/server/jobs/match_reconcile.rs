//! `match_reconcile` background job: periodically re-enqueues every live
//! government cell so the `match_refresh` drain rebuilds it.
//!
//! This is the safety net the design relies on (design ~line 282): the only
//! real failure mode of the incremental path is a *dropped* enqueue, and it is
//! self-repairing only if something re-enqueues everything on a schedule. Until
//! this job existed the sweep was CLI-only (`compare reconcile`), which cannot
//! run while the server holds the DB — DuckDB is single-writer — so the safety
//! net required stopping the service.
//!
//! It is safe against a live server precisely because it does not touch the
//! serving tables itself: it only appends to `match_dirty_cells` and lets the
//! per-cell drain do the rebuilding, one committed cell at a time. A serving
//! table is therefore never empty or partial for a reader.

use anyhow::{Context, Result};

use crate::server::jobs::{Job, JobContext};

pub struct MatchReconcileJob;

impl Job for MatchReconcileJob {
    fn name(&self) -> &'static str {
        "match_reconcile"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        let enqueued = crate::compare::reconcile::enqueue_all(&conn)?;
        tracing::info!(enqueued, "match_reconcile sweep enqueued every live cell");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_is_match_reconcile() {
        assert_eq!(MatchReconcileJob.name(), "match_reconcile");
    }
}
