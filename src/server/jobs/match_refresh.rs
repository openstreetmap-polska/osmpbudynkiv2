//! `match_refresh` background job: drains the dirty-cell queue populated by
//! the government/OSM producers (see `compare::drain`), keeping the
//! `*_unmatched` serving tables fresh.

use anyhow::{Context, Result};

use crate::server::jobs::{Job, JobContext};

/// `job_run_log` key this job reports under (see `Job::log_keys`).
const JOB_LOG_KEY: &str = "match_refresh";

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

    fn log_keys(&self) -> &'static [&'static str] {
        &[JOB_LOG_KEY]
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        let outcome =
            crate::compare::drain::drain_batch(&conn, self.batch_size, &|| ctx.is_cancelled());

        match &outcome {
            Ok(stats) => {
                if stats.cells > 0 {
                    tracing::info!(cells = stats.cells, "match_refresh drained dirty cells");
                }
                if stats.failed > 0 {
                    tracing::warn!(
                        failed = stats.failed,
                        "match_refresh: some cells failed to recompute and remain queued for retry"
                    );
                }
                // Still "Success" -- a failed cell is rolled back and left
                // queued for retry by `drain_batch` itself, not an error
                // this job bails on, so the message is where the failure
                // count belongs, not the outcome. `run()` returning `Ok(())`
                // below (the registry's own `last_outcome`) agrees.
                let msg = if stats.failed > 0 {
                    format!(
                        "drained {} cells, {} failed and remain queued for retry",
                        stats.cells, stats.failed
                    )
                } else {
                    format!("drained {} cells", stats.cells)
                };
                let _ = crate::job_log::record(&conn, JOB_LOG_KEY, "Success", Some(&msg));
            }
            Err(e) => {
                let _ =
                    crate::job_log::record(&conn, JOB_LOG_KEY, "Error", Some(&format!("{e:#}")));
            }
        }

        outcome.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::config::Config as AppConfig;
    use crate::db::init_db;

    #[test]
    fn name_is_match_refresh() {
        assert_eq!(MatchRefreshJob::new(100).name(), "match_refresh");
    }

    // An empty queue is still a run worth showing in /status, not silence.
    #[test]
    fn logs_zero_cells_when_the_queue_is_empty() {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let pool = crate::server::build_pool(conn, 2).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(crate::osm::kvstore::open(dir.path(), 8, 4).unwrap());
        let ctx = JobContext {
            pool,
            kv,
            config: Arc::new(AppConfig::default()),
            cancel: Arc::new(AtomicBool::new(false)),
        };

        MatchRefreshJob::new(100).run(&ctx).unwrap();

        let conn = ctx.pool.get().unwrap();
        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(log[JOB_LOG_KEY].outcome, "Success");
        assert_eq!(log[JOB_LOG_KEY].message.as_deref(), Some("drained 0 cells"));
    }
}
