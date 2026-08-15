//! `match_reconcile` background job: periodically re-enqueues every live
//! government cell so the `match_refresh` drain rebuilds it.
//!
//! This is the safety net the design relies on (design ~line 282): the only
//! real failure mode of the incremental path is a *dropped* enqueue, and it is
//! self-repairing only if something re-enqueues everything on a schedule. Until
//! this job existed the sweep was CLI-only (`queue reconcile`), which cannot
//! run while the server holds the DB — DuckDB is single-writer — so the safety
//! net required stopping the service.
//!
//! It is safe against a live server precisely because it does not touch the
//! serving tables itself: it only appends to `match_dirty_cells` and lets the
//! per-cell drain do the rebuilding, one committed cell at a time. A serving
//! table is therefore never empty or partial for a reader.

use anyhow::{Context, Result};

use crate::server::jobs::{Job, JobContext};

/// `job_run_log` key this job reports under (see `Job::log_keys`).
const JOB_LOG_KEY: &str = "match_reconcile";

pub struct MatchReconcileJob;

impl Job for MatchReconcileJob {
    fn name(&self) -> &'static str {
        "match_reconcile"
    }

    fn log_keys(&self) -> &'static [&'static str] {
        &[JOB_LOG_KEY]
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        let outcome = crate::compare::reconcile::enqueue_all(&conn);

        match &outcome {
            Ok(enqueued) => {
                tracing::info!(enqueued, "match_reconcile sweep enqueued every live cell");
                let _ = crate::job_log::record(
                    &conn,
                    JOB_LOG_KEY,
                    "Success",
                    Some(&format!("enqueued {enqueued} cells")),
                );
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
    fn name_is_match_reconcile() {
        assert_eq!(MatchReconcileJob.name(), "match_reconcile");
    }

    // A run over an empty database still writes a job_run_log row saying
    // "enqueued 0 cells" -- silence would be indistinguishable from the job
    // never having run at all.
    #[test]
    fn logs_zero_cells_on_an_empty_database() {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET GLOBAL geometry_always_xy = true".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        // `bdot10k_buildings`/`egib_buildings`/`prg_addresses` are created by
        // the import path (`CREATE TABLE AS SELECT`), not `create_schema`,
        // so a bare `init_db` doesn't have them -- `enqueue_all` needs at
        // least empty tables to select from.
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        let pool = crate::server::build_pool(conn, 2).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(crate::osm::kvstore::open(dir.path(), 8, 4).unwrap());
        let ctx = JobContext {
            pool,
            kv,
            config: Arc::new(AppConfig::default()),
            cancel: Arc::new(AtomicBool::new(false)),
        };

        MatchReconcileJob.run(&ctx).unwrap();

        let conn = ctx.pool.get().unwrap();
        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(log[JOB_LOG_KEY].outcome, "Success");
        assert_eq!(
            log[JOB_LOG_KEY].message.as_deref(),
            Some("enqueued 0 cells")
        );
    }
}
