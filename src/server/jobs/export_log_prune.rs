//! Background job that deletes package_exports rows older than
//! `config.jobs.export_log_prune.retention_days`.

use anyhow::{Context, Result};

use crate::server::jobs::{Job, JobContext};

/// `job_run_log` key this job reports under (see `Job::log_keys`).
const JOB_LOG_KEY: &str = "export_log_prune";

pub struct ExportLogPruneJob;

impl Job for ExportLogPruneJob {
    fn name(&self) -> &'static str {
        "export_log_prune"
    }

    fn log_keys(&self) -> &'static [&'static str] {
        &[JOB_LOG_KEY]
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        let days = ctx.config.jobs.export_log_prune.retention_days;
        let outcome = conn.execute(
            &format!(
                "DELETE FROM package_exports WHERE exported_at < (now() - INTERVAL '{days} days')"
            ),
            [],
        );

        match &outcome {
            Ok(deleted) => {
                let _ = crate::job_log::record(
                    &conn,
                    JOB_LOG_KEY,
                    "Success",
                    Some(&format!("pruned {deleted} rows older than {days} days")),
                );
            }
            Err(e) => {
                let _ =
                    crate::job_log::record(&conn, JOB_LOG_KEY, "Error", Some(&format!("{e:#}")));
            }
        }

        outcome
            .map(|_| ())
            .context("Failed to prune package_exports")
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

    fn make_ctx(retention_days: u64) -> (JobContext, tempfile::TempDir) {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET GLOBAL geometry_always_xy = true".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let pool = crate::server::build_pool(conn, 2).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(crate::osm::kvstore::open(dir.path(), 8, 4).unwrap());

        let mut config = AppConfig::default();
        config.jobs.export_log_prune.retention_days = retention_days;

        let ctx = JobContext {
            pool,
            kv,
            config: Arc::new(config),
            cancel: Arc::new(AtomicBool::new(false)),
        };
        (ctx, dir)
    }

    #[test]
    fn deletes_rows_older_than_retention_keeps_newer_ones() {
        let (ctx, _dir) = make_ctx(365);
        {
            let conn = ctx.pool.get().unwrap();
            conn.execute_batch(
                "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
                 VALUES (now() - INTERVAL '400 days', ST_Point(21.0, 52.0), ['prg'], 1, 1);
                 INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
                 VALUES (now() - INTERVAL '10 days', ST_Point(21.0, 52.0), ['prg'], 2, 2);",
            )
            .unwrap();
        }

        ExportLogPruneJob.run(&ctx).unwrap();

        let conn = ctx.pool.get().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM package_exports", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let remaining_count: i32 = conn
            .query_row("SELECT address_count FROM package_exports", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(remaining_count, 2, "the 10-day-old row must survive");

        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(log[JOB_LOG_KEY].outcome, "Success");
        assert_eq!(
            log[JOB_LOG_KEY].message.as_deref(),
            Some("pruned 1 rows older than 365 days")
        );
    }

    #[test]
    fn no_op_when_nothing_is_old_enough() {
        let (ctx, _dir) = make_ctx(365);
        {
            let conn = ctx.pool.get().unwrap();
            conn.execute(
                "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
                 VALUES (now(), ST_Point(21.0, 52.0), ['prg'], 1, 1)",
                [],
            )
            .unwrap();
        }

        ExportLogPruneJob.run(&ctx).unwrap();

        let conn = ctx.pool.get().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM package_exports", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);

        // A no-op run still writes a job_run_log row -- 0 pruned is a fact
        // worth showing in /status, not silence indistinguishable from "this
        // job never ran".
        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(
            log[JOB_LOG_KEY].message.as_deref(),
            Some("pruned 0 rows older than 365 days")
        );
    }
}
