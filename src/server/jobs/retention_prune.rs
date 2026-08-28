//! Background job that deletes rows older than a per-table retention window.
//!
//! Table-driven: `run` builds a small slice of (log key, table, timestamp
//! column, retention days) tuples and runs `prune_one` over each, so adding a
//! third pruned table is one array entry rather than another copy of the
//! function. Table and column names are interpolated into SQL text from
//! compile-time constants only, never user input -- the same as the `days`
//! interpolation below, which was already doing this before the table was
//! generalized.

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::server::jobs::{Job, JobContext};

/// `job_run_log` keys this job reports under (see `Job::log_keys`).
const PACKAGE_EXPORTS_KEY: &str = "prune:package_exports";
const CHANGE_AREAS_KEY: &str = "prune:change_areas";

pub struct RetentionPruneJob;

impl Job for RetentionPruneJob {
    fn name(&self) -> &'static str {
        "retention_prune"
    }

    fn log_keys(&self) -> &'static [&'static str] {
        &[PACKAGE_EXPORTS_KEY, CHANGE_AREAS_KEY]
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;

        let tables = [
            (
                PACKAGE_EXPORTS_KEY,
                "package_exports",
                "exported_at",
                ctx.config.jobs.retention_prune.package_exports_days,
            ),
            (
                CHANGE_AREAS_KEY,
                "dataset_change_areas",
                "detected_at",
                ctx.config.jobs.retention_prune.change_areas_days,
            ),
        ];

        // Run every table regardless of which one failed, then surface an
        // aggregate error if any did -- same shape (and same reasoning) as
        // `building_types_update::run`'s bdot10k/egib pair: never let one
        // side's failure skip the other's attempt. Collecting into a `Vec`
        // first is what makes that true -- a `Result`-short-circuiting
        // `collect` applied directly to the mapped iterator would stop at
        // the first `Err` and never attempt the tables after it.
        let results: Vec<Result<()>> = tables
            .iter()
            .map(|&(log_key, table, ts_column, days)| {
                prune_one(&conn, log_key, table, ts_column, days)
            })
            .collect();

        results.into_iter().collect::<Result<Vec<()>>>().map(|_| ())
    }
}

/// Deletes rows older than `days` from `table`, keyed by `ts_column`, and
/// records the outcome under `log_key`. `table`/`ts_column` are always one of
/// the compile-time constants in `run`'s tuple list above, never user input.
fn prune_one(
    conn: &Connection,
    log_key: &str,
    table: &str,
    ts_column: &str,
    days: u64,
) -> Result<()> {
    let outcome = conn.execute(
        &format!("DELETE FROM {table} WHERE {ts_column} < (now() - INTERVAL '{days} days')"),
        [],
    );

    match &outcome {
        Ok(deleted) => {
            let _ = crate::job_log::record(
                conn,
                log_key,
                "Success",
                Some(&format!("pruned {deleted} rows older than {days} days")),
            );
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, log_key, "Error", Some(&format!("{e:#}")));
        }
    }

    outcome
        .map(|_| ())
        .with_context(|| format!("Failed to prune {table}"))
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::config::Config as AppConfig;
    use crate::db::init_db;

    fn make_ctx(
        package_exports_days: u64,
        change_areas_days: u64,
    ) -> (JobContext, tempfile::TempDir) {
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
        config.jobs.retention_prune.package_exports_days = package_exports_days;
        config.jobs.retention_prune.change_areas_days = change_areas_days;

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
        let (ctx, _dir) = make_ctx(365, 90);
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

        RetentionPruneJob.run(&ctx).unwrap();

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
        assert_eq!(log[PACKAGE_EXPORTS_KEY].outcome, "Success");
        assert_eq!(
            log[PACKAGE_EXPORTS_KEY].message.as_deref(),
            Some("pruned 1 rows older than 365 days")
        );
    }

    #[test]
    fn no_op_when_nothing_is_old_enough() {
        let (ctx, _dir) = make_ctx(365, 90);
        {
            let conn = ctx.pool.get().unwrap();
            conn.execute(
                "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
                 VALUES (now(), ST_Point(21.0, 52.0), ['prg'], 1, 1)",
                [],
            )
            .unwrap();
        }

        RetentionPruneJob.run(&ctx).unwrap();

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
            log[PACKAGE_EXPORTS_KEY].message.as_deref(),
            Some("pruned 0 rows older than 365 days")
        );
    }

    #[test]
    fn deletes_change_areas_rows_older_than_retention_keeps_newer_ones() {
        let (ctx, _dir) = make_ctx(365, 90);
        {
            let conn = ctx.pool.get().unwrap();
            conn.execute_batch(
                "INSERT INTO dataset_change_areas (source, detected_at)
                 VALUES ('bdot10k', now() - INTERVAL '200 days');
                 INSERT INTO dataset_change_areas (source, detected_at)
                 VALUES ('bdot10k', now());",
            )
            .unwrap();
        }

        RetentionPruneJob.run(&ctx).unwrap();

        let conn = ctx.pool.get().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 1, "the 200-day-old row must be pruned, today's kept");

        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(log[CHANGE_AREAS_KEY].outcome, "Success");
        assert_eq!(
            log[CHANGE_AREAS_KEY].message.as_deref(),
            Some("pruned 1 rows older than 90 days")
        );
    }

    /// The behaviour the multi-table split actually introduces: a failure
    /// pruning one table must not skip the attempt on the other, and each
    /// table's outcome is recorded under its own log key. `DROP TABLE
    /// package_exports` induces a real `DELETE` failure without touching any
    /// production code path.
    #[test]
    fn a_failure_pruning_one_table_does_not_skip_the_other() {
        let (ctx, _dir) = make_ctx(365, 90);
        {
            let conn = ctx.pool.get().unwrap();
            conn.execute_batch(
                "INSERT INTO dataset_change_areas (source, detected_at)
                 VALUES ('bdot10k', now() - INTERVAL '200 days');
                 DROP TABLE package_exports;",
            )
            .unwrap();
        }

        let result = RetentionPruneJob.run(&ctx);
        assert!(
            result.is_err(),
            "package_exports pruning fails, so the aggregate result must be Err"
        );

        let conn = ctx.pool.get().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            count, 0,
            "dataset_change_areas must still be pruned even though package_exports failed"
        );

        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(log[PACKAGE_EXPORTS_KEY].outcome, "Error");
        assert_eq!(log[CHANGE_AREAS_KEY].outcome, "Success");
    }
}
