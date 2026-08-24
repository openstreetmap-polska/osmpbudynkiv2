//! Background job that retires reports whose government record has changed or
//! disappeared, so the object becomes importable again.
//!
//! **This is a safety net, not the primary path**, exactly like
//! `match_reconcile` next to `match_refresh`. `crate::reports::reconcile_source`
//! already runs inside every `update::dataset::refresh` apply transaction and
//! after every `import` arm, which between them cover every way a government
//! record can change. What this job covers is the case where one of those ran
//! against a *different* process (an offline `import` while the server was
//! stopped, a database restored from a snapshot) and left reports describing
//! records that no longer look the way they did.
//!
//! Off by default for that reason. Unlike `match_reconcile` the cost is not the
//! concern -- reconcile is `O(active reports)`, not `O(source table)` -- it is
//! simply that with the two real call sites in place there is normally nothing
//! for it to find, and a job that always reports zero is noise in `/status`.

use anyhow::{Context, Result};

use crate::server::jobs::{Job, JobContext};

/// `job_run_log` key this job reports under (see `Job::log_keys`).
const JOB_LOG_KEY: &str = "reports_reconcile";

pub struct ReportsReconcileJob;

impl Job for ReportsReconcileJob {
    fn name(&self) -> &'static str {
        "reports_reconcile"
    }

    fn log_keys(&self) -> &'static [&'static str] {
        &[JOB_LOG_KEY]
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;

        let outcome = crate::reports::reconcile_all(&conn);

        match &outcome {
            Ok(stats) => {
                let _ = crate::job_log::record(
                    &conn,
                    JOB_LOG_KEY,
                    "Success",
                    Some(&format!(
                        "expired {} changed and {} removed records, enqueued {} cells",
                        stats.expired_changed, stats.expired_removed, stats.cells_enqueued
                    )),
                );
            }
            Err(e) => {
                let _ =
                    crate::job_log::record(&conn, JOB_LOG_KEY, "Error", Some(&format!("{e:#}")));
            }
        }

        outcome.map(|_| ()).context("Failed to reconcile reports")
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

    /// Same shape as `export_log_prune`'s fixture, plus the three source tables
    /// the reconcile reads. They are built by their importers rather than by
    /// `create_schema`, so a test has to declare them — and all three, not just
    /// the one this test reports against: `reconcile_all` sweeps every spec, so
    /// a missing table fails the whole run rather than skipping that source.
    fn make_ctx() -> (JobContext, tempfile::TempDir) {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET GLOBAL geometry_always_xy = true".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR,
                 geom GEOMETRY, centroid GEOMETRY, WERSJA VARCHAR,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR,
                 LICZBAKONDYGNACJI SMALLINT, KATEGORIAISTNIENIA VARCHAR,
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (
                 id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR,
                 kondygnacje_nadziemne INTEGER, kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR,
                 geom GEOMETRY);
             INSERT INTO bdot10k_buildings
                 (PRZESTRZENNAZW, LOKALNYID, geom, centroid, WERSJA, KATEGORIAISTNIENIA)
             VALUES ('04', 'bud-1',
                     ST_MakeEnvelope(21.0, 52.0, 21.0002, 52.0002),
                     ST_Point(21.0001, 52.0001), 'v1', 'eksploatowany');",
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
        (ctx, dir)
    }

    #[test]
    fn expires_a_report_whose_record_changed_and_logs_what_it_did() {
        let (ctx, _dir) = make_ctx();
        {
            let conn = ctx.pool.get().unwrap();
            crate::reports::insert(
                &conn,
                &[crate::reports::ReportTarget {
                    spec: &crate::dataset::BDOT10K,
                    key: vec!["04".to_string(), "bud-1".to_string()],
                }],
            )
            .unwrap();
            // WERSJA is in BDOT10K.compared_columns, so this is a content
            // change by the same definition the refresh diff uses.
            conn.execute("UPDATE bdot10k_buildings SET WERSJA = 'v2'", [])
                .unwrap();
        }

        ReportsReconcileJob.run(&ctx).unwrap();

        let conn = ctx.pool.get().unwrap();
        let counts = crate::reports::counts(&conn).unwrap();
        assert_eq!(counts.active, 0);
        assert_eq!(counts.expired, 1);

        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(log[JOB_LOG_KEY].outcome, "Success");
        assert_eq!(
            log[JOB_LOG_KEY].message.as_deref(),
            Some("expired 1 changed and 0 removed records, enqueued 1 cells")
        );
    }

    #[test]
    fn a_run_with_nothing_to_do_still_records_its_zero() {
        let (ctx, _dir) = make_ctx();

        ReportsReconcileJob.run(&ctx).unwrap();

        // Same reasoning as export_log_prune's no-op test: a zero is a fact
        // worth showing in /status, not silence indistinguishable from "this
        // job never ran".
        let conn = ctx.pool.get().unwrap();
        let log = crate::job_log::read_all(&conn).unwrap();
        assert_eq!(
            log[JOB_LOG_KEY].message.as_deref(),
            Some("expired 0 changed and 0 removed records, enqueued 0 cells")
        );
    }
}
