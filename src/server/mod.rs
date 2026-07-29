pub mod jobs;
mod package;
mod tiles;
mod updates;

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::{Router, http::StatusCode};
use duckdb::Connection;
use r2d2::Pool;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::Config as AppConfig;
use crate::shutdown;

/// An `r2d2::ManageConnection` that hands out `try_clone()`s of one shared
/// base connection instead of opening the database file itself.
///
/// `duckdb::DuckdbConnectionManager` (the crate's built-in r2d2 manager)
/// always opens its own independent connection internally
/// (`Connection::open`), which would create a second, unsynchronized DuckDB
/// engine instance on the same file — see
/// docs/duckdb_connection_visibility_investigation.md for the staleness bug
/// this caused. Cloning from the app's one already-open, already-initialized
/// connection instead means every pooled connection shares live MVCC state:
/// a write committed through one pooled connection is immediately visible to
/// every other one.
///
/// A clone inherits write capability (cloning does not downgrade to
/// read-only), so this pool has no engine-enforced guarantee against writes
/// from read-path handlers — same trust level the old single `write` mutex
/// already relied on.
#[derive(Debug)]
pub struct ClonedConnectionManager {
    base: Arc<Mutex<Connection>>,
}

impl ClonedConnectionManager {
    pub fn new(base_conn: Connection) -> Self {
        Self {
            base: Arc::new(Mutex::new(base_conn)),
        }
    }
}

impl r2d2::ManageConnection for ClonedConnectionManager {
    type Connection = Connection;
    type Error = duckdb::Error;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        self.base.lock().unwrap().try_clone()
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.execute_batch("")
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

pub type DbPool = Pool<ClonedConnectionManager>;

/// Builds the shared pool from an already-open, already-initialized base
/// connection (extensions loaded, schema created, `duckdb_init_commands` run
/// — see `db::init_db`). No further per-connection setup is needed: extension
/// loads, custom table/scalar function registrations, and `SET GLOBAL`
/// settings are all instance-wide and already visible to every `try_clone()`
/// (verified empirically — docs/duckdb_connection_visibility_investigation.md).
pub fn build_pool(base_conn: Connection, pool_size: u32) -> Result<DbPool> {
    Pool::builder()
        .max_size(pool_size)
        .build(ClonedConnectionManager::new(base_conn))
        .context("Failed to build DB connection pool")
}

#[derive(Clone)]
pub struct AppState {
    pub pool: DbPool,
    pub registry: Arc<jobs::JobRegistry>,
    pub config: Arc<AppConfig>,
}

pub async fn run(
    conn: Connection,
    kv: Arc<crate::osm::kvstore::RocksDB>,
    config: Arc<AppConfig>,
) -> Result<()> {
    let pool = build_pool(conn, config.db_pool_size)?;

    check_startup_conditions(&pool)?;

    let osm_cfg = jobs::JobConfigResolved::from(&config.jobs.osm_update);
    let export_prune_cfg = jobs::JobConfigResolved {
        enabled: config.jobs.export_log_prune.enabled,
        interval: std::time::Duration::from_secs(config.jobs.export_log_prune.interval_seconds),
        timeout: std::time::Duration::from_secs(config.jobs.export_log_prune.timeout_seconds),
    };
    let job_list: Vec<(Arc<dyn jobs::Job>, jobs::JobConfigResolved)> = vec![
        (
            Arc::new(jobs::osm_update::OsmUpdateJob) as Arc<dyn jobs::Job>,
            osm_cfg,
        ),
        (
            Arc::new(jobs::export_log_prune::ExportLogPruneJob) as Arc<dyn jobs::Job>,
            export_prune_cfg,
        ),
        (
            Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
                &crate::dataset::BDOT10K,
                "bdot10k_update",
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved::from(&config.jobs.bdot10k_update),
        ),
        (
            Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
                &crate::dataset::EGIB,
                "egib_update",
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved::from(&config.jobs.egib_update),
        ),
        (
            Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
                &crate::dataset::PRG,
                "prg_update",
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved::from(&config.jobs.prg_update),
        ),
        (
            Arc::new(jobs::match_refresh::MatchRefreshJob::new(
                config.jobs.match_refresh.batch_size,
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved {
                enabled: config.jobs.match_refresh.enabled,
                interval: std::time::Duration::from_secs(
                    config.jobs.match_refresh.interval_seconds,
                ),
                timeout: std::time::Duration::from_secs(config.jobs.match_refresh.timeout_seconds),
            },
        ),
    ];
    let scheduler = jobs::Scheduler::start(job_list, pool.clone(), kv, config.clone());
    let registry = scheduler.registry.clone();
    let shutdown_notify = scheduler.shutdown_notify();

    let state = AppState {
        pool,
        registry,
        config: config.clone(),
    };

    let app = Router::new()
        .route("/health", axum::routing::get(|| async { StatusCode::OK }))
        .route(
            "/status",
            axum::routing::get(jobs::status_handler::get_status),
        )
        .route("/tiles/{z}/{x}/{y}", axum::routing::get(tiles::serve_tile))
        .route(
            "/package",
            axum::routing::get(package::get_package).post(package::post_package),
        )
        .route("/updates", axum::routing::get(updates::get_updates))
        .with_state(state);

    let listener = TcpListener::bind(&config.http_listen_addr).await?;
    info!(addr = %config.http_listen_addr, "HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                if shutdown::is_requested() {
                    shutdown_notify.notify_waiters();
                    break;
                }
            }
        })
        .await?;

    scheduler
        .shutdown(tokio::time::Duration::from_secs(30))
        .await;

    Ok(())
}

const REQUIRED_TABLES: &[&str] = &[
    "metadata",
    "osm_addresses",
    "osm_buildings",
    "prg_addresses",
    "bdot10k_buildings",
    "egib_buildings",
];

/// The serving tables `/tiles` and `/package` read directly. Unlike
/// `REQUIRED_TABLES` these always exist (created by `CREATE TABLE IF NOT
/// EXISTS` in `db::create_schema` on every startup), so there is nothing to
/// bail on -- only "empty" is worth flagging, since an in-place upgrade of an
/// existing database gains these tables empty and would otherwise start
/// serving zero features with no indication why (see README).
const UNMATCHED_TABLES: &[&str] = &["bdot10k_unmatched", "egib_unmatched", "prg_unmatched"];

fn check_startup_conditions(pool: &DbPool) -> Result<()> {
    let conn = pool
        .get()
        .context("Failed to acquire a connection for startup checks")?;

    conn.query_row("SELECT 1", [], |_| Ok(()))
        .context("Startup health check failed")?;

    for table in REQUIRED_TABLES {
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = 'main' AND table_name = ?",
                [table],
                |row| row.get(0),
            )
            .with_context(|| format!("Failed to check existence of table '{table}'"))?;

        if exists == 0 {
            anyhow::bail!(
                "Required table '{table}' is missing — run the import commands before starting the server"
            );
        }

        let rows: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .with_context(|| format!("Failed to count rows in table '{table}'"))?;

        info!("Table {} has {} rows.", table, rows);
    }

    for table in UNMATCHED_TABLES {
        let rows: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .with_context(|| format!("Failed to count rows in table '{table}'"))?;

        info!("Table {} has {} rows.", table, rows);
        if rows == 0 {
            tracing::warn!(
                "serving table '{table}' is empty -- /tiles and /package will return no \
                 features for this source until an offline `compare full` populates it \
                 (see README)"
            );
        }
    }

    info!("Startup checks passed: pool connection OK, all required tables present");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::{Router, routing::get};
    use duckdb::Connection;
    use tower::ServiceExt;

    use super::jobs::{JobOutcome, JobRegistry, JobState, JobStatus};
    use super::{AppState, build_pool, jobs};

    fn make_test_state(initial: Vec<JobStatus>) -> AppState {
        let conn = Connection::open_in_memory().unwrap();
        let pool = build_pool(conn, 2).unwrap();
        AppState {
            pool,
            registry: Arc::new(JobRegistry::new_for_tests(initial)),
            config: Arc::new(crate::config::Config::default()),
        }
    }

    #[tokio::test]
    async fn health_returns_200() {
        let app = Router::new().route("/health", get(|| async { StatusCode::OK }));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn status_returns_jobs_json() {
        let preset = JobStatus {
            name: "osm_update",
            enabled: true,
            interval_seconds: 60,
            timeout_seconds: 600,
            state: JobState::Idle,
            last_started_at: Some("2026-05-28T12:00:00Z".to_string()),
            last_finished_at: Some("2026-05-28T12:00:03Z".to_string()),
            last_duration_ms: Some(3000),
            last_outcome: Some(JobOutcome::Success),
            next_run_at: Some("2026-05-28T12:01:03Z".to_string()),
            run_count: 7,
        };
        let state = make_test_state(vec![preset]);

        let app = Router::new()
            .route("/status", get(jobs::status_handler::get_status))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 8192).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let jobs = v["jobs"].as_array().expect("jobs array");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["name"], "osm_update");
        assert_eq!(jobs[0]["state"], "idle");
        assert_eq!(jobs[0]["run_count"], 7);
        assert_eq!(jobs[0]["last_outcome"]["kind"], "Success");
    }

    /// An in-place upgrade of an existing database gains the `*_unmatched`
    /// tables via `CREATE TABLE IF NOT EXISTS`, empty -- `check_startup_conditions`
    /// must not bail (empty is not a hard requirement), but must warn loudly
    /// enough that an operator can tell why `/tiles`/`/package` are serving
    /// nothing, rather than the server starting silently.
    #[test]
    fn check_startup_conditions_warns_when_a_serving_table_is_empty() {
        use std::io;
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::fmt::MakeWriter;

        #[derive(Clone)]
        struct SharedBuf(Arc<Mutex<Vec<u8>>>);
        impl io::Write for SharedBuf {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl<'a> MakeWriter<'a> for SharedBuf {
            type Writer = SharedBuf;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = crate::db::init_db(std::path::Path::new(":memory:"), &init, None).unwrap();
        // REQUIRED_TABLES also needs these three -- create empty, matching a
        // freshly-upgraded database whose government tables just haven't
        // been (re)compared yet.
        conn.execute_batch(
            "CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        let pool = build_pool(conn, 2).unwrap();

        tracing::subscriber::with_default(subscriber, || {
            super::check_startup_conditions(&pool).unwrap();
        });

        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            out.contains("bdot10k_unmatched") && out.contains("empty"),
            "expected a warning naming the empty serving table, got: {out}"
        );
    }
}
