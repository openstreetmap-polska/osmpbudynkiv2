pub mod jobs;
mod package;
mod tiles;
mod updates;

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::{Router, http::StatusCode};
use duckdb::{AccessMode, Config, Connection, DuckdbConnectionManager};
use r2d2::Pool;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::Config as AppConfig;
use crate::shutdown;

const READ_POOL_SIZE: u32 = 4;

#[derive(Clone)]
pub struct AppState {
    // Held by the AppState so write-path handlers can serialize against
    // background jobs through the same DuckDB connection. No current
    // handler exercises this, but it is part of the established server
    // contract — see src/server/jobs/osm_update.rs.
    #[allow(dead_code)]
    pub write: Arc<Mutex<Connection>>,
    pub read_pool: Pool<DuckdbConnectionManager>,
    pub registry: Arc<jobs::JobRegistry>,
    pub config: Arc<AppConfig>,
}

pub async fn run(
    conn: Connection,
    kv: Arc<crate::osm::kvstore::RocksDB>,
    config: Arc<AppConfig>,
) -> Result<()> {
    let read_pool = build_read_pool(Path::new(&config.db_path), &config.duckdb_init_commands)?;

    check_startup_conditions(&conn, &read_pool)?;

    let write = Arc::new(Mutex::new(conn));

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
    ];
    let scheduler = jobs::Scheduler::start(job_list, write.clone(), kv, config.clone());
    let registry = scheduler.registry.clone();
    let shutdown_notify = scheduler.shutdown_notify();

    let state = AppState {
        write,
        read_pool,
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

fn check_startup_conditions(
    write: &Connection,
    read_pool: &Pool<DuckdbConnectionManager>,
) -> Result<()> {
    write
        .query_row("SELECT 1", [], |_| Ok(()))
        .context("Write connection health check failed")?;

    let read_conn = read_pool
        .get()
        .context("Failed to get read connection for startup check")?;

    for table in REQUIRED_TABLES {
        let exists: i64 = read_conn
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

        let rows: i64 = read_conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .with_context(|| format!("Failed to count rows in table '{table}'"))?;

        info!("Table {} has {} rows.", table, rows);
    }

    info!("Startup checks passed: write connection OK, all required tables present");
    Ok(())
}

fn build_read_pool(
    db_path: &Path,
    init_commands: &[String],
) -> Result<Pool<DuckdbConnectionManager>> {
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .context("Failed to set read-only access mode")?
        .with("storage_compatibility_version", "latest")
        .context("Failed to set storage compatibility version")?;

    let manager = DuckdbConnectionManager::file_with_flags(db_path, config)
        .context("Failed to create read connection manager")?;

    let pool = Pool::builder()
        .max_size(READ_POOL_SIZE)
        .build(manager)
        .context("Failed to build read connection pool")?;

    let conn = pool
        .get()
        .context("Failed to get connection from read pool")?;
    for cmd in init_commands {
        conn.execute_batch(cmd)
            .with_context(|| format!("Failed to execute init command on read pool: {cmd}"))?;
    }

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::{Router, routing::get};
    use duckdb::{AccessMode, Config as DuckConfig, Connection, DuckdbConnectionManager};
    use r2d2::Pool;
    use tower::ServiceExt;

    use super::jobs::{JobOutcome, JobRegistry, JobState, JobStatus};
    use super::{AppState, jobs};

    fn make_test_state(initial: Vec<JobStatus>) -> (AppState, tempfile::TempDir) {
        // DuckDB rejects read-only on :memory:, so back the read pool with a
        // throwaway file-backed DB. The /status handler never queries it.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("status_test.duckdb");
        // Create the file first via a writable connection, then drop.
        let _ = Connection::open(&db_path).unwrap();
        let read_cfg = DuckConfig::default()
            .access_mode(AccessMode::ReadOnly)
            .unwrap();
        let manager = DuckdbConnectionManager::file_with_flags(&db_path, read_cfg).unwrap();
        let read_pool = Pool::builder().max_size(1).build(manager).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        let state = AppState {
            write: Arc::new(StdMutex::new(conn)),
            read_pool,
            registry: Arc::new(JobRegistry::new_for_tests(initial)),
            config: Arc::new(crate::config::Config::default()),
        };
        (state, dir)
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
        let (state, _dir) = make_test_state(vec![preset]);

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
}
