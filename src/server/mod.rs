mod tiles;

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::Router;
use duckdb::{AccessMode, Config, Connection, DuckdbConnectionManager};
use r2d2::Pool;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::Config as AppConfig;
use crate::shutdown;

const READ_POOL_SIZE: u32 = 4;

#[derive(Clone)]
pub struct AppState {
    pub write: Arc<Mutex<Connection>>,
    pub read_pool: Pool<DuckdbConnectionManager>,
}

pub async fn run(conn: Connection, config: &AppConfig) -> Result<()> {
    let read_pool = build_read_pool(Path::new(&config.db_path), &config.duckdb_init_commands)?;
    let state = AppState {
        write: Arc::new(Mutex::new(conn)),
        read_pool,
    };

    let app = Router::new()
        .route("/tiles/{z}/{x}/{y}", axum::routing::get(tiles::serve_tile))
        .with_state(state);

    let listener = TcpListener::bind(&config.http_listen_addr).await?;
    info!(addr = %config.http_listen_addr, "HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                if shutdown::is_requested() {
                    break;
                }
            }
        })
        .await?;

    Ok(())
}

fn build_read_pool(db_path: &Path, init_commands: &[String]) -> Result<Pool<DuckdbConnectionManager>> {
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

    let conn = pool.get().context("Failed to get connection from read pool")?;
    for cmd in init_commands {
        conn.execute_batch(cmd)
            .with_context(|| format!("Failed to execute init command on read pool: {cmd}"))?;
    }

    Ok(pool)
}
