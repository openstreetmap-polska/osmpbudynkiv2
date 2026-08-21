mod cli;
mod compare;
mod config;
mod dataset;
mod db;
mod download;
mod import;
mod job_log;
mod mappings;
mod osm;
mod reports;
mod server;
mod serving_version;
mod shutdown;
mod tile_math;
mod update;
mod utils;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use cli::{Cli, Command};
use config::load_config;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = load_config(cli.config.as_deref())?;

    // RUST_LOG env var takes precedence over config log_level
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    shutdown::install_handler();

    info!(db_path = %config.db_path, rocksdb_path = %config.rocksdb_path, "Initializing databases");
    let kv = Arc::new(osm::kvstore::open(
        Path::new(&config.rocksdb_path),
        config.rocksdb_block_cache_mb,
        config.rocksdb_write_buffer_mb,
    )?);
    let conn = db::init_db(
        Path::new(&config.db_path),
        &config.duckdb_init_commands,
        Some(kv.clone()),
    )?;
    // Lets the first Ctrl+C abort a statement already in flight, not just one
    // that hasn't started yet -- see `shutdown::INTERRUPT_HANDLES`'s doc
    // comment. This covers the CLI's single connection (import/update/
    // compare, all below); it does not cover `run`'s HTTP server, whose
    // `ClonedConnectionManager` hands out independent `try_clone()`s each
    // with their own handle -- the server relies on its graceful-shutdown
    // path and per-job cancel flags instead. Registering this base
    // connection's handle is harmless for that path too: `run` only ever
    // clones it, never queries it directly.
    shutdown::register_interrupt_handle(conn.interrupt_handle());

    match cli.command {
        Command::Import { source } => {
            import::run(&conn, &kv, source, &config, &config.download_urls)?
        }
        Command::Update { source } => {
            // The CLI has no job supervisor to cancel it, unlike the
            // scheduled background path (`server::jobs::dataset_update`
            // passes `&|| ctx.is_cancelled()`). Ctrl+C still reaches the
            // refresh, though: `crate::shutdown::is_requested()` is polled
            // inside `dataset::refresh`/`osm::update` regardless of what
            // this closure returns, and the DuckDB interrupt handle
            // registered above aborts a statement already in flight.
            update::run(
                &conn,
                &kv,
                source,
                &config,
                &config.download_urls,
                true,
                &|| false,
            )?
        }
        Command::Compare { target } => compare::run(&conn, target)?,
        Command::Queue { action } => compare::run_queue(&conn, action)?,
        Command::Reports { action } => reports::run(&conn, action)?,
        Command::Run => {
            let rt = tokio::runtime::Runtime::new()?;
            let config = Arc::new(config);
            rt.block_on(server::run(conn, kv.clone(), config))?;
        }
    }

    Ok(())
}
