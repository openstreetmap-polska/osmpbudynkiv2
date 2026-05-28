mod cli;
mod compare;
mod config;
mod db;
mod download;
mod import;
mod osm;
mod server;
mod shutdown;
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

    match cli.command {
        Command::Import { source } => {
            import::run(&conn, &kv, source, &config, &config.download_urls)?
        }
        Command::Update { source } => {
            update::run(&conn, &kv, source, &config, &config.download_urls)?
        }
        Command::Compare { target } => compare::run(&conn, target)?,
        Command::Run => {
            let rt = tokio::runtime::Runtime::new()?;
            let config = Arc::new(config);
            rt.block_on(server::run(conn, kv.clone(), config))?;
        }
    }

    Ok(())
}
