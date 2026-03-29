mod cli;
mod config;
mod db;
mod download;
mod import;
mod osm;
mod update;

use std::path::Path;

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

    info!(db_path = %config.db_path, "Initializing database");
    let conn = db::init_db(Path::new(&config.db_path), &config.duckdb_init_commands)?;

    match cli.command {
        Command::Import { source } => import::run(&conn, source, &config.download_urls)?,
        Command::Update { source } => update::run(&conn, source, &config.download_urls)?,
        Command::Run => {
            anyhow::bail!("Run command is not yet implemented");
        }
    }

    Ok(())
}
