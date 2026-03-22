mod cli;
mod db;
mod download;
mod import;
mod osm;
mod update;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use cli::{Cli, Command};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    info!(db_path = %cli.db_path.display(), "Initializing database");
    let conn = db::init_db(&cli.db_path)?;

    match cli.command {
        Command::Import { source } => import::run(&conn, source)?,
        Command::Update { source } => update::run(&conn, source)?,
        Command::Run => {
            anyhow::bail!("Run command is not yet implemented");
        }
    }

    Ok(())
}
