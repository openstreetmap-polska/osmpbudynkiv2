mod cli;
mod compare;
mod config;
mod dataset;
mod db;
mod db_memory;
mod download;
mod import;
mod job_log;
mod mappings;
mod osm;
mod reports;
mod server;
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
    // Exactly the commands that end in `compact_osm_families` -- see
    // `open_for_bulk_load` for why only they get it.
    let bulk_load = matches!(
        cli.command,
        Command::Import {
            source: cli::ImportSource::Osm { .. } | cli::ImportSource::Full { .. }
        } | Command::Init { .. }
            | Command::Kv {
                action: cli::KvAction::Compact
            }
    );
    let open_kv = if bulk_load {
        osm::kvstore::open_for_bulk_load
    } else {
        osm::kvstore::open
    };
    let kv = Arc::new(open_kv(
        Path::new(&config.rocksdb_path),
        config.rocksdb_block_cache_mb,
        config.rocksdb_write_buffer_mb,
        config.rocksdb_tile_block_cache_mb,
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
            import::run(&conn, &kv, source, &config, &config.download_urls)?;
            clear_tile_store(&kv)?;
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
        Command::Compare { target } => {
            compare::run(&conn, target)?;
            clear_tile_store(&kv)?;
        }
        Command::Queue { action } => compare::run_queue(&conn, action)?,
        Command::Reports { action } => reports::run(&conn, action)?,
        Command::Init {
            osm_file,
            bdot10k_file,
            egib_file,
            prg_file,
            terc_file,
            street_mappings_file,
            bdot10k_building_types_file,
            egib_building_types_file,
        } => {
            import::run(
                &conn,
                &kv,
                cli::ImportSource::Full {
                    osm_file,
                    bdot10k_file,
                    egib_file,
                    prg_file,
                    terc_file,
                    street_mappings_file,
                    bdot10k_building_types_file,
                    egib_building_types_file,
                },
                &config,
                &config.download_urls,
            )?;
            shutdown::check_requested()?;
            update::run(
                &conn,
                &kv,
                cli::UpdateSource::Osm,
                &config,
                &config.download_urls,
                true,
                &|| false,
            )?;
            shutdown::check_requested()?;
            compare::run(&conn, cli::CompareTarget::Full)?;
            shutdown::check_requested()?;
            compare::run_queue(&conn, cli::QueueAction::Drain { batch_size: 512 })?;
            clear_tile_store(&kv)?;
        }
        Command::Tiles { action } => server::tile_warm::run(&conn, kv.clone(), action, &config)?,
        Command::Kv {
            action: cli::KvAction::Compact,
        } => {
            // No `clear_tile_store`: compaction rewrites files, not values, so
            // nothing a tile renders can change.
            osm::kvstore::compact_osm_families(&kv)?;
            info!("RocksDB compaction complete");
        }
        Command::Run => {
            let rt = tokio::runtime::Runtime::new()?;
            let config = Arc::new(config);
            rt.block_on(server::run(conn, kv.clone(), config))?;
        }
    }

    Ok(())
}

/// Empty the rendered-tile store after an offline command that rewrote what
/// tiles render without enqueueing anything.
///
/// `import <source>` rebuilds a raw source table wholesale and `compare <any
/// target>` rewrites `*_unmatched` nationally; neither produces dirty cells,
/// and with no read-time freshness check left, every warmed tile would
/// otherwise be stale forever. `compare buildings` and `compare addresses` are
/// included deliberately, not just `full`: a single-source compare still
/// rewrites its serving table across the whole country.
///
/// Done here in the dispatch rather than inside the loaders, so there is one
/// obvious place per command rather than a wipe buried in each arm. Both
/// commands hold exclusive DB access, so this races nothing.
fn clear_tile_store(kv: &osm::kvstore::RocksDB) -> anyhow::Result<()> {
    osm::kvstore::clear_tiles(kv)?;
    info!("Cleared the rendered-tile store; run `tiles warm` to repopulate it");
    Ok(())
}
