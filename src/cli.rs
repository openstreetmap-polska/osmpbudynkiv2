use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "osmpbudynkiv2",
    about = "Compare Polish government data with OpenStreetMap"
)]
pub struct Cli {
    /// Path to TOML config file
    #[arg(long)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Import data from various sources
    Import {
        #[command(subcommand)]
        source: ImportSource,
    },
    /// Update data from various sources
    Update {
        #[command(subcommand)]
        source: UpdateSource,
    },
    /// Run HTTP service with background data updates
    Run,
}

#[derive(Subcommand)]
pub enum ImportSource {
    /// Import OpenStreetMap data from PBF file
    Osm {
        /// Path to local PBF file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Import BDOT10k building data from GeoParquet
    Bdot10k {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Import EGIB building data from GeoParquet
    Egib {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Import PRG address data
    Prg {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Run all imports in sequence
    Full,
}

#[derive(Subcommand)]
pub enum UpdateSource {
    /// Update OpenStreetMap data from replication feed
    Osm,
    /// Update BDOT10k building data
    Bdot10k,
    /// Update EGIB building data
    Egib,
    /// Update PRG address data
    Prg,
}
