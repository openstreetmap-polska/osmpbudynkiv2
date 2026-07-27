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
    /// Compare government data against OSM
    Compare {
        #[command(subcommand)]
        target: CompareTarget,
    },
    /// Run HTTP service with background data updates
    Run,
}

#[derive(Subcommand)]
pub enum CompareTarget {
    /// Compare building datasets against OSM buildings
    Buildings {
        #[command(subcommand)]
        source: Option<BuildingsSource>,
    },
    /// Compare address datasets against OSM addresses
    Addresses {
        #[command(subcommand)]
        source: Option<AddressesSource>,
    },
    /// Run all available comparisons
    Full,
    /// Enqueue every cell containing a government object, so the drain
    /// rebuilds them (safety net for a dropped enqueue; also usable as an
    /// offline rebuild path or a daily job).
    Reconcile,
}

#[derive(Subcommand)]
pub enum AddressesSource {
    /// Compare only PRG addresses against OSM
    Prg,
    /// Compare all address sources against OSM
    All,
}

#[derive(Subcommand)]
pub enum BuildingsSource {
    /// Compare only BDOT10k buildings against OSM
    Bdot10k,
    /// Compare only EGIB buildings against OSM
    Egib,
    /// Compare all building sources against OSM
    All,
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
        /// Path to a TERC (TERYT) dictionary file (.zip or .xml). Required for
        /// the 2021 schema; overrides `teryt.file_path` from the config file.
        #[arg(long)]
        terc_file: Option<PathBuf>,
    },
    /// Run all imports in sequence
    Full,
}

#[derive(Subcommand)]
pub enum UpdateSource {
    /// Update OpenStreetMap data from replication feed
    Osm,
    /// Update BDOT10k building data from a fresh snapshot
    Bdot10k {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Update EGIB building data from a fresh snapshot
    Egib {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Update PRG address data from a fresh snapshot
    Prg {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
        /// Path to a TERC (TERYT) dictionary file (.zip or .xml).
        #[arg(long)]
        terc_file: Option<PathBuf>,
    },
}
