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
    /// Operate on the match_dirty_cells queue that keeps the `*_unmatched`
    /// serving tables incrementally fresh between full compares
    Queue {
        #[command(subcommand)]
        action: QueueAction,
    },
    /// Inspect and manage user reports that exclude government objects from
    /// matching (see `POST /report`)
    Reports {
        #[command(subcommand)]
        action: ReportsAction,
    },
    /// Run HTTP service with background data updates
    Run,
}

#[derive(Subcommand)]
pub enum ReportsAction {
    /// List reports, newest first
    List {
        /// Restrict to one source: bdot10k, egib or prg
        #[arg(long)]
        source: Option<String>,
        /// Restrict to one lifecycle state: active, expired or revoked
        #[arg(long)]
        status: Option<String>,
        /// Only reports submitted at or after this timestamp (e.g. 2026-08-16
        /// or '2026-08-16 14:00:00')
        #[arg(long)]
        since: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Retire a report so its object is proposed for import again. Either one
    /// report by id, or -- because nothing identifying the submitter is
    /// stored -- every active report in a time window, which is the only way
    /// to unwind an abusive burst.
    Revoke {
        /// Report id to revoke
        id: Option<i64>,
        /// Revoke every active report submitted at or after this timestamp
        #[arg(long, conflicts_with = "id")]
        since: Option<String>,
        /// Narrow a --since revoke to one source
        #[arg(long, requires = "since")]
        source: Option<String>,
    },
    /// Retire reports whose government record has changed or disappeared, so
    /// the object becomes importable again. Runs automatically after every
    /// import and every dataset refresh; this is the manual safety net.
    Reconcile {
        /// Restrict to one source; default is every source
        #[arg(long)]
        source: Option<String>,
    },
    /// Write every report to a JSONL file.
    ///
    /// `object_reports` is the only table in this database that cannot be
    /// rebuilt from an external source, so this is the backup path -- a
    /// database rebuilt with `import full` starts with no reports at all.
    Export {
        /// Destination file ('-' for stdout)
        file: std::path::PathBuf,
    },
    /// Load reports from a JSONL file written by `reports export`
    Import {
        /// Source file
        file: std::path::PathBuf,
    },
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
}

#[derive(Subcommand)]
pub enum QueueAction {
    /// Enqueue every cell containing a government object, so the next
    /// drain rebuilds it. A safety net for a dropped enqueue, an offline
    /// rebuild path, or a scheduled sweep.
    Reconcile,
    /// Drain the queue: recompute the `*_unmatched` serving tables for every
    /// dirty cell, oldest first, until none remain or the run is
    /// interrupted. Requires exclusive access to the database — do not run
    /// this against a database a `run` server also has open.
    Drain {
        /// Cells recomputed per transaction
        #[arg(long, default_value_t = 512)]
        batch_size: usize,
    },
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
    /// Import the curated PRG -> OSM street name mappings from CSV
    StreetMappings {
        /// Path to a local mapping CSV (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
        /// Override the configured download URL
        #[arg(long)]
        url: Option<String>,
    },
    /// Import the curated BDOT10k/EGIB building-type mappings from CSV
    BuildingTypes {
        /// Path to a local BDOT10k mapping CSV (skips download)
        #[arg(long)]
        bdot10k_file: Option<PathBuf>,
        /// Path to a local EGIB mapping CSV (skips download)
        #[arg(long)]
        egib_file: Option<PathBuf>,
        /// Override the configured BDOT10k mapping download URL
        #[arg(long)]
        bdot10k_url: Option<String>,
        /// Override the configured EGIB mapping download URL
        #[arg(long)]
        egib_url: Option<String>,
    },
    /// Run all imports in sequence (OSM, BDOT10k, EGIB, PRG)
    Full {
        /// Path to local OSM PBF file (skips download)
        #[arg(long)]
        osm_file: Option<PathBuf>,
        /// Path to local BDOT10k file (skips download)
        #[arg(long)]
        bdot10k_file: Option<PathBuf>,
        /// Path to local EGIB file (skips download)
        #[arg(long)]
        egib_file: Option<PathBuf>,
        /// Path to local PRG file (skips download)
        #[arg(long)]
        prg_file: Option<PathBuf>,
        /// Path to a TERC (TERYT) dictionary file (.zip or .xml), for the PRG import
        #[arg(long)]
        terc_file: Option<PathBuf>,
    },
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
