# Config File Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a TOML config file that centralizes runtime settings (db path, log level, DuckDB init commands, download URLs) loaded via `--config` CLI flag.

**Architecture:** New `src/config.rs` module owns the `Config` struct with serde deserialization and defaults. CLI changes from `--db-path` to `--config`. Config is threaded through to `db::init_db`, import modules, and update modules replacing hardcoded constants.

**Tech Stack:** Rust, `toml` crate, `serde`

---

### Task 1: Add `toml` dependency

**Files:**
- Modify: `Cargo.toml:11` (dependencies section)

- [ ] **Step 1: Add the dependency**

Add `toml` to `[dependencies]` in `Cargo.toml`. Add it after the `tokio` line:

```toml
toml = "0.8"
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check`
Expected: compiles without errors

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "add toml dependency for config file support"
```

---

### Task 2: Create `src/config.rs` with `Config` struct and tests

**Files:**
- Create: `src/config.rs`
- Modify: `src/main.rs:1` (add `mod config;`)

- [ ] **Step 1: Write the tests for config loading**

Create `src/config.rs` with the `Config` struct, `DownloadUrls` struct, `Default` impls, `load_config` function, and all unit tests. The module declaration in `main.rs` will come later.

```rust
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub db_path: String,
    pub log_level: String,
    pub duckdb_init_commands: Vec<String>,
    pub download_urls: DownloadUrls,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DownloadUrls {
    pub osm_pbf: String,
    pub bdot10k: String,
    pub egib: String,
    pub prg: String,
    pub osm_replication: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_path: "./osmpbudynkiv2.duckdb".to_string(),
            log_level: "info".to_string(),
            duckdb_init_commands: vec![
                "INSTALL spatial".to_string(),
                "LOAD spatial".to_string(),
                "SET preserve_insertion_order = false".to_string(),
                "SET geometry_always_xy = true".to_string(),
                "SET memory_limit = '4GB'".to_string(),
                "SET threads = 8".to_string(),
            ],
            download_urls: DownloadUrls::default(),
        }
    }
}

impl Default for DownloadUrls {
    fn default() -> Self {
        Self {
            osm_pbf: "https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf"
                .to_string(),
            bdot10k: "https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GeoParquet/Polska_BDOT10k_GeoParquet.zip"
                .to_string(),
            egib: "https://opendata.geoportal.gov.pl/InneDane/latest_exports/eziudp_wfs/PARQUET/0_budynki.parquet"
                .to_string(),
            prg: String::new(),
            osm_replication:
                "https://download.openstreetmap.fr/replication/europe/poland/minute".to_string(),
        }
    }
}

pub fn load_config(path: Option<&Path>) -> Result<Config> {
    match path {
        Some(p) => {
            let content =
                fs::read_to_string(p).with_context(|| format!("Failed to read config file: {p:?}"))?;
            let config: Config =
                toml::from_str(&content).with_context(|| format!("Failed to parse config file: {p:?}"))?;
            Ok(config)
        }
        None => Ok(Config::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_load_config_none_returns_defaults() {
        let config = load_config(None).unwrap();
        assert_eq!(config.db_path, "./osmpbudynkiv2.duckdb");
        assert_eq!(config.log_level, "info");
        assert_eq!(config.duckdb_init_commands.len(), 6);
        assert_eq!(
            config.download_urls.osm_pbf,
            "https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf"
        );
        assert_eq!(
            config.download_urls.osm_replication,
            "https://download.openstreetmap.fr/replication/europe/poland/minute"
        );
    }

    #[test]
    fn test_load_config_partial_file() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, "db_path = \"/custom/path.duckdb\"\n").unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.db_path, "/custom/path.duckdb");
        // Other fields should be defaults
        assert_eq!(config.log_level, "info");
        assert_eq!(config.duckdb_init_commands.len(), 6);
    }

    #[test]
    fn test_load_config_full_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
db_path = "/other/db.duckdb"
log_level = "debug"
duckdb_init_commands = ["INSTALL spatial", "LOAD spatial"]

[download_urls]
osm_pbf = "https://example.com/osm.pbf"
bdot10k = "https://example.com/bdot10k.zip"
egib = "https://example.com/egib.parquet"
prg = "https://example.com/prg.zip"
osm_replication = "https://example.com/replication"
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.db_path, "/other/db.duckdb");
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.duckdb_init_commands.len(), 2);
        assert_eq!(config.download_urls.osm_pbf, "https://example.com/osm.pbf");
        assert_eq!(
            config.download_urls.osm_replication,
            "https://example.com/replication"
        );
    }

    #[test]
    fn test_load_config_nonexistent_file() {
        let result = load_config(Some(Path::new("/nonexistent/config.toml")));
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_invalid_toml() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, "this is not valid toml [[[").unwrap();

        let result = load_config(Some(tmp.path()));
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: Add `tempfile` dev-dependency for tests**

Add to `[dev-dependencies]` in `Cargo.toml`:

```toml
tempfile = "3"
```

- [ ] **Step 3: Add `mod config;` to `main.rs`**

Add `mod config;` after `mod cli;` (line 1) in `src/main.rs`:

```rust
mod cli;
mod config;
```

- [ ] **Step 4: Run the tests**

Run: `cargo test config`
Expected: all 5 tests pass

- [ ] **Step 5: Commit**

```bash
git add src/config.rs src/main.rs Cargo.toml Cargo.lock
git commit -m "add config module with Config struct, load_config, and tests"
```

---

### Task 3: Update CLI — replace `--db-path` with `--config`

**Files:**
- Modify: `src/cli.rs`

- [ ] **Step 1: Update the `Cli` struct**

Replace the contents of `src/cli.rs` with:

```rust
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
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check`
Expected: compiles (may have warnings about unused `config` field — that's fine, Task 4 will wire it up)

- [ ] **Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "replace --db-path CLI flag with --config"
```

---

### Task 4: Update `main.rs` to load config and use it

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Rewrite `main.rs` to use config**

Replace the contents of `src/main.rs` with:

```rust
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
```

- [ ] **Step 2: Check compilation (will fail — `db::init_db` and `import::run`/`update::run` signatures don't match yet)**

Run: `cargo check`
Expected: compile errors about mismatched function signatures. This is expected — Tasks 5-8 will fix them.

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "wire config into main startup sequence"
```

---

### Task 5: Update `db.rs` — accept init commands from config

**Files:**
- Modify: `src/db.rs`

- [ ] **Step 1: Update `init_db` to accept init commands**

Replace the `init_db` function (lines 6-24) in `src/db.rs` with:

```rust
pub fn init_db(path: &Path, init_commands: &[String]) -> Result<Connection> {
    let conn =
        Connection::open(path).with_context(|| format!("Failed to open database at {path:?}"))?;

    for cmd in init_commands {
        conn.execute_batch(cmd)
            .with_context(|| format!("Failed to execute DuckDB init command: {cmd}"))?;
    }

    create_schema(&conn)?;

    Ok(conn)
}
```

- [ ] **Step 2: Update tests to pass init commands**

Replace the test helper calls in `src/db.rs` tests. Update `test_init_db_creates_tables` (line 84):

```rust
    #[test]
    fn test_init_db_creates_tables() -> Result<()> {
        let init_commands = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;

        // Verify all tables exist by querying them
        let tables = [
            "metadata",
            "osm_nodes",
            "osm_way_nodes",
            "osm_relations",
            "osm_addresses",
            "osm_buildings",
        ];
        for table in tables {
            let count: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0, "Table {table} should be empty initially");
        }

        Ok(())
    }
```

Update `test_init_db_is_idempotent` (line 108):

```rust
    #[test]
    fn test_init_db_is_idempotent() -> Result<()> {
        let init_commands = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
        // Re-run schema creation — should not fail
        create_schema(&conn)?;
        Ok(())
    }
```

- [ ] **Step 3: Run db tests**

Run: `cargo test db::tests`
Expected: both tests pass

- [ ] **Step 4: Commit**

```bash
git add src/db.rs
git commit -m "update init_db to accept init commands from config"
```

---

### Task 6: Update import modules to accept download URLs from config

**Files:**
- Modify: `src/import/mod.rs`
- Modify: `src/import/osm.rs`
- Modify: `src/import/bdot10k.rs`
- Modify: `src/import/egib.rs`

- [ ] **Step 1: Update `src/import/osm.rs`**

Remove the `OSM_PBF_URL` constant (line 10) and change the `import` function signature to accept a `url` parameter:

Replace lines 10-16:

```rust
pub fn import(conn: &Connection, file: Option<&Path>, url: &str) -> Result<()> {
    let pbf_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_file(url, Path::new("./data"))?,
    };
```

(The rest of the function stays the same.)

- [ ] **Step 2: Update `src/import/bdot10k.rs`**

Remove the `BDOT10K_URL` constant (line 9) and change the `import` function signature. Also update `download_and_extract` to accept the URL.

Replace lines 9-17:

```rust
pub fn import(conn: &Connection, file: Option<&Path>, url: &str) -> Result<()> {
    let parquet_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_and_extract(url)?,
    };
```

Replace the `download_and_extract` function signature (line 51):

```rust
fn download_and_extract(url: &str) -> Result<PathBuf> {
    let zip_path = download_file(url, Path::new("./data"))?;
```

(The rest of both functions stays the same.)

- [ ] **Step 3: Update `src/import/egib.rs`**

Remove the `EGIB_URL` constant (line 9) and change the `import` function signature.

Replace lines 9-14:

```rust
pub fn import(conn: &Connection, file: Option<&Path>, url: &str) -> Result<()> {
    let parquet_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_file(url, Path::new("./data"))?,
    };
```

(The rest of the function stays the same.)

- [ ] **Step 4: Update `src/import/mod.rs`**

Change `run` to accept and pass `DownloadUrls`:

```rust
pub mod bdot10k;
pub mod egib;
pub mod osm;

use anyhow::{Result, bail};
use duckdb::Connection;

use crate::cli::ImportSource;
use crate::config::DownloadUrls;

pub fn run(conn: &Connection, source: ImportSource, urls: &DownloadUrls) -> Result<()> {
    match source {
        ImportSource::Osm { file } => osm::import(conn, file.as_deref(), &urls.osm_pbf),
        ImportSource::Bdot10k { file } => bdot10k::import(conn, file.as_deref(), &urls.bdot10k),
        ImportSource::Egib { file } => egib::import(conn, file.as_deref(), &urls.egib),
        ImportSource::Prg { .. } => bail!("PRG import is not yet implemented"),
        ImportSource::Full => {
            bail!("Full import is not yet implemented");
        }
    }
}
```

- [ ] **Step 5: Verify compilation**

Run: `cargo check`
Expected: compiles (update module may still have errors — Task 7 fixes that)

- [ ] **Step 6: Commit**

```bash
git add src/import/
git commit -m "pass download URLs from config to import modules"
```

---

### Task 7: Update `update/osm.rs` to accept replication URL from config

**Files:**
- Modify: `src/update/osm.rs`
- Modify: `src/update/mod.rs`

- [ ] **Step 1: Update `src/update/osm.rs`**

Remove the `REPLICATION_BASE_URL` constant (lines 15-16) and change the `update` function signature to accept the base URL. Also update `fetch_latest_sequence` and `apply_sequence` to accept it.

Replace lines 15-18:

```rust
pub fn update(conn: &Connection, replication_base_url: &str) -> Result<()> {
    let current_seq = get_current_sequence(conn)?;
```

Replace `fetch_latest_sequence` (line 64):

```rust
fn fetch_latest_sequence(replication_base_url: &str) -> Result<u64> {
    let url = format!("{replication_base_url}/state.txt");
```

Replace `apply_sequence` (line 73):

```rust
fn apply_sequence(conn: &Connection, seq: u64, replication_base_url: &str) -> Result<()> {
    let path = sequence_to_path(seq);
    let url = format!("{replication_base_url}/{path}");
```

Update the three call sites within `update()` function body:

Replace:
```rust
    let latest_seq = fetch_latest_sequence()?;
```
with:
```rust
    let latest_seq = fetch_latest_sequence(replication_base_url)?;
```

Replace:
```rust
        apply_sequence(conn, seq)?;
```
with:
```rust
        apply_sequence(conn, seq, replication_base_url)?;
```

- [ ] **Step 2: Update `src/update/mod.rs`**

```rust
pub mod osm;

use anyhow::{Result, bail};
use duckdb::Connection;

use crate::cli::UpdateSource;
use crate::config::DownloadUrls;

pub fn run(conn: &Connection, source: UpdateSource, urls: &DownloadUrls) -> Result<()> {
    match source {
        UpdateSource::Osm => osm::update(conn, &urls.osm_replication),
        UpdateSource::Bdot10k => bail!("BDOT10k update is not yet implemented"),
        UpdateSource::Egib => bail!("EGIB update is not yet implemented"),
        UpdateSource::Prg => bail!("PRG update is not yet implemented"),
    }
}
```

- [ ] **Step 3: Verify everything compiles and tests pass**

Run: `cargo check && cargo test`
Expected: compiles and all tests pass

- [ ] **Step 4: Commit**

```bash
git add src/update/
git commit -m "pass replication URL from config to update module"
```

---

### Task 8: Fix tests that call `init_db` with old signature

**Files:**
- Modify: `src/import/osm.rs` (test helper)
- Modify: `src/update/osm.rs` (test helper)

- [ ] **Step 1: Update `src/import/osm.rs` test helpers**

The `setup_test_db` function (line 205) and `test_import_fixture_*` tests call `init_db(Path::new(":memory:"))` with the old 1-arg signature. Update all of them to pass init commands.

Replace `setup_test_db` (line 205):

```rust
    fn setup_test_db() -> Result<Connection> {
        let init_commands = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
```

Replace every occurrence of `init_db(Path::new(":memory:"))` in `src/import/osm.rs` test functions (lines 381, 402, 442, 482, 510) with:

```rust
        let init_commands = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
```

- [ ] **Step 2: Update `src/update/osm.rs` test helper**

Replace `setup_test_db` (line 476):

```rust
    fn setup_test_db() -> Result<Connection> {
        let init_commands = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init_commands)?;
```

- [ ] **Step 3: Run all tests**

Run: `cargo test`
Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add src/import/osm.rs src/update/osm.rs
git commit -m "update test helpers to use new init_db signature"
```

---

### Task 9: Add example config file and update README

**Files:**
- Create: `example_config.toml`
- Modify: `README.md`

- [ ] **Step 1: Create `example_config.toml`**

Create `example_config.toml` at the repo root:

```toml
# Path to DuckDB database file
db_path = "./osmpbudynkiv2.duckdb"

# Log verbosity: trace, debug, info, warn, error
# Can be overridden by RUST_LOG environment variable
log_level = "info"

# SQL statements executed on DuckDB initialization (after opening the database).
# These run before schema creation.
# WARNING: replacing this list removes the defaults — include everything you need.
duckdb_init_commands = [
    "INSTALL spatial",
    "LOAD spatial",
    "SET preserve_insertion_order = false",
    "SET geometry_always_xy = true",
    "SET memory_limit = '4GB'",
    "SET threads = 8",
]

# Download URLs for data sources
[download_urls]
osm_pbf = "https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf"
bdot10k = "https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GeoParquet/Polska_BDOT10k_GeoParquet.zip"
egib = "https://opendata.geoportal.gov.pl/InneDane/latest_exports/eziudp_wfs/PARQUET/0_budynki.parquet"
prg = ""
osm_replication = "https://download.openstreetmap.fr/replication/europe/poland/minute"
```

- [ ] **Step 2: Update `README.md`**

Replace the "Running" section (lines 16-30) with:

```markdown
## Running

```bash
# Run directly with cargo
cargo run -- <command>

# Or use the compiled binary
./target/release/osmpbudynkiv2 <command>
```

### Configuration

The app can be configured via a TOML config file. Pass its path with `--config`:

```bash
cargo run -- --config config.toml import osm
```

If no `--config` is provided, built-in defaults are used (database at `./osmpbudynkiv2.duckdb`, log level `info`, etc.). See [`example_config.toml`](example_config.toml) for all available settings and their defaults.

The config file controls:
- **`db_path`** — location of the DuckDB database file
- **`log_level`** — log verbosity (`trace`, `debug`, `info`, `warn`, `error`)
- **`duckdb_init_commands`** — SQL statements run on database initialization
- **`download_urls`** — URLs for downloading data sources

All fields are optional — only specify what you want to override. Note that `duckdb_init_commands` is fully replaced if specified (not merged with defaults).

Log level can also be set via the `RUST_LOG` environment variable, which takes precedence over the config file:

```bash
RUST_LOG=debug cargo run -- --config config.toml import osm
```
```

- [ ] **Step 3: Run clippy and fmt**

Run: `cargo fmt && cargo clippy`
Expected: no warnings or errors

- [ ] **Step 4: Run all tests one final time**

Run: `cargo test`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add example_config.toml README.md
git commit -m "add example config file and update README"
```
