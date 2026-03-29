# Config File Design

## Summary

Add a TOML config file to centralize runtime settings that are currently spread across CLI flags, hardcoded constants, and environment variables. The config is loaded via an explicit `--config <path>` CLI flag. All fields are optional with sensible defaults in code. The config fully overrides defaults (no merging with built-in values).

## Config file format

TOML. Parsed via the `toml` crate with `serde` deserialization.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `db_path` | string | `"./osmpbudynkiv2.duckdb"` | Path to DuckDB database file |
| `log_level` | string | `"info"` | Log verbosity: `trace`, `debug`, `info`, `warn`, `error` |
| `duckdb_init_commands` | array of strings | see below | SQL statements executed on DB init |
| `download_urls.osm_pbf` | string | current hardcoded URL | OSM PBF download URL |
| `download_urls.bdot10k` | string | current hardcoded URL | BDOT10k download URL |
| `download_urls.egib` | string | current hardcoded URL | EGIB download URL |
| `download_urls.prg` | string | `""` | PRG download URL (not yet implemented) |
| `download_urls.osm_replication` | string | current hardcoded URL | OSM minutely replication base URL |

### Default `duckdb_init_commands`

```toml
duckdb_init_commands = [
    "INSTALL spatial",
    "LOAD spatial",
    "SET preserve_insertion_order = false",
    "SET geometry_always_xy = true",
    "SET memory_limit = '4GB'",
    "SET threads = 8",
]
```

### Full example

```toml
db_path = "/data/osmpbudynkiv2.duckdb"
log_level = "debug"

duckdb_init_commands = [
    "INSTALL spatial",
    "LOAD spatial",
    "SET preserve_insertion_order = false",
    "SET geometry_always_xy = true",
    "SET memory_limit = '4GB'",
    "SET threads = 8",
]

[download_urls]
osm_pbf = "https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf"
bdot10k = "https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GeoParquet/Polska_BDOT10k_GeoParquet.zip"
egib = "https://opendata.geoportal.gov.pl/InneDane/latest_exports/eziudp_wfs/PARQUET/0_budynki.parquet"
prg = ""
osm_replication = "https://download.openstreetmap.fr/replication/europe/poland/minute"
```

## Precedence

```
CLI flags > Config file > Built-in defaults
```

- `--config <path>`: if provided, load and parse the file; error on missing file or parse failure
- If `--config` is not provided, no config file is loaded — all built-in defaults apply
- For log level specifically: `RUST_LOG` env var > `config.log_level` > default `"info"`

## CLI changes

### Remove

- `--db-path` flag (replaced by `db_path` in config)

### Add

- `--config <path>` global flag (optional, no default location)

### Resulting CLI

```
osmpbudynkiv2 [--config <path>] <command> [args...]
```

## Code changes

### New file: `src/config.rs`

Responsibilities:
- `Config` struct with `serde::Deserialize` and all fields
- `DownloadUrls` struct nested inside `Config`
- `impl Default for Config` providing all built-in defaults
- `load_config(path: Option<&Path>) -> Result<Config>`: reads and parses TOML file, or returns defaults

The `Config` struct uses `#[serde(default)]` on all fields so that a partial config file works — only specified fields override defaults.

### Modified: `src/cli.rs`

- Remove `db_path` field
- Add `config` field (`Option<PathBuf>`) with `--config` flag
- The `Cli` struct becomes just `--config` + subcommand

### Modified: `src/main.rs`

Updated startup sequence:
1. Parse CLI args
2. Load config via `load_config(cli.config.as_deref())`
3. Set up tracing: check `RUST_LOG` first, fall back to `config.log_level`
4. Init DB: pass `config.db_path` and `config.duckdb_init_commands` to `db::init_db`

### Modified: `src/db.rs`

- `init_db` signature changes: accepts `db_path` and `init_commands` (or the full `Config`) instead of just a path
- Remove hardcoded `INSTALL spatial; LOAD spatial;` and `SET` statements
- Execute `init_commands` from config, then create schema

### Modified: `src/import/osm.rs`, `src/import/bdot10k.rs`, `src/import/egib.rs`

- Remove hardcoded `*_URL` constants
- Accept download URL from config (passed through from `main.rs` or via `Config` reference)

### Modified: `src/update/osm.rs`

- Remove hardcoded `REPLICATION_BASE_URL` constant
- Accept replication base URL from config

### New dependency: `Cargo.toml`

```toml
toml = "0.8"
```

### New file: `example_config.toml`

An example config file at the repo root with all fields set to their default values, with comments explaining each field.

### Modified: `README.md`

- Remove `--db-path` references
- Add section about `--config` flag and config file usage
- Reference `example_config.toml`

## Testing

- Unit test: `load_config(None)` returns expected defaults
- Unit test: `load_config(Some(path))` with a partial TOML file correctly overrides only specified fields
- Unit test: `load_config(Some(path))` with a complete TOML file overrides all fields
- Unit test: `load_config(Some(nonexistent))` returns an error
- Unit test: `load_config(Some(invalid_toml))` returns an error
- Existing `db::init_db` tests updated to pass config values
