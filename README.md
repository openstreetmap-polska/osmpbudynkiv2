# osmpbudynkiv2

_ENG: Tool that prepares packages for JOSM (OpenStreetMap data editor) for easy imports of data from Polish government registries (addresses, buildings). Rewrite of: https://github.com/openstreetmap-polska/gugik2osm_

Narzędzie do porównywania uwolnionych danych państwowych (adresy, budynki) do danych OpenStreetMap (OSM) i przygotowywania paczek danych ułatwiających dodawanie i aktualizację danych w OSM. Kontynuacja (przepisanie na nowo) poprzedniej wersji: https://github.com/openstreetmap-polska/gugik2osm

## Building

Requires Rust toolchain (install via [rustup](https://rustup.rs/)). No external DuckDB or RocksDB installation needed — both are compiled from source as part of the build (first build takes a while due to C++ compilation).

```bash
cargo build             # debug build
cargo build --release   # optimized release build
```

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
- **`rocksdb_path`** — location of the RocksDB directory (stores raw OSM node coordinates and structural mappings used to build geometries)
- **`rocksdb_block_cache_mb`** — RocksDB block cache size in MB (default: 512)
- **`rocksdb_write_buffer_mb`** — RocksDB write buffer size in MB per column family (default: 64)
- **`log_level`** — log verbosity (`trace`, `debug`, `info`, `warn`, `error`)
- **`download_dir`** — directory for downloaded files (default: system temp directory; files are cleaned up after processing)
- **`duckdb_init_commands`** — SQL statements run on database initialization
- **`download_urls`** — URLs for downloading data sources

All fields are optional — only specify what you want to override. Note that `duckdb_init_commands` is fully replaced if specified (not merged with defaults).

## CLI commands

### import — bulk-load data

```bash
# Import everything (OSM, BDOT10k, EGIB, PRG) in sequence
cargo run -- import full

# Import OpenStreetMap data (downloads Poland PBF extract automatically)
cargo run -- import osm

# Import from a local PBF file instead of downloading
cargo run -- import osm --file example_data/OSM/poland-latest.osm.pbf

# Import BDOT10k building data (downloads GeoParquet automatically)
cargo run -- import bdot10k

# Import from a local file
cargo run -- import bdot10k --file bdot10k.parquet

# Import EGIB building data
cargo run -- import egib
cargo run -- import egib --file egib.parquet

# Import PRG address data
cargo run -- import prg
cargo run -- import prg --file prg.zip
```

### update — apply incremental updates

```bash
# Update OSM data from minutely replication feed
cargo run -- update osm

# Update government datasets
cargo run -- update bdot10k
cargo run -- update egib
cargo run -- update prg
```

### run — HTTP service (not yet implemented)

```bash
cargo run -- run
```

Will serve vector tiles, GeoJSON data packages, and a web map with background data updates.

## Development

```bash
cargo test              # run all tests
cargo test <name>       # run a single test by name
cargo clippy            # lint
cargo fmt               # format code
```

Log level can be set via the `RUST_LOG` environment variable (takes precedence) or the config file's `log_level` setting:

```bash
RUST_LOG=debug cargo run -- import osm
cargo run -- --config config.toml import osm  # uses log_level from config
```

### Profiling
```bash
samply record --save-only -o osm_import_before.json.gz \
  ./target/profiling/osmpbudynkiv2 \
  --config ./example_config.toml \
  import osm --file ./example_data/OSM/poland-latest.osm.pbf
```

Then `samply load osm_import_before.json.gz` to inspect.
