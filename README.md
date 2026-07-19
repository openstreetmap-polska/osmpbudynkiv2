# osmpbudynkiv2

_ENG: Tool that prepares packages for JOSM (OpenStreetMap data editor) for easy imports of data from Polish government registries (addresses, buildings). Rewrite of: https://github.com/openstreetmap-polska/gugik2osm_

Narzędzie do porównywania uwolnionych danych państwowych (adresy, budynki) do danych OpenStreetMap (OSM) i przygotowywania paczek danych ułatwiających dodawanie i aktualizację danych w OSM. Kontynuacja (przepisanie na nowo) poprzedniej wersji: https://github.com/openstreetmap-polska/gugik2osm

# Feature roadmap

Current implementation status against the planned scope (see [`docs/project_ideas.md`](docs/project_ideas.md)):

## Implemented

- [x] CLI with TOML configuration (`--config`), built-in defaults, `RUST_LOG` override
- [x] Storage layer: embedded DuckDB (geospatial/analytical queries) + RocksDB (raw OSM node coordinates and way/relation structure)
- [x] `import osm` — Poland PBF extract (auto-download or local file)
- [x] `import prg` — address registry ZIP parsed via [prg_convert](https://github.com/ttomasz/prg_convert/), with TERC dictionary support
- [x] `import bdot10k` / `import egib` — building registries from GeoParquet (auto-download or local file)
- [x] `update osm` — incremental updates from the minutely OSM replication feed
- [x] `compare buildings` (BDOT10k, EGIB) — spatial matching of government buildings against OSM buildings
- [x] `compare addresses` (PRG) — matching government addresses against OSM, producing import candidate tables
- [x] `run` HTTP server basics: `/health`, `/status` (background job status), startup checks, graceful shutdown, read-only connection pool + single writer
- [x] Background job scheduler with a periodic OSM update job (no overlapping runs, timeout handling)
- [x] Vector tile endpoint `/tiles/{z}/{x}/{y}` (MVT; zoom 14 only, serving raw address and building layers)

## Not yet implemented

- [ ] `import full` — running all imports in one command (individual imports work)
- [ ] `update prg` / `update bdot10k` / `update egib` — re-downloading government datasets
- [ ] Background refresh jobs for government datasets (only the OSM update job exists)
- [ ] GeoJSON data package download endpoint (bbox in GET / polygon in POST) — the core JOSM import deliverable
- [ ] Serving comparison results via the API (tiles currently show raw datasets, not comparison output)
- [ ] Vector tiles for lower zoom levels with aggregation/clustering (DBSCAN or H3) and tile caching
- [ ] Web map frontend for browsing data status and downloading packages
- [ ] Endpoint for reporting records to exclude (bad source data, comparison mismatches)
- [ ] Random location endpoint (jump to an area with data to review)
- [ ] Street name corrections for addresses to match the osm conventions
- [ ] Mappings of building types in egib/bdot10k to osm tags

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
# Import everything (OSM, BDOT10k, EGIB, PRG) in sequence (not yet implemented)
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

# Update government datasets (not yet implemented)
cargo run -- update bdot10k
cargo run -- update egib
cargo run -- update prg
```

### compare — compare government data against OSM

```bash
# Run every comparison
cargo run -- compare full

# Compare buildings (all sources, or just one)
cargo run -- compare buildings
cargo run -- compare buildings bdot10k
cargo run -- compare buildings egib

# Compare addresses
cargo run -- compare addresses
cargo run -- compare addresses prg
```

### run — HTTP service (partially implemented)

```bash
cargo run -- run
```

Currently serves `/health`, `/status` (background job status as JSON), and `/tiles/{z}/{x}/{y}` (Mapbox Vector Tiles, zoom 14 only), and runs a periodic OSM update job in the background. GeoJSON data packages and a web map are planned — see the feature roadmap above.

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
