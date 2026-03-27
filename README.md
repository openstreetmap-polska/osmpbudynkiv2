# osmpbudynkiv2

_ENG: Tool that prepares packages for JOSM (OpenStreetMap data editor) for easy imports of data from Polish government registries (addresses, buildings). Rewrite of: https://github.com/openstreetmap-polska/gugik2osm_

Narzędzie do porównywania uwolnionych danych państwowych (adresy, budynki) do danych OpenStreetMap (OSM) i przygotowywania paczek danych ułatwiających dodawanie i aktualizację danych w OSM. Kontynuacja (przepisanie na nowo) poprzedniej wersji: https://github.com/openstreetmap-polska/gugik2osm

## Building

Requires Rust toolchain (install via [rustup](https://rustup.rs/)). No external DuckDB installation needed — it's compiled from source as part of the build (first build takes a while due to C++ compilation).

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

By default the database is stored in `./osmpbudynkiv2.duckdb`. Use `--db-path` to change it:

```bash
cargo run -- --db-path /path/to/data.duckdb import osm
```

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

Set log level via the `RUST_LOG` environment variable:

```bash
RUST_LOG=debug cargo run -- import osm
```

