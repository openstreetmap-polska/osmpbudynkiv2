# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Rewrite of [gugik2osm](https://github.com/openstreetmap-polska/gugik2osm) — a tool that compares Polish government registry data (addresses from PRG, buildings from BDOT10k and EGIB) with OpenStreetMap data and generates GeoJSON packages for import into JOSM.

## Build & Test Commands

```bash
cargo build           # Build (first build is slow due to bundled DuckDB compilation)
cargo test            # Run all tests
cargo test <name>     # Run a single test by name
cargo run             # Run the binary
cargo clippy          # Lint
cargo fmt             # Format code
cargo fmt -- --check  # Check formatting without modifying
cargo build --profile profiling   # Release build with debug symbols for samply/perf
```

Note: Both `duckdb` and `rocksdb` dependencies use bundled C++ compilation, so no external installations are needed, but first build takes significant time.

## Architecture

**Tech stack:** Rust + DuckDB (embedded, file-based) + RocksDB (KV store). Goal is a single binary that's easy to deploy.

**CLI commands** (`cargo run -- <command>`):
- `import <source>` — bulk-load data (OSM from PBF, PRG addresses from ZIP via `--file <ZIP> --terc-file <TERC>`, BDOT10k/EGIB buildings from GeoParquet)
- `update <source>` — apply incremental updates (OSM minutely replication, re-download gov datasets)
- `compare <target>` — compare government data against OSM. Targets: `buildings` (optionally `bdot10k`, `egib`, or `all` — default runs all), `addresses` (optionally `prg` or `all` — default runs all), `full` (runs every comparison)
- `run` — HTTP service (`/health`, `/status`, `/tiles/{z}/{x}/{y}`, `/package` GeoJSON import packages, `/updates` recent export activity) with background OSM updates and export log pruning

**Storage:**
- **DuckDB** — main analytical database for geospatial queries, stores processed OSM data and government datasets
- **RocksDB** — KV store for raw OSM node coordinates and structural mappings (way node refs, relation members, reverse indexes)

**Key design decisions (see `adr/`):**
- DuckDB chosen for its geospatial support and file-based storage (ADR-002)
- API returns GeoJSON, not OSM XML — JOSM can read GeoJSON (ADR-003)
- Single process, multithreaded — DuckDB doesn't support multiple writer processes (docs/project_ideas.md)

## Data Sources

- **OSM:** Poland PBF extract from OSM France, minutely replication feed
- **PRG:** Government address registry (ZIP, parsed via [prg_convert](https://github.com/ttomasz/prg_convert/) library)
- **BDOT10k:** Government building registry (GeoParquet)
- **EGIB:** Government building registry (GeoParquet)

## Configuration

The binary accepts `--config <path>` pointing to a TOML file (see `example_config.toml`). Without it, defaults are used. The `RUST_LOG` env var overrides the config's `log_level`.

**Gotcha:** The `duckdb_init_commands` config replaces the entire default list — if you override it, include everything you need (spatial extension, memory limits, etc.).

## Testing

- **Unit tests:** Inline `#[cfg(test)]` modules within source files
- **Integration tests:** `tests/` directory, using `assert_cmd` to test CLI behavior with `tempfile` for isolated DB instances
- Run a single integration test: `cargo test --test cli_import_osm`
- **Fixtures:** Regenerate with `fixtures/scripts/prepare_fixtures.sh` (uses local OSM PBF + GeoParquet inputs)
