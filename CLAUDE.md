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
```

Note: The `duckdb` dependency uses the `bundled` feature, so no external DuckDB installation is needed, but C++ compilation of DuckDB takes significant time on first build.

## Architecture

**Tech stack:** Rust + DuckDB (embedded, file-based). Goal is a single binary that's easy to deploy.

**Planned CLI commands:**
- `import` — bulk-load data (OSM from PBF, PRG addresses from ZIP, BDOT10k/EGIB buildings from GeoParquet)
- `update` — apply incremental updates (OSM minutely replication, re-download gov datasets)
- `run` — HTTP service with background data updates; serves vector tiles, GeoJSON data packages, and a web map

**Key design decisions (see `adr/`):**
- DuckDB chosen for its geospatial support and file-based storage (ADR-002)
- API returns GeoJSON, not OSM XML — JOSM can read GeoJSON (ADR-003)
- Single process, multithreaded — DuckDB doesn't support multiple writer processes (docs/project_ideas.md)

## Data Sources

- **OSM:** Poland PBF extract from OSM France, minutely replication feed
- **PRG:** Government address registry (ZIP, parsed via [prg_convert](https://github.com/ttomasz/prg_convert/) library)
- **BDOT10k:** Government building registry (GeoParquet)
- **EGIB:** Government building registry (GeoParquet)
