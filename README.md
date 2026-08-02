# osmpbudynkiv2

_ENG: Tool that prepares packages for JOSM (OpenStreetMap data editor) for easy imports of data from Polish government registries (addresses, buildings). Rewrite of: https://github.com/openstreetmap-polska/gugik2osm_

Narzędzie do porównywania uwolnionych danych państwowych (adresy, budynki) do danych OpenStreetMap (OSM) i przygotowywania paczek danych ułatwiających dodawanie i aktualizację danych w OSM. Kontynuacja (przepisanie na nowo) poprzedniej wersji: https://github.com/openstreetmap-polska/gugik2osm

# Feature roadmap

Current implementation status against the planned scope (see [`docs/project_ideas.md`](docs/project_ideas.md)):

## Implemented

- [x] CLI with TOML configuration (`--config`), built-in defaults, `RUST_LOG` override
- [x] Storage layer: embedded DuckDB (geospatial/analytical queries) + RocksDB (raw OSM node coordinates and way/relation structure)
- [x] `import full` — running all imports (OSM, BDOT10k, EGIB, PRG) in one command
- [x] `import osm` — Poland PBF extract (auto-download or local file)
- [x] `import prg` — address registry ZIP parsed via [prg_convert](https://github.com/ttomasz/prg_convert/), with TERC dictionary support
- [x] `import bdot10k` / `import egib` — building registries from GeoParquet (auto-download or local file)
- [x] `update osm` — incremental updates from the minutely OSM replication feed
- [x] `update prg` / `update bdot10k` / `update egib` — re-download a government dataset and apply only the delta, skipping the refresh entirely when the source ETag is unchanged
- [x] `compare buildings` (BDOT10k, EGIB) / `compare addresses` (PRG) — spatial matching of government objects against OSM, writing precomputed `*_unmatched` serving tables (`bdot10k_unmatched`, `egib_unmatched`, `prg_unmatched`) that `/tiles` and `/package` read directly
- [x] Incremental freshness for `*_unmatched` between full compares — government refreshes and OSM updates enqueue the z14 cells they touched into `match_dirty_cells`; the `match_refresh` background job drains that queue by recomputing just those cells; `compare reconcile` re-enqueues every live cell as a safety net or offline rebuild path; `/status` reports queue staleness
- [x] `run` HTTP server basics: `/health`, `/status` (background job status + match-queue staleness), startup checks, graceful shutdown, read-only connection pool + single writer
- [x] Background job scheduler (no overlapping runs, timeout handling) with periodic OSM refresh, government-dataset refresh, and `match_refresh` jobs
- [x] Per-tile change tracking — every refresh records which z14 cells changed (`dataset_change_areas`) alongside a refresh log (`dataset_refreshes`)
- [x] Vector tile endpoint `/tiles/{z}/{x}/{y}` (MVT; zoom 14 only) — serves the precomputed `*_unmatched` tables (unmatched government objects, not raw datasets)
- [x] GeoJSON data package endpoint `GET/POST /package` — reads the precomputed `*_unmatched` tables, OSM-ready tags for direct JOSM import (bbox in GET, polygon in POST)
- [x] `GET /updates` — recent `/package` export activity as a GeoJSON `FeatureCollection`, browser-cacheable for 60 seconds (`?minutes=`, default 60, capped at 1440)

## Not yet implemented

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
- **`[package]`** — `/package` endpoint limits (`max_area_sq_deg`, default 0.04)
- **`[updates]`** — `/updates` time window limits (`default_minutes`, `max_minutes`)
- **`[jobs.*]`** — background jobs, each with `enabled`, `interval_seconds` and a per-run timeout: `osm_update`, `bdot10k_update`, `egib_update`, `prg_update`, `match_refresh` (drains the dirty-cell queue to keep `*_unmatched` serving tables current; also takes `batch_size`), and export-log pruning. Only one dataset refresh runs at a time, regardless of how the schedules line up.

All fields are optional — only specify what you want to override. Note that `duckdb_init_commands` is fully replaced if specified (not merged with defaults).

## CLI commands

### import — bulk-load data

```bash
# Import everything (OSM, BDOT10k, EGIB, PRG) in sequence
cargo run -- import full

# Import everything from local files instead of downloading (any subset of flags works;
# omitted sources still download)
cargo run -- import full \
  --osm-file poland-latest.osm.pbf \
  --bdot10k-file bdot10k.parquet \
  --egib-file egib.parquet \
  --prg-file prg.zip \
  --terc-file terc.zip

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

# Update government datasets (re-downloads unless --file is given)
cargo run -- update bdot10k
cargo run -- update egib
cargo run -- update prg

# Update from a local snapshot instead of downloading
cargo run -- update bdot10k --file bdot10k.parquet
cargo run -- update egib --file egib.parquet
cargo run -- update prg --file prg.zip --terc-file terc.csv
```

A government-dataset update stages the new snapshot alongside the live table,
diffs it by whole-row hash, and applies only the delta — so an unchanged row is
never rewritten and the spatial index stays intact. The delta, the refresh
record and the per-tile change areas all commit in one transaction, so readers
never observe a partially-applied update.

When the source is downloaded rather than passed with `--file`, a `HEAD` request
compares the remote `ETag` against the last one recorded; an unchanged source
skips the refresh entirely and records a zero-count row, so "ran and found
nothing" stays distinguishable from "never ran".

These refreshes also run on a schedule in the background under `run` — see the
`[jobs]` config section.

#### Row-hash version

The diff works by comparing a whole-row hash, so an import and a later update
must compute that hash identically. The expression lives in exactly one place,
`hashed_select` in `src/dataset.rs`, and the version it was built with is
stamped into the `metadata` table under the key `row_hash_version`.

**If you change `hashed_select` in a way that alters its output, bump the
`ROW_HASH_VERSION` constant next to it.** Nothing else needs changing — every
import and every update reads that one constant.

What happens after a bump: the stamp in an existing database still names the
old version, so the next update logs a `row hash version mismatch` warning and
every row compares as modified. That refresh is effectively a full rewrite —
correct, just slower than usual, and it reports a changeset the size of the
whole dataset. On success it re-stamps the new version, so the warning appears
once per bump, not on every run afterwards. A refresh that fails leaves the old
stamp alone, so the warning survives until a rewrite actually lands.

The check only detects changes you make and declare. It is not derived from the
DuckDB version, so a DuckDB upgrade that silently changed `hash()` output would
produce the same full rewrite without the explanatory warning.

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

# Re-enqueue every cell containing a government object, so the drain rebuilds
# them (safety net for a dropped enqueue; also usable as an offline rebuild
# path or a daily job)
cargo run -- compare reconcile
```

`compare` recomputes the `*_unmatched` serving tables (`bdot10k_unmatched`,
`egib_unmatched`, `prg_unmatched`) from scratch — the tables `/tiles` and
`/package` read. Between full re-compares, government refreshes and OSM
updates keep them current incrementally: each producer enqueues the z14 cells
it touched into `match_dirty_cells`, and the `match_refresh` background job
(see `run` below) drains that queue by recomputing just those cells.
`compare reconcile` re-enqueues every live cell instead of comparing directly,
for when the queue can't be trusted or a full serving rebuild is wanted
without redoing the whole comparison.

### run — HTTP service (partially implemented)

```bash
cargo run -- run
```

Currently serves:
- `/health` — liveness check
- `/status` — background job status as JSON, including match-queue staleness (`match_staleness`: pending cell count overall and per source, oldest enqueued timestamp) and per-job last-run outcome (`job_run_log`, keyed by job name)
- `/tiles/{z}/{x}/{y}` — Mapbox Vector Tiles (zoom 14 only), reading the precomputed `*_unmatched` serving tables
- `/package` — GeoJSON `FeatureCollection` of government-registry records missing
  from OSM in the requested area, tagged for direct JOSM import. Reads the same
  precomputed `*_unmatched` serving tables as `/tiles` (no live comparison per
  request). The request area (bounding box) is capped by the
  `[package] max_area_sq_deg` config setting (default 0.04 sq deg).
- `/updates` — recent `/package` export activity (timestamp, area, datasets, feature counts) as GeoJSON, `Cache-Control: public, max-age=60`. A background job prunes entries older than `[jobs.export_log_prune] retention_days` (default 365).

The `match_refresh` background job keeps `/tiles` and `/package` fresh between
full `compare` runs, by draining `match_dirty_cells` on a schedule (see
`[jobs.match_refresh]`).

**Upgrading an existing database in place:** the `*_unmatched` serving tables
are created with `CREATE TABLE IF NOT EXISTS`, so starting the server against
an older database that predates them leaves all three empty — `/tiles` and
`/package` will start up cleanly but serve zero features (a startup warning
names each empty table). Run an offline `compare full` before restarting the
server against that database. `compare reconcile` is **not** an equivalent
fast path here: it only re-enqueues every live cell for the incremental
drain, so populating a fully empty database through it means draining the
entire country cell-by-cell, which is orders of magnitude slower than a
direct full `compare`.

```bash
# bbox: minLon,minLat,maxLon,maxLat; datasets: prg, bdot10k, egib, or all (default)
curl 'http://127.0.0.1:3000/package?bbox=20.99,52.19,21.02,52.22&datasets=prg,bdot10k'

# Or POST a GeoJSON Polygon/MultiPolygon for an exact area
curl -X POST 'http://127.0.0.1:3000/package?datasets=all' \
  -d '{"type":"Polygon","coordinates":[[[20.99,52.19],[21.02,52.19],[21.02,52.22],[20.99,52.19]]]}'

# Recent export activity (default: last 60 minutes)
curl 'http://127.0.0.1:3000/updates'
curl 'http://127.0.0.1:3000/updates?minutes=1440'
```

A periodic OSM update job runs in the background. A web map is planned — see the feature roadmap above.

### Street name mappings

PRG publishes abbreviated street names (`gen. Kruka`); OSM Poland uses expanded
ones (`Generała Kruka`). `mappings/street_names_mappings.csv` maps between them
and is applied to `addr:street` when `/package` builds its response, so
downloaded data is importable without hand-editing.

Load it with:

    cargo run -- import street-mappings --file mappings/street_names_mappings.csv

A row with an empty `teryt_simc_code` applies nationwide; one with a code
applies only to that settlement and overrides the nationwide row. Lookup is
case-insensitive. The file is optional — without it, names are served exactly
as PRG publishes them.

To propose a change, edit the CSV and open a PR; `cargo test --test
street_mappings_file` checks its structure.

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
