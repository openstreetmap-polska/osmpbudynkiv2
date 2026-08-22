# osmpbudynkiv2

_ENG: Tool that prepares packages for JOSM (OpenStreetMap data editor) for easy imports of data from Polish government registries (addresses, buildings). Rewrite of: https://github.com/openstreetmap-polska/gugik2osm_

Narzędzie do porównywania uwolnionych danych państwowych (adresy, budynki) do danych OpenStreetMap (OSM) i przygotowywania paczek danych ułatwiających dodawanie i aktualizację danych w OSM. Kontynuacja (przepisanie na nowo) poprzedniej wersji: https://github.com/openstreetmap-polska/gugik2osm

# Feature roadmap

Current implementation status against the planned scope (see [`docs/project_ideas.md`](docs/project_ideas.md)):

## Implemented

- [x] CLI with TOML configuration (`--config`), built-in defaults, `RUST_LOG` override
- [x] Storage layer: embedded DuckDB (geospatial/analytical queries) + RocksDB (raw OSM node coordinates and way/relation structure)
- [x] `import full` — running all imports (OSM, BDOT10k, EGIB, PRG, street-name and building-type mappings) in one command
- [x] `import osm` — Poland PBF extract (auto-download or local file)
- [x] `import prg` — address registry ZIP parsed via [prg_convert](https://github.com/ttomasz/prg_convert/), with TERC dictionary support
- [x] `import bdot10k` / `import egib` — building registries from GeoParquet (auto-download or local file)
- [x] `update osm` — incremental updates from the minutely OSM replication feed
- [x] `update prg` / `update bdot10k` / `update egib` — re-download a government dataset and apply only the delta, skipping the refresh entirely when the source ETag is unchanged
- [x] `compare buildings` (BDOT10k, EGIB) / `compare addresses` (PRG) — spatial matching of government objects against OSM, writing precomputed `*_unmatched` serving tables (`bdot10k_unmatched`, `egib_unmatched`, `prg_unmatched`) that `/tiles` and `/package` read directly
- [x] Incremental freshness for `*_unmatched` between full compares — government refreshes and OSM updates enqueue the z14 cells they touched into `match_dirty_cells`; the `match_refresh` background job drains that queue by recomputing just those cells; `queue reconcile` re-enqueues every live cell as a safety net or offline rebuild path; `/status` reports queue staleness
- [x] `run` HTTP server basics: `/health`, `/status` (background job status + match-queue staleness), startup checks, graceful shutdown, read-only connection pool + single writer
- [x] Background job scheduler (no overlapping runs, timeout handling) with periodic OSM refresh, government-dataset refresh, `match_refresh`/`match_reconcile`, mapping-file refresh and export-log pruning jobs
- [x] Per-tile change tracking — every refresh records which z14 cells changed (`dataset_change_areas`) alongside a refresh log (`dataset_refreshes`)
- [x] Vector tile endpoint `/tiles/{z}/{x}/{y}` (MVT) — serves the precomputed `*_unmatched` tables (unmatched government objects, not raw datasets) at every zoom from 5 to 14: aggregated bins at z5–z11, individual points at z12–z13, full geometry with attributes and resolved OSM tags at z14
- [x] GeoJSON data package endpoint `GET/POST /package` — reads the precomputed `*_unmatched` tables, OSM-ready tags for direct JOSM import (bbox in GET, polygon in POST)
- [x] `GET /updates` — recent `/package` export activity as a GeoJSON `FeatureCollection`, browser-cacheable for 60 seconds (`?minutes=`, default 60, capped at 1440)
- [x] Street name corrections for addresses to match the osm conventions (`import street-mappings`)
- [x] Mappings of building types in egib/bdot10k to osm tags (`import building-types`)
- [x] Web map frontend (MapLibre GL JS, `web/`) — layer legend, per-feature popups showing source attributes and the tags an import would write, status panel, and package download for either the visible area or a drawn one (rectangle, polygon or freehand)
- [x] Ignore buildings that are mapped as no longer existing or ruins — OSM ways/relations tagged with a lifecycle-prefixed key (`demolished:building`, `destroyed:building`, `abandoned:building`, `was:building`, `razed:building`, `removed:building`, `disused:building`, `ruins:building`) are imported into `osm_former_buildings` and suppress the government building they overlap from `compare buildings`, instead of proposing it for import. Requires an `import osm` re-run to populate on a database from before this feature (see [`docs/former_buildings.md`](docs/former_buildings.md))
- [x] Tile caching
- [x] Endpoint for reporting records to exclude (bad source data, comparison mismatches) — `POST /report` marks a government object as one that should not be proposed for import, keyed on its registry identity. An active report vetoes the object out of the `*_unmatched` serving tables (so out of `/tiles`, `/package` and JOSM) until the underlying record changes, at which point the report is retired automatically and the object becomes importable again. Reported from the map's feature popup, where a reported object stays visible on the "Wszystkie" layer with a "Zgłoszony" status explaining why it is no longer being proposed; managed offline with `reports list|revoke|reconcile|export|import`. Nothing identifying the submitter is stored — see the `[reports]` section of `example_config.toml` for what that means for abuse control

## Not yet implemented

- [ ] Random location endpoint (jump to an area with data to review)

## Building

Requires:

- Rust toolchain (install via [rustup](https://rustup.rs/)) — the exact version is pinned in `rust-toolchain.toml` and rustup installs it automatically on the first cargo command
- A C/C++ compiler
- **CMake** and a generator it can drive (**Ninja**, or GNU Make) — DuckDB is built through CMake rather than from the single-file amalgamation, so that its bundled jemalloc allocator is compiled in

No external DuckDB or RocksDB installation is needed — both are compiled from source as part of the build (first build takes a while due to C++ compilation). DuckDB comes from a git dependency pinned to a tag, so the first build also clones the DuckDB source tree.

```bash
cargo build             # debug build
cargo build --release   # optimized release build
```

The build reads `.cargo/config.toml`, which points CMake at `cmake/duckdb_version.cmake` to stamp DuckDB's version. Building from outside the repo root, or with `CMAKE_TOOLCHAIN_FILE` already set in the environment, skips that and produces a DuckDB that reports `v0.0.1` and cannot install the `spatial` extension it needs. When bumping the pinned `duckdb` tag, update `cmake/duckdb_version.cmake` to match and force a DuckDB rebuild with `rm -rf target/*/build/libduckdb-sys-* target/*/.fingerprint/libduckdb-sys-*` (`cargo clean -p libduckdb-sys` does not work on a git-sourced package).

Both storage engines are built against jemalloc: DuckDB uses its own bundled, `duckdb_je_`-prefixed copy, while RocksDB pulls in `tikv-jemalloc-sys`, which also replaces the process-wide `malloc`. Set `DUCKDB_DISABLE_JEMALLOC=1` to build DuckDB with its standard allocator instead. On platforms where either build does not support jemalloc (macOS, musl, 32-bit, BSD) the respective flag is a silent no-op and the standard allocator is used.

## Running

```bash
# Run directly with cargo
cargo run -- <command>

# Or use the compiled binary
./target/release/osmpbudynkiv2 <command>
```

## Setting up a working instance

The `init` command bootstraps a fresh database in one shot: `import full`
(OSM, BDOT10k, EGIB, PRG, then both mapping CSVs), `update osm` (catches OSM
up to the current replication sequence, since the bulk PBF extract is
already somewhat stale by the time it downloads), `compare full` (populates
the `*_unmatched` serving tables), then `queue drain` (a safety net for
anything the OSM catch-up enqueued — normally a no-op right after a full
compare).

```bash
# 1. Bootstrap everything in one command
#    (accepts the same --osm-file/--bdot10k-file/--egib-file/--prg-file/
#    --terc-file/--street-mappings-file/--bdot10k-building-types-file/
#    --egib-building-types-file flags as `import full`, to use local files
#    instead of downloading; slowest step by far)
cargo run --release -- --config config.toml init

# 2. Start the HTTP service (API + web frontend)
cargo run --release -- --config config.toml run
```

The mapping CSVs in [`mappings/`](mappings/) are the same files `init`/
`import full` download by default, so pass them directly (as shown in the
`import full` example below) to skip the download. Individual `import
street-mappings` / `import building-types` commands still exist for
reloading just one mapping later (e.g. after editing a CSV) without
re-running the whole bulk import — as do standalone `import full` / `update
osm` / `compare full` / `queue drain`, if you'd rather run (or retry) the
steps `init` bundles one at a time. Step 1 writes to the database and DuckDB
allows only one writer process, so **stop the server before re-running it**.

### What happens if you skip a step

| Skipped | Symptom |
| --- | --- |
| `import full` (or a single dataset import) | The server refuses to start: `Required table '<name>' is missing`. |
| `import street-mappings` (or its part of `import full`) | Everything works, but `addr:street` is served exactly as PRG publishes it (`gen. Kruka` instead of `Generała Kruka`). |
| `import building-types` (or its part of `import full`) | Everything works, but **every** building is exported and previewed as plain `building=yes`, no matter what its BDOT10k/EGIB classification says. There is no warning — the mapping tables are created empty by the schema and the tag resolution simply falls through to its default. |
| `compare full` | The server starts and logs `serving table '<name>' is empty`; `/tiles` and `/package` return zero features. |

To check what a given database has actually had run against it, read
`job_run_log` from `/status` — every import records itself there under
`import:<source>`:

```bash
curl -s http://127.0.0.1:3000/status | jq '.job_run_log | keys'
# a fully set-up database lists, at minimum:
# ["import:bdot10k", "import:building-types", "import:egib", "import:street-mappings", ...]
```

### Keeping it current

Once set up, the `run` service can keep itself current — enable the background
jobs in the config (`[jobs.osm_update]`, `[jobs.bdot10k_update]`,
`[jobs.egib_update]`, `[jobs.prg_update]` to refresh the data,
`[jobs.match_refresh]` to drain the dirty-cell queue into the serving tables,
`[jobs.match_reconcile]` as a periodic safety net, and
`[jobs.street_mappings_update]` / `[jobs.building_types_update]` to re-fetch
the mapping CSVs). They are all `enabled = false` in
[`example_config.toml`](example_config.toml), so a config copied from it
updates nothing until you turn them on. The equivalent offline commands are
`update osm` / `update bdot10k` / `update egib` / `update prg` followed by
`compare full` (or `queue reconcile`).

Editing a mapping CSV needs only its `import` command re-run — both mappings
are applied when a response is built, so they never change *which* objects are
unmatched and require no `compare`, reconcile or drain.

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
- **`http_listen_addr`** — address and port the `run` server listens on (default `127.0.0.1:3000`)
- **`web_dir`** — directory the `run` server serves the static frontend from (default `./web`). Mounted as a fallback route, so it never shadows an API path; a missing directory is not a startup error
- **`download_dir`** — directory for downloaded files (default: system temp directory)
- **`cleanup_downloaded_files`** — delete files the app downloaded itself once they are consumed (default `true`)
- **`duckdb_init_commands`** — SQL statements run on database initialization
- **`download_urls`** — URLs for downloading data sources, including the two mapping CSVs (`street_mappings`, `bdot10k_building_types`, `egib_building_types`)
- **`[teryt]`** — TERYT/TERC dictionary settings for the PRG import (download vs. local `file_path`)
- **`[package]`** — `/package` endpoint limits (`max_area_sq_deg`, default 0.04)
- **`[updates]`** — `/updates` time window limits (`default_minutes`, `max_minutes`)
- **`[reports]`** — `POST /report` (`enabled`, default true — false makes the route a 404; `max_objects_per_request`, default 100)
- **`[jobs.*]`** — background jobs, each with `enabled`, `interval_seconds` and a per-run timeout: `osm_update`, `bdot10k_update`, `egib_update`, `prg_update`, `match_refresh` (drains the dirty-cell queue to keep `*_unmatched` serving tables current; also takes `batch_size`), `match_reconcile` (periodically re-enqueues every live cell as a safety net), `reports_reconcile` (retires reports whose government record changed while this process wasn't the one applying the change), `street_mappings_update` and `building_types_update` (re-fetch the mapping CSVs from `download_urls`), and `export_log_prune` (`retention_days`, default 365). Only one dataset refresh runs at a time, regardless of how the schedules line up.

All fields are optional — only specify what you want to override. Note that `duckdb_init_commands` is fully replaced if specified (not merged with defaults).

## CLI commands

### init — bootstrap a fresh database

```bash
# Everything needed to get started: import full, update osm, compare full,
# queue drain
cargo run -- init

# Same, using local files instead of downloading (any subset of flags works;
# omitted ones still download) -- takes the same flags as `import full`
cargo run -- init \
  --osm-file poland-latest.osm.pbf \
  --bdot10k-file bdot10k.parquet \
  --egib-file egib.parquet \
  --prg-file prg.zip \
  --terc-file terc.zip \
  --street-mappings-file mappings/street_names_mappings.csv \
  --bdot10k-building-types-file mappings/bdot10k_building_types.csv \
  --egib-building-types-file mappings/egib_building_types.csv
```

Each step still stops the whole command on the first failure. `update osm`
needs network access to the replication feed regardless of which `--*-file`
flags are given (there's no local-file equivalent for it, unlike the bulk
imports); if you don't want that, run `import full` and `compare full`
individually instead (see below).

### import — bulk-load data

```bash
# Import everything (OSM, BDOT10k, EGIB, PRG, then the street-name and
# building-type mappings) in sequence
cargo run -- import full

# Import everything from local files instead of downloading (any subset of flags works;
# omitted sources still download)
cargo run -- import full \
  --osm-file poland-latest.osm.pbf \
  --bdot10k-file bdot10k.parquet \
  --egib-file egib.parquet \
  --prg-file prg.zip \
  --terc-file terc.zip \
  --street-mappings-file mappings/street_names_mappings.csv \
  --bdot10k-building-types-file mappings/bdot10k_building_types.csv \
  --egib-building-types-file mappings/egib_building_types.csv

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

# Load just one mapping file later (e.g. after editing a CSV), without
# re-running the whole bulk import. Both download from this repository
# when no file is given.
cargo run -- import street-mappings --file mappings/street_names_mappings.csv
cargo run -- import building-types \
  --bdot10k-file mappings/bdot10k_building_types.csv \
  --egib-file mappings/egib_building_types.csv
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

**If you change the hashed row content in a way that alters its output, bump
the `ROW_HASH_VERSION` constant next to it.** Nothing else needs changing —
every import and every update reads that one constant.

That means `hashed_select` itself, but also anything feeding *into* it: a
source's inner select is part of the hash input, so a change there moves the
stored hashes just as surely. Version 2 is exactly that case — PRG's import
gained a street-name normalization inside its inner select
(`import::prg::ULICA_PREFIX_STRIP_SQL`). Transformations deliberately wrapped
*outside* `hashed_select` — `DatasetSpec::with_centroid_select`,
`mappings::egib::with_rodzaj_kod_select` — do not count.

What happens after a bump: the stamp in an existing database still names the
old version, so the next update logs a `row hash version mismatch` warning and
every row compares as modified. That refresh is effectively a full rewrite —
correct, just slower than usual, and it reports a changeset the size of the
whole dataset. On success it re-stamps the new version, so the warning appears
once per bump, not on every run afterwards. A refresh that fails leaves the old
stamp alone, so the warning survives until a rewrite actually lands. The stamp
is global, so a bump made for one source also costs the others one full-rewrite
refresh apiece.

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
```

`compare` recomputes the `*_unmatched` serving tables (`bdot10k_unmatched`,
`egib_unmatched`, `prg_unmatched`) from scratch — the tables `/tiles` and
`/package` read. Between full re-compares, government refreshes and OSM
updates keep them current incrementally: each producer enqueues the z14 cells
it touched into `match_dirty_cells`, and the `match_refresh` background job
(see `run` below) drains that queue by recomputing just those cells.

### queue — operate on the match_dirty_cells queue by hand

```bash
# Re-enqueue every cell containing a government object, so the next drain
# rebuilds it (safety net for a dropped enqueue, an offline rebuild path, or
# a scheduled sweep)
cargo run -- queue reconcile

# Drain the queue: recompute *_unmatched for every dirty cell, oldest first,
# until none remain
cargo run -- queue drain
cargo run -- queue drain --batch-size 1000
```

Both actions require exclusive access to the database (like any CLI command)
— do not run them against a database a `run` server also has open; the
server drains the same queue itself via its `match_refresh`/`match_reconcile`
background jobs. `queue reconcile` re-enqueues every live cell instead of
comparing directly, for when the queue can't be trusted or a full serving
rebuild is wanted without redoing the whole comparison. `queue drain` is the
manual, one-shot equivalent of what `match_refresh` does on a schedule inside
`run`.

### reports — manage user reports offline

```bash
# What has been reported, newest first
cargo run -- reports list
cargo run -- reports list --source bdot10k --status active --limit 200
cargo run -- reports list --since '2026-08-16 14:00:00'

# Retire one report so its object is proposed for import again
cargo run -- reports revoke 41
# ...or every active report submitted in a window. Because nothing identifying
# the submitter is stored, this is the only way to unwind an abusive burst
cargo run -- reports revoke --since '2026-08-16 14:00:00' --source prg

# Retire reports whose government record has changed or disappeared. Runs
# automatically inside every dataset refresh and after every import; this is
# the manual safety net (also available as the reports_reconcile job)
cargo run -- reports reconcile

# Back up and restore. object_reports is the only table in this database that
# cannot be rebuilt from an external source -- `import full` restores
# everything else and starts with no reports at all
cargo run -- reports export reports.jsonl
cargo run -- reports import reports.jsonl
```

A revoked, expired or imported report only changes what is served once the
affected cells are recomputed, so follow any of these with `queue drain` (or
let a running server's `match_refresh` job get to it).

`reports import` reallocates ids from the current maximum rather than
preserving them, so importing into a database that already has reports cannot
collide; a round trip is faithful in content, not in id.

### run — HTTP service

```bash
cargo run -- run
```

Serves:
- the web frontend from `web_dir` (default `./web`) — open `http://127.0.0.1:3000/` in a browser
- `/health` — liveness check
- `/status` — background job status as JSON, including match-queue staleness (`match_staleness`: pending cell count overall and per source, oldest enqueued timestamp) and per-job last-run outcome (`job_run_log`, keyed by job name — imports appear there as `import:<source>`)
- `/tiles/{z}/{x}/{y}` — Mapbox Vector Tiles reading the precomputed `*_unmatched` serving tables, in three zoom tiers: z5–z11 aggregated bins (layers `agg_cells`/`agg_points`, same aggregate rendered two ways), z12–z13 one point per unmatched object (layer `points`), z14 full geometry with source attributes and the resolved OSM tags an import would write (layers `buildings`/`addresses`, plus `buildings_all`/`addresses_all` showing every government object, matched or not). Any other zoom returns `204 No Content`
- `/package` — GeoJSON `FeatureCollection` of government-registry records missing
  from OSM in the requested area, tagged for direct JOSM import. Reads the same
  precomputed `*_unmatched` serving tables as `/tiles` (no live comparison per
  request). The request area (bounding box) is capped by the
  `[package] max_area_sq_deg` config setting (default 0.04 sq deg).
- `/updates` — recent `/package` export activity (timestamp, area, datasets, feature counts) as GeoJSON, `Cache-Control: public, max-age=60`. A background job prunes entries older than `[jobs.export_log_prune] retention_days` (default 365).
- `POST /report` — mark government objects as ones that should not be proposed
  for import. Body is `{"note": ..., "objects": [{"source": ...,
  "key": {...}}]}`, where `key` names exactly the source's key columns
  (`PRZESTRZENNAZW` + `LOKALNYID` for bdot10k, `id_budynku` for egib,
  `lokalny_id` for prg). A key that matches no live record is rejected per
  object rather than failing the request. Each accepted report enqueues its
  z14 cell, so the object leaves `*_unmatched` on the next drain — from then
  on it appears only on `/tiles`' `addresses_all`/`buildings_all` layers,
  carrying a `reported` attribute the frontend renders as "Zgłoszony". Capped at
  `[reports] max_objects_per_request` objects; `[reports] enabled = false`
  turns the route into a 404 with no redeploy. Nothing about the submitter is
  captured or stored — see the `[reports]` comment in `example_config.toml`
  for what that means for abuse control, and the `reports` CLI above for the
  time-scoped cleanup path it leaves.

The `match_refresh` background job keeps `/tiles` and `/package` fresh between
full `compare` runs, by draining `match_dirty_cells` on a schedule (see
`[jobs.match_refresh]`).

**Upgrading an existing database in place:** the `*_unmatched` serving tables
are created with `CREATE TABLE IF NOT EXISTS`, so starting the server against
an older database that predates them leaves all three empty — `/tiles` and
`/package` will start up cleanly but serve zero features (a startup warning
names each empty table). Run an offline `compare full` before restarting the
server against that database. `queue reconcile` is **not** an equivalent
fast path here: it only re-enqueues every live cell for the incremental
drain, so populating a fully empty database through it means draining the
entire country cell-by-cell, which is orders of magnitude slower than a
direct full `compare`.

The same applies to columns added by a later version. Every table is created
with `CREATE TABLE IF NOT EXISTS` and there is no `ALTER TABLE`/backfill path
anywhere in this codebase, so an existing table gains no new column merely by
running a newer binary — the table has to be rebuilt by the command that
creates it. Columns carried on the `*_unmatched` serving tables (the
classification and display attributes `/tiles` and the popups show) come from
`compare`; columns precomputed on the source tables
(`bdot10k_buildings.centroid`, `egib_buildings.centroid`,
`egib_buildings.rodzaj_kod`) come from `import bdot10k` / `import egib`, which
rebuild those tables wholesale. Re-running the whole setup sequence above is
the reliable way to bring an old database forward.

`POST /report` in particular needs `bdot10k_unmatched.PRZESTRZENNAZW`, which is
the other half of BDOT10k's composite key and was added with the feature — a
database predating it needs `bdot10k_unmatched` recreated and `compare bdot10k`
re-run, or every BDOT10k tile fails to render. With one exception, everything
here is a rebuild-and-recompute story: `object_reports` is the only table in
this database that **cannot** be reconstructed from an external source, so run
`reports export` before rebuilding a database that has any.

```bash
# bbox: minLon,minLat,maxLon,maxLat; datasets: prg, bdot10k, egib, or all (default)
curl 'http://127.0.0.1:3000/package?bbox=20.99,52.19,21.02,52.22&datasets=prg,bdot10k'

# Or POST a GeoJSON Polygon/MultiPolygon for an exact area
curl -X POST 'http://127.0.0.1:3000/package?datasets=all' \
  -d '{"type":"Polygon","coordinates":[[[20.99,52.19],[21.02,52.19],[21.02,52.22],[20.99,52.19]]]}'

# Recent export activity (default: last 60 minutes)
curl 'http://127.0.0.1:3000/updates'
curl 'http://127.0.0.1:3000/updates?minutes=1440'

# Report an object as one that should not be proposed for import
curl -X POST 'http://127.0.0.1:3000/report' -H 'content-type: application/json' \
  -d '{"objects":[{"source":"egib","key":{"id_budynku":"146509_8.0001.120.1_BUD"}}]}'
```

Background jobs (OSM and government-dataset refreshes, the dirty-cell drain,
mapping refreshes, export-log pruning) run on the schedules in `[jobs.*]` —
all of them ship disabled in `example_config.toml`.

### Street name mappings

PRG publishes abbreviated street names (`gen. Kruka`); OSM Poland uses expanded
ones (`Generała Kruka`). `mappings/street_names_mappings.csv` maps between them
and is applied to `addr:street` when `/package` builds its response (and to the
tag preview shown on `/tiles` and in the frontend's popups), so downloaded data
is importable without hand-editing.

Load it with:

    cargo run -- import street-mappings --file mappings/street_names_mappings.csv

A row with an empty `teryt_simc_code` applies nationwide; one with a code
applies only to that settlement and overrides the nationwide row. Lookup is
case-insensitive. The file is optional — without it, names are served exactly
as PRG publishes them.

To propose a change, edit the CSV and open a PR; `cargo test --test
street_mappings_file` checks its structure.

### Building type mappings

BDOT10k and EGIB classify buildings in their own schemes
(`budynek jednorodzinny`, `rodzaj = m`, …); `mappings/bdot10k_building_types.csv`
and `mappings/egib_building_types.csv` translate those into OSM tags
(`building=house`, `building=detached`, …). Like the street names, they are
applied when a response is built — on `/package`, on `/tiles`, and in the
frontend's feature popups.

Load them with:

    cargo run -- import building-types \
      --bdot10k-file mappings/bdot10k_building_types.csv \
      --egib-file mappings/egib_building_types.csv

Each row is `tier,key,min_levels,max_levels,max_neighbours,tags` (the EGIB file
adds a free-text `note`). Tier 1 matches the detailed function (BDOT10k
`PRZEWAZAJACAFUNKCJABUDYNKU`, EGIB `rodzaj_kod` — a single-letter class derived
from `rodzaj` when EGIB is imported), tier 2 the general one (BDOT10k
`FUNKCJAOGOLNABUDYNKU`; EGIB has no second tier); tier 1 wins, and among rows
of the same tier the most constrained one wins. The optional level and neighbour constraints let one key resolve
differently by context — an isolated one-to-two-storey single-family building
becomes `building=detached`, one touching another becomes `building=house`.
Adjacency is counted at serve time against the full building tables.

**These files are not optional in practice:** with the mapping tables empty,
every building resolves to plain `building=yes` and nothing warns about it.

To propose a change, edit the CSV and open a PR; `cargo test --test
building_types_files` checks its structure. See
[`docs/building_type_mappings.md`](docs/building_type_mappings.md) for how the
mappings were derived.

## Development

The frontend in [`web/`](web/) (`index.html`, `app.js`, `style.css`, MapLibre
GL JS) is plain static files served from `web_dir` at runtime, not embedded in
the binary — editing them needs no rebuild, just a reload (a hard one: the
browser caches `app.js` over plain HTTP). It talks to `/tiles`, `/status`,
`/package` and `/updates` on the same origin.

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
