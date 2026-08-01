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
- `compare <target>` — compare government data against OSM, writing the precomputed `*_unmatched` serving tables. Targets: `buildings` (optionally `bdot10k`, `egib`, or `all` — default runs all), `addresses` (optionally `prg` or `all` — default runs all), `full` (runs every comparison), `reconcile` (re-enqueues every live cell for the incremental drain — safety net / offline rebuild)
- `run` — HTTP service (`/health`, `/status`, `/tiles/{z}/{x}/{y}` and `/package` serving `*_unmatched`, `/updates` recent export activity) with background OSM/government-dataset updates, the `match_refresh` drain job, and export log pruning

**Storage:**
- **DuckDB** — main analytical database for geospatial queries, stores processed OSM data and government datasets
- **RocksDB** — KV store for raw OSM node coordinates and structural mappings (way node refs, relation members, reverse indexes)

**Key design decisions (see `adr/`):**
- DuckDB chosen for its geospatial support and file-based storage (ADR-002)
- API returns GeoJSON, not OSM XML — JOSM can read GeoJSON (ADR-003)
- Single process, multithreaded — DuckDB doesn't support multiple writer processes (docs/project_ideas.md)

**Government-dataset updates** stage a fresh snapshot in `<table>__staging`, diff it against the live table by whole-row hash, and apply only the delta. The delta, the `dataset_refreshes` row and the per-tile `dataset_change_areas` rows all commit in one transaction; change areas are written *before* the delta, because they read the pre-update geometry of removed/modified rows out of the live table.

**Gotcha — row-hash version.** The diff only works if import and update hash a row identically, so the hash expression exists in exactly one place: `hashed_select` in `src/dataset.rs`. **If you change it in a way that alters its output, bump the `ROW_HASH_VERSION` constant beside it.** The version in force is stamped into `metadata.row_hash_version` by `dataset::stamp_row_hash_version`, called from the import dispatch and from the apply transaction in `update::dataset::refresh`. After a bump the next update warns, compares every row as modified (a full rewrite — correct but slow), then re-stamps, so the warning fires once rather than forever; a failed refresh leaves the old stamp. Only paths that rebuild a table's hashes wholesale may stamp — never a partial delta.

**Precomputed unmatched serving.** `compare` doesn't just log a comparison — it writes the unmatched government objects into `bdot10k_unmatched` / `egib_unmatched` / `prg_unmatched` serving tables, and `/tiles` + `/package` read those directly instead of comparing live. Between full `compare` runs, government refreshes and OSM updates keep the tables current incrementally: each producer enqueues the z14 cells it touched into `match_dirty_cells`, and the `match_refresh` background job drains that queue by recomputing just those cells (`compare::drain::drain_batch` → `compare::incremental::recompute_cell_in_txn`). `compare reconcile` re-enqueues every live cell as a safety net (a dropped enqueue) or an offline rebuild path.

**Gotcha — the match rule has one home.** The predicate deciding whether a government object counts as "matched" lives in `src/compare/rule.rs`. The per-cell incremental recompute (`compare::incremental::recompute_cell_in_txn`) and the full **building** compare (`compare::buildings::compare_buildings`) both call `rule::unmatched_buildings_sql` directly, so they share the actual predicate text. The full **address** compare (`compare::addresses::compare_addresses`) uses its own grid-key SQL for performance instead (see the design doc: the iteration strategy legitimately differs by path) and shares only `rule::MATCH_DISTANCE_METERS`, not the predicate — `addresses::full_and_per_cell_paths_agree` pins that grid-key path against the shared per-cell rule, and `compare::full_vs_incremental_equivalence` (`src/compare/mod.rs`) pins full `compare` against reconcile+drain end-to-end for bdot10k and prg. Never re-derive the match *distance* or the containment condition anywhere else — a second copy would silently drift from what the serving tables actually contain.

**Gotcha — bdot10k/egib's representative point is a stored column, not computed.** `bdot10k_buildings` and `egib_buildings` carry a `centroid GEOMETRY` column, populated by `import::bdot10k::load_into` / `import::egib::load_into` (shared by `import` and `update`'s staging load) and RTREE-indexed the same way `geom` is. `rule::unmatched_buildings_sql`, `compare::buildings`, `compare::incremental`, `compare::reconcile::enqueue_all`, and `update::changeset` (via `DatasetSpec::representative_point_sql`) all read this column instead of computing `ST_Centroid(geom)` inline — an RTREE index cannot be used through a function wrapped around the indexed column, which was the root cause of the full-table-scan bottleneck in `docs/per_cell_recompute_full_scan.md` (measured fix: `docs/centroid_index_measured.md`, ~10–100× faster per z14 cell on real data). The column is added *outside* `hashed_select`'s projection (`DatasetSpec::with_centroid_select`), so it never affects `_row_hash` and needs no `ROW_HASH_VERSION` bump. Scope is bdot10k/egib only — PRG's `geom` already is its representative point, and `bdot10k_unmatched`/`egib_unmatched` (the serving tables) and `osm_buildings` are untouched, so `server/package.rs` and `update/dirty_cells.rs` still compute `ST_Centroid` inline on those. **No migration path exists for databases built before this change** — `import bdot10k` / `import egib` must be re-run (which rebuilds the table wholesale) to gain the column; there is no `ALTER TABLE`/auto-backfill.

**Gotcha — `now()` is transaction-start-scoped.** DuckDB evaluates `now()` at the transaction's BEGIN, not at statement time. The government refresh enqueues its dirty cells *inside* the apply transaction, so every cell a 5-minute BDOT10k refresh touched is stamped with that transaction's start time. `/status`'s `oldest_enqueued_at` therefore reads ~5 minutes worse than reality right after a refresh. This is cosmetic — the drain's cutoff is snapshot-based and unaffected — but don't "fix" the metric by reaching for `now()` somewhere else in the drain (see the next gotcha).

**Gotcha — the drain's cutoff is load-bearing.** `drain_batch` takes one `batch_start` timestamp and uses it for *both* the read (`enqueued_at <= batch_start`, selecting which dirty cells this tick owns) and the paired queue-delete after recomputing each cell. Both sides must use that same stored value, not `now()`: a cell re-dirtied after `batch_start` must survive the delete (its edit wasn't seen by this tick's recompute) and be picked up by the next one. Using `now()` on either side — or two different timestamps — either strands a cell dirty forever or deletes a queue row for a change the recompute never read.

**Gotcha — serving tables store rows, not id references.** `*_unmatched` tables copy the columns needed to render a feature (geometry, tags, `cell_x`/`cell_y`, `computed_at`) instead of pointing back at the source table by id or rowid. BDOT10k's `LOKALNYID` isn't unique, and DuckDB rowids aren't stable across the DELETE+INSERT that every recompute (and every refresh) does — so id/rowid references would go stale silently. Recompute is always DELETE-then-INSERT for the affected cell, never an in-place UPDATE.

**Gotcha — invalid government geometry is dropped, not repaired.** A small number of BDOT10k/EGIB rows have topologically invalid geometry (`ST_IsValid = false`), which crashes `ST_AsMVTGeom` and takes down the whole tile (see `docs/invalid_geometry_tile_500s.md`). `dataset::filter_invalid_geometry` deletes those rows immediately after `import::bdot10k::load_into` / `import::egib::load_into` create their table — the one place both `import` and `update`'s staging load funnel through — so `compare::buildings` and `compare::incremental` never see them and need no changes of their own. `import()` and `update::dataset::refresh()` each self-report their outcome (including any skipped-row summary) to the `job_run_log` table via the `job_log` module, under job names `import:<source>` / `update:<source>`; `/status` reads it back as `job_run_log`. Invalid-geometry filtering is bdot10k/egib-only — PRG's loader was never given a `filter_invalid_geometry` call. `job_run_log` reporting has wider reach, though: PRG's `update_prg` shares the same `refresh()` that self-reports, so `update:prg` also appears in `job_run_log` (with no skip-count clause, since PRG performs no filtering). `import:prg` does not report, since PRG's import path never goes through `refresh()`. A government refresh whose ETag is unchanged returns early via `record_noop_refresh` (`src/update/mod.rs:38`/`:63`/`:88`), before `dataset::refresh` — and its self-report — ever runs, so that job's `job_run_log` entry is left untouched from its last real run. Because no-op refreshes are the common case, `job_run_log["update:<source>"].ran_at` can be days older than the corresponding `jobs[].last_finished_at` for the same job without indicating anything is wrong.

**Gotcha — dirty-queue source strings must match everywhere.** `match_dirty_cells.source` is a plain string (`"bdot10k"` / `"egib"` / `"prg"`), not an enum, and every producer must spell it identically: the government refresh's `spec.name` (defined in `src/dataset.rs`; enqueued at the call site in `update::changeset::insert_dirty_cells`, `src/update/changeset.rs`), the OSM update's flush (`src/update/dirty_cells.rs`), `compare reconcile` (`src/compare/reconcile.rs`), and the drain's dispatch in `recompute_cell_in_txn` (`src/compare/incremental.rs`). A mismatched string silently orphans that source's dirty cells — enqueued but never drained.

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
