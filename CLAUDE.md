# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

**How to read this file.** It is an index of invariants — the rules that are not
recoverable by reading one file, and the traps whose failure mode is silent. The
*reasoning* behind each lives in the doc comment at the named symbol, or in
`docs/`. When they disagree, the code is right; fix this file.

## Project Overview

Rewrite of [gugik2osm](https://github.com/openstreetmap-polska/gugik2osm) — compares
Polish government registry data (addresses from PRG, buildings from BDOT10k and
EGIB) with OpenStreetMap and generates GeoJSON packages for import into JOSM.

## Build & Test Commands

```bash
cargo build           # first build is slow: DuckDB + RocksDB compile from source
cargo test            # run all tests
cargo test <name>     # single test by name
cargo clippy          # lint
cargo fmt -- --check  # check formatting
cargo build --profile profiling   # release + debug symbols for samply/perf
```

Needs **CMake plus Ninja or GNU Make** on `PATH`. No external DuckDB/RocksDB
installation — both are bundled.

**Gotcha — two separate jemallocs, each fragile in a different way.** DuckDB's
bundled copy renames its API to `duckdb_je_*` and serves DuckDB only;
`rust-rocksdb`'s `jemalloc` feature pulls an *unprefixed* build whose
`malloc`/`free` override glibc's, backing RocksDB, Rust's `alloc`, and
everything non-DuckDB. Four load-bearing points:

1. **The `duckdb` git dep is deliberate; reverting it to crates.io silently
   drops DuckDB's jemalloc.** jemalloc lives in the `duckdb-sources` submodule,
   which the published crate `exclude`s, and the `bundled` amalgamation backend
   has no jemalloc sources at all. Only `bundled-cmake` has it, and it *requires*
   a git checkout. The tag pins the exact version the registry dep used
   (`v1.10505.0`) — the git source buys the build backend and nothing else.
   `bundled-cmake` is marked experimental upstream.
2. **The dev-dependency must name the same source as the normal dependency.**
   Two packages both declaring `links = "duckdb"` is a hard cargo error.
3. **Neither flag errors on an unsupported platform; both go quiet.** On macOS a
   build succeeds with neither engine on jemalloc and says nothing. The only
   proof is symbols in the built binary — `duckdb_je_malloc` (DuckDB),
   `_rjem_je_*` (RocksDB) — never a config setting or a log line.
4. **`bundled-cmake` statically links the parquet extension.** This removes a
   runtime autoload dependency rather than changing behaviour.
   `DUCKDB_DISABLE_JEMALLOC=1` is the escape hatch for the DuckDB side only.

**Gotcha — DuckDB's version comes from `git describe`, cargo's checkout has no
tags, so it must be forced.** `describe` fails, DuckDB continues with the dummy
version **`v0.0.1`** (a `message(WARNING)`, never an error), and since the
version string *is* the extension repository path, `INSTALL spatial` 404s. That
is in the default `duckdb_init_commands`, so *every* CLI command and hundreds of
tests fail from this one cause.

1. **The fix is a CMake toolchain file** (`cmake/duckdb_version.cmake`, forcing
   `OVERRIDE_GIT_DESCRIBE`), pointed at by `.cargo/config.toml`'s `[env]` with
   `relative = true`. There is no other injection point: `libduckdb-sys` forwards
   a fixed env list and rejects `DUCKDB_EXTENSION_CONFIGS`. Side effect: with a
   toolchain file set, the `cmake` crate stops passing
   `-DCMAKE_C_COMPILER`/`CXX_COMPILER`. Fine natively, load-bearing for a
   cross-compile.
2. **`cmake/duckdb_version.cmake` must move in lockstep with the `duckdb` tag in
   `Cargo.toml`** (`v1.10505.0` ⇒ DuckDB `v1.5.5`). Nothing links them
   automatically. `db::tests::duckdb_reports_a_real_version_so_extensions_resolve`
   is the guard; it pins the *shape*, so a routine bump needs no test edit.
3. **Editing the toolchain file does not rebuild DuckDB, and `cargo clean -p
   libduckdb-sys` does not either** — the spec silently matches nothing for a git
   source and exits 0. Delete by hand: `rm -rf target/*/build/libduckdb-sys-*
   target/*/.fingerprint/libduckdb-sys-*`, then rebuild (≈12 min). Confirm by
   grepping `OVERRIDE_GIT_DESCRIBE` out of the generated `CMakeCache.txt`.
4. **`spatial` cannot be statically linked.** Only DuckDB's *in-tree* extensions
   can be; `spatial` is out-of-tree and `bundled-cmake` panics on the mechanism
   that would pull it in. So a correct version stamp is non-negotiable.

## Architecture

**Tech stack:** Rust + DuckDB (embedded, file-based) + RocksDB (KV store). Single
binary, easy to deploy.

**CLI commands** (`cargo run -- <command>`):

- `import <source>` — bulk-load (OSM from PBF, PRG from ZIP via `--file`/`--terc-file`,
  BDOT10k/EGIB from GeoParquet)
- `update <source>` — incremental (OSM minutely replication, re-download gov datasets)
- `compare <target>` — write the precomputed `*_unmatched` serving tables.
  Targets: `buildings`, `addresses`, `full`
- `queue <action>` — operate on the `match_dirty_cells` queue offline (requires
  exclusive DB access — never against a database a `run` server also has open).
  `reconcile` re-enqueues every live cell; `drain` runs the per-cell recompute
  until empty
- `reports <action>` — manage user reports offline: `list`, `revoke`,
  `reconcile`, `export`/`import` (JSONL). A revoke/expire only reaches the
  serving tables after a drain
- `run` — HTTP service (`/health`, `/status`, `/tiles/{z}/{x}/{y}`, `/package`,
  `/updates`, `POST /report`) plus background update, drain and reconcile jobs

**Storage:** DuckDB for geospatial queries and processed data; RocksDB for raw
OSM node coordinates and structural mappings (way node refs, relation members,
reverse indexes).

**Key design decisions** (see `adr/`): DuckDB for geospatial + file-based storage
(ADR-002); API returns GeoJSON, not OSM XML (ADR-003); single multithreaded
process, since DuckDB has no multi-writer support.

### Government-dataset updates

A refresh stages a snapshot in `<table>__staging`, diffs it against the live
table, and applies only the delta. The delta, the `dataset_refreshes` row and the
per-tile `dataset_change_areas` rows all commit in one transaction; change areas
are written **before** the delta, because they read the pre-update geometry of
removed/modified rows out of the live table.

**Gotcha — `dataset_change_areas` is read by `/tiles`, and three numbers in
three files have to stay ordered.** `server::tiles::agg_bin_ctes` LEFT JOINs it
into every z5–z11 tile as a per-bin, per-source `ts_*` timestamp, which the
frontend compares against its own window. So: `web/app.js`'s
`CHANGES_WINDOW_HOURS` (24) ≤ `[changes] max_age_days` (7) ≤
`[jobs.retention_prune] change_areas_days` (90). Violate either inequality and
nothing errors — the overlay just goes blind for the tail of the window it
still advertises. Two further consequences:

- **Per-tile cost is linear in `max_age_days` and independent of the tile.**
  The table has no index and no useful cell ordering (`insert_change_areas`
  writes via `GROUP BY cell_x, cell_y` under `preserve_insertion_order =
  false`), so the `detected_at` bound is the only pruning there is — zonemaps
  work on it because rows are appended per refresh. Widening `max_age_days`
  slows the **existing grid**, not just the overlay, because the read sits
  inside the main aggregate query. Measured on a 90-day, 3.6M-row table: the
  7-day bound scans 280k rows.
- **`LEFT JOIN`, never a fifth `UNION ALL` branch into `bins`.** A union branch
  *creates* bins, and a bin standing on change data alone has no denominator,
  so `ratio_sql` gives it `RATIO_UNKNOWN` and the frontend paints a cell that
  was not in the grid before. `cell_totals` gets to be a union branch precisely
  because a totals row *is* a denominator. Pinned by
  `tiles::tests::change_rows_in_a_cell_with_no_grid_data_add_no_bin`.

**Gotcha — change detection is configured per source, and each configuration is
a measured choice.** A refresh decides "modified" by joining on the record key
and comparing a *named list* of columns — never a whole-row hash, which cannot
tell a record changing from its *serialization* changing (a routine BDOT10k
re-export rewrote all 16,344,762 rows). Both halves live on `DatasetSpec`
(`src/dataset.rs`): `key_columns` for identity, `compared_columns` +
`compare_geometry` for change. `DatasetSpec::changed_predicate_sql` is the single
home for the comparison text. The three configurations look inconsistent from
outside; each is deliberate. Measurements:
`docs/superpowers/plans/2026-08-14-key-based-diff.md`.

- **BDOT10k — `compare_geometry: false`.** BDOT10k periodically re-serializes
  every geometry wholesale (0.94%, 4.5%, then **100%** of rows across measured
  snapshot pairs). Re-serialized bytes are indistinguishable from real movement.
  Accepted cost: a geometry-only edit with no `WERSJA` bump and no attribute
  change is missed until that record next changes. **The comparison is `IS
  DISTINCT FROM`, never `>`** — attributes change *without* a `WERSJA` bump
  (6,395 records in one pair, including 64 `KATEGORIAISTNIENIA` transitions that
  `rule::BDOT10K_EKSPLOATOWANY_FILTER` gates on), so `>` would miss them entirely.
- **PRG — the version columns are excluded.** PRG bulk-republishes by gmina:
  `wersja_id` moved 34–147× more often than any content changed (149,198 version
  bumps vs 1,012 real changes in one pair). `poczatek_wersji_obiektu` moves in
  exact lockstep with it (0 disagreements in 8.6M records), so adding *it*
  silently reinstates the churn. `wersja_id` is still load-bearing as
  `deduplicate_by_key`'s ordering column, nothing else. `teryt_miejscowosc` is
  compared for a non-obvious reason: it selects which `street_name_mappings` row
  applies, so a change to it rewrites the exported `addr:street` *and* can flip
  the address between matched and unmatched. `miejscowosc` is a match input too,
  via the locality rule for streetless addresses.
- **EGIB — `czas_pozyskania` (99.7% churn per export) and `pozostale_atrybuty`
  (32.8%, carries a per-export `gml_id`) are excluded**, and geometry is *not*
  optional: 617,207 records have all three compared attributes NULL, so geometry
  is their only signal. Compare it as `ST_AsWKB(...)`, never bare `GEOMETRY` —
  24.18s vs 2.50s on the real 17.5M-row table for the identical answer.

**Gotcha — the key diff needs a non-null, unique key, and nothing at diff time
can check that.** `update::diff::compute` joins with plain equality. A NULL key
never matches itself, so a NULL-keyed record lands in *both* `diff_added` and
`diff_removed`, and the apply then deletes nothing and inserts nothing —
silently, forever. This was real: EGIB shipped 210,080 NULL `id_budynku` rows
(1.2%). A duplicate key fans the join out instead. Both guarantees are
established **at load and only there**: `dataset::non_null_key_sql` inside the
load SELECT (with `null_key_sql`, its exact complement, for the skip count) and
`dataset::deduplicate_by_key` after the table is built. All three loaders do
both — PRG included, whose `lokalny_id` is unique in four of five national
snapshots but not the fifth.

**Gotcha — a value derived outside `compared_columns` never self-heals.**
`centroid` (`DatasetSpec::with_centroid_select`), `rodzaj_kod`
(`mappings::egib::RODZAJ_KOD_CASE_SQL`) and PRG's `ULICA_PREFIX_STRIP_SQL` are
recomputed for every *staged* row, so a record modified for some other reason
picks up a new expression — but an **unmodified record keeps its old value
forever**. Editing any of them requires a re-import, not a refresh.
`ULICA_PREFIX_STRIP_SQL` is the mild case (`ulica` is compared, so an edit
surfaces as an ordinary modification and self-heals per record); BDOT10k is the
sharp one, since geometry is outside its predicate too.
`update::dataset::check_column_shapes_match` compares staging and live column
lists (**ordered** — the apply's `INSERT ... SELECT s.*` is positional, so a
reordering is as fatal as an addition) and bails before the diff runs.

### The match rule and its three vetoes

**Gotcha — the match rule has one home.** The predicate deciding whether a
government object is "matched" lives in `src/compare/rule.rs`. The per-cell
incremental recompute (`compare::incremental::recompute_cell_in_txn`) and the
full **building** compare (`compare::buildings::compare_buildings`) both call
`rule::unmatched_buildings_sql`, sharing the actual predicate text. The full
**address** compare (`compare::addresses::compare_addresses`) uses its own
grid-key SQL for performance and so restates the predicate — but it must never
re-derive what it *does* share: both distance constants
(`MATCH_DISTANCE_METERS`, `NAME_MATCH_DISTANCE_METERS`), `normalized_name_sql`,
and the street-resolution builders in `mappings::street_names`.
`addresses::full_and_per_cell_paths_agree` pins the two paths, and its fixture
must exercise every branch *and its negative* — two paths that both dropped a
rule would agree perfectly and pass. `compare::full_vs_incremental_equivalence`
pins full `compare` against reconcile+drain end-to-end. **Never re-derive the
match distance or the containment condition anywhere else.**

**Gotcha — the former-building suppression veto, layered on the match rule.**
`osm_former_buildings` holds OSM ways/relations tagged with a lifecycle-prefixed
building key (`demolished:building`, `ruins:building`, …) — OSM's record that a
building here is gone — and `rule::unmatched_buildings_sql` excludes a government
building it substantially overlaps.

1. **The key list has one home**, `osm::lifecycle::LIFECYCLE_BUILDING_KEYS`
   (18 keys, in priority order — 24 Polish ways carry more than one).
   `import osm` reads it as a DuckDB list literal, `update osm` as plain Rust
   (`lifecycle::key_of`); `osm::lifecycle::tests::matched_key_sql_agrees_with_key_of`
   is the only thing pinning the two extractions together. The disjointness half
   — an object also carrying a live `building` key is a standing building, not a
   former one — has one home per language for the same reason. **Keep them
   composed, not split:** the parity test pairs
   `CASE WHEN is_former_building_sql THEN matched_key_sql END` against `key_of`,
   so inlining the disjointness at the call sites instead would spell the rule
   three times with nothing pinning them together, and the test would pass either
   way.
2. **The veto lives only inside `unmatched_buildings_sql`** (via the shared
   `former_building_covers_sql` builder, whose sibling `osm_building_covers_sql`
   holds the live-`osm_buildings` half), so both compare paths inherit it. It
   uses its own constant, `FORMER_BUILDING_MIN_OVERLAP_FRACTION`, deliberately
   kept separate from `MIN_OVERLAP_FRACTION` even though both are 0.10 — one
   answers "did OSM already map this", the other "is this veto trustworthy
   enough to suppress an import", and they must be free to move apart. The floor
   is not a rounding nicety: 1,228 of 6,088 raw `ST_Intersects` hits nationally
   (20%) are under 2% overlap — party-wall touches and slivers against a
   *neighbouring* demolished building — so a bare `ST_Intersects` veto would
   wrongly suppress those. See `docs/former_buildings.md` and
   `rule.rs`'s `building_barely_touching_former_neighbor_stays_unmatched`.
3. **A suppressed building still counts in `cell_totals`**, unlike a
   `KATEGORIAISTNIENIA`-filtered one: it is comparable and OSM has effectively
   handled it, whereas an `extra_filter`-excluded row is out of comparison
   entirely. `suppressed_buildings_sql` (the mirror of the veto) returns exactly
   the rows the veto removes, so `matched + unmatched + suppressed = total` stays
   exact. Without a separate count every suppressed row would read as `matched`,
   which is precisely the number an operator checks to see whether the feature
   works at all.

   **`suppressed_buildings_sql`'s clause *order* is load-bearing**, and it is why
   it does not share `unmatched_buildings_sql`'s flat shape. It is the one
   predicate run **once over the whole source extent**, so the two clauses meet
   wildly different row counts: the veto is savagely selective (~6k of 16.35M
   rows), while the `osm_buildings` anti-join covers 17.99M rows. Written flat,
   DuckDB plans the anti-join *underneath* the semi-join and the delim machinery
   materializes `b.geom`: **2.18 GB of WKB**, dying with
   `Out of Memory Error` after 71 s having spilled ~15 GB. Building the veto's
   candidates in a CTE first and anti-joining over *those* measures ~4.7 s.
   **The index is a red herring and believing otherwise is the trap:**
   `osm_buildings` is an `RTREE_INDEX_SCAN` in *both* shapes, but its window is
   `Bounds: deferred (from join filter)`, so probing with all of Poland yields a
   whole-country bound that prunes nothing. Only shrinking the probe side works.
   **`MATERIALIZED` is insurance, not the active ingredient — the CTE is**: a
   bare `WITH` plans identically apart from `CTE_SCAN`.
   `rule::tests::suppressed_buildings_predicate_filters_by_the_veto_first` pins
   the order structurally.
4. **`update osm` must maintain `osm_former_buildings` at every site that
   maintains `osm_buildings`** — way/relation delete, and both the changeset and
   *inferred* tag-determination arms of `rebuild_way_geometry` /
   `rebuild_relation_geometry`. The inferred arm is the one to watch: its early
   return must consider the former key too, or a former-building way whose node
   moved keeps a stale pre-move geometry forever. `Layer::Buildings` is the
   correct dirty-cell layer (its `flush` maps to `bdot10k` + `egib`).
   `prg_unmatched` is deliberately unreachable from this table — "former building
   ⇒ nearby address is bogus too" needs its own design, not a drive-by wire-up.
5. **`db::create_schema` and `import::osm::reset_osm_tables` both declare this
   table and must agree on its shape.** `create_schema` creates it *without* its
   RTREE index — `create_spatial_indexes` only runs inside `import osm`.

**Gotcha — the user-report veto is a third layer, and its rules are not the
former-building ones repeated.** `object_reports` holds user submissions
(`POST /report`, `src/server/reports.rs`) saying a government object should not
be proposed at all. An `active` report vetoes its object out of
`<source>_unmatched`.

1. **The clause has one home**, `compare::rule::reported_sql`, and unlike the
   former-building pair it is `pub`, because the address full compare
   legitimately restates the surrounding query and splices the same builder in.
   To enumerate its callers, grep — do not trust a count written here:
   `grep -rn 'reported_sql(' src/`. Both rule entry points take a
   `spec: &DatasetSpec` for it, because the source table string is
   `"candidates"` on the incremental path and the spec cannot be recovered from it.
2. **`EXISTS`, never a `LEFT JOIN`.** The table has no UNIQUE constraint, and two
   reports on one object are ordinary rather than an error. Guard:
   `reports::tests::two_reports_on_one_object_still_suppress_exactly_one_row`.
3. **Precedence is OSM-covered → suppressed → reported → unmatched**, and
   `suppressed_buildings_sql` was deliberately left alone: a row that is both
   former-covered and reported counts as *suppressed*, which keeps that count's
   definition byte-identical and the four categories disjoint.
   `reported_buildings_sql` follows the **CTE-first** shape for exactly the
   reason documented above — it runs once over the national extent.
4. **A reported object stays in `cell_totals`.** The denominator is "objects that
   could be imported here", not "objects currently offered".
5. **Expiry reuses the diff's own notion of change, and that is the whole
   design.** `DatasetSpec::content_signature_sql` is built from the same
   `compared_columns` + `compare_geometry` as `changed_predicate_sql`, so "has
   this record changed" has one answer for both, and each source's measured
   column choices are inherited for free (PRG ignores its version churn, so a
   gmina bulk-republish expires nothing; BDOT10k includes `WERSJA` but not
   geometry, so a bump does expire — precisely the "new version ⇒ importable
   again" behaviour). `dataset::tests::signature_changes_exactly_when_the_diff_says_modified`
   pins them together. It hashes the *curated* compared set for
   `O(active reports)` rows, never `O(source table)` — do not widen it.
6. **`reconcile_source` has four call sites and the `import` one is not
   redundant**: `update::dataset::refresh` (inside the apply transaction), every
   `import` arm (a re-import rebuilds the table with *no diff at all*), `reports
   reconcile`, and `server::jobs::reports_reconcile` (off by default). It
   enqueues dirty cells **before** the status `UPDATE`s — the same
   read-before-write ordering `update::changeset::insert_change_areas` needs.
7. **BDOT10k's key forced a serving-table change.** `bdot10k_unmatched` carried
   only `LOKALNYID`, which is not guaranteed unique, so a client could not name a
   complete identity; `PRZESTRZENNAZW` is now carried through
   `compare::columns::classification_columns`, the schema and `BUILDINGS_MVT_SQL`.
   The frontend reads it as key material only — `POPUP_HIDDEN_ATTRIBUTES` in
   `web/app.js` keeps it out of the displayed attribute list.
8. **A vetoed object is still visible — on the `*_all` layers, and only there.**
   The veto removes it from `<source>_unmatched`, so it leaves the
   `buildings`/`addresses` tile layers entirely; the "wszystkie" layers read the
   raw government tables and still draw it, where it was indistinguishable from a
   record OSM already has. `ALL_ADDRESSES_MVT_SQL`/`ALL_BUILDINGS_MVT_SQL`
   therefore carry a `reported` attribute built from the same `reported_sql` the
   compare paths negate, and `web/app.js`'s `featureStatus` turns it into a
   "Zgłoszony" chip. Three traps: it is `CASE WHEN … THEN TRUE END` so `ST_AsMVT`
   drops it on unreported features — but the *key* still enters the layer
   dictionary, so a test grepping tile bytes for `"reported"` passes with nothing
   reported (`all_buildings_sql`/`all_addresses_sql` take a `projection` seam so
   tests read the flag per row); the two source scans had to move into
   `MATERIALIZED` CTEs, since a correlated `EXISTS` alongside the spatial filter
   lets the optimizer re-plan the RTREE scan into a SEQ_SCAN; and this needs **no
   migration at all**, being computed at read time.

Two things `object_reports` is alone in. It is **the only table that cannot be
reconstructed from an external source** — everything else is an `import full`
away — so `reports export`/`import` (JSONL) is part of the feature; `import`
reallocates ids from the current maximum, so a round trip is faithful in content
but not in id. And it is the only endpoint where an **anonymous client writes
rows that change what every other user sees**, with — by explicit decision — no
rate limit and **nothing identifying the submitter stored: no address, no hash of
one, no `User-Agent`**. So `server::mod`'s `axum::serve(listener, app)` must stay
as it is (no `into_make_service_with_connect_info`, no `X-Forwarded-For`), and
cleanup after an abusive burst is time-scoped rather than actor-scoped:
`reports revoke --since <ts> [--source S]`, with `reports.enabled = false` as the
immediate stop.

**Gotcha — BDOT10k buildings are pre-filtered by `KATEGORIAISTNIENIA` inside the
shared match rule, not at import.** `rule::unmatched_buildings_sql` takes an
`extra_filter`; both bdot10k paths pass `rule::BDOT10K_EKSPLOATOWANY_FILTER`, so
a row whose category is `w budowie`, `nieczynny` or `zniszczony` counts as
neither matched *nor* unmatched — excluded from comparison entirely. EGIB has no
equivalent column and passes `None`. The raw `bdot10k_buildings` table is
untouched: this is a compare-time filter, unlike invalid-geometry filtering,
which deletes rows at import.

### Precomputed unmatched serving

`compare` writes unmatched government objects into `bdot10k_unmatched` /
`egib_unmatched` / `prg_unmatched`, and `/tiles` + `/package` read those directly
instead of comparing live. Between full runs, each producer enqueues the z14
cells it touched into `match_dirty_cells`, and the `match_refresh` job drains
that queue by recomputing just those cells.

**Gotcha — serving tables store rows, not id references.** They copy the columns
needed to render a feature instead of pointing back at the source by id or
rowid. BDOT10k's identity is the composite `(PRZESTRZENNAZW, LOKALNYID)`, and
DuckDB rowids aren't stable across the DELETE+INSERT every recompute does — so id
references would go stale silently. (Measured on the live national table:
`LOKALNYID` alone happens to be unique across all 16,351,813 rows. Nothing in the
schema or the export *guarantees* that, `key_columns` declares the composite, and
the raw pre-dedup export does carry duplicate composite keys.) Recompute is always
DELETE-then-INSERT for the affected cell, never an in-place UPDATE.

**Gotcha — bdot10k/egib's representative point is a stored column, not
computed.** Both tables carry a `centroid GEOMETRY`, populated by their
`load_into` and RTREE-indexed like `geom`. Every consumer reads this column
instead of computing `ST_Centroid(geom)` inline — **an RTREE index cannot be used
through a function wrapped around the indexed column**, which was the root cause
of the full-table-scan bottleneck (`docs/per_cell_recompute_full_scan.md`; fix
measured in `docs/centroid_index_measured.md`, ~10–100× per z14 cell). The column
is deliberately *not* in `compared_columns`, so it can never affect the diff — at
the cost that it never self-heals. Scope is bdot10k/egib only: PRG's `geom`
already is its representative point, and the `*_unmatched` and `osm_buildings`
tables still compute `ST_Centroid` inline.

**Gotcha — `now()` is transaction-start-scoped.** DuckDB evaluates it at BEGIN,
not at statement time. The government refresh enqueues its dirty cells *inside*
the apply transaction, so every cell a 5-minute refresh touched is stamped with
that transaction's start time, and `/status`'s `oldest_enqueued_at` reads ~5
minutes worse than reality right afterwards. Cosmetic — the drain's cutoff is
snapshot-based — but don't "fix" the metric by reaching for `now()` in the drain.

**Gotcha — the drain's cutoff is load-bearing.** `drain_batch` takes one
`batch_start` and uses it for *both* the read (`enqueued_at <= batch_start`) and
the paired queue-delete. Both sides must use that same stored value, never
`now()`: a cell re-dirtied after `batch_start` must survive the delete (its edit
wasn't seen by this tick) and be picked up by the next one. Using `now()` on
either side — or two different timestamps — either strands a cell dirty forever
or deletes a queue row for a change the recompute never read.

**Gotcha — dirty-queue source strings must match everywhere.**
`match_dirty_cells.source` is a plain string (`"bdot10k"` / `"egib"` / `"prg"`),
not an enum, and every producer must spell it identically. To enumerate the
producers, grep rather than trusting a list here:
`grep -rn 'INSERT INTO match_dirty_cells' src/`. A mismatched string silently
orphans that source's cells — enqueued but never drained.

**Gotcha — the OSM producer's enqueue reach is exact, derived from the match
rule's OSM read envelope.** `update::dirty_cells::note_existing` takes the cell
range of the edited row's bbox, widened by `layer_buffer_deg` — **`0.0` for
`Layer::Buildings`, `rule::OSM_MATCH_BUFFER_DEG` for `Layer::Addresses`**. That
asymmetry is not tuning: the building rule tests OSM against the cell's
*unbuffered* envelope, the address rule reads `osm_addresses` from the buffered
one. **If either rule's OSM read gains or loses a buffer, `layer_buffer_deg` must
move with it** — that function is the one home for the coupling, and it imports
the constant rather than restating the number. The value is coupled to the
*widest* distance any branch uses (`NAME_MATCH_DISTANCE_METERS`), not the
narrowest; `rule::tests::osm_match_buffer_covers_the_widest_match_distance`
computes that requirement rather than trusting prose, because the failure mode is
silent. Four further things:

1. A fixed 3x3 neighbourhood measured **5.6× amplification** on the live queue
   for a margin 98.08% of real building footprints never needed.
2. **Y is inverted** — higher latitude means a *smaller* `cell_y` — so `ST_YMax`
   maps to the min index; getting this backwards yields an empty range and
   silently enqueues nothing.
3. None of the standing safety nets protect this: the equivalence and concurrency
   tests seed the queue via `reconcile::enqueue_all`, which never goes through
   `DirtyCells` at all. The tests that do are `dirty_cells`'s straddling/corner
   cases and `update::osm::tests::osc_xml_straddling_cell_boundary_updates_the_neighbouring_cells_serving_table`.
4. `MAX_ENQUEUE_CELLS_PER_ROW` (1024) skips the enqueue for a row demanding more
   cells, with a warning. It guards an input class government exports don't have:
   a node dragged to (0, 0) makes a Polish building way span **2,659,592** z14
   cells from a single edit. The widest real `osm_buildings` row spans 0.7306
   cells, leaving 256× headroom. `queue reconcile` is the backstop, so skipping
   rather than clamping degrades to "stale until the next sweep".

Do not confuse this with the two unrelated "3x3" mechanisms: `compare::addresses`'
grid-key neighbourhood (a different 0.005° grid) and
`serving_version::z14_tile_version`'s read-time ring (exact by the
`filter_oversized_geometry` invariant — **must not be narrowed**).

### Mappings

**Gotcha — the street-name mapping is a match input, and that is why its loader
enqueues dirty cells.** The resolution chain has **one home** —
`mappings::street_names::resolved_street_join_sql` + `resolved_street_expr_sql` —
and four callers: `server::package::unmatched_addresses`, `server::tiles`'s
`ADDRESSES_MVT_SQL`, and both compare paths. Lookup is `lower(trim(...))` on both
sides; priority is settlement row → global row (NULL `teryt_simc_code`) → raw
name, so an empty table serves PRG names verbatim instead of failing.
`ALL_ADDRESSES_MVT_SQL` deliberately does not apply it — that layer shows every
PRG address including matched ones, which is never tag-preview material.

1. **A mapping edit changes which addresses are unmatched**, so
   `validate_and_swap` enqueues prg dirty cells, and an offline
   `import street-mappings` leaves queue work behind for `match_refresh` (or an
   explicit `queue drain`).
2. **The delta is a symmetric difference over the full
   `(teryt_simc_code, lower(prg_street_name), osm_street_name)` triple**, computed
   *before* the swap's `DELETE` — the live table's pre-swap contents are half the
   difference and gone a statement later. Each `EXCEPT` must be parenthesized
   (`EXCEPT` and `UNION` have equal precedence and are left-associative, so the
   bare form silently means something else), and `EXCEPT`'s NULL-as-equal
   semantics are what keep unchanged *global* rows out of the delta — a
   `NOT EXISTS ... AND teryt_simc_code = ...` rewrite would put every global row
   in the delta on every reload. Only the *name* is projected out, which is
   deliberately over-broad: a settlement-scoped edit dirties that name's
   addresses nationally, which is cheap and removes a class of reasoning.
   Measured: a no-op reload enqueues **0** cells, a full 3,272-row replacement
   enqueues **8,964 of 112,264**.
3. **The fan-out guarantee is borrowed, not local.** The two `LEFT JOIN`s can
   each match at most one row *only because* `validate_and_swap` rejects
   duplicate `(lower(prg_street_name), teryt_simc_code)` keys. The table carries
   no UNIQUE constraint, so a hand-INSERTed duplicate duplicates rows in
   `prg_unmatched`. Guard:
   `rule::tests::a_global_and_a_settlement_mapping_for_the_same_name_emit_one_row_per_address`.
4. **The epoch bump stays and is not superseded by the enqueue.** An undrained
   cell serves the old match decision with the new serve-time `addr:street`, and
   `addresses_all` plus z5–z13 are epoch-only regardless.
5. `make_seeded_state` in `server/package.rs` builds its tables from a local
   `SEED` constant rather than `create_schema` — a new serving table has to be
   added there too.

**Gotcha — building-type mapping is serving-time only, with adjacency computed
live, not stored.** `bdot10k_building_types`/`egib_building_types` are applied by
the same `LEFT JOIN LATERAL` shape in `server::package` and `server::tiles`
(reusing the shared `ADJACENCY_READ_BUFFER_DEG` / `*_ADJACENCY_KEY` constants
rather than re-typing them). **Unlike** the street-name mapping, none of this can
change which buildings are unmatched — `unmatched_buildings_sql` reads no
classification column — so a building-type edit needs no `compare`, reconcile or
drain. The classification *columns* themselves are carried at compare time via
`compare::columns::classification_columns`. Same-class adjacency is computed
inline against the live source tables (buffered read,
`ADJACENCY_READ_BUFFER_DEG = 0.0005°`) — a spatial bbox read, not an id lookup, so
it does not violate the "serving tables store rows, not id references" invariant.
See `docs/superpowers/specs/2026-08-03-building-type-mappings-design.md`. As with
street names, `make_seeded_state` (`server/package.rs`) and `make_full_state`
(`server/updates.rs`) build tables from local constants — a schema change has to
land in both, plus `db.rs`.

**Gotcha — `egib_buildings.rodzaj_kod` is precomputed at import.** The EGIB
`rodzaj` cascade (`mappings::egib::RODZAJ_KOD_CASE_SQL`, Appendix B of
`docs/building_type_mappings.md`) runs once per import via
`with_rodzaj_kod_select`. This is the one piece of the feature that isn't purely
serving-time: running the cascade at serve time measured +1.0s (regexp cascade
over ~96k candidate rows). Like `centroid`, it never self-heals.

**Gotcha — PRG's `ulica` is normalized at import.** Warsaw's PRG records embed
the street type in the name (`ulica Wał Miedzeszyński`) *while also* declaring
`<prgad:rodzaj>1</prgad:rodzaj>` (= ulica); `prg_convert` only ever prepends a
*missing* type word, so it passes the duplicate through.
`import::prg::ULICA_PREFIX_STRIP_SQL` strips it in `materialize_into` — the one
funnel both `import prg` and `update prg`'s staging load pass through. See
`docs/prg_ulica_prefix.md` (122,826 rows, all but 4 of them Warsaw). Three
load-bearing points: **(1)** Only `ulica`/`ul.` are stripped, never the other
cecha words Warsaw spells out — `Aleja`, `Aleje`, `Trakt`, `Osiedle` are part of
the correct name and OSM uses them verbatim, so generalizing the pattern would
corrupt 70,574 rows. **(2)** Unlike `centroid` and `rodzaj_kod`, this rewrites a
*stored* value that is itself compared, so an edit surfaces as an ordinary
modification and self-heals per record. **(3)** **It is a match input**: both
compare paths resolve `ulica` through `street_name_mappings` and compare the
result against OSM's `addr:street`, so an edit can flip an address between
matched and unmatched.

### Geometry: three inputs, three different rules

**Gotcha — invalid *government* geometry is dropped, not repaired.** A small
number of BDOT10k/EGIB rows have topologically invalid geometry, which crashes
`ST_AsMVTGeom` and takes down the whole tile
(`docs/invalid_geometry_tile_500s.md`). `dataset::filter_invalid_geometry` deletes
them immediately after `load_into` creates the table — the one place both
`import` and `update`'s staging load funnel through — so the compare paths never
see them.

Its sibling `dataset::filter_oversized_geometry` drops a different bad row: one
whose bbox spans at least one full z14 cell in either axis — too wide to be a real
building, and in practice a corrupted merge of two unrelated features that each
individually pass `ST_IsValid`. The threshold is expressed in **cell units**,
never degrees or metres, because the latitude threshold is not constant in
degrees (0.0135° at 52°N vs 0.0126° at 55°N). Measured over the full tables: 0
BDOT10k rows dropped (the longest genuine building measures 0.696 cells) and 85
EGIB rows. The motivating record is a 2-part MULTIPOLYGON whose two parts are real
buildings ~44 km apart. **Build it from `tile_math::cell_x_frac_sql`/`cell_y_frac_sql`
(the *unfloored* fractional cell coordinate), never the floored pair** — flooring
first would compare which cell each edge lands in rather than how far apart they
are, deleting a legitimate ~10 m shed sitting on a cell boundary. The threshold
also isn't insurance: because a surviving row's bbox is strictly narrower than one
cell in both axes, its reach from its own centroid's cell is `<= 1` **by
construction** — which is what makes the 3x3 serving-version ring exact rather
than approximate.

Both filters are row *filters* — they change which rows exist, never the content
of a surviving row. The one real ordering constraint: both must run **before**
`deduplicate_by_key`, so a duplicate pair whose newest member has bad geometry
falls back to the older valid member instead of collapsing to a row a filter then
deletes, losing the object entirely. `LoadStats::merge_oversized` folds both
counts into the one `LoadStats` each loader returns, self-reported to
`job_run_log` under `import:<source>` / `update:<source>` and read back by
`/status`. Note the reporting asymmetry: PRG's `update_prg` shares the same
`refresh()` that self-reports, so `update:prg` appears (with no skip clause);
`import:prg` does not, since PRG's import path never goes through `refresh()`;
`import:osm` self-reports directly, and its message carries a *repaired*-geometry
clause where the others report rows *skipped*. A refresh whose ETag is unchanged
returns early via `record_noop_refresh` before `dataset::refresh` runs, so
`job_run_log["update:<source>"].ran_at` can be days older than the corresponding
`jobs[].last_finished_at` without indicating anything is wrong.

**Gotcha — *request* geometry is repaired, and an unrepairable one is a 400.**
`/package`'s polygon body is arbitrary client input: `parse_polygon_body` checks
JSON shape, geometry type, coordinate ranges and a non-degenerate envelope, but
never topological validity, so a self-intersecting "bowtie" reaches SQL intact.
Two mechanisms handle that. First, every SQL site consuming the request geometry
wraps it as `ST_MakeValid(ST_GeomFromGeoJSON(?))` — including the
`package_exports` INSERT in `log_export`, so the logged area is always the
geometry actually queried. Second, `check_request_geometry` decides whether that
repair is *meaningful*: already valid → proceed; invalid but repairing to a
non-degenerate Polygon/MultiPolygon (a bowtie correctly becomes a MultiPolygon —
that is the intended repair, not something to normalise away) → proceed; empty,
zero-area or non-polygonal → 400, because a near-collinear freehand scribble
repairs to a LineString and "everything intersecting a line" is not the package
anyone asked for. The check runs only when `RequestArea::is_user_supplied` —
`parse_bbox` cannot produce invalid geometry, so `GET /package` must not pay for a
pool acquisition and a GEOS round trip; don't "simplify" that flag away.

**Gotcha — invalid *OSM* geometry is repaired, never deleted, and that direction
is the whole point.** OSM enforces no validity and both OSM paths build polygons
with a bare `ST_MakePolygon`, so a self-intersecting building way lands intact —
and the overlap fraction calls `ST_Intersection`, whose GEOS overlay throws where
the `ST_Intersects` above it tolerates the same input happily. One such way rolls
back an entire national `compare full` and, in the server, makes one z14 cell fail
on every drain tick forever. Measured on the 2026-08 Poland extract: 3 invalid
rows in 17,986,820 — and only *one* actually throws, so the count measures
exposure, not breakage. Five points:

1. **Repair, not delete — the opposite of the government rule, deliberately.** A
   government row is a *candidate*, so dropping a corrupt one is a safe false
   negative; an OSM row is *evidence* that something is already mapped, so
   dropping it makes the government building it covered look unmatched and get
   proposed — a duplicate added to OSM.
   `osm::geometry::tests::repair_fixes_the_overlay_crash_on_the_real_failing_pair`
   asserts that counterfactual directly.
2. **The expression has one home**, `osm::geometry::repaired_geom_sql`, and it is
   `ST_CollectionExtract(ST_MakeValid(g), 3)` — the extraction is not dressing:
   MakeValid preserves every vertex, so a zero-area spike comes back as a
   LINESTRING inside a GEOMETRYCOLLECTION (2 of the 3 real rows do exactly that).
3. **The two paths apply it at different moments on purpose**: `import osm` runs
   `repair_invalid_geometry` as a post-pass once per polygon table (covering all
   insert passes, including any added later) while `update osm` wraps the
   expression inline at each per-object INSERT. Not a performance split — over
   2,000,000 real rows the `ST_IsValid` scan costs 0.344 s and unconditional
   wrapping 0.466 s — but a "how many sites can forget" one.
4. **An inline wrapper must be paired with `has_polygon_sql` in the same
   statement's WHERE.** A fully degenerate ring repairs to a linestring and
   extracts to `MULTIPOLYGON EMPTY`; an empty geometry makes `ST_XMin` read NULL,
   failing `note_existing`'s `r.get::<_, i32>` on the next edit. The import path
   needs no guard because its post-pass deletes those rows afterwards — the one
   case where OSM data *is* dropped, counted separately as `dropped_degenerate`.
5. An existing database keeps its invalid rows until `import osm` is re-run.

**Gotcha — `/package` membership is intersection, not centroid containment.**
Both building queries select with `ST_Intersects(b.geom, <request polygon>)`, so a
building the request area merely clips is exported. This is deliberate: the user
picks the area explicitly, so everything it touches is what they asked for, and a
building landing in two separately-drawn exports is theirs to resolve. The
tempting "optimization" back to a centroid test is wrong on both counts — it
changes behaviour, and the `*_unmatched` serving tables have no stored `centroid`
column to index anyway. `unmatched_bdot10k_buildings_includes_building_clipped_by_edge`
and its EGIB twin pin this. Note the `nb` adjacency CTE's buffer has slightly less
headroom under intersection semantics — a selected building can sit entirely at the
area's edge — which is still comfortable, but is the number to revisit if adjacency
counts ever look wrong at request boundaries.

### Storage and schema

**Gotcha — no migration path exists, anywhere.** Every table is
`CREATE TABLE IF NOT EXISTS`, and no `ALTER TABLE` migrates a live database. A
new carried column, a new derived column, or a changed loader projection needs
the table rebuilt and `compare` (or `queue reconcile` + drain) re-run; until then
the column reads NULL. The one `ALTER TABLE` in the codebase
(`dataset::drop_ordering_column`) runs against a table the *same load's own*
`CREATE TABLE AS SELECT` built moments earlier, never a pre-existing database.

**Gotcha — the government loaders store only the columns anything reads.** They
project an explicit column list, not `SELECT *`/`EXCLUDE`/`REPLACE` — a source
publishing a column nothing reads no longer means storing it forever
(`docs/superpowers/plans/2026-08-14-column-trimming.md`; dropped PRG columns and
their restore cost in `docs/prg_dropped_columns.md`). Two loaders keep one column
that exists ONLY to feed `deduplicate_by_key`'s `ORDER BY` (EGIB's
`czas_pozyskania`, PRG's `wersja_id`): the dedup runs as a window function against
the table *after* `CREATE TABLE AS SELECT` built it, so the ordering column has to
be real at that point and cannot be projected away by the same statement. Both
call `dataset::drop_ordering_column` immediately after. Adding a consumer for a
dropped column means adding it back to the projection AND re-running
`import <source>`.

**Gotcha — a `LEFT JOIN` downstream of a filtered CTE silently defeats the RTREE
index unless the CTE is `MATERIALIZED`.** `WITH candidates AS (SELECT ... WHERE
ST_Intersects(...))` followed by `candidates LEFT JOIN other` looks like it
preserves the constant-argument `ST_Intersects` property, but DuckDB's join-order
optimizer can re-plan the filtered CTE into a plain `SEQ_SCAN` plus a separate
`FILTER` once a join consumes it — verified by `EXPLAIN`, not assumed.
`MATERIALIZED` forces the CTE to be computed independently first, restoring the
`RTREE_INDEX_SCAN`. `ADDRESSES_MVT_SQL`'s `candidates` and `BUILDINGS_MVT_SQL`'s
four `*_pkg`/`*_nb` CTEs are all `MATERIALIZED` for this reason — removing the
keyword reintroduces a full table scan while every functional test still passes.
Only `server::tiles::tests::mvt_bbox_filter_uses_the_rtree_index`, which asserts
on `EXPLAIN` text, catches it; note that test searches for the substring
`"RTREE_IN"` because wide plans truncate operator labels — don't "fix" it back to
the full name.

**Gotcha — the same index loss has a second, unrelated trigger: an expression
equality filter on the indexed column alongside the `ST_Intersects`.** Both
per-cell recomputes append a write-narrow guard
(`cell_x_sql(b.centroid) = X AND cell_y_sql(b.centroid) = Y`) to the shared rule.
That guard is an expression filter on the *same column* the RTREE indexes, and it
alone flips `RTREE_INDEX_SCAN` to `Sequential Scan`. Isolated clause by clause on
real data, only the guard loses the index — so cost was **independent of cell
contents** (a rural cell measured 1.089 s, a dense Warsaw cell 1.091 s), paid
twice per drained cell. Both now wrap the source scan in a `candidates` CTE via a
`build_sql` seam. Four things:

1. Unlike the tiles.rs case, **`MATERIALIZED` is not the active ingredient here —
   the CTE is.** Measured on the real 16.35M-row table: flat = 2 RTREE + 1
   `Sequential Scan` @ 0.974 s, bare `WITH` = 3 RTREE @ 0.098 s, `MATERIALIZED` =
   3 RTREE @ 0.099 s. It is kept as insurance against a future re-plan — don't
   infer from its presence that a test pins it.
2. The envelope and `extra_filter` are applied **twice** (once building
   `candidates`, once inside the predicate) deliberately: both are idempotent,
   and trimming the second copy would mean giving `rule.rs` a "skip the redundant
   filter" mode — two predicate texts, breaking "the match rule has one home".
3. The `build_sql` seams exist so the `EXPLAIN` regression tests assert on the
   *real* generated SQL — `rule.rs`'s own index test cannot catch this, because it
   asserts before the guard is appended.
4. **Do not apply this to the full compare.** The full paths have a structurally
   identical guard over a 0.5° grid, and the same wrap measured **worse**
   (0.955 s → 1.097 s). A 0.5° cell is ~1/264 of the table — not selective enough
   for an RTREE walk to beat a sequential scan — whereas a z14 cell is
   ~1/340,000. Both full paths *want* their sequential scan.

**Gotcha — the RocksDB store's byte layout is versioned, because none of it is
self-describing.** `kvstore::KV_FORMAT_VERSION` exists for one reason: an old
store read by a new binary decodes to *plausible* garbage rather than failing. An
8-byte `i32` coordinate pair read out of a 16-byte `f64` value yields real-looking
numbers in the wrong place, so every building silently lands in the Gulf of Guinea
— no error, no warning, and `/tiles` renders an empty Poland. There is no in-place
migration and none is wanted; the version's entire job is to make the mismatch
loud. **Bump it whenever any key or value layout changes.** Note `clear` drops
*every* CF including `meta`, so it must re-stamp
(`clear_restamps_the_format_version` pins that). The store is backed by the
`rust-rocksdb` fork (RocksDB 11.8.1) rather than the upstream crate — a *crate*
swap, not a layout change. Three encoding decisions, each with its own reason:

1. **Node values are two `i32` decimicrodegrees, not two `f64`.** OSM coordinates
   live on an exact 1e-7 degree grid and `180 * 1e7` fits in `i32`, so this is
   lossless and halves the largest column family. The cost: there is no memcpy
   shortcut from stored bytes into a WKB geometry buffer —
   `multi_get_nodes_wkb_coords` widens through `encoding::push_wkb_coords`, and it
   is named that way so nobody re-derives the opposite. **Convert back with
   `/ 1e7`, never `* 1e-7`**: `1e7` is exactly representable so the division
   rounds once, correctly. In the other direction `f64_to_decimicro` **rounds
   rather than truncates**, because the `.osc` path parses decimal text whose
   nearest `f64` can land a hair below the true value.
2. **Keys are big-endian.** RocksDB sorts lexicographically and delta-encodes
   within a block, and a block is the unit of I/O; little-endian sorts by the
   *least* significant byte, scattering numerically-adjacent ids across the
   keyspace. A building's nodes are usually consecutive ids, so big-endian
   co-locates them in one block. Negative ids would sort above positives as
   unsigned bytes, which never happens in a PBF or a replication diff.
3. **There are two id-list encodings and they are not interchangeable.** Way node
   refs use `encode_delta_id_list` (delta + zigzag varint): near-consecutive ids
   cost one byte instead of eight. It is order-preserving and **must never be
   sorted** — a way's ref order *is* its polygon vertex order. The reverse-index
   CFs keep the fixed-width `encode_fixed_id_list` because they carry a merge
   operator whose *partial* merge concatenates bare 8-byte operands without
   decoding them, which only works on a fixed-width format.

**Gotcha — `import osm` streams all three key spaces in one pass, and that is
safe for a specific reason.** All three are **pure writes** — none reads back out
of RocksDB — so they have no ordering dependency and this does *not* rely on the
PBF being sorted by element type. The passes that do read run after it returns.
Four things:

1. **`BlobReader`, not `IndexedReader`** — `create_index` reads only blob
   *headers*, so it knows offsets but not contents, and the per-blob id ranges
   that would let it skip anything are filled in lazily only once a blob has been
   decompressed. Reading every blob exactly once beats any amount of skipping.
2. **`way.refs()`, never `way.raw_refs()`** — the latter returns *delta-coded*
   values straight out of the protobuf, so using it silently stores garbage ids.
3. **The sequential blob loop is a measured decision, not an oversight.** It is
   genuinely *safe* to parallelize (every write is a blind put or a commutative
   merge), and an earlier version did with `par_bridge`. Removed after
   measurement: 12 cores bought **41s on a 5m 00s pass**, because the pass is
   bound by RocksDB write throughput and the sequential blob read, not decode CPU.
   Nearly all of the win over three scans comes from decompressing once.
   **The same measurement covers the decompression backend, where the trap is
   subtler**: forcing `zlib-ng` measured 4m 46s vs 5m 00s, because the default
   build is *already* on a fast zlib — `zip` (via `prg_convert`) enables
   `flate2/zlib-rs` for the whole graph, so `osmpbf`'s nominal `rust-zlib` default
   never selects miniz_oxide here. Anyone re-benchmarking a flate2 feature must
   check the *resolved* feature set (`cargo tree -f "{p} [{f}]"`).
4. The shutdown flag is polled once per blob (~8k elements), frequent enough to
   stay responsive without hammering an atomic millions of times.

**Gotcha — `import osm`'s replication stamp is written last, not first.**
`import::osm::import` reads the PBF header's replication info immediately so a
malformed header fails fast, but does not *write* it until every data-loading step
has succeeded. An interrupted import must never leave the stamp visible: it would
make a half-imported database look complete to a later `update osm` or `run`.
Pinned by `failed_import_does_not_stamp_replication_metadata`.

### Serving, caching and the HTTP layer

**Gotcha — the serving version has one home.** `serving_version::z14_tile_version`
folds a global `serving_epoch` counter together with the per-cell `*_unmatched`
state visible from a tile. Bump rule: **bump wherever a table `/tiles` reads and
no per-cell version tracks is rewritten.**

1. **Bump sites**: the `import` dispatch's bdot10k/egib/prg/full arms;
   `update::dataset::refresh`'s apply transaction; both mapping loaders, each
   inside the loader itself in the same swap transaction as its `DELETE`+`INSERT`
   rather than at either call site; and `compare::run`'s `Full` target (pure
   insurance so a cached `ETag` can't survive an offline rebuild).
2. **Must-not-bump — this list matters more than the one above.** Bumping here
   would be silently "correct" while defeating the whole point of the `ETag`:
   `update::record_noop_refresh` (an ETag-unchanged poll rewrote nothing, so
   bumping would flush the world's cached tiles daily, three times over, for
   nothing); the mapping jobs' own ETag-match early returns, which `return`
   strictly *before* calling into the loader — which is also why putting the bump
   inside the loader gets this right for free; `queue reconcile`, which only
   enqueues for a drain whose per-cell recompute already moves `computed_at`;
   `import osm`/`update osm`, since `/tiles` reads no `osm_*` table directly —
   bumping would flush every tile in the country on every minutely update; and
   single-source `compare buildings`/`compare addresses`, whose `*_unmatched`
   rewrite already moves its own per-cell `computed_at`. `POST /report` is on this
   list too — the insert enqueues the object's cell, so per-cell `computed_at`
   covers it, and bumping would flush every tile once per report.
3. **The `*_all` legend layers and the adjacency `nb` CTEs are epoch-only, and
   that's sound rather than a gap.** They read the raw source tables, never the
   serving tables, so per-cell `computed_at` never moves for them; the `nb` CTEs
   specifically read a *neighbouring* cell's raw rows, which no cell-local version
   could cover even in principle. The only writers of those raw tables are
   `import` and `refresh` — both already bump sites.
4. **The version is a per-table `(count, max(computed_at))` pair per source, never
   one merged `max`.** With a single merged max: a cell holding 3 bdot10k rows at
   T0 and 5 prg rows at T1 reads version `...T1...`; the drain then recomputes
   bdot10k down to zero rows — a real, common mutation — and the merged max is
   *still* T1, because prg's never moved. Same version, three buildings gone. The
   per-table pair is faithful by construction: a recompute either inserts ≥1 row
   (its max moves) or 0 (its count drops). Pinned by
   `version_changes_when_a_cell_empties_out`.
5. **The 3x3-cell ring is an invariant enforced by
   `dataset::filter_oversized_geometry`, not a guess.** Rows are *selected* for a
   tile by `ST_Intersects` but *tagged* with the cell of their representative
   point, so a tile can render rows owned by a neighbour. The oversize filter makes
   a surviving building's reach `<= 1` by construction, so radius 1 is exactly
   enough.
6. **`serving_version::TILE_FORMAT_VERSION` must be bumped by hand whenever the
   MVT SQL changes shape** — a new attribute, a renamed one, a different
   simplification. Nothing about *rows* changed, so neither the epoch nor any
   `computed_at` moves on its own; without this, a binary that changes what a tile
   *contains* keeps producing the same version string, every cached `ETag` keeps
   matching, and every existing client serves the stale shape forever with no way
   to self-heal.
7. **`refresh`'s placement is deliberate.** The bump runs **inside** the apply
   transaction, so a rollback can't leave a bumped epoch describing a delta that
   never landed, and it is **unconditional** — it fires even on a zero-delta
   refresh, because the raw columns `/tiles` reads that sit outside
   `compared_columns` are recomputed for every staged row and can have moved.

**Gotcha — the HTTP cache layer has one home too.** `src/server/http_cache.rs`
builds every `Cache-Control` and `ETag` this server sends — nothing else should
format one by hand. The API default, `Cache-Control: no-store`, is applied by a
single **outermost** `SetResponseHeaderLayer::if_not_present` in `build_router`,
and it must stay the *last* call in that chain: `Router::layer` only wraps routes
that already exist when it is called, so applying it before `.fallback_service(...)`
would leave the static frontend and axum's own 404/405/rejection responses
unwrapped. `/package` (both verbs) is `no-store` for a **correctness** reason, not
freshness: a cached response never reaches `package::log_export`, so
`package_exports` would under-count and `/updates` would under-report. Tile 500s
are deliberately left header-less so the outer default stamps them — an errored
tile cached for even a minute turns a transient DB hiccup into an outage that
outlives it, which is also why a 500 never carries an `ETag`. Only z14 gets an
`ETag`: z5..=z13 are the aggregated/point tiers and `serving_version`'s coverage
is z14-cell-shaped, so falling back to the epoch alone would pin them stale until
the next bump — strictly worse than the plain TTL they already get. **A second
reason has since attached itself to that:** the z5–z11 tile's `ts_*` attributes
are bounded by `now() - [changes] max_age_days`, so its *content* moves with
wall-clock time while nothing in `serving_version` does. Giving that tier an
`ETag` — or a `TileCache` entry — would freeze the recently-updated overlay at
whatever the first request happened to compute. The
static-asset middleware classifies purely on **response status**, not a filename
list: a 404 is left header-less and inherits `no-store`, while anything `ServeDir`
actually served — including its own `304` — gets at least `no-cache`, with
`/fonts/` and `/vendor/` upgraded to a long `max-age`.

`TileCache::new(0)` is a genuine working no-op, not a special-cased `Option`:
`max_bytes == 0` makes every `get` miss and every `insert` a no-op before either
touches the lock, so `tile_cache_max_bytes = 0` reverts the feature via config
alone. The cache is keyed on `(z, x, y)` with the version stored **inside** the
entry rather than folded into the key, so a recompute makes the next `get` a miss
and the following `insert` *replaces* it in place — there is never a dead,
superseded-version entry counted against the budget waiting for eviction.
`tiles::z14_tile_response` is the single response-shaping path for every
successful z14 response, fresh *or* cached, precisely so a cache hit cannot differ
from a miss on status, headers, or the **empty-tile 204** — a z14 tile over open
country renders no features at all, the common case across most of Poland.
`z14_response_shaping_is_identical_for_empty_and_cached_empty_tiles` records a
real bug caught in review: an earlier draft returned a bare 200 from the cached
path, making an open-country tile flip between 204 and 200 depending purely on
cache residency.

**Gotcha — cancellation has two signals and one message, and `Ok` vs `Err` is
per-path, never a style choice.** Ctrl+C/SIGTERM sets a process-global flag
(`shutdown::is_requested`, installed by `ctrlc` with `termination`, so
SIGINT/SIGTERM/SIGHUP all land there). Separately, each background job carries a
per-run cancel flag (`JobContext::is_cancelled`) set by a supervisor timeout or
`Scheduler::shutdown`. Long-running work polls whichever can reach it; the ones
reachable from both take the injected `is_cancelled: &dyn Fn() -> bool`. The CLI
passes `&|| false` there and still gets Ctrl+C, because those functions poll the
global flag as well.

1. **`shutdown::check_requested()` is the one home for "stop, the user asked".**
   It bails with `shutdown::SHUTDOWN_BAIL_MESSAGE`, and every purely-global seam
   calls it rather than spelling out its own check. The message being identical is
   the point: an operator reading `job_run_log` needs an interrupted run to look
   the same whichever seam noticed first. Two seams deliberately don't call it —
   `compare_buildings_with_cancel` and `update::dataset::check_cancelled` must
   consult their *injected* closure, so they use the constant directly.
2. **`Ok(())` vs `Err` is decided by whether the path has a durable checkpoint,
   and the two conventions must not be "made consistent".** `update::osm::update`
   returns **`Ok(())`**: it commits one batch at a time and resumes from the
   `metadata` stamp, so stopping early is real partial progress. Everything else
   returns **`Err`**, having no such checkpoint. `compare_buildings`'s grid loop is
   inside a clear-then-repopulate transaction, so an early `Ok` would COMMIT a
   `dest` missing every cell after the interrupt — the silent-outage failure mode
   that transaction exists to prevent. `refresh` lands nothing until its apply
   commits, so an early `Ok` would write a **`Success`** row for a refresh that
   did nothing.
3. **`refresh` checks at exactly three points, all outside the apply
   transaction.** Do not add a fourth inside it: that transaction is the atomic
   unit two tests depend on, and a check inside buys nothing over checking before
   `BEGIN` while adding a rollback path to reason about.
4. **A flag polled between statements cannot touch a statement already running**,
   which is most of the wall time. `shutdown::register_interrupt_handle` closes
   that gap by failing the in-flight DuckDB statement. **Scope is the CLI's single
   connection only** — `try_clone` opens a brand-new connection with its own
   handle, so the server relies on graceful shutdown and per-job cancel flags
   instead. `interrupting_a_running_statement_does_not_poison_the_connection` pins
   both that the interrupt lands and that the connection still works afterwards,
   which is what makes `in_transaction`'s `ROLLBACK`-after-error work on the way
   out.

**Gotcha — the prefetch thread's dedup has a floor, not just a ceiling.**
`update::osm`'s prefetcher and the apply loop share a download directory, so
whichever downloads a sequence first, the other finds it on disk and skips the
network call — but the apply loop *deletes* each `.osc.gz` right after
decompressing it, and `apply_batch` fetches a whole batch before recursing, so
`last_applied` can jump by an entire batch in one stride. If that commit lands
inside a single 50 ms poll interval, the prefetcher wakes to a window that jumped
past several `next` values it was sitting behind and re-downloads every one for
real, because their files are gone. `spawn_prefetcher` skips `next` straight to
`last_applied + 1` whenever `next <= last_applied`. Reproduced empirically under
sustained CPU load; without it the same waste hits the real OSM replication server
during a fast catch-up burst.

**Gotcha — two ordering rules in the server's shutdown path, each fixing a silent
hang.** (1) `run` binds `axum::serve(...).await`'s result instead of `?`-ing it,
always calls `scheduler.shutdown(...)`, and only then propagates: a serve error is
exactly when the scheduler most needs telling to stop. (2) `supervise` constructs
its `Notify` waiter and calls `notified.as_mut().enable()` **before** checking
`stop`, not inside the `select!`. `notify_waiters()` only wakes waiters already
registered — it is not a latch — so a supervisor that read `stop == false` a moment
before `shutdown()` set it would register too late and sleep until its next tick,
up to `interval_seconds` (86400 for the dataset jobs), turning a 30s grace into a
de facto 24-hour one. Moving the pair back inside the `select!` silently
reintroduces that race, and **no test catches it** (a deterministic one needs
tokio's `test-util` feature, which this repo does not enable).

## Data Sources

- **OSM:** Poland PBF extract from OSM France, minutely replication feed
- **PRG:** Government address registry (ZIP, parsed via
  [prg_convert](https://github.com/ttomasz/prg_convert/))
- **BDOT10k / EGIB:** Government building registries (GeoParquet)

## Configuration

`--config <path>` points at a TOML file (see `example_config.toml`). Without it,
defaults are used. `RUST_LOG` overrides the config's `log_level`.

**Gotcha:** `duckdb_init_commands` replaces the entire default list — if you
override it, include everything you need (spatial extension, memory limits, etc.).

## Testing

- **Unit tests:** inline `#[cfg(test)]` modules. **Integration tests:** `tests/`,
  using `assert_cmd` with `tempfile` for isolated DBs.
- Run one: `cargo test --test cli_import_osm`
- **Fixtures:** regenerate with `fixtures/scripts/prepare_fixtures.sh`
- **Gotcha — hand-written geometry fixtures need exact binary fractions.**
  Ordinary decimals aren't exactly representable in `f64`, so a ring written to be
  collinear (`21.0`/`21.005`/`21.01`) rounds to a point ~1e-18 off the line, and
  GEOS reports the result as a *valid* sliver polygon rather than the degenerate
  geometry the test meant to exercise — the test then passes for the wrong reason.
  Use eighths (`21.0`/`21.0625`/`21.125`) when a test needs genuine collinearity.
  Related: assert on the *exact* error string (via the named constant) rather than
  a substring, since several guards reject with messages sharing words like
  "degenerate", and a substring assertion can silently pin a different guard.
- **Gotcha — reproducing a CPU-load-dependent flaky test.** Run the whole test
  module (the compiled binary, filtered e.g. `update::osm::tests`), not an isolated
  `--exact` single test, under synthetic CPU load (background `while true; do :;
  done` loops). An isolated run rarely reproduces contention-dependent races: the
  shared `download_runtime`/`download_client` statics and real OS thread scheduling
  only get stressed when sibling tests genuinely compete for CPU in the same process.

## Web frontend (`web/`) & browser testing

`web/index.html` / `app.js` / `style.css` is a static MapLibre GL JS frontend
served from a config-set disk directory (not embedded in the binary). It reads
`/tiles/{z}/{x}/{y}` and `/status` from the running server.

To verify a frontend change in a real browser (required for anything touching
MapLibre style/paint — type-checking and unit tests don't catch rendering bugs):

1. Run the server: `cargo run -- --config example_config.toml run` — **redirect
   output to a file** (`> server.log 2>&1`), don't pipe through `tail -N` without
   `-f`. A non-follow `tail` buffers everything until the process exits, which
   looks identical to a hung server; poll the log with `grep` instead.
2. Drive a browser with `npx playwright cli <command>`. Needs no install — it
   reuses the already-cached `playwright-core`. The `mcp__playwright__*` MCP tool
   has failed in this environment before (missing system Chrome, no sudo). **`npx
   playwright cli open` bare (or `--browser=chrome`) fails the same way** — pass
   `--browser=chromium` explicitly; a plain Chromium build is already cached.
3. **After editing `app.js`, close and reopen the CLI browser session rather than
   reloading the page.** The browser caches the script over plain HTTP, so a
   same-session reload can silently re-run the stale pre-edit version.
4. Check console output before trusting a screenshot — a layer can fail to parse
   and simply not render, with no symptom except a console error.
5. **This environment has no outbound network**, so `tile.openstreetmap.org` 404s
   and the raster basemap renders blank. Vector-tile layers still draw normally — a
   blank basemap is expected, not a failed verification. Corollary: a color chosen
   to read well *against the basemap* can't be judged here, only against white.

**Gotcha — map interaction handlers must respect `appDrawState`.** The
area-drawing tool (rectangle / "Punkty" / freehand, feeding `POST /package`) is a
modal state machine — `"idle"` / `"drawing"` / `"selected"` — layered over map
handlers registered once at startup that know nothing about it. Any handler
reacting to click or hover needs an `if (appDrawState === "drawing") return;`
guard, or it fires *during* drawing: the feature-popup handlers on
`CLICKABLE_LAYERS` opened a popup on every vertex click landing on a building, and
the `mouseenter`/`mouseleave` pair fought the crosshair cursor. All four are
guarded now, and **nothing catches a missing guard automatically** — a popup is not
a console error and no test covers it. In the other direction, `teardownDrawMode`
is the single place undoing everything drawing mode changed (`dragPan`,
`doubleClickZoom`, the cursor, the mode's own listeners), unconditionally and
without needing to know which mode was active; every exit path goes through it,
which keeps "no path leaves the map stuck" true by construction.

**Gotcha — the `hidden` attribute loses to any class that sets `display`.**
`.source-toggle` and `.ratio-legend` set `display: flex`, which ties the UA
stylesheet's `[hidden] { display: none }` on specificity and wins because author
CSS beats the UA sheet — so toggling `hidden` from JS silently does nothing.
`style.css` carries an explicit `[hidden]` override per affected element; a new
class-styled element that JS shows/hides via the attribute needs its own.

**Gotcha — MapLibre paint properties don't resolve CSS `var(...)`.** They're
MapLibre's own expression language, evaluated against the style JSON at
construction time. Baking `var(--x)` into a paint property fails style validation
immediately — the layer never renders, and patching it later via
`setPaintProperty` in a `load` handler is too late, since construction already
threw. Resolve real values from `getComputedStyle(document.documentElement)`
*before* building the style object — `app.js` reads them into a block of
`const …Color` bindings (`buildingAccentColor`, `addressUnmatchedColor`, …) at
the top of its IIFE — so `style.css` stays the single source of truth for colors,
light and dark included.
