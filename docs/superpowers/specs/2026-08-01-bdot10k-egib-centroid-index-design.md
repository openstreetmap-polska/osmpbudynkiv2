# Persist and index a `centroid` column on bdot10k/egib

Written 2026-08-01. Fixes the bottleneck measured in
`docs/per_cell_recompute_full_scan.md` (item 7 of
`docs/followups_precomputed_unmatched_serving.md`): the per-cell recompute
that backs `match_refresh`, `compare reconcile`, and full `compare` all
full-scan `bdot10k_buildings` / `egib_buildings` (16.3M / 17.8M rows) on every
cell, because the match predicate wraps the indexed `geom` column in
`ST_Centroid()`, which an RTREE index cannot see through. Measured cost:
~0.9 s/cell, ~85 h for a full reconcile sweep.

## Scope

**In scope:** `bdot10k_buildings` and `egib_buildings` only (both
`GeomKind::Polygon`). Every place in the codebase that currently computes
`ST_Centroid(geom)` against these two tables — live or staging — switches to
reading a new, persisted, indexed `centroid` column instead.

**Out of scope, deliberately:**
- `prg_addresses` — `GeomKind::Point`, its `geom` already *is* the
  representative point. No new column, no call-site changes.
- `server/package.rs::unmatched_buildings`'s `ST_Centroid(b.geom)` — that
  query reads `bdot10k_unmatched` / `egib_unmatched` (the precomputed serving
  tables), a different table with no `centroid` column, already RTREE-indexed
  on `geom`, and operating on a small per-request result set. Not the
  measured bottleneck.
- `update/dirty_cells.rs::note_existing`'s `ST_Centroid(geom)` — operates on
  `osm_buildings` (OSM data), unrelated to the government tables.
- Any migration path for already-existing databases. Confirmed with the
  user: ship as "re-import bdot10k/egib to get the speedup," documented in
  CLAUDE.md. No `ALTER TABLE` / auto-backfill code.

## Schema & population

Add `centroid GEOMETRY` to `bdot10k_buildings` and `egib_buildings`,
populated by `import::bdot10k::load_into` / `import::egib::load_into` — the
one place both `import` and `update`'s staging load already funnel through
(same pattern `filter_invalid_geometry` uses).

Added as an **outer wrap around `hashed_select`'s output**, not inside it:

```sql
CREATE TABLE {target} AS
SELECT *, ST_Centroid(geom) AS centroid FROM ({hashed_select(inner)}) t
```

`hash(s)` inside `hashed_select` runs over the inner subquery's columns
before `centroid` is added, so no row's `_row_hash` changes and no
`ROW_HASH_VERSION` bump is needed. This is a load-bearing invariant, not an
incidental detail — get it wrong and every refresh silently rewrites the
whole table as "modified" forever.

Implementation: a new `DatasetSpec` method next to `representative_point_sql`,
e.g. `with_centroid_select(&self, hashed_select_sql: &str) -> String`,
matching on `geom_kind`: `Polygon` wraps as above, `Point` passes through
unchanged (PRG needs no column).

Both `import::bdot10k::load_into` and `import::egib::load_into` call it after
`hashed_select`. Since `refresh()`'s `load` closure calls these same
functions directly for the staging table (`src/update/mod.rs:47,72`), staging
tables gain the column automatically too — required for
`INSERT INTO {live} SELECT * FROM {staging} WHERE ...` in
`update/dataset.rs::refresh` to keep working (column count/order must match
between live and staging).

**Index:** a twin `CREATE INDEX ..._centroid_idx ON ... USING RTREE
(centroid)` next to each table's existing `..._geom_idx`, in `import()`
(`src/import/bdot10k.rs`, `src/import/egib.rs`). Same hard-fail-on-error
behavior as the existing geom index — this mirrors an existing import-time
index, not the warn-and-continue pattern `db.rs::create_serving_indexes` uses
for the serving tables. No index needed on staging tables (transient, never
queried spatially, dropped by `ScratchGuard` after every refresh).

## Call-site changes

| File | Change |
|---|---|
| `compare/rule.rs::unmatched_buildings_sql` | The shared predicate: `ST_Intersects(b.centroid, ...)` and `ST_Contains(osm.geom, b.centroid)` instead of `ST_Centroid(b.geom)`. This is the actual fix — both full `compare_buildings` and `recompute_cell_in_txn` call this function, so both get faster from one change (see CLAUDE.md's "match rule has one home"). |
| `compare/buildings.rs::compare_buildings` | `cell_x_sql`/`cell_y_sql` fed `b.centroid`; the grid boundary write-narrow guard becomes `ST_X(b.centroid)`/`ST_Y(b.centroid)`. |
| `compare/incremental.rs::recompute_cell_in_txn` | Same `cell_x_sql`/`cell_y_sql` swap for the `bdot10k`/`egib` branch's cell-tagging and write-narrow guard. |
| `compare/reconcile.rs::enqueue_all` | The bdot10k/egib tuples' point-expression literal changes from `"ST_Centroid(geom)"` to `"centroid"` (this query has no table alias, bare column reference). |
| `dataset.rs::DatasetSpec::representative_point_sql` | Signature changes from taking a geometry expression to taking a table **alias**: `Point => "{alias}.geom"`, `Polygon => "{alias}.centroid"`. |
| `update/changeset.rs::insert_change_areas`, `insert_dirty_cells` | Callers of `representative_point_sql` pass `"l"`/`"s"` instead of `"l.geom"`/`"s.geom"`. PRG's behavior is unchanged (still resolves to `alias.geom`). |

## Testing & verification

**Unit/integration tests:**
- `dataset.rs`: update `representative_point_sql` tests to the alias-based
  signature; add tests for `with_centroid_select` (asserts the wrap adds
  `centroid` outside the hash for `Polygon`, is a no-op for `Point`); extend
  the existing hash-invariance test to prove adding `centroid` via the wrap
  does **not** change `_row_hash` for a fixed input — the load-bearing
  invariant from the schema section above.
- `import/bdot10k.rs`, `import/egib.rs`: extend `load_into` tests to assert
  `centroid` is populated and equals `ST_Centroid(geom)` per row; extend the
  invalid-geometry test to confirm dropped rows leave no stray centroids.
- `compare/rule.rs`: existing `unmatched_buildings_sql` tests updated to seed
  `centroid` in fixtures; new test asserting the predicate plans as
  `RTREE_INDEX_SCAN` (mirrors `server::tiles::tests::mvt_bbox_filter_uses_the_rtree_index`)
  — the actual regression guard for the perf fix, since a future edit could
  otherwise silently reintroduce the sequential scan with no test failure.
- `compare::full_vs_incremental_equivalence` (`src/compare/mod.rs`, existing):
  must keep passing unchanged — proves this change doesn't alter *which* rows
  are unmatched, only how fast the query runs.
- `compare/buildings.rs`, `compare/incremental.rs`, `compare/reconcile.rs`,
  `update/changeset.rs`: audit each test's hand-built `CREATE TABLE`/`INSERT`
  fixtures and add `centroid` alongside `geom` wherever a bdot10k/egib-shaped
  table is created by hand.

**Real-data measurement:** there's an existing Poland-scale DB at
`./osmpbudynkiv2.duckdb` (14 GB, from the 2026-07-30 investigation these docs
reference). Re-run `import bdot10k` / `import egib` against that same file
using the dated snapshots in `example_data/`
(`BDOT10k/OT_BUBD_A_2026-08-01.parquet`, `EGiB/0_budynki_2026-08-01.parquet`)
— `import`'s `DROP TABLE IF EXISTS` + `CREATE TABLE AS` rebuilds just those
two tables in place; OSM, PRG, and the serving tables are untouched. Then,
against that one rebuilt DB:
- `EXPLAIN` the old predicate form (`ST_Intersects(ST_Centroid(geom), env)`)
  vs. the new form (`ST_Intersects(centroid, env)`) to confirm
  `SEQ_SCAN` → `RTREE_INDEX_SCAN`.
- Time both forms on a handful of real cells (reusing or resembling the ones
  `per_cell_recompute_full_scan.md` used) for a real, freshly-measured
  before/after per-cell number.
- Time a `compare full` run and/or a seeded drain batch for an end-to-end
  number.
- Write the results into a short follow-up doc in the same style as
  `docs/per_cell_recompute_full_scan.md`.
- The undated `old_OT_BUBD_A.parquet` / `OT_BUBD_A.parquet` /
  `PRG-punkty_adresowe.zip` files in `example_data/` are for exercising the
  `update` diff path (old snapshot → new snapshot) and are not needed for
  this measurement.

A full import into a fresh DB is ~40+ min per
`docs/followups_precomputed_unmatched_serving.md`; reimporting just these two
tables into the existing file should be closer to their individual import
times (bdot10k ~part of the original run, not separately measured — this
session will note the actual wall time). Run as a background command rather
than blocking the conversation.

**Docs:** update CLAUDE.md's "match rule has one home" and "invalid
government geometry is dropped" gotchas to mention the persisted `centroid`
column, and note that existing databases need `import bdot10k` / `import
egib` re-run to gain it (no automatic migration).

## Risks considered

- **RTREE churn under continuous per-cell rewrite.** Already measured for the
  analogous `geom` index and the serving-table `geom` index in
  `docs/followups_precomputed_unmatched_serving.md` (Findings C/D): no
  cumulative degradation after 15,000 cell recomputes, ~1% read-latency noise
  from having the index at all. The `centroid` index sits on the same tables
  under the same DELETE+INSERT-per-refresh write pattern as the existing
  `geom` index, so this is not new risk, just the same risk already accepted
  once.
- **Row-hash version bump.** Addressed directly by keeping `centroid` outside
  `hashed_select`'s projection (see Schema section). Verified by a dedicated
  test, not just argued.
- **Staging/live schema mismatch.** Since both `import` and the refresh's
  `load` closure call the same `load_into` functions, staging and live tables
  are always built by identical code and cannot drift in column layout.
