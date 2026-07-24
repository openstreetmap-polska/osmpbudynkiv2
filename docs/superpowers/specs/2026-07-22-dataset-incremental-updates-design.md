# Incremental Updates for PRG, BDOT10k and EGIB

## Summary

Implement `update prg`, `update bdot10k` and `update egib` (currently three `bail!("not yet implemented")` arms in `src/update/mod.rs`), plus matching background jobs for the `run` server.

These sources publish **full snapshots**, not change feeds, so "incremental" here means: download the new snapshot, diff it against what is already in the database, and apply only the difference. The two goals are:

1. **No service disruption.** Today's import does `DROP TABLE` → `CREATE TABLE AS SELECT` → `CREATE INDEX`. Running that against a live server makes `prg_addresses` / `bdot10k_buildings` / `egib_buildings` vanish and reappear underneath in-flight `/tiles` and `/package` requests.
2. **Know what changed.** Produce an aggregated per-area changeset, usable for tile-cache invalidation and for the "recently updated areas" idea in `docs/project_ideas.md`.

Explicitly **not** goals (considered and rejected during design): skipping the full comparison run, and reducing download bandwidth.

## Measured baseline

Taken from the two real BDOT10k snapshots in `example_data/BDOT10k/` (2026-03-15 and 2026-04-19, ~5 weeks apart), because the whole design depends on churn actually being small:

| Metric | Value |
|---|---|
| Old snapshot | 16,256,082 rows / 16,256,079 distinct `LOKALNYID` |
| New snapshot | 16,284,639 rows / 16,284,637 distinct `LOKALNYID` |
| Added | 37,715 |
| Removed | 9,157 |
| `WERSJA` changed | 304,061 |
| **Total churn** | **~2.1% over five weeks** |
| Distinct z14 tiles touched by those changes | **5,198** |
| Time to hash all 16.28M rows (incl. geometry blob) | **~2 s** |

Two facts worth carrying forward:

- **`LOKALNYID` is not unique** — 3 duplicate IDs in the old snapshot and 2 in the new. The diff must not assume one row per ID.
- **Changes are heavily clustered** — the 341,776 added-or-modified objects fall into only 5,198 z14 tiles, ~66 changed objects per touched tile. (That count is total churn minus the 9,157 removed, which by definition are absent from the new snapshot and so carry no new geometry.) The registry is maintained per administrative unit, so changes arrive in geographic batches. This is what makes an aggregated changeset small.

## Verified DuckDB behavior (bundled build, tested directly before finalizing)

Three findings that eliminated the two obvious designs:

- **A table with an index cannot be renamed.** `ALTER TABLE t__new RENAME TO t` fails with `Dependency Error: Cannot alter entry "t__new" because there are entries that depend on it` when an RTREE index exists on `t__new`. This rules out the "build a fully-indexed staging table, then swap it in by rename" approach. The workable variant — rename unindexed, then `CREATE INDEX` — leaves a window (~3 s for BDOT10k, per `docs/import_time.md`) in which every spatial query falls back to scanning 16M rows. That is exactly the disruption this design exists to avoid.

- **A view defeats the RTREE index.** Querying a table directly with a constant spatial predicate produces `RTREE_INDEX_SCAN`; the identical query through `CREATE VIEW v AS SELECT * FROM t` produces `SEQ_SCAN`. This rules out generation-column-plus-view indirection: the swap would be instant but every subsequent spatial query would lose its index permanently.

- **`DELETE` + `INSERT` keeps the RTREE correct.** After deleting 1,000 rows and inserting 100 into an indexed 200k-row table, an index-accelerated query and a forced sequential scan returned identical results (101 = 101), with the row count correctly reflecting the delta. The DuckDB docs confirm RTREE indexes "support inserts, updates and deletes to the base table."

The relevant caveat, from the spatial extension docs:

> If you find that the performance of querying the R-tree starts to deteriorate after a large number of updates or deletions, dropping and re-creating the index might produce a higher quality R-tree.

So delta-apply is correct but leaves the index progressively less well-balanced than a bulk-loaded (Sort-Tile-Recursive) one. A periodic rebuild is scheduled maintenance, not part of each refresh.

## Chosen approach: delta-apply on the live table

Each refresh runs five phases; only phase 4 touches the live table, and it is a single transaction.

```
download ──▶ stage ──▶ diff ──▶ apply (1 txn) ──▶ cleanup
            (unindexed  (hash    DELETE + INSERT    DROP staging
             CTAS)      join)    + changeset
```

Because the apply is one transaction, DuckDB's MVCC guarantees every reader sees either the complete old snapshot or the complete new one. The table is never dropped, never renamed, and its RTREE index stays live and correct throughout — so no reader-facing SQL changes at all.

### Phase 1: download

Reuses the existing `download_file` / `download_file_as` retry-and-backoff logic.

Before downloading, a **skip-if-unchanged** check: a `HEAD` request compares `ETag` / `Last-Modified` against `dataset_refreshes.source_etag` from the last successful refresh for that source. If unchanged, record a no-op refresh and return early. This is what makes a daily schedule affordable — a no-op refresh costs one HTTP round-trip rather than a 2GB download.

### Phase 2: stage

Load the snapshot into `<table>__staging` using the same SQL the import path uses, with **no index** (the diff joins by ID, not spatially, so an index would be wasted work).

### Phase 3: diff

Each source table gains a persisted `_row_hash UBIGINT` column, written at import and at update. `hash()` returns a UBIGINT, so this costs ~8 bytes/row (~130 MB for BDOT10k) and lets the diff read the live side's hashes instead of recomputing them.

The hash is computed by hashing a **whole row reference over a subquery alias**, not an explicit column list:

```sql
CREATE TABLE <target> AS
SELECT *, hash(s) AS _row_hash
FROM (<the existing per-source SELECT, producing geom>) s;
```

`hash(s)` hashes every column of `s`, including `GEOMETRY`, and `s` deliberately does not contain `_row_hash`, so the hash is never self-referential. Verified: identical content produces identical hashes across separate table creations, and NULL geometry hashes consistently. Because there is no column list to maintain, a source gaining or losing a column cannot silently desynchronize the import and update hash expressions — the drift shows up as a schema mismatch instead (see Error handling).

IDs are not unique, so the diff compares an **ID → row-set hash**, never row-to-row:

```sql
WITH live_h AS (
    SELECT <id_column> AS id, hash(list_sort(list(_row_hash))) AS h
    FROM <table> GROUP BY <id_column>
), new_h AS (
    SELECT <id_column> AS id, hash(list_sort(list(_row_hash))) AS h
    FROM <table>__staging GROUP BY <id_column>
)
```

- `added` = `new_h` ANTI JOIN `live_h` on `id`
- `removed` = `live_h` ANTI JOIN `new_h` on `id`
- `modified` = `new_h` JOIN `live_h` USING (`id`) WHERE `h` differs

An ID's entire row-set is replaced as a unit, so duplicate IDs cannot drift out of sync.

The hash expression is generated by **one shared function** used by both the import and the update path. If those two ever disagree on the expression, every row would compare as modified on every refresh, forever. This is the single most important invariant in the feature, and the whole-row `hash(s)` form above exists specifically to make disagreement structurally difficult rather than merely discouraged.

### Phase 4: apply

```sql
BEGIN;
  -- snapshot_id is allocated here, inside the transaction (see below).
  SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dataset_refreshes;

  -- Change areas FIRST, while <table> still holds the old snapshot.
  INSERT INTO dataset_change_areas SELECT ...;

  DELETE FROM <table>
   WHERE <id_column> IN (SELECT id FROM removed UNION ALL SELECT id FROM modified);

  INSERT INTO <table>
  SELECT * FROM <table>__staging
   WHERE <id_column> IN (SELECT id FROM added UNION ALL SELECT id FROM modified);

  INSERT INTO dataset_refreshes VALUES (...);
COMMIT;
```

**The change-area insert must come before the delta**, and this ordering is load-bearing. It reads the *old* geometry of removed and modified objects out of `<table>` — the cell an object is leaving. Once the `DELETE` has run, removed rows are gone and modified rows have been overwritten with their new geometry, so the same query silently produces a wrong changeset rather than an error: removed objects contribute nothing at all, and a moved object marks its destination cell twice instead of marking both the cell it left and the cell it entered. Any refresh with removals or moves is affected. Nothing in the schema enforces the ordering, so it is easy to reintroduce.

Data and changeset commit together, so the changeset can never describe a state the database is not in.

### Phase 5: cleanup

`DROP TABLE <table>__staging`, via a `Drop` guard so it runs on every exit path including early returns and errors.

## Data model

Both tables are added to `src/db.rs::create_schema()` as idempotent `CREATE TABLE IF NOT EXISTS`, consistent with the rest of the schema. The source tables additionally gain `_row_hash UBIGINT`.

There is no migration machinery: the application is not yet running in production, so the development database is dropped and recreated when the schema changes.

```sql
CREATE TABLE IF NOT EXISTS dataset_refreshes (
    snapshot_id    BIGINT PRIMARY KEY,
    source         VARCHAR,        -- 'prg' | 'bdot10k' | 'egib'
    started_at     TIMESTAMP WITH TIME ZONE,
    finished_at    TIMESTAMP WITH TIME ZONE,
    source_etag    VARCHAR,        -- ETag/Last-Modified for skip-if-unchanged
    added          INTEGER,
    modified       INTEGER,
    removed        INTEGER
);

CREATE TABLE IF NOT EXISTS dataset_change_areas (
    snapshot_id    BIGINT,
    source         VARCHAR,
    cell_z         INTEGER,        -- 14
    cell_x         INTEGER,
    cell_y         INTEGER,
    added          INTEGER,
    modified       INTEGER,
    removed        INTEGER,
    detected_at    TIMESTAMP WITH TIME ZONE
);
```

`dataset_refreshes` holds one row per refresh attempt and owns `snapshot_id`; it also provides summary counts for free.

`snapshot_id` is assigned inside the apply transaction as `SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dataset_refreshes` — monotonic, gapless, and shared across all three sources. Reading and writing it in the same transaction that applies the delta makes concurrent refreshes of different sources safe without a separate sequence object.

A **no-op refresh** (skip-if-unchanged, or a diff that found nothing) still writes a `dataset_refreshes` row, with `added`/`modified`/`removed` all `0` and no `dataset_change_areas` rows. This distinguishes "the job ran and there was nothing to do" from "the job never ran", which matters when reading `/status`.

`cell_z` / `cell_x` / `cell_y` are stored as separate integers rather than a single `'z/x/y'` string so that "changes within this bbox" is a range predicate rather than string parsing. Rendering back to `'14/2276/1345'` for output is trivial.

Cells are XYZ tiles at **z14**, matching the highest zoom `/tiles` serves, derived from `ST_Centroid(geom)` for buildings and from the point itself for PRG addresses. **Both old and new geometry contribute**, so an object that moves marks the cell it left as well as the cell it entered. Expected volume: ~5,200 rows per BDOT10k refresh.

Reuses the XYZ math already present in `src/server/tiles.rs`; no H3 or other new extension dependency.

## Module layout

The three import modules are near-copies of each other today. Rather than adding three more near-copies on the update side, the reusable half is extracted.

```
src/update/
    mod.rs         — dispatch; replaces the three bail!s
    osm.rs         — unchanged
    dataset.rs     — NEW: shared five-phase refresh, driven by DatasetSpec
    changeset.rs   — NEW: cell derivation and change-area aggregation
```

```rust
pub struct DatasetSpec {
    name: &'static str,          // "bdot10k"
    table: &'static str,         // "bdot10k_buildings"
    id_column: &'static str,     // "LOKALNYID"
    geom_kind: GeomKind,         // Polygon (use centroid) | Point
}
```

`src/import/{bdot10k,egib,prg}.rs` are refactored so that "load rows into table X" is separable from "replace the live table":

- import = `load_into(<live table>)` + `CREATE INDEX`
- update = `load_into(<staging table>)` + diff + apply

This removes existing duplication rather than adding more.

## CLI and job wiring

**CLI:** `update bdot10k|egib|prg [--file <path>]`, plus `--terc-file` for PRG, mirroring the existing `import` subcommands. `--file` uses a local snapshot and skips the download.

**Jobs:** a single `DatasetUpdateJob(DatasetSpec)` type with three registered instances, alongside the existing `osm_update` and `export_log_prune`:

```toml
[jobs.bdot10k_update]  # enabled = true, interval_seconds = 86400, timeout_seconds = 3600
[jobs.egib_update]     # enabled = true, interval_seconds = 86400, timeout_seconds = 3600
[jobs.prg_update]      # enabled = true, interval_seconds = 86400, timeout_seconds = 7200
```

`JobsConfig` gains three `JobConfig` fields. Daily intervals are affordable because of the skip-if-unchanged check. PRG gets a longer timeout because it parses ~16 voivodeship GML files out of a ~1.7GB zip rather than reading a single parquet.

**Concurrency:** the scheduler's supervisor guarantees no overlap *per job*, but nothing stops all three dataset jobs from staging ~16M rows simultaneously against a 4GB `memory_limit`. A shared "heavy refresh" mutex, acquired by all three before staging, sequences them without modifying the scheduler.

## Error handling

| Condition | Behavior |
|---|---|
| Source file missing, zero-byte, or unreadable | **Abort** in a pre-flight check, before any staging work |
| Staging yields 0 rows | **Abort** before apply — a truncated or empty download would otherwise delete the entire live dataset |
| Download / parse failure | Existing retry with backoff; live table untouched |
| Apply transaction failure | Rolls back; live data and changeset both unchanged |
| Staging table left behind | Dropped on every exit path via a `Drop` guard |
| Source schema drift | **Abort** with a message naming the fix (`import <source>`). Staging columns not matching live columns cannot be inserted. BDOT10k has changed schema before |
| `metadata.row_hash_version` mismatch | **Warn** and proceed. A DuckDB upgrade changing `hash()` output makes every row compare as modified; the refresh is then effectively a full rewrite, which is correct but slow and produces a misleadingly large changeset. The warning explains the cause |
| Implausible churn (>50% of rows) | **Warn** and proceed. Normal churn is ~2%, so this signals an upstream restructuring worth a human look, but it should not block a refresh that may well be legitimate |

The empty-staging abort is the load-bearing safety check: it is the specific failure that would otherwise silently destroy the live dataset. Hash-version mismatch and implausible churn are diagnostics, not stop conditions.

## Testing

**Unit** (`src/update/dataset.rs`, in-memory DuckDB, following the existing `#[cfg(test)]` convention in `compare/buildings.rs`):

- diff classification: added, removed, modified, unchanged
- duplicate IDs: an ID with multiple rows is replaced as a unit; changing one of its rows marks the whole ID modified
- NULL geometry rows do not panic and are excluded from change areas
- staging with 0 rows aborts and leaves the live table untouched
- hash expression parity: the import-path and update-path generators produce identical SQL

**Unit** (`src/update/changeset.rs`):

- cell derivation matches `tiles.rs` XYZ math for known lon/lat inputs, round-tripped through the existing tile→bbox helper
- an object that moves between cells produces change rows for both

**Integration** (`tests/cli_update_bdot10k.rs`, `cli_update_egib.rs`, `cli_update_prg.rs`, using `assert_cmd` + `tempfile` as the existing CLI tests do):

- import fixture A → update with fixture B → assert live row count, spot-check a modified row's new value, assert `dataset_change_areas` and `dataset_refreshes` contents
- update with an unchanged snapshot produces a no-op refresh row and zero change areas

Requires a "B" fixture per source, generated by perturbing the existing fixtures via a new script in `fixtures/scripts/`.

**Concurrency:** a reader thread polling `SELECT count(*)` across the apply commit observes only the before-count or the after-count, never an intermediate value.

## Out of scope

- **Re-running `compare` after a refresh.** `bdot10k_comparison`, `egib_comparison` and `prg_import_candidates` are written by the `compare` CLI but read by nothing — `/package` and `/tiles` query the source tables and recompute matches live. A refresh therefore leaves no stale reader-visible state. Wiring comparison refresh into the pipeline is separate work.
- **Periodic RTREE rebuild.** Needed eventually (see the docs caveat above), but it is scheduled maintenance independent of this feature, and the degradation should be measured before being designed for.
- **Serving the changeset over HTTP.** `dataset_change_areas` is written here; exposing it via an endpoint is a follow-up.
