# Plan 1 — Give BDOT10k and EGIB a usable record key at import and update

**Status:** not started. Execute this plan **first** — [Plan 2](2026-08-14-key-based-diff.md) depends on it.

## Context

`bdot10k_buildings` and `egib_buildings` are loaded with `CREATE TABLE AS SELECT *` straight from the
source GeoParquet, so whatever duplicate records the government publishes land in the database verbatim.
This is currently worked around rather than fixed: `DatasetSpec::id_column` carries the comment "NOT
unique — BDOT10k has duplicate IDs, so the diff compares an ID's whole row-set, never row to row"
(`src/dataset.rs:66`), and `src/update/diff.rs` folds every row of an ID into
`hash(list_sort(list(_row_hash)))` purely to cope with it. The non-uniqueness also forces
`*_unmatched` to copy whole rows rather than reference source rows (CLAUDE.md, "serving tables store
rows, not id references"), and `docs/building_type_mappings.md:422` avoids id joins entirely in favour
of `ST_Equals`.

Making the identifier actually identify one row removes the reason for the per-ID row-set fold and
unlocks the key-based diff in Plan 2.

### Measured on the real snapshots (2026-08-10)

| | BDOT10k | EGIB |
|---|---|---|
| rows | 16,351,817 | 17,777,751 |
| duplicate-key groups | 2 | 851 |
| rows removed by dedup | **2** | **1,611** |
| version column breaks the tie? | yes, both groups | no — 843 of 851 groups tie on `czas_pozyskania` |
| rows removed by the NULL-key drop | 0 | **210,080 (1.2%)** |

### Decision: EGIB's NULL `id_budynku` rows are dropped, not kept (2026-08-14)

`id_budynku` is NULL for 210,080 EGIB rows. A record with no identifier cannot be diffed at all — Plan
2's Measurement 2 shows these rows are already frozen at their import-time state today, because
`ANTI JOIN ... USING (id)` never matches NULL to NULL and the apply's `WHERE id IN (...)` is never true
for them. They also cannot be deduplicated, since there is no key to group on.

The decision is to **drop them at load**, accepting the data loss and expecting it to be fixed
upstream. The alternative considered and rejected was a geometry-derived surrogate key (Plan 2's
original recommendation) — it works, but it invents an identifier the source does not have, and the
source is the thing that should be fixed.

Consequences to be aware of, none of them blockers:

- Those 210,080 buildings leave `egib_buildings`, so they stop appearing in `egib_unmatched`, `/tiles`
  and `/package` — EGIB stops proposing them for import. `cell_totals` drops them too, so the ratio
  legend shifts slightly in the affected cells.
- BDOT10k is unaffected (0 NULL keys) but gets the same treatment, so Plan 2's precondition — the key
  is unique **and** non-null — holds for both sources by construction rather than by source luck.

## Goal

Both loaders make the record key usable at load time and report what they dropped through the existing
`LoadStats` → `job_run_log` channel. Because both `import` and `update`'s staging load funnel through
the same `load_into`, one change covers both paths — the same property `filter_invalid_geometry` and
`filter_oversized_geometry` already rely on.

Rules, as specified:

- **Both sources** — a row with NULL in any key column is dropped outright.
- **BDOT10k** — key `(PRZESTRZENNAZW, LOKALNYID)`, keep the row with the greatest `WERSJA`.
- **EGIB** — key `id_budynku`, keep the row with the greatest `czas_pozyskania`; **ties are broken
  arbitrarily** (see below).

## Design

The two rules are enforced at **different points**, deliberately: NULL keys are filtered in the load
`SELECT` so the rows are never written, duplicates are deleted after the table exists. The reasons are
in "Why not do both at insert" below; each site's doc comment should point at the other so the pair
reads as one mechanism.

### NULL keys — filtered in the load SELECT

`load_into`'s inner select already ends in `FROM '{parquet_path}'` in both loaders
(`src/import/bdot10k.rs:32-36`, `src/import/egib.rs:20-24`). Append a `WHERE`, built by one shared
helper in `src/dataset.rs` so the predicate is written once:

```rust
/// `k1 IS NOT NULL AND k2 IS NOT NULL ...` — a record with no identifier
/// cannot be diffed (see `update::diff`) or deduplicated, so it is dropped
/// before it is ever written. Paired with `deduplicate_by_key`, which
/// enforces the other half of the same guarantee.
pub fn non_null_key_sql(key_columns: &[&str]) -> String
```

```sql
-- egib
SELECT * EXCLUDE(geometry, geometry_bbox), ST_Transform(...) AS geom
FROM '{parquet_path}' WHERE id_budynku IS NOT NULL
```

Reporting needs the count, which a filtered CTAS does not return, so `load_into` runs one extra query
against the same parquet path:

```sql
SELECT count(*) FROM '{parquet_path}' WHERE {k1} IS NULL [OR {k2} IS NULL ...]
```

Cheap — parquet column pruning reads only the key column(s). **BDOT10k trap:** this query binds the
same GeoParquet that forces `SET enable_geoparquet_conversion = false` around the CTAS
(`src/import/bdot10k.rs:26-30, 39-44`); DuckDB rejects the file's CRS at bind time regardless of which
columns are projected, so the count must run inside that same disabled window, not after it is
restored.

No example ids for this reason: the id column *is* what is missing, so the list would be a column of
NULLs. Count only.

### Duplicate keys — deleted after load

Add `deduplicate_by_key` as a sibling of `filter_invalid_geometry` (`src/dataset.rs:202`) and
`filter_oversized_geometry` (`src/dataset.rs:284`), matching their shape exactly: scan for up to
`MAX_EXAMPLE_IDS` example ids, `DELETE`, return a `LoadStats`.

```rust
pub fn deduplicate_by_key(
    conn: &Connection,
    table: &str,
    key_columns: &[&str],   // ["PRZESTRZENNAZW", "LOKALNYID"] / ["id_budynku"]
    order_by: &str,         // "WERSJA DESC" / "czas_pozyskania DESC"
    id_column: &str,        // for example ids in the log line
) -> Result<LoadStats>
```

Two-phase SQL, so the ordered window only touches rows that are actually duplicated (851 groups, not
17.7M rows):

```sql
DELETE FROM {table} WHERE rowid IN (
  WITH dup_keys AS (
    SELECT {keys} FROM {table}
    GROUP BY {keys} HAVING count(*) > 1
  ),
  ranked AS (
    SELECT t.rowid AS rid,
           row_number() OVER (PARTITION BY {keys} ORDER BY {order_by} NULLS LAST) AS rn
    FROM {table} t JOIN dup_keys USING ({keys})
  )
  SELECT rid FROM ranked WHERE rn > 1
)
```

- **No `IS NOT NULL` guard is needed here**, unlike an earlier draft: NULL-keyed rows never reached the
  table. State that in the doc comment — without it, a reader has to work out why a `PARTITION BY` over
  a nullable column is safe. (It would not be: a window puts all 210,080 NULL-keyed rows in one
  partition and would keep exactly one of them.)
- **`row_number()` rather than `DISTINCT ON` or `arg_max`/`max_by`** — measured, not assumed (see the
  table below). In this two-phase shape all three are within noise of each other, because the window
  only sees ~2.5k rows and the cost is the `GROUP BY`; `row_number()` wins on the SQL being shorter.
  `DISTINCT ON` yields the *survivors* and this is a DELETE, so it needs the `dups` CTE materialized and
  referenced twice with a `NOT IN` over the keepers. `arg_max(rid, ver) GROUP BY key` is shorter still
  and **must not be used**: for a group whose ordering column is all-NULL it returns NULL, that NULL
  lands in the `NOT IN` list, and the anti-predicate then evaluates to NULL for *every* row — the DELETE
  silently removes nothing at all, table-wide. Verified on a 4-row synthetic table. Latent rather than
  live today (0 NULL `czas_pozyskania`, 0 NULL `WERSJA` on the real snapshots), which is what makes it
  dangerous — it would land with a future export, not in review.
- **No tiebreak column, deliberately (2026-08-14).** 843 of 851 EGIB groups tie on `czas_pozyskania`,
  so which row survives is whatever the scan happened to order first. An earlier draft added
  `ST_AsWKB(geom)` as a final sort key to make that deterministic; it is dropped, to keep the sort as
  cheap as possible rather than risk spilling. What this costs: if the scan order for a tying group
  flips between the import and a later refresh, that group's row reports as *modified* once. Bounded at
  ~843 rows per EGIB refresh, against a table of 17.5M, and self-correcting. If EGIB refresh churn ever
  shows a persistent ~843-row floor, this is the cause and a tiebreak is the fix. (The window is small
  either way — it only ever sees rows inside `dup_keys`, ~2.5k for EGIB and 4 for BDOT10k; the
  memory-hungry step is the `GROUP BY` over the full table, which no tiebreak affects.)
- **`NULLS LAST` is spelled out** rather than left to DuckDB's default, because `default_null_order` is
  settable and this project *does* override `duckdb_init_commands` wholesale. A NULL version must never
  win over a dated one.
- **`rowid` here is safe** despite CLAUDE.md's "serving tables store rows, not id references" warning.
  That invariant is about *storing* a rowid across a DELETE+INSERT; this one lives and dies inside a
  single DELETE statement.

### Measured 2026-08-14 — where to rank, and with what

DuckDB v1.5.5, the real `egib_buildings` (17,773,876 rows, 210,073 NULL keys, 1,611 duplicates),
read-only, two runs each, times reproducible to ±0.02 s. "Two-phase" = the `dup_keys` `GROUP BY` then
rank only duplicated groups; "insert-shape" = rank the whole table in one pass, with every column
carried through the operator (forced with `hash(t)`, since a bare `count(*)` lets DuckDB prune the
columns away and understates it ~4×).

| variant | EGIB | vs plain scan |
|---|---|---|
| plain scan, no dedup (baseline) | 1.99 s | — |
| **two-phase + `row_number()`** | **0.60 s** | n/a (runs after the load) |
| two-phase + `DISTINCT ON` | 0.53 s | n/a |
| two-phase + `arg_max` | 0.53 s | n/a — and incorrect, see above |
| insert-shape `QUALIFY row_number()` | 7.29 s | **+5.3 s** |
| insert-shape `DISTINCT ON *` | 20.32 s | **+18.3 s** |

BDOT10k's composite key behaves the same way (two-phase: 0.69 s `row_number()`, 0.62 s `DISTINCT ON`,
2 rows deleted).

Two things worth reading off this table. **`DISTINCT ON` is the slower choice at insert scale, not the
faster one** — 2.8× worse than `QUALIFY row_number()` there, with 108 s of system time against 11 s,
i.e. it thrashes: it has to sort every column by `(key, version)` where the window can hash-partition.
The intuition that `row_number()` is DuckDB's slow path does not survive contact with all columns being
carried. **And the two-phase structure is worth far more than the choice of ranking function** — 0.60 s
against 7.29 s — because it ranks ~2.5k rows instead of 17.5M.

### Why not do both at insert

Two independent reasons, both specific to the dedup half:

1. **Cost**, per the table above: +5.3 s at best (`QUALIFY`), +18.3 s at worst (`DISTINCT ON`), against
   +0.6 s for the two-phase delete. On a multi-minute import that is not a disaster — it is not a spill
   cliff, and this plan should not claim one — but it is 9–30× the ranking overhead for no benefit.
   (A third form — `read_parquet(..., file_row_number = true)`, rank the dup rows in a first pass,
   anti-join them out in a second — keeps the cheap ranking but reads the parquet twice and is more SQL
   than the DELETE it replaces.)
2. **Ordering**, which is the reason that actually decides it. The dedup must run **after**
   `filter_invalid_geometry` and `filter_oversized_geometry`, so a duplicate pair whose newest member
   has bad geometry falls back to the older valid one rather than being collapsed down to a row the
   geometry filter then deletes — losing the object entirely. Those filters run on the loaded table, so
   anything that must follow them cannot happen at insert.

Neither reason applies to the NULL-key filter: it needs no ranking (measured cost is the `WHERE` alone,
which is free), and a NULL-keyed row is dropped regardless of whether its geometry is valid, so it has
no ordering relationship with anything.

### Call sites

- `src/import/bdot10k.rs` — `WHERE {non_null_key_sql(&["PRZESTRZENNAZW", "LOKALNYID"])}` on the inner
  select, plus the null count; then, after `filter_oversized_geometry` (`:48`),
  `deduplicate_by_key(conn, target_table, &["PRZESTRZENNAZW", "LOKALNYID"], "WERSJA DESC", "LOKALNYID")`
- `src/import/egib.rs` — same, with `&["id_budynku"]`, `"czas_pozyskania DESC"`, `"id_budynku"`

The key-column arrays are literals at the call sites for now; Plan 2 moves them onto
`DatasetSpec::key_columns` and both sites switch to `spec.key_columns`.

PRG is untouched: `lokalny_id` is already unique across all 8,607,258 rows, with no NULLs (verified).

### Reporting

Extend `LoadStats` (`src/dataset.rs:154`) with:

- `skipped_null_key: i64` — no example-ids field, per above
- `skipped_duplicate_key: i64` and `skipped_duplicate_example_ids: Vec<String>`

plus a `merge_unique_key` folder alongside the existing `merge_oversized` (`src/dataset.rs:174`), and
two more clauses in each source's `summarize` and in `update::dataset::summarize_refresh`. `LoadStats`'
own doc comment ("for one of two reasons") needs rewording for four.

`format_skip_clause` (`src/dataset.rs:187`) needs one small change: **omit the `(ids: ...)` parenthetical
entirely when the id list is empty**, or the null-key clause renders as
`skipped 210080 null-key rows (ids: )`. Shared change, benefits every reason; pin it with
`format_skip_clause_omits_the_ids_clause_when_there_are_none`.

Both `import()` and `update::dataset::refresh()` then self-report to `job_run_log` with no further
changes, since they already log whatever `LoadStats` carries. Note both `summarize` functions currently
fall back to the string `"no invalid or oversized geometry"` when nothing was skipped — reword, since
it now under-describes what was checked.

## Files

- `src/dataset.rs` — `non_null_key_sql`, `deduplicate_by_key`, `LoadStats` fields + doc, `merge_unique_key`,
  `format_skip_clause`
- `src/import/bdot10k.rs`, `src/import/egib.rs` — the `WHERE` and null count in `load_into`, the dedup
  call after the geometry filters, two more clauses in `summarize`
- `src/update/dataset.rs` — two more clauses in `summarize_refresh`
- `fixtures/scripts/prepare_update_fixtures.sh` — add a duplicate-key row and a NULL-key row to
  `bdot10k_v2` / `egib_v2` (see Testing)
- `tests/cli_import_bdot10k.rs`, `tests/cli_import_egib.rs` — assert the fixture's duplicate collapses
  and its NULL-key row is gone

## `ROW_HASH_VERSION`: no bump required

The dedup only **deletes** rows, after the table is built and outside `hashed_select`'s projection —
the same argument `filter_oversized_geometry` already relies on.

The NULL-key `WHERE` **does** sit inside `hashed_select`'s input, which CLAUDE.md's row-hash gotcha
flags as bump territory ("a source's inner select is part of the hash input"). It still needs no bump,
and the distinction is worth stating explicitly in the comment because it is exactly the kind of thing a
careful reader will challenge: that rule is about expressions that change a **value**
(`ULICA_PREFIX_STRIP_SQL` is the version-2 case). A row filter changes which rows exist, never the
content of a surviving row, so every surviving `_row_hash` is bit-identical.

Pin both halves, mirroring `dataset::tests::filter_oversized_geometry_does_not_change_surviving_row_hashes`
(`src/dataset.rs:698`):

- `deduplicate_by_key_does_not_change_surviving_row_hashes`
- `non_null_key_filter_does_not_change_surviving_row_hashes` — build the same rows twice, once with the
  filter and once without, and compare `_row_hash` for the ids present in both

## Existing databases: dedup self-heals, the NULL-key drop does not

**Duplicates self-heal on the next refresh.** The staging table is deduplicated while the live table
still holds its duplicates; the current per-ID diff sees a different row-set hash for those ids, marks
them modified, and replaces the whole id row-set with the single staged row. 2 rows for BDOT10k and
1,611 for EGIB — invisible in the churn warning.

**The NULL-key rows do not, and cannot.** A refresh's `DELETE FROM live WHERE id IN (SELECT id FROM
diff_removed)` evaluates to NULL for a NULL-keyed live row, so it is never deleted — the same defect
Plan 2 documents. Staging simply stops containing them, and the 210,080 rows already in an existing
`egib_buildings` stay there, still served, until **`import egib` is re-run** (a wholesale table
rebuild). Plan 2's migration already requires a full re-import, so executing both plans covers this;
executing Plan 1 alone requires re-running `import egib` explicitly to realise the change.

Side effect worth noting: once staging has no NULL group, EGIB refreshes stop over-reporting a phantom
`added = 1`. The phantom `removed = 1` persists until the re-import, since the live NULL group is still
there.

## Testing

Unit tests in `src/dataset.rs`, built from inline `VALUES` like the existing filter tests:

- `non_null_key_sql_covers_every_key_column` — composite key produces both conjuncts
- `non_null_key_filter_does_not_change_surviving_row_hashes` (above)
- `deduplicate_by_key_keeps_the_newest_version` — two rows, one key, distinct versions
- `deduplicate_by_key_keeps_exactly_one_row_per_key_when_versions_tie` — asserts the *count*, not which
  row survived; there is deliberately no determinism guarantee to pin
- `deduplicate_by_key_leaves_unique_tables_untouched`
- `deduplicate_by_key_does_not_change_surviving_row_hashes`
- `deduplicate_by_key_composite_key` — BDOT10k's two-column key; same `LOKALNYID` under two different
  `PRZESTRZENNAZW` values must both survive
- `format_skip_clause_omits_the_ids_clause_when_there_are_none`

In `src/import/{bdot10k,egib}.rs`, mirroring the existing
`load_into_drops_a_deliberately_oversized_row` pair (which is what proves a filter also runs on the
*update* staging path, since both go through `load_into`):

- `load_into_drops_null_keyed_rows` — the count lands in `skipped_null_key`, and no NULL-keyed row is
  in the table. This one has to go through `load_into` with a real parquet path, not a hand-seeded
  table, because the filter now lives in the load select rather than in a helper that can be called in
  isolation — which is precisely why the fixture work below is not optional.

Fixture work: the four committed parquet fixtures have no duplicate or NULL keys (74/74 distinct on
both), so nothing currently exercises either path end to end. Add one duplicate row and one NULL-key row
per source in `fixtures/scripts/prepare_update_fixtures.sh` (it is already pure DuckDB and already
synthesizes an `_ADDED` row), then assert in the CLI tests that the imported count is two lower than the
source parquet's.

## Verification

```bash
cargo test && cargo clippy && cargo fmt -- --check

# Real-data check: the key must now be unique and non-null.
cargo run --release -- --config example_config.toml import bdot10k \
  --file example_data/BDOT10k/OT_BUBD_A_2026-08-10.parquet
duckdb -readonly osmpbudynkiv2.duckdb -c "
  SELECT count(*) total,
         count(*) FILTER (WHERE PRZESTRZENNAZW IS NULL OR LOKALNYID IS NULL) nulls,
         count(DISTINCT (PRZESTRZENNAZW, LOKALNYID)) distinct_keys
  FROM bdot10k_buildings;"
# expect nulls = 0 and total = distinct_keys, around 16,351,815

cargo run --release -- --config example_config.toml import egib \
  --file example_data/EGiB/0_budynki_2026-08-10.parquet
duckdb -readonly osmpbudynkiv2.duckdb -c "
  SELECT count(*) total,
         count(*) FILTER (WHERE id_budynku IS NULL) nulls,
         count(DISTINCT id_budynku) distinct_keys
  FROM egib_buildings;"
# expect nulls = 0 and total = distinct_keys, around 17,566,060
#   (17,777,751 raw - 210,080 NULL-keyed - 1,611 duplicates)
```

`nulls = 0 AND total = distinct_keys` is the assertion that matters — it is the exact precondition
Plan 2 relies on. The absolute totals are approximate because `filter_invalid_geometry` and
`filter_oversized_geometry` also remove their own handful of rows (85 oversized on EGIB).

Watch the import's own timing log too: the NULL-key filter adds one narrow parquet scan to `load_into`,
and the dedup adds a full-table `GROUP BY` plus a ~2.5k-row window — **~0.6 s on EGIB**, invisible next
to the RTREE index build. If it costs seconds instead, the two-phase form was flattened into a
full-table window (7.3 s) or, worse, a full-table `DISTINCT ON` (20.3 s).

Also confirm `/status` reports both counts, with the null-key clause carrying no `(ids: ...)` part:
`duckdb -readonly osmpbudynkiv2.duckdb -c "SELECT * FROM job_run_log WHERE job LIKE 'import:%';"`
