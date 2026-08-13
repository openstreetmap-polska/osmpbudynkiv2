# The per-cell recompute full-scans the government table — again, via a different clause

Written 2026-08-13, measured on a Poland-scale DB (16,351,815 bdot10k /
17,773,876 egib / 8,607,258 prg rows, `compare full` already applied).

Direct sequel to `docs/per_cell_recompute_full_scan.md`. That note diagnosed
`ST_Intersects(ST_Centroid(b.geom), env)` — a function wrapped around the
indexed column — and fixed it by storing and indexing `centroid` as its own
column (`docs/centroid_index_measured.md`). This note is the *same symptom
reintroduced by a different clause*, on top of that fix. The stored-centroid
work was correct and is not being undone.

Found while investigating why, after `import osm` from a ~1-day-old PBF,
`match_refresh` takes ~an hour to drain a ~4000-entry dirty-cell queue.

---

## Symptom

Per drained z14 cell, wall clock, warm cache:

| source | before | after | speedup |
|---|---|---|---|
| bdot10k (Warsaw, dense) | 1.091 s | 0.051 s | 21× |
| egib (Warsaw, dense) | 1.237 s | 0.048 s | 26× |
| prg (Warsaw, dense) | 0.546 s | 0.033 s | 17× |
| **bdot10k (rural, near-empty cell)** | **1.089 s** | **0.018 s** | **60×** |

Read the rural row first. **The cost before the fix is independent of how much
data the cell contains** — a cell over empty countryside costs the same as one
over central Warsaw, because the cost *is* the table scan, not the work. After
the fix the cost tracks density, as it should, so the national-average speedup
is nearer the rural 60× than the Warsaw 21×.

Extrapolated to the queue actually sitting in the DB (1277 bdot10k + 1277 egib
+ 1182 prg cells): **~60 min → ~2.8 min**.

## Why

`compare::incremental::recompute_cell_in_txn` appends a write-narrow guard to
the shared match rule, so that a building whose geometry straddles a cell
boundary is written by exactly one cell's recompute:

```sql
<rule::unmatched_buildings_sql(...)>  AND cell_x_sql(b.centroid) = X
                                      AND cell_y_sql(b.centroid) = Y
```

`cell_x_sql`/`cell_y_sql` expand to `floor(...)::INTEGER` over `ST_X(b.centroid)`.
That is an expression filter on the *same column* the RTREE indexes, and it is
what makes the optimizer drop the index scan.

Isolated clause by clause with `EXPLAIN` on real data (dense Warsaw cell
9149/5404):

| query shape | scan on `bdot10k_buildings` |
|---|---|
| `ST_Intersects(b.centroid, envelope)` only | `RTREE_INDEX_SCAN` |
| + `extra_filter` (plain column) | `RTREE_INDEX_SCAN` |
| + `NOT EXISTS osm_buildings` | `RTREE_INDEX_SCAN` |
| + `NOT EXISTS osm_former_buildings` | `RTREE_INDEX_SCAN` |
| + both `NOT EXISTS` | `RTREE_INDEX_SCAN` |
| **+ write-narrow cell guard** | **`Sequential Scan`** |
| **full production shape** | **`Sequential Scan`** |

The two anti-joins are innocent — they plan fine, and were the first suspects.
It is the cell guard alone.

`compare::totals::recompute_cell_in_txn` carries the same guard and planned as
`Sequential Scan` too, so **a single drained cell scanned the source table
twice**: once for the `*_unmatched` INSERT, once for the `cell_totals` INSERT.

The existing regression test
`rule::tests::unmatched_buildings_predicate_uses_the_centroid_rtree_index` does
not catch this, because it asserts on `unmatched_buildings_sql`'s output
*before* `incremental` appends the guard. That gap is why the new tests call a
`build_sql` seam that produces the real generated SQL.

## The fix

Wrap the source scan in a candidate CTE, so the envelope filter is computed
through the index before the guard is applied on top of it:

```sql
WITH candidates AS MATERIALIZED (
  SELECT * FROM bdot10k_buildings b
  WHERE ST_Intersects(b.centroid, <cell envelope>) AND <extra_filter>
)
<rule::unmatched_buildings_sql("candidates", select, write, extra_filter)>
  AND cell_x_sql(b.centroid) = X AND cell_y_sql(b.centroid) = Y
```

`rule.rs` needed **no predicate change** — it already interpolated a bare
`FROM {source_table} b` (and `a` for addresses), so passing the CTE name is
enough and the match rule keeps its single home. The only addition there is
`envelope_sql`, so a CTE's envelope and its own predicate's envelope cannot
drift apart; a mis-ordered argument in one but not the other would silently
narrow to the wrong cell with no error.

The envelope and `extra_filter` end up applied twice (once building
`candidates`, once inside the predicate). That is deliberate: both are
idempotent, and trimming the second copy would mean giving `rule.rs` a
"skip the redundant filter" mode, i.e. two predicate texts.

**Verified result-identical, not just faster.** Baseline and fixed shapes run
over the same 60 cells produce 10,868 `bdot10k_unmatched` rows each, with
`EXCEPT ALL` empty in *both* directions (geometry compared as WKB), and
identical `cell_totals`.

## `MATERIALIZED` is insurance here, not the active ingredient

The fix was designed on the assumption that `MATERIALIZED` was doing the work —
by analogy with the CLAUDE.md gotcha about DuckDB's join-order optimizer folding
a filtered CTE back into a joint plan (the `server::tiles` case). **That
assumption was wrong**, and it is recorded here because the natural next
"simplification" is to drop the keyword.

Measured on the full production shape, same cell:

| shape | plan | time |
|---|---|---|
| flat (today's, no CTE) | 2 `RTREE_INDEX_SCAN` + 1 `Sequential Scan` | 0.974 s |
| bare `WITH candidates AS (...)` | 3 `RTREE_INDEX_SCAN` | 0.098 s |
| `WITH candidates AS MATERIALIZED (...)` | 3 `RTREE_INDEX_SCAN` | 0.099 s |

So **the CTE is what restores the index**; `MATERIALIZED` is indistinguishable
on this query, on this DuckDB build (crate `1.10505.0`, engine 1.5.5). It is
kept anyway, because the tiles.rs failure mode is real and a future plan change
could reintroduce the fold — but nobody should believe a test is pinning it.
The regression tests assert `RTREE_IN` only, and their comments say plainly that
removing `MATERIALIZED` alone was *not* reproducible as a failure at 20k or
500k synthetic rows either.

Attempts to reproduce the fold-back without it, all still showing `RTREE_IN`:
20k and 500k synthetic rows, with and without production-like
`threads`/`memory_limit`. Two plausible explanations, neither verified: the
anti-join (`NOT EXISTS`) shape folds differently from tiles.rs's `LEFT JOIN`,
or the fold needs a non-empty *indexed* table on the other side of the join to
trigger cost-based re-planning.

## Measured negative result: do NOT do this to the full compare

`compare::buildings::compare_buildings` has a structurally identical
index-defeating guard of its own (`ST_X(b.centroid) >= x AND < x_hi`, iterating
a 0.5° grid), and `totals::rebuild_all_in_txn` likewise. Applying the same CTE
wrap there measured **worse, not better**: 0.955 s → 1.097 s for one 0.5° grid
cell.

The reason is selectivity, and it is worth stating as a rule of thumb: a 0.5°
grid cell is ~1/264 of the table, which is not selective enough for an RTREE
walk to beat a sequential scan — and materializing the candidate set means
materializing ~60k rows for no gain. A z14 cell is ~1/340,000 of the table,
which is. **Both paths are correct as they stand; the full compare *wants* its
sequential scan.** Leave them alone.

## Alternative considered: store the cell tag as a column

Add `cell_x`/`cell_y` INTEGER columns to the three source tables at import, the
way `centroid` and `rodzaj_kod` were added (outside `hashed_select`, so no
`ROW_HASH_VERSION` bump). The guard becomes `b.cell_x = X AND b.cell_y = Y` — a
plain column comparison, which keeps the RTREE (row 2 of the clause table above
proves plain-column filters are harmless) and additionally gets zonemap pruning.
It would also speed up `reconcile::enqueue_all` and `totals::rebuild_all_in_txn`,
which recompute the projection for every row of both tables.

Not taken: it needs a full re-import of bdot10k + egib (no `ALTER TABLE`/backfill
path exists in this codebase) and adds another entry to the growing "no migration
path" list. The CTE fix gets essentially the same win with no re-import. The two
are not exclusive — revisit C only if the full-compare/reconcile paths become a
problem in their own right.

## Not affected

Nothing about correctness, the match rule, `ROW_HASH_VERSION` or the serving
version changes. It is purely a plan fix.

## Method / caveats

- The measurement DB was a 16 GB copy of the live DB whose `osm_buildings` was
  **empty** and whose three `osm_*` RTREE indexes were absent — the copy was
  taken while `import osm` was between `reset_osm_tables` and
  `create_spatial_indexes`. So the OSM side of the match rule was reconstructed
  in a scratch DB that `ATTACH`es the copy read-only: 215,091 synthetic building
  polygons (80 % of BDOT10k footprints over a Warsaw region, translated ~1 m so
  the overlap test does real work) plus the region's real `osm_addresses`, all
  RTREE-indexed. Government tables were read from the attached DB through their
  real indexes.
- `ATTACH` was verified **not** to defeat the RTREE index on its own (`EXPLAIN`
  shows `RTREE_INDEX_SCAN` both through `ATTACH` and directly), so the sequential
  scan above is a property of the query shape, not of the harness.
- The synthetic `osm_buildings` is 215k rows against a real-world ~8M, so its
  RTREE is shallower than production's. That makes the *fixed* numbers slightly
  optimistic. It does not touch the baseline numbers at all, which are dominated
  by the government-table scan.
- The recompute SQL was regenerated verbatim from `incremental.rs` + `rule.rs` +
  `totals.rs` (including the carried classification columns) rather than
  approximated by hand.
- A leading hypothesis that turned out **wrong**, recorded so it is not
  re-investigated: the per-node `SELECT ... FROM osm_addresses WHERE osm_id = ?`
  lookups in `rebuild_way_geometry` look like unindexed full scans, but measure
  at **0.45 ms** — DuckDB's zonemaps prune to a single row group because the
  table is loaded in id order. Adding an `osm_id` index is not the win it looks
  like. (Caveat: that clustering decays as updates append re-inserted rows at
  the end, so it is worth re-measuring on a long-lived DB.)

## Reproduction

Server stopped, populated DB:

```sql
LOAD spatial;
SET explain_output='physical_only';
-- index-eligible on its own:
EXPLAIN SELECT LOKALNYID FROM bdot10k_buildings b
WHERE ST_Intersects(b.centroid, ST_MakeEnvelope(21.0058,52.2278,21.0278,52.2413));
-- RTREE_INDEX_SCAN

-- with the write-narrow guard appended:
EXPLAIN SELECT LOKALNYID FROM bdot10k_buildings b
WHERE ST_Intersects(b.centroid, ST_MakeEnvelope(21.0058,52.2278,21.0278,52.2413))
  AND floor((ST_X(b.centroid) + 180.0) / 360.0 * 16384)::INTEGER = 9149;
-- Sequential Scan, ~16.35M rows
```
