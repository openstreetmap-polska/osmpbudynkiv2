# The per-cell recompute full-scans the government table (every cell)

Written 2026-07-30, measured on a Poland-scale DB (16.3M bdot10k / 17.8M egib /
8.6M prg buildings+addresses, `compare full` already applied).

Found while sizing follow-up item 3 (scheduling `compare reconcile` as a
background job). Not fixed — the clean remedy is a schema change and deserves a
decision.

**This is the hot loop of the whole precomputed-serving design.** Every
government refresh, every OSM minutely diff and every reconcile funnels through
`compare::incremental::recompute_cell_in_txn`, one cell at a time.

---

## Symptom

A single `match_refresh` tick with the default `batch_size = 512` **cannot
finish inside its default `timeout_seconds = 300`**. Measured with a seeded
queue of 1,517 real cells (≈512 per source):

| tick | outcome | cells drained | duration |
|---|---|---|---|
| 1 | `TimedOut` | 332 | 300.2 s |
| 2 | cut short by shutdown | 118 | ~112 s |

That is **~0.9 s per cell**, consistently, with no warm-up effect between ticks.

It is not *broken* — the drain polls `is_cancelled()` between cells, stops
cleanly, and the undrained cells stay queued for the next tick. But:

- `/status` shows `match_refresh` permanently `TimedOut`, which reads as a fault
  when it is really just "the batch is far too big for the timeout";
- effective throughput is ~330 cells / 300 s ≈ **4,000 cells/h**.

## Why

`compare::rule::unmatched_buildings_sql` filters the government table with:

```sql
WHERE ST_Intersects(ST_Centroid(b.geom), ST_MakeEnvelope(...))
```

The indexed column is `b.geom`, but the predicate wraps it in `ST_Centroid()`.
An RTREE index cannot be used through a function applied to the indexed column,
so this plans as a **`SEQ_SCAN` of all 16.3M `bdot10k_buildings` rows — per
cell**. Confirmed by `EXPLAIN`:

```
SEQ_SCAN  Table: osmpbudynkiv2.main.bdot10k_buildings
Filters: ST_Intersects(ST_Centroid(geom), POLYGON (...))
```

The *inner* `NOT EXISTS` against `osm_buildings` does use `RTREE_INDEX_SCAN` —
so the fix is only needed on the outer scan.

Measured on one Warsaw cell:

| predicate | time | rows |
|---|---|---|
| `ST_Intersects(ST_Centroid(geom), env)` (current) | **0.773 s** | 961 |
| `ST_Intersects(geom, env)` alone (index-eligible) | **0.129 s** | 984 |
| both in the same `WHERE` | 0.973 s (still `SEQ_SCAN`) | 961 |
| index-eligible filter isolated in a CTE, exact test after | **0.123 s** | 961 |

So there is a **~6.3× win** available on the dominant cost.

## The trap: the obvious prefilter is wrong

Adding `ST_Intersects(geom, env)` as a prefilter looks safe — "if the centroid
is in the box, surely the polygon meets the box" — and it is **not**.

A centroid can fall outside its own polygon (C-shapes, multipolygons). Measured
across both building tables:

| | count |
|---|---|
| rows whose centroid lies outside their own geometry | **100,013** (0.3%) |
| of those, rows whose z14 cell envelope misses the geometry entirely | **56** |

Those **56 rows would be silently dropped** from the served unmatched set. That
is precisely the kind of quiet divergence the "match rule has one home" rule in
CLAUDE.md exists to prevent, and the equivalence tests would not necessarily
catch it on small fixtures.

Note also that both `compare::buildings::compare_buildings` and the per-cell
path share `unmatched_buildings_sql`, so any change here changes both — which is
the point, and is what keeps `compare::full_vs_incremental_equivalence` honest.

## Recommended fix: persist and index the centroid

Store the representative point as its own column on the government tables and
index *that*:

```sql
ALTER TABLE bdot10k_buildings ADD COLUMN centroid GEOMETRY;  -- ST_Centroid(geom)
CREATE INDEX ... ON bdot10k_buildings USING RTREE (centroid);
```

Then the rule becomes `ST_Intersects(b.centroid, ST_MakeEnvelope(...))` —
**semantically identical** to today (no dropped rows) and index-eligible.

Consequences to weigh:

- the column must be maintained at import *and* in the dataset-refresh apply, so
  it belongs next to `DatasetSpec::representative_point_sql`;
- adding a stored column changes the row layout; check whether it lands inside
  `hashed_select`'s `SELECT *`, because if it does it changes every row hash and
  requires a `ROW_HASH_VERSION` bump (see the CLAUDE.md gotcha). Keeping it
  *out* of the hashed projection is preferable;
- ~34M extra points plus two RTREE indexes of storage.

Expected effect: per-cell recompute ~0.9 s → ~0.15 s, so a 512-cell batch fits
comfortably inside the 300 s timeout, and a full reconcile of 339,140 cells
drops from ~85 h to ~14 h.

## Also worth knowing: two `&&` footguns found on the way

- `geom && ST_MakeEnvelope(...)` returns **0 rows, always**. `&&` wants a
  `BOX_2D` on the right; `ST_MakeEnvelope` returns `GEOMETRY`, and the mismatch
  fails silently rather than erroring.
- `geom && ST_Extent(ST_MakeEnvelope(...))` folds to **`EMPTY_RESULT`** — the
  planner discards the whole query. `ST_Extent` is an aggregate, and using it
  inline in a `WHERE` clause is not the same as computing it in a CTE.

Both return "no rows" rather than an error, so a serving query written either
way would look fine and serve nothing. The working form is a one-row CTE
(`SELECT ST_Extent(...) AS geom`) joined in — which is what `/tiles` used, and
which in turn is what made its RTREE index unusable (see
`docs/followups_precomputed_unmatched_serving.md` item 1).

## Reproduction

Server stopped, populated DB:

```sql
LOAD spatial;
SET explain_output='physical_only';
EXPLAIN SELECT LOKALNYID FROM bdot10k_buildings
WHERE ST_Intersects(ST_Centroid(geom),
                    ST_MakeEnvelope(21.0058,52.2278,21.0278,52.2413));
-- SEQ_SCAN, ~16.3M rows
```
