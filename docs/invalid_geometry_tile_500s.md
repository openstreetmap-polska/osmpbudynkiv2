# Invalid government geometry makes 23 z14 tiles return HTTP 500

Written 2026-07-30. Found while verifying the `/tiles` index+rewrite work
(see `docs/followups_precomputed_unmatched_serving.md`, item 1).

**This is pre-existing and unrelated to the precomputed-unmatched-serving
feature** — it reproduces identically with the old `geom && bbox.geom`
predicate. It is recorded separately because it is a live production fault with
its own decision attached, not a follow-up of that feature.

Not fixed. The remedy needs a deliberate choice (below).

---

## Symptom

`GET /tiles/14/{x}/{y}` returns **500** for 23 tiles. `serve_tile` logs
`tile query failed` and the whole tile is lost — including every *valid*
building in it, and the addresses layer, since both layers are concatenated
into one response.

The underlying error comes from GEOS via `ST_AsMVTGeom`:

```
Invalid Input Error: TopologyException: unable to assign free hole to a shell at 3010 3506
Invalid Input Error: TopologyException: side location conflict at 964 1189
```

One invalid row aborts the entire query, so the blast radius is the tile, not
the row.

## Root cause

A small number of government building polygons are topologically invalid
(`ST_IsValid = false`). `ST_AsMVTGeom` does not tolerate them.

Example: egib `062008_2.0016.283/13.3_BUD` in `z14/9231/5505` — a 6-point
MULTIPOLYGON, `ST_IsValid = false`.

## Scope (measured on the current Poland dataset)

| | count |
|---|---|
| invalid geometries in `bdot10k_unmatched` | 1 |
| invalid geometries in `egib_unmatched` | 198 |
| **invalid geometries total** | **199** |
| z14 cells containing ≥1 invalid geometry | 125 |
| **z14 tiles whose buildings MVT query actually errors** | **23** |

Note the gap between 125 and 23: `ST_IsValid` is false for more geometries than
`ST_AsMVTGeom` actually chokes on. Treat 199/125 as the population at risk and
23 as today's blast radius — a different DuckDB/GEOS version could move the
boundary in either direction, which is an argument for fixing the data rather
than the symptom.

### The 23 failing tiles

| tile | GEOS failure |
|---|---|
| z14/8941/5438 | free hole |
| z14/8960/5533 | free hole |
| z14/9059/5421 | free hole |
| z14/9071/5472 | free hole |
| z14/9101/5497 | free hole |
| z14/9101/5501 | free hole |
| z14/9115/5392 | free hole |
| z14/9120/5440 | free hole |
| z14/9123/5499 | free hole |
| z14/9124/5541 | free hole |
| z14/9140/5478 | side location conflict |
| z14/9140/5481 | side location conflict |
| z14/9141/5476 | side location conflict |
| z14/9142/5476 | side location conflict |
| z14/9142/5477 | side location conflict |
| z14/9143/5476 | side location conflict |
| z14/9164/5550 | free hole |
| z14/9188/5463 | side location conflict |
| z14/9206/5338 | free hole |
| z14/9222/5530 | free hole |
| z14/9225/5438 | free hole |
| z14/9231/5505 | free hole |
| z14/9233/5438 | free hole |

## Options

### 1. Repair on the way into the serving tables (recommended)

Normalise once at write time instead of paying a check on every tile read.
**`ST_MakeValid` alone is not enough** — measured over all 199 rows:

| before | after `ST_MakeValid` | n |
|---|---|---|
| MULTIPOLYGON | GEOMETRYCOLLECTION | 108 |
| MULTIPOLYGON | MULTIPOLYGON | 69 |
| MULTIPOLYGON | POLYGON | 18 |
| POLYGON | MULTIPOLYGON | 3 |
| POLYGON | POLYGON | 1 |

It repairs all 199, but turns **108 into `GEOMETRYCOLLECTION`**, which is wrong
for a buildings layer — the collection can carry the stray lines/points the
repair sheds, and downstream consumers (JOSM, the `/package` GeoJSON) expect
polygonal geometry.

Pairing it with `ST_CollectionExtract(..., 3)` (keep polygons only) is clean —
measured over the same 199 rows:

- 199/199 `ST_IsValid`
- 0 empty results
- only `POLYGON` / `MULTIPOLYGON` remain

So the expression to apply is:

```sql
ST_CollectionExtract(ST_MakeValid(geom), 3)
```

**Where it has to go, and the trap:** the serving tables are written by *both*
the full compares (`compare::buildings`) and the per-cell incremental recompute
(`compare::incremental::recompute_cell_in_txn`). Those two paths must stay
row-identical — that invariant is pinned by
`compare::full_vs_incremental_equivalence` and is the reason the match rule has
exactly one home (see CLAUDE.md). A repair applied to one path and not the
other would break that test, which is the desired behaviour: fix both, in one
place, or not at all.

Also note this changes *what is served* for those 199 rows — geometry is
altered, not just filtered — so it is a data decision, not only a bug fix.

### 2. Filter invalid geometry out of the tile query

`WHERE ST_IsValid(geom)` in `src/server/tiles.rs`. Smallest change, but:

- it adds a per-row validity check to the read path the index work just
  optimised, on every tile request;
- it silently *omits* buildings an importer may still want. A visible 500 is
  traded for an invisible omission, which is arguably worse for a tool whose
  job is "show me what is missing from OSM".

### 3. Repair at government-dataset import time

Keeps the serving tables clean and fixes `compare` inputs too, but touches more
of the pipeline and interacts with the row-hash version (`ROW_HASH_VERSION` in
`src/dataset.rs`) — changing stored geometry changes every row hash, forcing a
full rewrite on the next update.

## Reproduction

Requires a populated DB (post `import` + `compare full`), server stopped:

```sql
LOAD spatial;
-- the population at risk
SELECT COUNT(*) FROM egib_unmatched WHERE NOT ST_IsValid(geom);

-- reproduce one failure (tile z14/9231/5505)
WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(
  22.82958984375, 50.70863440082822, 22.8515625, 50.7225468336323)) AS geom)
SELECT COUNT(*) FROM (
  SELECT ST_AsMVTGeom(t.geom, bbox.geom, 4096, 256, true) AS g
  FROM egib_unmatched t, bbox
  WHERE ST_Intersects(t.geom, ST_MakeEnvelope(
    22.82958984375, 50.70863440082822, 22.8515625, 50.7225468336323))
) WHERE g IS NOT NULL;
-- Invalid Input Error: TopologyException: unable to assign free hole to a shell
```
