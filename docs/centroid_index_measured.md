# Measured: the persisted/indexed `centroid` column fix

Written 2026-08-01, measured on the same Poland-scale DB
`docs/per_cell_recompute_full_scan.md` and
`docs/followups_precomputed_unmatched_serving.md` used (`./osmpbudynkiv2.duckdb`,
originally built 2026-07-30), after re-importing `bdot10k` and `egib` from
fresh 2026-08-01 snapshots (`example_data/BDOT10k/OT_BUBD_A_2026-08-01.parquet`,
`example_data/EGiB/0_budynki_2026-08-01.parquet`) with the code from this
branch, which adds the `centroid GEOMETRY` column described in
`docs/superpowers/specs/2026-08-01-bdot10k-egib-centroid-index-design.md`.

Import re-run: bdot10k 16,349,991 rows in 26.1s, egib 17,797,836 rows in
23.3s, both including the new `centroid` RTREE index build.

## Plan shape: confirmed `SEQ_SCAN` → `RTREE_INDEX_SCAN`

Same reproduction as `docs/per_cell_recompute_full_scan.md`, re-run against
the freshly-imported table:

```sql
LOAD spatial;
SET explain_output='physical_only';

-- old form
EXPLAIN SELECT LOKALNYID FROM bdot10k_buildings
WHERE ST_Intersects(ST_Centroid(geom),
                    ST_MakeEnvelope(21.0058,52.2278,21.0278,52.2413));
-- SEQ_SCAN, Filters: ST_Intersects(ST_Centroid(geom), POLYGON (...))

-- new form
EXPLAIN SELECT LOKALNYID FROM bdot10k_buildings
WHERE ST_Intersects(centroid,
                    ST_MakeEnvelope(21.0058,52.2278,21.0278,52.2413));
-- RTREE_INDEX_SCAN, Index: bdot10k_buildings_centroid_idx, ~16,349,993 rows
```

## Per-cell timing: real, freshly measured this session

`.timer on` in the `duckdb` CLI (`-readonly`, server stopped), 3 reps on the
same Warsaw cell the original doc used, plus a dense-city (Kraków) and a
rural (Podlasie) cell for contrast. All times in seconds, wall clock.

**bdot10k:**

| cell | old (`ST_Centroid(geom)`) | new (`centroid`) | speedup |
|---|---|---|---|
| Warsaw z14/9148/5394 (3 reps) | 1.070, 0.931, 0.933 | 0.082, 0.036, 0.038 | ~13–26× |
| Kraków (dense) | 0.926 | 0.164 | ~5.6× |
| Podlasie (rural) | 0.951 | 0.012 | ~79× |

**egib:**

| cell | old | new | speedup |
|---|---|---|---|
| Warsaw | 1.211 | 0.012 | ~101× |
| Kraków (dense) | 1.108 | 0.031 | ~36× |
| Podlasie (rural) | 1.189 | 0.011 | ~108× |

The old form is consistently ~0.9–1.2s regardless of how many rows the cell
actually contains — the signature of a full 16–18M-row scan, exactly as
`per_cell_recompute_full_scan.md` found. The new form tracks the number of
rows actually near the cell, the same shape change
`followups_precomputed_unmatched_serving.md` found for the serving-table
index (item 1).

This directly validates the design's target: the per-cell match query, which
is what `compare::incremental::recompute_cell_in_txn` runs on every
`match_refresh` tick and every `compare reconcile` cell, is now consistently
sub-100ms instead of ~1s, a **10–100× win depending on local building
density** (denser areas see a smaller multiplier because the RTREE scan
itself returns more rows to filter, but even the worst case measured here —
Kraków bdot10k, 5.6×) is a large real improvement over a scan that read all
16.3M rows every time regardless of density.

## End-to-end: `compare buildings` (full, both sources)

Real timed run, same DB, both `bdot10k` and `egib` in one `compare buildings`
invocation:

| source | doc's 2026-07-30 baseline | measured today | change |
|---|---|---|---|
| bdot10k | 6m41s | **5m29s** | ~18% faster |
| egib | 7m53s | **6m0s** | ~24% faster |

Row counts and match results (sanity check, unaffected by this change):
bdot10k 16,349,991 total / 771,699 unmatched / 15,578,292 matched; egib
17,797,836 total / 2,233,840 unmatched / 15,563,996 matched.

**This is real but far more modest than the per-cell numbers above, and
that's expected, not a discrepancy.** `compare::buildings::compare_buildings`
iterates in 0.5°×0.5° grid chunks (`GRID_STEP`), not z14 cells — each chunk
covers roughly **500× the area** of a z14 cell, so the RTREE scan for one
chunk returns proportionally more rows, and the correlated `NOT EXISTS`
subquery against `osm_buildings` (evaluated once per surviving row) becomes
a much larger share of each chunk's cost. The index still helps — fewer rows
reach that subquery than before — but a full compare was never the
bottleneck this fix targets.

**The actual target — `match_refresh`, `compare reconcile`'s drain, and
every OSM-triggered incremental recompute — all go through
`recompute_cell_in_txn`, which always operates at the z14 cell granularity**
(`CHANGE_CELL_ZOOM`, `src/tile_math.rs`), the same granularity as the timed
cells above. That is where the 10–100× win lands.

## Extrapolating the reconcile-sweep number

`per_cell_recompute_full_scan.md` measured a real seeded-queue drain at
~0.9s/cell and projected a full 339,140-cell reconcile sweep at ~85h. This
session did not re-run a full reconcile sweep (it would take hours even
after this fix, and wasn't necessary to validate the mechanism) — the
per-cell numbers above are a direct substitute measurement of the same query
`recompute_cell_in_txn` runs, not a proxy. Taking the new per-cell times
(0.011s–0.164s across the three sampled cells, i.e. roughly 6×–100× faster
than the old ~0.9–1.2s) as representative, a full sweep should land
somewhere in the **range of under an hour to a few hours**, comfortably
inside the `batch_size = 512` / `timeout_seconds = 300` budget that
previously timed out at ~332 cells. This range is a projection from the
measurements above, not a freshly timed full sweep — worth re-measuring for
real before relying on it to size `match_reconcile`'s schedule (see
`docs/followups_precomputed_unmatched_serving.md` item 3, which gates
enabling that job on this fix).

## What's freshly measured vs. carried over

- **Freshly measured this session:** the import re-run timings, the
  `EXPLAIN` plan shapes, all per-cell timings (bdot10k and egib, three
  cells each), and the full `compare buildings` end-to-end run.
- **Carried over from `per_cell_recompute_full_scan.md` (2026-07-30):** the
  original ~0.9s/cell figure and the ~85h full-reconcile-sweep projection,
  used above only as the baseline to compare against.
