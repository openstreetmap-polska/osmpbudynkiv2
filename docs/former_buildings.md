# OSM "former building" lifecycle tags suppress government building proposals

Measured during planning, against `example_data/OSM/poland-2026-08-11.osm.pbf`
and the BDOT10k parquet from the same date.

## What this is

`compare buildings` proposes every BDOT10k/EGIB building OSM does not already
cover. Until this feature it had no way to tell "OSM has never mapped this
building" from "an OSM mapper looked, found the building gone, and recorded
that fact" — the second case is exactly where re-proposing an import is wrong,
since the government registries lag demolitions.

OSM records the second case with lifecycle-prefixed keys
(https://wiki.osm.org/Lifecycle_prefix): `demolished:building`,
`destroyed:building`, `abandoned:building`, `was:building`, `razed:building`,
`removed:building`, `disused:building`, `ruins:building`. The key list, in
priority order, lives in `osm::lifecycle::LIFECYCLE_BUILDING_KEYS` — see the
CLAUDE.md gotcha for why that ordering and the SQL/Rust duplication are
load-bearing.

Plain *values* like `building=ruins`, `building=no`, `building=collapsed` are
a different thing and were already imported into `osm_buildings` before this
change — they already counted as matches, which already suppressed the
proposal. Only the eight prefixed *keys* needed new plumbing, since
`import osm` filtered on `element_at(tags, 'building')[1] IS NOT NULL` only,
making an object tagged e.g. `demolished:building=yes` invisible.

## Object counts

15,748 ways + 12 relations nationally carry at least one lifecycle-prefixed
building key:

| key | ways | rels | | key | ways | rels |
|---|---:|---:|---|---|---:|---:|
| `demolished:building` | 12,207 | 6 | | `razed:building` | 426 | 1 |
| `was:building` | 1,983 | 0 | | `removed:building` | 223 | 0 |
| `abandoned:building` | 590 | 3 | | `ruins:building` | 184 | 1 |
| `destroyed:building` | 50 | 1 | | `disused:building` | 85 | 0 |

- 15,745 of the 15,748 ways are closed rings with `len(refs) >= 4`; the other
  3 are open ways and are skipped, exactly as they would be for
  `osm_buildings`.
- **All 15,745 build into valid polygons** — `ST_IsValid` is true for every
  one, so `osm_former_buildings` needed no invalid-geometry filtering step,
  unlike the government sources (`dataset::filter_invalid_geometry`).
- 24 ways carry **more than one** lifecycle key, which is why the key list
  needs a defined priority order shared by import and `update osm` — see
  `LIFECYCLE_BUILDING_KEYS`'s doc comment and the parity test
  (`osm::lifecycle::tests::matched_key_sql_agrees_with_key_of`).
- 378 objects (374 of them ways) carry a lifecycle key **and** a live
  `building` key at the same time — e.g. `building=service` +
  `disused:building=train_station` (a building that still stands, no longer a
  station), or `building=construction` + `demolished:building=retail` (a
  redevelopment site). These are deliberately excluded from
  `osm_former_buildings` (`AND element_at(tags, 'building')[1] IS NULL` in
  both import passes, and the same rule in `lifecycle::key_of`, which returns
  `None` when a live `building` key coexists). This makes no live difference —
  those 378 objects are already in `osm_buildings` and already match at the
  same threshold — it is purely about what the table means: without the
  exclusion, `osm_former_buildings` would assert that a standing building is
  gone, and any future consumer would inherit that error.
- Values are real building types, not just `yes`: `demolished:building=outbuilding`
  (1,863 occurrences), `=detached` (631), `=house` (520), `=yes` (2,131).

## Suppression yield

BDOT10k (pre-filtered to `KATEGORIAISTNIENIA = 'eksploatowany'`, the same
filter `compare_buildings` already applies) checked against all 15,745 former
polygons, sweeping the overlap-fraction threshold:

| overlap threshold | gov buildings vetoed |
|---|---:|
| any intersection | 6,088 |
| ≥ 2% | 4,860 |
| ≥ 5% | 4,785 |
| **≥ 10% (chosen)** | **4,718** |
| ≥ 25% | 4,588 |
| ≥ 50% | 4,431 |
| ≥ 90% | 3,969 |

Two conclusions, and they are the justification for
`FORMER_BUILDING_MIN_OVERLAP_FRACTION`:

1. **A floor is genuinely required.** 1,228 of the 6,088 bare intersections
   (20%) are under 2% of the government building's own area — party walls and
   digitization slivers against a *neighbouring* demolished building, not the
   government building itself being gone. A bare `ST_Intersects` veto would
   wrongly suppress those. This is the same failure mode
   `MIN_OVERLAP_FRACTION`'s doc comment (`src/compare/rule.rs`) already
   documents its own floor as existing to reject.
2. **The exact value barely matters.** Between 2% and 50% the vetoed count
   moves only ~9% — no elbow, the same flat curve `MIN_OVERLAP_FRACTION`'s own
   doc comment describes for the match threshold. There is no evidence for a
   value other than 0.10, which is why `FORMER_BUILDING_MIN_OVERLAP_FRACTION`
   reuses it (as its own constant, not a shared one — see the CLAUDE.md
   gotcha for why the two must stay free to move apart).

**Upper bound on effect:** ~4,718 of BDOT10k's ~771k unmatched rows, i.e.
**0.6%**. The true figure is lower, since some of those 4,718 are already
matched by a live `osm_buildings` polygon and were never in the unmatched set
to begin with. Small in volume, concentrated exactly where an import would
otherwise be wrong.

## Scan cost

A full-PBF `ST_ReadOSM` pass with the lifecycle tag filter measured **~10 s**
wall clock. Two extra passes (one for ways, one for relations) therefore add
**+20–25 s to a 22m58s `import osm` run (~1.7%)**. The
`resolve_node_coords`/`resolve_way_coords` cost is negligible next to the
existing passes: 15,745 and 12 calls respectively, versus ~18M for
`osm_buildings`.

## Query plan

With the second `NOT EXISTS` (the former-building veto) added to
`unmatched_buildings_sql`, `EXPLAIN` still yields `RTREE_INDEX_SCAN ...
Index: bsrc_centroid_idx` for the outer scoping filter, and both anti-joins
(against `osm_buildings` and against `osm_former_buildings`) plan as
`DELIM_JOIN(ANTI)` over a `SPATIAL_JOIN`. `rule::tests::unmatched_buildings_predicate_uses_the_centroid_rtree_index`
keeps passing unchanged, and the veto is index-accelerated on both sides —
`osm_former_buildings.geom` reaches its own RTREE index
(`osm_former_buildings_geom_idx`) the same way `osm_buildings.geom` does.

That holds **per grid cell**, which is how `unmatched_buildings_sql` is
always called. It does *not* hold for the whole-extent `suppressed` count —
see below.

## The whole-extent `suppressed` count (measured 2026-08-15, national data)

`compare_buildings` runs `suppressed_buildings_sql` once over the source
table's full extent rather than per 0.5° cell. On the first national run with
a populated `osm_former_buildings` (15,412 rows), that query died:

```
Error: Failed to count suppressed rows for bdot10k
Caused by: Out of Memory Error: failed to pin block of size 256.0 KiB (3.7 GiB/3.7 GiB used)
```

Note what had *not* failed: the grid loop's transaction had already
committed (486,451 `bdot10k_unmatched` rows, 114,792 `cell_totals`). Only the
statistic in the log line failed — but it returns `Err`, so `compare full`
aborted before EGIB and PRG ran at all.

**Diagnosis.** Isolating the two halves at full extent:

| clause | full extent |
|---|---|
| `EXISTS osm_former_buildings` only | 3.4 s, fine |
| `NOT EXISTS osm_buildings` only | **OOM, same error**, 71 s |

Four things compound, and only the last one is fixable:

1. The outer `bdot10k_buildings` scan drops from `RTREE_INDEX_SCAN` to
   `Sequential Scan` — see the threshold table below. **This is correct**:
   forcing the index with `SET rtree_index_scan_ratio=1.0` measured 13.4 s
   against the sequential scan's 0.53 s.
2. Both correlated subqueries de-correlate into nested `LEFT_DELIM_JOIN`s
   whose duplicate elimination hashes and materializes the correlated column
   — which is `b.geom`, the polygon blob. The join condition is literally
   `geom IS NOT DISTINCT FROM geom`, over 2.18 GB of WKB (16.0M
   `eksploatowany` rows), against a 3.7 GiB budget.
3. The plan order is inverted: the `ANTI` join (`osm_buildings`, 17,986,808
   rows) sits *below* the `SEMI` join (`osm_former_buildings`, 15,412 rows),
   so it computes Poland's entire unmatched set and then discards 99.97% of
   it.
4. Spilling doesn't help — the failing run wrote ~15 GB to the temp
   directory and still died. `max_temp_directory_size` is not the lever.

**The index is a red herring, which is the counter-intuitive part.**
`osm_buildings` gets an `RTREE_INDEX_SCAN` in the failing plan too:

```
RTREE_INDEX_SCAN
  Table:  osm_buildings
  Index:  osm_buildings_geom_idx
  Bounds: deferred (from join filter)
  ~17,986,823 rows
```

`Bounds: deferred (from join filter)` means the search window is derived at
runtime from the probe side. Per cell, that's one cell and the index prunes
18M rows down to a few thousand. At full extent the probe side is every
building in Poland, so the derived bound is Poland: the index is used, works
perfectly, and prunes nothing. An R-tree only pays in proportion to what the
query window *excludes*.

**Fix (implemented).** `suppressed_buildings_sql` filters by the veto first,
in a `candidates` CTE, and anti-joins `osm_buildings` over just those rows —
shrinking the deferred bound's probe side from ~16M geometries to ~4k:

| source | before | after | answer |
|---|---|---|---|
| bdot10k | OOM @ 71 s | **4.7 s / 3.8 GB** | 4,154 |
| egib | (never reached) | **4.9 s / 3.9 GB** | 3,803 |

bdot10k's 4,154 is consistent with the 4,718 veto yield measured above:
the difference is the rows already covered by a live `osm_buildings` polygon,
which the suppression count deliberately excludes so
`matched + unmatched + suppressed = total` stays exact.

`MATERIALIZED` is kept as insurance but is **not** the active ingredient —
bare `WITH` plans identically apart from losing the `CTE_SCAN` operator, and
runs in 5.4 s / 3.8 GB for the same answer. The CTE is what matters.

### R-tree index-scan threshold (DuckDB 1.5.5, spatial `eb1e57c`)

The spatial extension takes an R-tree scan only when the estimated match
count is at or below `max(rtree_index_scan_min_rows, rtree_index_scan_ratio ×
table_rows)` — defaults 8192 and 0.075, so 1,226,385 rows for
`bdot10k_buildings`. Measured cutover:

| window (lat 49–55) | rows matched | plan |
|---|---:|---|
| lon 14–15.33 | 525,715 | `RTREE_INDEX_SCAN` |
| lon 14–16 | 1,023,865 | `Sequential Scan` |
| full extent | 16,351,813 | `Sequential Scan` |

## DuckDB syntax notes

Verified on DuckDB 1.5.5, matching the bundled `duckdb = 1.10505.0`:

- `list_filter(<const list>, lambda k: map_contains(tags, k))[1]` and
  `list_has_any(map_keys(tags), <const list>)` both work, and behave
  correctly for a NULL `tags` map (an untagged way — the expression evaluates
  to NULL, which the caller's `WHERE` then filters out like any other NULL)
  and for an empty map.
- **Do not use the `->` lambda arrow.** 1.5.5 emits a deprecation warning for
  it; `osm::lifecycle::matched_key_sql` and `has_any_key_sql` use `lambda k:`
  instead.
- `list_intersect` also works and happens to preserve the first list's order
  in testing, but that ordering is undocumented behaviour. `list_filter` is
  documented order-preserving, which is why it — not `list_intersect` — backs
  `matched_key_sql`'s priority-order guarantee.

## Rejected alternative: insert lifecycle-tagged polygons straight into `osm_buildings`

No new table, no schema sync, no `update osm` edits, no rule change needed —
the veto and the match would become literally the same clause. Rejected for
three reasons:

1. `osm_buildings` would stop meaning "OSM has a building here" for every
   future reader of the schema.
2. The veto could no longer be tuned or reported separately from the match
   rule — `matched` (as logged by `compare_buildings`) would become
   permanently, invisibly wrong, since a suppressed row would just look like
   any other matched row with no way to recover the `suppressed` count.
3. `rebuild_way_geometry`'s inferred arm (the branch of `update osm` that
   determines tags for a way it wasn't directly told about, from existing
   DuckDB state) re-inserts a hardcoded `'yes'` for `building`. A demolished
   building whose node moved would, under this alternative, silently become a
   normal `building=yes` row instead of keeping its lifecycle tag.

Recorded here because it is the shortcut a future reader will independently
think of.

## Rollout / backfill

The in-between state — new binary deployed, `import osm` not yet re-run — is
safe and a true no-op: `osm_former_buildings` is created empty by
`db::create_schema` (`CREATE TABLE IF NOT EXISTS`), so both former-building
sub-selects in `unmatched_buildings_sql`/`suppressed_buildings_sql` find
nothing and every unmatched set stays byte-identical to before. One subtlety:
from the moment the new binary runs, `update osm` *starts* populating the
table from live edits (retags, new demolition tags), so it fills in slowly
and partially — still safe, since every row it writes is correctly tagged,
but a non-empty table must not be read as "backfill done".

The supported path to a full backfill is `import osm` (~23 minutes; it drops
and rebuilds `osm_buildings`/`osm_addresses`, clears RocksDB and resets the
replication sequence, so the service is effectively offline for the
duration), followed by `compare buildings` (or `compare reconcile` + drain).
**`import osm` alone changes nothing an editor-facing endpoint serves** — the
veto only takes effect once the following compare (or reconcile+drain) runs.

There is no `--only-former-buildings` fast-path backfill (~25 s instead of 23
minutes) — deliberately not built, to avoid new CLI surface for what is a
one-time transition. If the 23-minute window ever turns out to matter: it
cannot be done by hand in the `duckdb` CLI, because
`resolve_node_coords`/`resolve_way_coords` are Rust UDFs registered by this
binary, not built-in DuckDB functions; and a manual replay would carry a
staleness caveat of its own — objects untagged or deleted since the PBF
snapshot would linger as stale vetoes (a government building silently not
proposed), and a PBF older than the database's replication sequence would
leave an unreplayable gap.
