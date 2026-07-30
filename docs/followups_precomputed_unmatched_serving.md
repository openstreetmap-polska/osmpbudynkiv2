# Follow-ups — precomputed unmatched serving

Written 2026-07-30, after merging `precomputed-unmatched-serving` into `main`
(merge commit `633eb3a`, feature commits `2a32ab5..2483f36`). Suite green at
257/257, clippy `--all-targets` and `fmt --check` clean.

These are the items the final whole-branch review raised that were deliberately
**not** fixed before merge. Nothing here is a known-broken build — each is a
real gap with a bounded fix. Ordered by value.

Design doc: `docs/superpowers/specs/2026-07-24-precomputed-unmatched-serving-design.md`
Plan: `docs/superpowers/plans/2026-07-24-precomputed-unmatched-serving.md`

---

## Status as of 2026-07-30 (investigation pass on a Poland-scale DB)

A full import + `compare full` now exists on this machine, which unblocked the
measurements items 1 and 3 were waiting on.

| Item | State |
|------|-------|
| 1. Serving-table indexes | **Done.** Conclusion changed by measurement: an index alone is a no-op, so the fix is index **+** query rewrite. Implemented and verified end-to-end. |
| 2. `osm.rs` de-tag gap | **Fixed** (working tree, uncommitted) + 2 regression tests. |
| 3. No periodic reconcile | **Sized** (measured, below). Not yet implemented. |
| 4. Drain-vs-refresh concurrency | **Tested — assumption confirmed.** Test added (working tree, uncommitted). |
| 5. Smaller gaps | Untouched. |
| **6. 23 tiles return HTTP 500** | **NEW — found during item 1.** Pre-existing, unrelated to this feature. See below. |

Suite after the above: **262 passed, 0 failed**; clippy `--all-targets` and
`fmt --check` clean. All changes are in the working tree, uncommitted.

### Baseline facts from the fresh DB

`compare full`: **14m40s** wall, 7.5 GB peak RSS. DuckDB file 9.1 GB, RocksDB
8.0 GB.

| source | total | unmatched | share | full-compare time |
|--------|-------|-----------|-------|-------------------|
| bdot10k | 16,349,993 | 771,700 | 4.7% | 6m41s |
| egib | 17,795,392 | 2,270,931 | 12.8% | 7m53s |
| prg | 8,603,851 | 554,885 | 6.4% | **4.8s** |

The address grid-key path is ~100× faster than the per-cell building path. That
asymmetry is the design's deliberate "iteration strategy differs by path"
choice (see the match-rule gotcha in CLAUDE.md), and it is much larger than the
doc implied — worth remembering before anyone "unifies" the two paths.

---

## 1. Serving tables have no indexes — read-path regression

> **MEASURED 2026-07-30 — the original diagnosis was right, but the proposed
> fix was incomplete. Adding an RTREE index alone changes nothing, because the
> query as written cannot use one.**
>
> **Finding A — the index is unusable by the current query.** With RTREE
> indexes present on all three serving tables, the `/tiles` MVT query still
> plans as `SEQ_SCAN` + `SPATIAL_JOIN` over every row. The reason is the query
> shape, not the index: the bbox arrives as a one-row CTE that is *joined*
> against the table (`FROM bdot10k_unmatched, bbox WHERE ... .geom && bbox.geom`),
> and DuckDB's RTREE scan optimizer only fires for a spatial predicate against
> a **constant** argument. Verified by `EXPLAIN`:
>
> | predicate form | plan |
> |---|---|
> | `geom && bbox.geom` (CTE join — current code) | `SEQ_SCAN` + `SPATIAL_JOIN` |
> | `ST_Intersects(geom, ST_MakeEnvelope(<literals>))` | `RTREE_INDEX_SCAN` |
> | `ST_Intersects(geom, ST_MakeEnvelope(?,?,?,?))` prepared | `RTREE_INDEX_SCAN` |
>
> The prepared-parameter form works, so the server keeps its `?` params.
>
> **Finding B — index + rewrite is a large win.** Median of 3 reps per tile,
> 587 MB scratch DB holding only the three serving tables:
>
> | layer | tile | before | after | speedup |
> |---|---|---|---|---|
> | addresses | Warszawa z14/9148/5394 | 22 ms | 1 ms | 22× |
> | addresses | Gdańsk | 21 ms | 4 ms | 5.2× |
> | buildings | Łódź | 72 ms | 26 ms | 2.8× |
> | buildings | Warszawa | 71 ms | 21 ms | 3.4× |
> | buildings | Kraków | 71 ms | 17 ms | 4.2× |
> | buildings | rural (Wielkopolska) | 60 ms | 1 ms | 60× |
> | buildings | rural (Podlasie) | 60 ms | 2 ms | 30× |
>
> Note the *shape* change, not just the magnitude: before, cost was flat
> (60–72 ms) whether the tile was central Warsaw or empty countryside — the
> signature of a full scan. After, cost tracks the number of features actually
> returned. The current worst case is paid on **every tile, everywhere**.
>
> **Finding C — the write-cost fear does not materialize.** Simulated drain
> batch of 512 per-cell `DELETE`+`INSERT` round trips, one transaction per cell,
> exactly as `recompute_cell_in_txn` does:
>
> | variant | per cell |
> |---|---|
> | no index | 1.89 ms |
> | RTREE on `geom` | 1.91 ms (+1%, noise) |
> | btree on `(cell_x, cell_y)` | 1.84 ms (−3%, noise) |
>
> **Finding D — no cumulative RTREE degradation.** After **15,000** cell
> recomputes (3 rounds over 5,000 cells) against the indexed table, read
> latencies were unchanged within ±1 ms across all 14 tile/layer probes, and the
> row count was intact (771,700). The prior design's RTREE-churn worry does not
> reproduce at this workload. File grew ~11% (676 → 753 MB); DuckDB does not
> reclaim in place.
>
> **Finding E — `(cell_x, cell_y)` index is unnecessary.** The recompute
> `DELETE` predicate already runs in **1–3 ms** unindexed, because `compare`
> writes rows in cell order and DuckDB's zone maps prune on it. The doc called
> this "the safer of the two"; it is simply not needed. 512 cells × ~2 ms ≈ 1 s
> per drain tick against a 30 s interval.
>
> **Correctness of the rewrite — verified, not assumed.** `&&` is a
> bounding-box test and `ST_Intersects` is exact, so they are not interchangeable
> in general. Here they are: `ST_AsMVTGeom` returns NULL for a feature that does
> not truly intersect the tile, and the outer `WHERE t.geom IS NOT NULL` already
> discards it. Feature counts were compared across 7 tiles × 2 tables (dense
> city through empty countryside) — **identical on all 14**, so the rewrite
> filters earlier without changing what is served.
>
> **IMPLEMENTED 2026-07-30** (working tree, uncommitted), both halves together:
>
> 1. `CREATE INDEX IF NOT EXISTS ... USING RTREE (geom)` on all three serving
>    tables in `src/db.rs::create_schema`. Confirmed against the real 9.1 GB DB:
>    the server built all three at startup with no measurable delay, and they
>    persist.
> 2. `src/server/tiles.rs` filters rewritten to
>    `ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?))`, with the `bbox` CTE kept
>    only as `ST_AsMVTGeom`'s BOX_2D bounds argument. The bbox is now bound three
>    times in the buildings query (CTE + one per UNION branch) and twice in the
>    addresses query; the call site comments the groups.
>
> **`src/server/package.rs` needed no change** — it was *already* written as
> `ST_Intersects(geom, ST_MakeEnvelope(<literals>))`, with a comment explaining
> that a constant predicate enables an R-tree index scan. It had been waiting on
> an index that was never created, so step 1 alone speeds `/package` up.
>
> **End-to-end, real DB, whole HTTP request** (median of 5, server with all
> background jobs disabled):
>
> | tile | request | tile bytes |
> |---|---|---|
> | Łódź z14/9077/5429 | 21.1 ms | 37,609 |
> | Warszawa z14/9148/5394 | 18.4 ms | 26,819 |
> | Kraków z14/9099/5551 | 12.8 ms | 24,936 |
> | Ełk z14/9209/5273 | 11.3 ms | 11,162 |
> | Gdańsk z14/9040/5233 | 9.7 ms | 14,021 |
> | rural Podlasie | 4.0 ms | 1,251 |
> | rural Wielkopolska | 2.0 ms | 79 |
>
> For comparison the *SQL alone* previously cost 85–111 ms per tile, so the
> whole request now costs less than the old query did.
>
> **Equivalence proven on real data, not just argued.** For 65 real z14 tiles ×
> 3 serving tables, the old `&&`-against-CTE form and the new form were run
> through the same `ST_AsMVTGeom` + `IS NOT NULL` pipeline and their **id sets**
> compared symmetrically: 8,008 features each, **identical on every pair**.
>
> **Regression guards.** Two tests, because either half alone is a silent no-op:
> `db::tests::test_init_db_creates_serving_table_rtree_indexes` pins the index,
> and `server::tiles::tests::mvt_bbox_filter_uses_the_rtree_index` asserts on the
> query *plan* containing `RTREE_INDEX_SCAN`. The latter was confirmed to fail
> when the predicate is reverted to `geom && bbox.geom` — without it, that
> reversion would keep every other test green while restoring a full table scan
> on every tile request.
>
> Reproduction scripts are in this session's scratchpad (`gen_bench.py`,
> `run_bench.sh`, `gen_churn.py`, `verify_equiv.py`); they are throwaway, not
> checked in.

### Original writeup

**Highest value.** Before this feature, `/tiles` and `/package` read
`bdot10k_buildings` / `egib_buildings` / `prg_addresses`, all RTREE-indexed at
import (`src/import/*.rs` are the only `CREATE INDEX` sites in the tree). They
now read `bdot10k_unmatched` / `egib_unmatched` / `prg_unmatched`, which
`src/db.rs::create_schema` creates with **no index at all** — so
`src/server/tiles.rs:28,42,46` and `src/server/package.rs:335,370` do sequential
scans where there used to be an index probe. Every tile request runs both the
address and building MVT queries.

Separately, `recompute_cell_in_txn`'s `DELETE FROM {dest} WHERE cell_x = ? AND
cell_y = ?` (`src/compare/incremental.rs`) full-scans the serving table **once
per cell** — 512 scans per drain tick at the default `batch_size`.

**Why it wasn't just fixed:** the prior dataset-refresh design records RTREE
degradation under churn, and these tables are rewritten per-cell continuously by
`match_refresh`. Adding an RTREE could trade a read win for a write cost that
compounds. This needs numbers, not a reflex.

**What to do:**
1. Rebuild a Poland-scale DB. Source data is in `example_data/`; per
   `docs/import_time3.md` the OSM pass alone is ~24 min, plus the three
   government datasets and a `compare full`. Budget ~40+ min and ~10 GB.
   (No such DB existed on this machine as of 2026-07-30.)
2. Measure `/tiles` latency at a few zooms, and drain-tick duration, before/after.
3. Candidates: `CREATE INDEX IF NOT EXISTS ... USING RTREE (geom)` on all three
   serving tables, plus an index on `(cell_x, cell_y)` for the recompute DELETE.
   The `(cell_x, cell_y)` index is the safer of the two — it helps the write path
   rather than fighting it.
4. If RTREE churn does turn out to be the problem, the prior design's periodic
   RTREE-rebuild idea is the fallback.

---

## 2. `osm.rs` de-tag gap — a government object stays suppressed

> **FIXED 2026-07-30** (working tree, uncommitted). The early return was
> removed from both `rebuild_way_geometry` and `rebuild_relation_geometry`; the
> `note_existing` calls and DELETEs now always run, and the re-inserts are
> skipped by their existing `is_some()` guards, so falling through simply
> deletes and stops.
>
> One thing worth recording, because it made the fix safe to apply as sketched:
> the early return was reachable **only** from the directly-affected branch (a
> changeset that carries the object's tags). The indirectly-affected branch
> already returns earlier when the row is absent, so if it fell through,
> `building_tag` or `housenumber` was always `Some`. The de-tag case was
> therefore the *only* traffic through that return — removing it changes nothing
> else.
>
> Two regression tests added in `src/update/osm.rs`
> (`test_apply_way_modify_stripping_tags_removes_row_and_enqueues` and the
> relation equivalent). Both were confirmed to fail against the unfixed code
> with the predicted symptom (stale row count 1, expected 0) before the fix
> landed.

Code-only, no data needed. In `src/update/osm.rs`, `rebuild_way_geometry` and
`rebuild_relation_geometry` **early-return before their DELETEs** (~`:374` and
~`:478`) when a `Modify` strips every building/address tag from a way or
relation. Two consequences: a stale row survives in `osm_buildings` /
`osm_addresses`, **and** no dirty cell is enqueued.

Net effect: the OSM object is gone, but the government object it was matching
stays out of the served unmatched set until the next `compare full` or
`compare reconcile`. Exactly backwards — the editor removed the OSM building, so
the government building should reappear as missing.

**Confirmed pre-existing** at the merge-base (`git show 2a32ab5:src/update/osm.rs`
has the identical early return at `:362` / `:460`). This feature did not
introduce it, but it widened the blast radius from "a stale row only the offline
compare reads" to "a government object suppressed from what editors see."

**Fix sketch:** move the `note_existing` calls and the DELETEs *above* the early
return, and skip only the re-inserts. Test: a Modify that strips all tags must
(a) remove the base row and (b) enqueue the cell.

---

## 3. No periodic reconcile — the safety net needs a human

> **SIZED 2026-07-30** (not implemented). The doc's estimate of "roughly 140k
> z14 cells × 3 sources" was close but high. Measured against the real DB, a
> full `enqueue_all` sweep enqueues:
>
> | source | distinct z14 cells |
> |---|---|
> | bdot10k | 115,162 |
> | egib | 111,719 |
> | prg | 112,259 |
> | **total** | **339,140** |
>
> At the default `batch_size` 512 / `interval_seconds` 30 → 61,440 cells/h, a
> full sweep occupies the drain for **~5.5 hours**. With a 24 h reconcile
> interval that is a ~23% duty cycle, which leaves headroom but is not
> negligible — and during those 5.5 h the queue's `oldest_enqueued_at` will read
> as badly stale even though nothing is wrong, which will confuse `/status`
> unless it is expected.
>
> Two knobs make this comfortable if it turns out to matter: the measured
> per-cell recompute cost is only ~2 ms (Finding C above), so the drain is
> nowhere near saturated at 512/30 s — roughly 1 s of work per 30 s tick. Raising
> `batch_size` is cheap. Worth deciding the default deliberately rather than
> inheriting 512/30 s from the incremental-only workload.

The design (line 282) argues that the only real failure mode — a dropped
enqueue — is self-repairing *because* a daily `match_reconcile` tick re-enqueues
everything. The plan softened that to "callable from a daily job if desired,"
and only the CLI path shipped. The CLI can't run while the server holds the DB
(DuckDB is single-writer), so today the safety net requires stopping the server.

**Fix:** register `compare::reconcile::enqueue_all` as a scheduled job next to
`match_refresh` in `src/server/mod.rs`'s `job_list`, with its own
`[jobs.match_reconcile]` config block (default interval ~24h). It rides the
existing per-cell drain, so it never drops a serving table — that's precisely
why the design considers it safe against a live server, unlike the CLI.

**Note the interaction with item 1's sizing:** a full reconcile enqueues roughly
140k z14 cells × 3 sources. At `batch_size` 512 / 30 s that is ~61k cells/h, so
a full sweep occupies the drain for hours. Worth confirming the default interval
and batch size leave headroom before scheduling it unattended.

---

## 4. The drain-vs-refresh concurrency assumption is untested

> **TESTED 2026-07-30 — assumption confirmed** (working tree, uncommitted).
> `compare::drain_refresh_concurrency::drain_and_dataset_refresh_do_not_collide`
> in `src/compare/mod.rs` drives `drain_batch` in a loop on one thread while 12
> `refresh()` cycles run against `bdot10k_buildings` on another, then asserts the
> queue converges and the serving table matches a full `compare_bdot10k`.
>
> The review's hand-traced conclusion holds: **no error from either side**, and
> the drain is not starved. Instrumented during development, it completed 6
> batches of 16 cells at a steady **276–372 ms** each across the refresh window —
> flat latency, no stall, no lock convoy, no write-write abort.
>
> One caution for whoever revisits this: an early instrumented run looked like
> the drain was blocked (0 cells drained across the first two refreshes). It
> wasn't — the refresh window was simply shorter than one drain batch. The test
> now runs 12 refresh cycles so the overlap is real, and asserts ≥2 productive
> batches rather than ≥1, so it fails loudly if the drain ever *does* get
> serialized behind the refresh.

The whole design rests on `match_refresh` not colliding with the dataset-refresh
writer. Both share one DuckDB instance: `src/server/mod.rs:36-63`'s
`ClonedConnectionManager` hands out `try_clone()`s of a single base connection,
and nothing serializes them — `refresh_lock`
(`src/server/jobs/dataset_update.rs:15`) only serializes the three dataset jobs
against each other, and `match_refresh` deliberately sits outside it (design
line ~230).

I traced this during review and found **no write-write abort path**: the only
table both touch is `match_dirty_cells`, and the overlap is
append-vs-delete-of-different-rows, which DuckDB's optimistic CC doesn't treat as
a conflict. The refresh's uncommitted queue rows are invisible to the drain, so
they can't be deleted unread. But that's analysis, not evidence.

**Fix:** a test shaped like the existing `readers_never_observe_a_partial_apply`
— drive `drain_batch` in a loop on one thread while `refresh()` runs on another,
assert no error and no partially-visible cell.

---

## 6. 23 z14 tiles return HTTP 500 on invalid government geometry

Found 2026-07-30 while verifying item 1. **Pre-existing and unrelated to this
feature** — it reproduces identically with the old `&&` predicate. 199 invalid
government polygons make `ST_AsMVTGeom` throw a GEOS `TopologyException`, and
one bad row aborts the whole tile query.

Written up in full, with scope, the list of failing tiles, and three remedies
(including why `ST_MakeValid` alone is the wrong one), in
**`docs/invalid_geometry_tile_500s.md`**. Not fixed.

---

## 5. Smaller gaps

- **No e2e for the OSM producer path.** Unit tests cover `apply_changes` logic;
  nothing exercises `parse_osc → apply_sequence → commit → drain` end to end.
  The Task 14 smoke test substituted `reconcile` for the `update osm` leg
  because the fixture was stale.
- **Thin reconcile test.** `enqueue_all`'s test uses one row per source and zero
  for egib, so it never covers the DISTINCT-collapse, the NULL-geometry skip, or
  the egib centroid path.
- **`cell_x_sql` / `cell_y_sql`** each recompute `pow(2, Z)` in
  `src/tile_math.rs`; the "only home" doc note sits on `cell_x_sql` alone and
  belongs at module level.
- **`/status` has no `last_drained_at`** (design line 298). Arguably covered by
  `jobs[].last_finished_at` for `match_refresh` — if so, say that somewhere
  rather than leaving a silent gap against the spec.
- **`oldest_enqueued_at` is biased by long refreshes.** DuckDB's `now()` is
  transaction-*start*-scoped (verified empirically during review), so a
  5-minute BDOT10k refresh stamps all its dirty cells with its BEGIN time and
  the staleness metric reads ~5 min worse than reality just after a refresh.
  Cosmetic — the cutoff logic is snapshot-based and unaffected — but worth a
  sentence in the CLAUDE.md gotcha.

---

## Context worth keeping: what the review caught that the plan got wrong

Two of the merged fixes restored requirements the **implementation plan had
silently dropped from the design doc**. Useful precedent when reading the plan
for any remaining work — it is not a faithful transcription of the design:

- Design line 258 specifies `ORDER BY enqueued_at` for the drain's selection.
  The plan wrote `ORDER BY source, cell_x, cell_y`, which is a strict
  alphabetical source priority: under sustained backlog **no `prg` cell drains
  at all** until the building sources finish, and `oldest_enqueued_at` stops
  meaning anything. Fixed in `d0ac267` (`GROUP BY ... ORDER BY MIN(enqueued_at)`).
- Design line 277 requires the drain loop to poll `ctx.is_cancelled()` between
  cells. The plan omitted it entirely. Fixed in `d0ac267`.
- Design lines 236 and 312-314 make "full `compare` and enqueue-all-then-drain
  produce **row-identical** serving tables" the central correctness contract.
  The plan reduced it to an address-only grid-key test — a different pair of
  paths. The real test now lives in `src/compare/mod.rs`
  (`full_vs_incremental_equivalence`), and writing it immediately exposed a real
  divergence: full building compare was clamped to a hardcoded Poland bbox while
  the incremental path had no clamp, so the two disagreed on any row outside it.
  Fixed in `1e37dd5`, and the extent derivation it introduced is capped in
  `2483f36` (one valid WGS84 outlier at (-180,-90) would otherwise turn Poland's
  264 grid cells into 259,200, one query each).
