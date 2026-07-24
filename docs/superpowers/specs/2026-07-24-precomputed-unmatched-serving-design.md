# Precomputed Unmatched Sets as the Serving Path

## Summary

Turn the comparison output — today written by the `compare` CLI and read by
nothing — into the data both `/tiles` and `/package` serve. Instead of every
request recomputing "which government object has no OSM match" live, the server
maintains three precomputed **unmatched** tables and keeps them current as OSM
and the government datasets change.

The problem this creates, and the one the whole design exists to bound, is
**staleness → duplicate imports**. Once nothing does a live anti-join, an OSM
building that someone just mapped stays in the precomputed unmatched set until
that area is recomputed. A JOSM user who imports it makes a duplicate. OSM
edits arrive every minute, so "how fresh is the unmatched set" is a correctness
question, not bookkeeping.

The chosen answer: a **dirty-cell queue**. Both sides of a change (OSM diffs
and government refreshes) enqueue the z14 cells they touch; a short-interval
background job drains the queue, recomputing each dirty cell's unmatched set
from scratch. Recompute is local and total — a cell is a pure function of
current live data — so a recomputed cell cannot be subtly wrong, and the only
failure mode (a dropped enqueue) is repaired by a daily reconciliation sweep.

This document supersedes the "Re-running `compare` after a refresh" and
"Serving the changeset over HTTP" items left out of scope in
`2026-07-22-dataset-incremental-updates-design.md`.

## Goals

1. **`/tiles` and `/package` serve the same precomputed unmatched set.** One
   match definition, one code path, no risk of the two endpoints disagreeing
   about what is importable.
2. **Bounded duplicate window.** After an OSM edit lands, the affected area's
   unmatched set is corrected within roughly one drain interval (target: tens
   of seconds), not at the next full recompute.
3. **Observable staleness.** `/status` reports how far behind the recompute is
   and, implicitly, where — as queue depth, not a single global timestamp.

## Non-goals

- **Object-level match maintenance.** Considered (approach B below) and
  rejected: it requires storing ~34M match edges and does spatial work inside
  every minutely OSM diff, with silent-wrong failure and no repair path.
- **A shadow-table full recompute on a timer** (approach C): ~9 min per pass
  can't approach tens-of-seconds freshness, and the table swap needs a rename
  that fails while an RTREE index exists (proven in the prior design, line 38).
- **Changing the match rules themselves.** The address rule (housenumber
  equality within 50 m) and building rule (OSM building contains the
  government centroid) are carried over unchanged.
- **Low-zoom aggregation / clustering** for tiles below z14. Independent work
  (`docs/project_ideas.md`); this design serves z14 only, as today.

## Approaches considered

**A — dirty-cell queue drained by a worker (chosen).** Producers enqueue z14
cell ids; a `match_refresh` job recomputes each dirty cell wholesale from
current state. Correctness is local and needs no reasoning about which edit
invalidated which match. Writes are per-cell `DELETE`+`INSERT`, so no
`DROP`/`CREATE` window and no rename — the RTREE and reader-visibility problems
never arise. Staleness is queue depth. `compare` stays a fast full recompute
for bootstrap; the job is the online incremental path.

**B — object-level inline maintenance.** Flip affected rows inside each OSM and
government transaction. Zero lag, but deletions force storing the match edge
(~34M rows, 10× the unmatched set) kept consistent through every edit; the
minutely job does spatial work inline and can fall behind; errors are silent.
Rejected.

**C — short-interval full recompute into a shadow table.** Trivially correct,
no new concepts, but ~4.5 min (BDOT10k) + ~4.3 min (EGIB) per pass and blocked
by the RTREE rename limitation. Rejected.

## Sizing

From `docs/import_time.md` (real Poland-wide runs):

| Source | Total objects | Unmatched | Full recompute |
|---|---|---|---|
| BDOT10k | 16.3M | 746k | ~4.5 min |
| EGIB | 17.8M | 2.30M | ~4.3 min |
| PRG | 8.55M | 514k | ~9 s |

Only the unmatched side is materialized (~3.5M rows total), against ~34M
objects. Materializing match *edges* for all 34M — approach B — is an order of
magnitude more state.

## Data model

Three schema changes, all idempotent `CREATE TABLE IF NOT EXISTS` in
`src/db.rs::create_schema()`, consistent with the rest of the schema. As with
the prior feature there is no migration machinery: the dev database is dropped
and recreated when the schema changes.

### Serving tables (replace the `*_comparison` tables)

`bdot10k_unmatched`, `egib_unmatched`, `prg_unmatched`. Each stores **only
unmatched rows** plus a cell key and a recompute timestamp:

- the source's serving columns (see below),
- `cell_x INTEGER`, `cell_y INTEGER` — the z14 cell the row's representative
  point falls in (`cell_z` is always `CHANGE_CELL_ZOOM`, so it is not stored),
- `computed_at TIMESTAMP WITH TIME ZONE` — when this cell was last rebuilt.

"Matched count", the only derived figure the current log lines report, is
`COUNT(live) − COUNT(unmatched)`.

Serving columns, taken from what the endpoints read today:

- **Addresses (`prg_unmatched`):** `geom`, `lokalny_id`, `numer_porzadkowy`,
  `ulica`, `miejscowosc`, `kod_pocztowy`, `teryt_miejscowosc`. This is the
  union of what the two endpoints read: `/package`'s `AddressRow` needs
  `numer_porzadkowy`/`ulica`/`miejscowosc`/`kod_pocztowy`/`teryt_miejscowosc`
  for JOSM tags; `/tiles`' address layer selects `lokalny_id`,
  `numer_porzadkowy`, `miejscowosc`.
- **Buildings (`bdot10k_unmatched`, `egib_unmatched`):** `geom` and the id
  column (`LOKALNYID` / `id_budynku`) `/tiles` labels features with;
  `/package` reads geometry only.

**Rows are stored, not references back to the live table.** Two reasons, both
of which actually bite:

- BDOT10k ids are not unique (the prior baseline: 16,256,082 rows for
  16,256,079 distinct `LOKALNYID`), so an id semi-join back to the live table
  can pull a *matched* duplicate in alongside its unmatched twin.
- DuckDB rowids are not stable across the government refresh's `DELETE`+`INSERT`,
  so they cannot substitute for a stored key either.

Storing rows also means serving is a plain filtered scan with no join.
Attribute drift is not a new risk: any government change to a row enqueues its
cell, which rewrites the row.

### Dirty-cell queue

```sql
CREATE TABLE IF NOT EXISTS match_dirty_cells (
    source      VARCHAR,   -- 'bdot10k' | 'egib' | 'prg'
    cell_z      INTEGER,   -- always tile_math::CHANGE_CELL_ZOOM
    cell_x      INTEGER,
    cell_y      INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE
);
```

- **Duplicates are allowed**, deduped on drain (`SELECT DISTINCT`). Every
  producer is then a plain `INSERT` inside a transaction it already holds — no
  upsert, no read-before-write.
- **Keyed per source.** An OSM *building* edit cannot affect PRG and an OSM
  *address* edit cannot affect the building sources. Without `source`, every
  OSM node edit would needlessly recompute 17M-row EGIB cells.

## Producers

### Government refresh — no new machinery

`update::changeset::insert_change_areas` already computes, inside the apply
transaction, the exact distinct z14 cell set a refresh touches. One additional
`INSERT INTO match_dirty_cells SELECT DISTINCT source, z, cell_x, cell_y, now()`
from the same subquery, in the same transaction, and the government side is
done. (This is why "build an OSM-side change feed" was the only real blocker
for the old item 4 — the government feed already exists.)

A government object that moves dirties its own before- and after-cells, which
is exactly where it lands after rematching — no neighbourhood expansion needed
on this side.

### OSM update — the one genuinely new piece

As `update::osm` applies a minutely diff it records the z14 cell of each object
it touches — the cell **before** a delete and **after** an insert, so a moved
object dirties both. Building changes enqueue for `bdot10k` and `egib`; address
changes enqueue for `prg`.

**OSM producers enqueue the 3×3 neighbourhood of the touched cell, not just the
cell itself.** When an OSM building appears, it can newly-*match* a government
object whose representative point sits in a *neighbouring* z14 cell (the match
rule reaches across the cell boundary — 50 m for addresses, centroid-contained
for buildings that straddle an edge). Recomputing only the OSM object's own
cell would leave that neighbour's now-matched government object still listed as
unmatched. This mirrors the 3×3 grid-key expansion already in
`compare::addresses`.

## The match rule: one shared home

The full recompute (`compare`) and the incremental recompute (`match_refresh`)
must agree exactly on what "unmatched" means, or a full `compare` produces a
different set than the job maintains — the same class of invariant as
"import and update must hash a row identically". So the match rule lives in
**one place**, parameterized by a spatial restriction, the way `hashed_select`
is the single home for the row hash.

- **Buildings:** a government building is unmatched iff no `osm_buildings`
  polygon contains its centroid.
- **Addresses:** a government address is unmatched iff no `osm_addresses` point
  has an equal normalized (`UPPER(TRIM(...))`) housenumber within 50 m;
  NULL housenumbers never match.

Under this design `/package`'s own live anti-joins
(`server/package.rs::unmatched_addresses` / `unmatched_buildings`) are
**removed** — the endpoint reads `*_unmatched` clipped to the request polygon
instead. So the codebase goes from two hand-maintained encodings of the rule
(compare + package) to one shared encoding used by both recompute paths. Net
simplification.

The iteration *strategy* around the shared predicate legitimately differs by
path and that is intended:

- full building compare chunks through 0.5° grid cells to bound R-tree memory;
- full address compare does one country-wide hash join with the grid-key trick;
- incremental recompute operates on a single z14 cell, small enough to need
  neither device.

Only the predicate is shared; the chunking is a per-path performance choice.

### Buffered read, exact write

A government object near a z14 edge can be matched by an OSM object in the
neighbouring cell. So recompute **reads OSM with a small buffer** around the
cell (the `MATCH_BUFFER_DEG` envelope expansion `/package` already uses) but
**writes back only** government rows whose representative point falls strictly
inside the cell. Read wide, write narrow. Without this, edge objects flap
between matched and unmatched depending on which neighbour was recomputed last.

## `compare` CLI — unchanged in spirit

`compare` remains a **synchronous full recompute**, run offline. DuckDB's
single-writer-process rule means the CLI cannot run while the server holds the
database, so a full recompute never has concurrent readers — its `DROP`+`CREATE`
of the serving tables is safe precisely because it is offline. This is why the
old item 3 ("compare can't be a background job — its `DROP`+`CREATE` has a
reader-visible empty window") is resolved by *not* making `compare` a job, and
adding a purpose-built incremental job instead.

`compare` now writes `*_unmatched` (unmatched rows, tagged with z14 cell and
`computed_at`) instead of `*_comparison`. It is the bootstrap path for an empty
database and the fast manual full rebuild. Its output must be row-identical to
draining an enqueue-all through the incremental path (pinned by a test, below).

## The `match_refresh` job

A new `MatchRefreshJob` implementing the existing `Job` trait, registered in
`server/mod.rs` with a `[jobs.match_refresh]` config block (`enabled`,
`interval_seconds`, `timeout_seconds`). Default interval in the tens of seconds
— it is the freshness knob, and a tick is cheap when the queue is near-empty.
It runs under the same supervisor that guarantees no per-job overlap, so a slow
drain cannot stack on itself.

Producers (OSM job, three dataset jobs) and this consumer are independent jobs
on independent timers; a producer commits dirty cells and the next
`match_refresh` tick picks them up, needing no coordination. `match_refresh`
recomputes small per-cell buffers, not 16M-row stages, so it stays **outside**
the `refresh_lock` that serializes the memory-heavy dataset jobs.

### A tick drains a bounded batch

1. **Snapshot a batch** with a cutoff:
   `SELECT DISTINCT source, cell_x, cell_y FROM match_dirty_cells
    WHERE enqueued_at <= :batch_start ORDER BY enqueued_at LIMIT :batch_size`.
2. **Per cell, one transaction:** recompute the cell's unmatched rows via the
   shared rule (buffered read, strict-inside write); `DELETE`+`INSERT` that
   cell's slice of `<src>_unmatched`; `DELETE FROM match_dirty_cells` the
   entries for that cell **with `enqueued_at <= :batch_start`**.
3. **Stop** at `batch_size` or an exhausted batch; the next tick continues.

Two ordering rules are load-bearing and unenforced by the schema, exactly like
the "change areas before the delta" discipline in the prior feature:

- The `enqueued_at <= :batch_start` cutoff on **both** the read and the queue
  delete is what lets a concurrent OSM diff re-dirty a cell *while it is being
  recomputed* without that edit being deleted unread. A cell re-enqueued after
  `batch_start` survives to the next tick.
- The serving-table rewrite and the queue delete for a cell commit in the
  **same** transaction, so the queue can never say "clean" for a cell whose
  rewrite rolled back.

Per-cell transactions keep each commit tiny (safe against live readers, no
window) and make cancellation trivial: the loop polls `ctx.is_cancelled()`
between cells, and each cell is already its own atomic unit.

### Reconciliation sweep

A daily `match_reconcile` tick enqueues every cell containing a government
object and lets the normal drain rebuild them. It is the safety net against a
dropped enqueue and the post-schema-change rebuild path. Because it rides the
per-cell drain, it never drops a serving table — unlike the offline `compare`
CLI, it is safe to run against the live server. Whether it is a separate job or
a mode of `match_refresh` is an implementation detail.

## Staleness on `/status`

`/status` gains a `match_refresh` block answering "are we keeping up, and if
not, how far behind and where":

- `pending_cells` — total and per source (`SELECT source, COUNT(DISTINCT
  (cell_x, cell_y)) FROM match_dirty_cells GROUP BY source`),
- `oldest_enqueued_at` — the front of the queue; the current worst-case
  duplicate window,
- `last_drained_at` — when the job last made progress.

A single global "last compared at" is deliberately rejected: it cannot express
that one cell is an hour stale while the rest is current. Queue depth can. No
staleness column is added to the serving tables — a row's `computed_at` says
when its cell was last rebuilt, and pending-queue depth says what has not been.
`dataset_refreshes.snapshot_id` still records the government baseline; the queue
records the live OSM delta on top of it.

## Testing

Unit (in-memory DuckDB, following the `#[cfg(test)]` convention in
`compare/` and `update/`):

- **Shared-rule equivalence (the central invariant):** on a fixture, a full
  `compare` and an enqueue-all-then-drain through the incremental path produce
  **row-identical** `*_unmatched` sets, for each source.
- **Buffered read / narrow write:** a government object one metre inside a cell,
  matched by an OSM object just across the boundary, is correctly excluded; and
  recomputing the neighbour cell does not rewrite this cell's rows.
- **3×3 OSM enqueue:** an OSM building inserted in cell C newly-matches a
  government object in an adjacent cell, and that adjacent cell is recomputed
  (its object leaves the unmatched set).
- **Re-dirty during drain:** a cell enqueued after `batch_start` is not deleted
  by the in-flight tick and is drained on the next one.
- **Atomic cell rewrite:** a reader polling `<src>_unmatched` across a cell
  rewrite sees the old slice or the new slice, never an empty or half-written
  cell (mirrors `readers_never_observe_a_partial_apply`).
- **Per-source keying:** an OSM address edit enqueues only `prg`; an OSM
  building edit enqueues only `bdot10k`/`egib`.
- **Government producer:** a dataset refresh with adds/moves/removals enqueues
  exactly the touched cells, in the same transaction as the delta.

Integration (`tests/`, `assert_cmd` + file-backed DB, matching the existing
`cli_update_*` tests): import → `compare` populates `*_unmatched`; a subsequent
`update osm` diff enqueues cells; a `match_refresh` drain (invoked via a small
CLI hook or by driving the job directly) corrects the affected cell.

## Build order

1. Schema: `*_unmatched` tables, `match_dirty_cells`; retire `*_comparison`.
2. Extract the shared match rule + the shared z14 cell-assignment SQL (today
   duplicated between `tile_math::lonlat_to_tile` and `changeset.rs`), pinned by
   a test against the Rust version.
3. Repoint `compare` to write `*_unmatched`; repoint `/tiles` and `/package` to
   read them (remove `/package`'s live anti-joins).
4. Government producer: enqueue in `insert_change_areas`' transaction.
5. OSM producer: record touched cells (3×3) as the diff applies.
6. `match_refresh` job: batched drain, per-cell transactions, cutoff ordering.
7. `/status` staleness block.
8. Reconciliation sweep.

Steps 1–3 are a coherent first slice: they move serving onto precomputed tables
with no freshness guarantee yet (equivalent to today plus a rename), and are
independently testable before any producer or job exists.

## Out of scope

- Serving `dataset_change_areas` or the dirty queue over HTTP beyond the
  `/status` summary.
- Periodic RTREE rebuild (unchanged from the prior design — scheduled
  maintenance, measure before designing).
- Low-zoom tile aggregation/clustering.
