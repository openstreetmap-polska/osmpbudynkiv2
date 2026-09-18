# RTREE index memory crowds out the daily refreshes

Written 2026-09-18. Follows `docs/prg_arrow_batch_leak.md`: once that leak was
fixed, total process memory levelled off (4.7–4.8 GiB from day 2 of the
v0.2.3 run), but the dataset refreshes started failing inside DuckDB.

## Symptom

| run | prg | egib | bdot10k |
|---|---|---|---|
| 2026-09-14 (day 1 after restart) | ok | ok | ok |
| 2026-09-15 | ok | ok | **failed** |
| 2026-09-16 | **failed** | **failed** | **failed** |
| 2026-09-17 | **failed** | **failed** | **failed** |

Every failure was
`Out of Memory Error: could not allocate block of size 256.0 KiB (3.7 GiB/3.7 GiB used)`
at the staging step's duplicate-key scan (`dataset::deduplicate_by_key`). Each
refresh ran alone, since `refresh_lock` serializes them. The temp directory was
empty with 12 GB allowed, so the memory in use was memory DuckDB could not
spill. A `tile_refresh` block failed with the same error on 09-17.

The failures reached only `/status` (`jobs[].last_outcome`), never the journal.
The supervisor now logs `job failed` at ERROR.

## Cause

**DuckDB cannot evict index memory, and the RTREE indexes load lazily, a
node at a time, as queries touch them.** A restarted server holds almost none
of the indexes. Serving `/tiles` from all over Poland loads more of every index
each day, and none of it is ever released. `memory_limit` stays fixed, so the
room left for everything else shrinks, until a staging load no longer fits.

The DuckDB 2.0 highlights post (Aug 2026, "Storage Format v2.0") confirms it:
ART index buffers are not buffer-managed yet, and making them evictable is
planned for "later this year". Spatial's RTREE uses the same allocator, and its
memory shows up under the `ART_INDEX` tag of `duckdb_memory()`.

## Reproduction (local copy of the 2026-09-06 database, prod's settings)

`memory_limit = '4GB'`, `threads = 3`, `preserve_insertion_order = false`.
The load step was `CREATE TABLE stg AS SELECT * FROM bdot10k_buildings`,
followed by the exact duplicate-key scan.

| session | before the load | result |
|---|---|---|
| cold | 3 MiB | OK: peak 3.2 GiB, all `BASE_TABLE` (evictable) |
| one small-bbox query per index (11 indexes) | `ART_INDEX` 10 MiB | OK |
| a 0.25° grid of queries over Poland on the 7 big indexes (6,888 queries) | `ART_INDEX` **2.37 GiB** | **the same OOM as prod** |

While the grid ran, `ART_INDEX` only ever grew, and `BASE_TABLE` was evicted
from 2.2 GiB down to 0.75 GiB to make room for it. Only index memory is pinned.
Production's database is larger (17 GB vs 9 GB) and has taken daily delta
inserts, so its fully loaded indexes are likely at least this big.

## What this implies

- `memory_limit` has to cover the resident indexes (≈2.4 GiB and up) *plus*
  the largest staging load (≈3.2 GiB), or the refreshes fail on whichever day
  the indexes get warm enough.
- A restart "fixes" it for a day or two, because the indexes start cold again.
- Once DuckDB makes index buffers evictable, the problem should go away on its
  own after upgrading.

`/status` now reports `duckdb_memory` per tag. Each dataset job logs
`DuckDB memory before/after refresh`, so `ART_INDEX` can be followed day by
day on production.
