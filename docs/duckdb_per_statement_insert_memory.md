# Every INSERT inside a transaction holds memory until COMMIT (DuckDB 1.5.x)

Measured 2026-09-26 against DuckDB 1.5.5 (the version `Cargo.toml` pins).
Fixed on our side by writing sets in chunked statements; fixed upstream on
DuckDB `main` for 2.0.

## Symptom

OSM catch-up stalled twice in production (2026-09-23 and 2026-09-26) with
`Out of Memory Error` inside `update::osm::apply_batch`, in the
`way geometry rebuild` phase. A mass revert had modified ~104k addressed
building ways across sequences 7298226–7298244. A single heavy sequence
(~8.5k ways) needed 1.4–2.7 GiB of `IN_MEMORY_TABLE` (transaction-local
data), far more than its rows: ~150 bytes of WKB per way.

## Cause

- With `threads > 1` and `preserve_insertion_order = false` (both set in
  production's `duckdb_init_commands`), the planner gives **every** INSERT
  the parallel insert path. `PhysicalPlanGenerator::CreatePlan(LogicalInsert&)`
  in `src/execution/physical_plan/plan_insert.cpp` sets
  `parallel_streaming_insert = !PreserveInsertionOrder(...)` without looking
  at the row count.
- On that path, `PhysicalInsert::Combine` copies a small insert's rows into
  transaction-local storage but **never calls `ResetOptimisticCollection`**,
  so each statement's optimistic collection stays allocated until
  COMMIT/ROLLBACK. `PhysicalBatchInsert` does reset; this path does not.
- Cost: **18 KiB per column per INSERT statement** for BIGINT columns
  (1/2/4/8/13 columns → 18/36/72/144/234 KiB), ~26–29 KiB per column with
  VARCHAR/GEOMETRY. It is independent of row count, table size and indexes;
  an empty unindexed table shows it, and so does a temp table.
- It counts against `memory_limit` even though RSS is lower (the buffers are
  reserved, not all touched), which is what produces the error.
- DELETEs hold no transaction memory. Per-row DELETEs cost *time* instead:
  none of the OSM tables has an index on `osm_id`, so each one scans.

Upstream fix: `575fd1b97b` "Parallelize merge of small collections in
PhysicalInsert" (DuckDB `main`, 2026-08-10); small inserts now reset their
collections. It is **not** on the `v1.5-variegata` branch. PyPI
`duckdb==2.0.0.dev2609250715` holds 1 MiB where 1.5.5 holds 254 MiB in the
repro below.

Minimal repro, no spatial needed:

```python
# uv run --no-project --with duckdb==1.5.5 python repro.py
import duckdb
c = duckdb.connect()
c.execute("SET threads=8; SET preserve_insertion_order=false")
c.execute("CREATE TABLE t (id BIGINT, s VARCHAR)")
c.execute("BEGIN")
for i in range(5000):
    c.execute(f"INSERT INTO t VALUES ({i}, 'x')")
print(c.execute("SELECT sum(memory_usage_bytes) FILTER (tag='IN_MEMORY_TABLE') // 1048576 "
                "FROM duckdb_memory()").fetchone())   # 1.5.5: 254 ; 2.0.0.dev: 1
c.execute("ROLLBACK")
```

## The first measurement

8,500 addressed building ways, 3 DELETEs + 2 INSERTs each (the shape of the
old `rebuild_way_geometry`), national tables with RTREE indexes,
`memory_limit` 6 GB, `threads` 8:

| variant | IN_MEMORY_TABLE | peak RSS | wall |
|---|---|---|---|
| per-row DELETE + per-row INSERT (old code) | 2,843 MB | 1.72 GB | 18.3 s |
| per-row DELETE + one INSERT per table | 8 MB | 0.10 GB | 11.1 s |
| one DELETE + one INSERT per table | 8 MB | 0.10 GB | 0.10 s |
| multi-row `VALUES`, 500 rows/statement (17 stmts) | 8 MB | — | 0.38 s |

`SET preserve_insertion_order = true` on the writing session also removes it
(2,843 MB → 4 MB), and was rejected: it is a DuckDB setting change the
owner did not want, and it leaks onto pooled connections.

## The fix: one statement per chunk, never one per row

Inside a transaction, write a set with one statement per table per chunk.
The input goes in as a parameterized multi-row `VALUES` list with a cast on
every placeholder (`db::values_sql`), and ids for DELETEs and lookups go in
as an interpolated integer `IN` list (`db::id_list_sql`). A temp table filled
by per-row INSERTs has the same bug.

| site | before | after |
|---|---|---|
| `update::osm` node addresses, way/relation deletes and rebuilds | up to 3 INSERTs + 3 DELETEs + 7 lookups per object | the same per chunk of 1,000 (`REBUILD_CHUNK`) |
| `reports::import_rows` | 1 INSERT per report | 1 per chunk of 1,000 (`IMPORT_CHUNK`) |
| `compare::drain::drain_batch` | 2 INSERTs per cell, 512 cells per transaction | the same, 64 cells per transaction (`CELLS_PER_COMMIT`) |

Every per-object *decision* in `update::osm` stayed where it was; only the
SQL moved. The relation inserts now assemble several relations in one
statement, grouped by relation id, with the unions ordered by the member's
position so the result does not depend on the plan.

### Results

Tests, all on the parallel insert path (`db_memory::use_parallel_insert_path`),
measured inside the open transaction:

| test | per-row (old) | chunked |
|---|---|---|
| `update::osm` `a_mass_rebuild_holds_no_per_row_transaction_memory` (300 addressed ways, 50 address nodes, 1 relation) | 118 MiB | 7 MiB |
| `reports` `importing_hundreds_of_reports_holds_no_per_row_transaction_memory` (300 reports) | 72 MiB | 1 MiB |

`db_memory::tests::per_row_inserts_in_one_transaction_hold_memory_until_commit`
is the negative control: it asserts the bug is still visible. When a DuckDB
upgrade makes it fail, the fix has arrived and the regression tests next to
it have become vacuous. Nothing broke.

Differential replay on real data: two copies of the local stores at OSM
sequence 7263772, the same local mirror of the feed, production-like
`duckdb_init_commands` (`memory_limit` 6 GB, `threads` 8,
`preserve_insertion_order = false`), the pre-change and the new release
binary. In both phases, `osm_buildings`, `osm_addresses` and
`osm_former_buildings` came out identical (`EXCEPT ALL` empty both ways,
geometry compared as WKB), as did the set of enqueued cells and the stamp.
The mass revert was applied onto the state a month earlier (the stamp moved
by hand; both binaries got the same input), and 19 pending sequences stay
under `batch_commit_threshold`, so each sequence ran in its own transaction:
the shape of production's temporary `batch_size = 1`.

| replay | binary | wall | peak RSS |
|---|---|---|---|
| 1,000 ordinary minutes (7263773–7264772), 20 per transaction | before | 133 s | 0.94 GB |
| | after | 19.9 s | 1.57 GB |
| then the mass revert (7298226–7298244) on top, one sequence per transaction | before | 1,598 s | 4.28 GB |
| | after | 57.6 s | 2.08 GB |

**The higher peak RSS on ordinary minutes is buffer pool, not transaction
memory.** A 1,000-id `IN` list is planned as a MARK hash join, so the scan
reads `geom` for every row of the type rather than only the matching ones:
one such lookup on `osm_buildings` leaves 817 MiB of `BASE_TABLE` blocks
cached, where 1,000 separate `osm_id = ?` lookups leave 511 MiB and take
five times as long. Those blocks are unpinned, evictable and bounded by
`memory_limit`; they cannot raise an `Out of Memory Error`. Neither
`list_contains` (511 MiB, 6× slower) nor `= ANY(...)` (same plan as `IN`)
was worth switching to.

### The match drain

`drain_batch` recomputes up to 512 cells in one transaction, two INSERTs per
cell (`<source>_unmatched`, 15/9/13 columns, and `cell_totals`, 4). Measured
with `compare::drain::tests::a_full_batch_s_transaction_memory_on_real_data`
on a copy of the national database, 512 cells per source:

| source | 512 cells (old: one transaction per batch) | 64 cells (now: one transaction per group) |
|---|---|---|
| bdot10k | 171 MiB | 23 MiB |
| egib | 123 MiB | 16 MiB |
| prg | 143 MiB | 19 MiB |

The per-cell SQL was deliberately left alone. It is shaped around RTREE plans
(CLAUDE.md's `candidates` CTE and `MATERIALIZED` gotchas), and a multi-cell
rewrite would put those at risk. Committing every 64 cells is correct for the
same reason cancellation already committed at any cell boundary: each cell's
recompute carries its own queue delete. A failure replays only the failed
group.

## Rules

- Inside a transaction, never issue one INSERT per row. Chunk the set and
  write it with one statement per table.
- A memory regression test needs `SET GLOBAL preserve_insertion_order = false`
  and `threads > 1`, or it passes vacuously; the test default hides the bug.
  Read `IN_MEMORY_TABLE` before COMMIT/ROLLBACK, since the memory is released
  at transaction end.
- Revisit on the DuckDB 2.0 upgrade: the negative control will say when.
