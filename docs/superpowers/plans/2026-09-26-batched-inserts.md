# Plan: stop per-row INSERTs from holding transaction memory (DuckDB 1.5.x)

Status: ready to execute. Written 2026-09-26 for a fresh agent session.
Read this whole file before touching code; the "Gotchas" sections are the
part a fresh session cannot rediscover cheaply.

## 1. Background: what was measured

Production OSM catch-up stalled twice (2026-09-23 and 2026-09-26) on
`Out of Memory Error` inside `update::osm::apply_batch`, during the
`way geometry rebuild` phase. A mass revert had modified ~104k addressed
building ways across sequences 7298226–7298244, and one heavy sequence
(~8.5k ways) needed 1.4–2.7 GiB of `IN_MEMORY_TABLE` (transaction-local
data), far more than the rows themselves (~150 B of WKB per way).

Root cause, measured with DuckDB 1.5.5 against a copy of the national DB:

- With `threads > 1` and `preserve_insertion_order = false` (both set in
  production's `duckdb_init_commands`), the planner gives **every** INSERT
  the parallel insert path (`plan_insert.cpp`: the choice ignores row
  count).
- For a small insert, `PhysicalInsert::Combine` copies the rows into
  transaction-local storage but **never calls `ResetOptimisticCollection`**,
  so each statement's optimistic collection stays allocated until
  COMMIT/ROLLBACK. `PhysicalBatchInsert` does reset; this path forgot to.
- Cost: **18 KiB per column per INSERT statement** for BIGINT columns
  (1/2/4/8/13 columns → 18/36/72/144/234 KiB), ~26–29 KiB per column with
  VARCHAR/GEOMETRY. Independent of row count, table size, and indexes (an
  empty unindexed table shows it too). Temp tables show it too.
- It counts against `memory_limit` even though RSS is lower (buffers are
  reserved, not all touched), which is what produces the OOM errors.
- Fixed on DuckDB `main` by `575fd1b97b` "Parallelize merge of small
  collections in PhysicalInsert" (2026-08-10; small inserts now reset their
  collections). **Not** on the `v1.5-variegata` branch. PyPI
  `duckdb==2.0.0.dev2609250715`: 1 MiB where 1.5.5 shows 254 MiB.
- DELETEs cost no transaction memory (they cost *time*: per-row DELETEs
  were 11 of 18 s in the measurement below).

Realistic measurement (8,500 addressed building ways; 3 DELETE + 2 INSERT
each, as `rebuild_way_geometry` does; national tables with RTREE indexes;
memory_limit 6 GB, threads 8):

| variant | IN_MEMORY_TABLE | peak RSS | wall |
|---|---|---|---|
| per-row DELETE + per-row INSERT (current code) | 2,843 MB | 1.72 GB | 18.3 s |
| per-row DELETE + one INSERT per table | 8 MB | 0.10 GB | 11.1 s |
| one DELETE + one INSERT per table | 8 MB | 0.10 GB | 0.10 s |
| multi-row `VALUES`, 500 rows/statement (17 stmts) | 8 MB | — | 0.38 s |

Scripts and the scratch copy live in `target/memexp/` (gitignored; the 12 GB
`db.duckdb` there is a copy of the local DB at OSM sequence 7263772 — reuse
it or delete it).

## 2. Decision

**Fix it in our code by issuing far fewer INSERT statements per
transaction.** Rejected alternatives:

- `SET SESSION preserve_insertion_order = true` on the writing connection
  (measured: 2,843 MB → 4 MB). The owner prefers not to change DuckDB
  settings; also leaks onto pooled connections. **Do not use.**
- `SET threads = 1` — database-wide, kills query parallelism. Do not use.
- Waiting for DuckDB 2.0.0 — the owner plans to switch once it is released
  (a major bump of the duckdb-rs git pin, `cmake/duckdb_version.cmake`, and
  the spatial extension), but the release date is unknown and production is
  hitting this now. The work here stays useful after the upgrade: the
  set-based writes were also ~150× faster (0.10 s vs 15.6 s for 8,500 ways),
  a gain that has nothing to do with the bug. Not in this plan: the upgrade
  itself, and any request for a 1.5.x backport.
- Patching DuckDB locally — the source comes from the duckdb-rs submodule;
  carrying a fork is not worth it for a one-line upstream fix.

## 3. Inventory: where per-row INSERTs run inside a long transaction

Only INSERTs *repeated inside one open transaction* accumulate. Single
statements in autocommit (job_log, metadata, POST /report) and set-based
`INSERT … SELECT` over a whole set are fine. Re-derive this list with
`grep -rn "INSERT INTO" src/` rather than trusting it.

| site | txn | statements per txn | est. cost | priority |
|---|---|---|---|---|
| `update::osm::apply_collapsed_phases` → node address path (`osm_addresses`, 7 cols) | `apply_batch` | 1 per address node | ~180 KiB each | **P1** |
| `update::osm::rebuild_way_geometry` (`osm_buildings` 4, `osm_former_buildings` 5, `osm_addresses` 7) | `apply_batch` | up to 3 per way | ~335 KiB per addressed way | **P1** |
| `update::osm::rebuild_relation_geometry` (same three tables) | `apply_batch` | up to 3 per relation | same | **P1** |
| `compare::drain::drain_one_cell` → `incremental::recompute_cell_in_txn` (`*_unmatched`: bdot10k 15, egib 9, prg 13 cols) + `compare::totals::recompute_cell_in_txn` (`cell_totals`, 4 cols) | `drain_batch`, 512 cells | 2 per cell | est. 200–300 MiB per batch, **unmeasured** | **P2: measure first** |
| `reports::import_rows` (`object_reports` 9 cols + `match_dirty_cells` 5 cols) | one txn for the whole import | 2 per report | ~350 KiB per report | P3 |

Already fine (do not touch): `update::dirty_cells::DirtyCells::insert_cells`
(one multi-row `VALUES` per source — the in-repo precedent), `import osm`
passes (set-based over `ST_ReadOSM`), dataset refresh apply (set-based),
mapping loaders (staging + one `INSERT … SELECT`), `compare buildings/addresses`
full paths (one INSERT per 0.5° grid cell, a few hundred per run — small).

## 4. Shared validation harness (build this first)

### 4.1 The measurement primitive

Add a test-support helper (e.g. in `src/db_memory.rs`, which already reads
`duckdb_memory()`), returning the `IN_MEMORY_TABLE` bytes. Measure **inside
the open transaction, before COMMIT/ROLLBACK** — the memory is released at
transaction end, so a post-commit reading shows nothing.

### 4.2 Test DB settings — the gotcha that makes every test vacuous

The unit-test DBs (`init_db(":memory:", &["INSTALL spatial", "LOAD spatial"], …)`)
leave `preserve_insertion_order` at its default `true`, **which hides the
bug completely**. Every memory regression test must first run:

```sql
SET GLOBAL threads = 4;
SET GLOBAL preserve_insertion_order = false;
```

### 4.3 Negative control (mandatory)

Add one test that does N=300 single-row `INSERT … SELECT` into a 4-column
table inside a transaction, with the settings above, and asserts
`IN_MEMORY_TABLE` **exceeds** ~15 MiB (expected ~21 MiB). It proves the
harness can see the bug. Name and document it so that when DuckDB is
upgraded to a build with the fix and this control starts failing, the reader
knows the regression tests next to it have become vacuous and can be
revisited — not that something broke.

### 4.4 Bounds, not exact numbers

Assert upper bounds with wide margin (e.g. "under 16 MiB for 300 ways"
where the per-row baseline would be ~100 MiB). Never assert exact bytes.

## 5. Task P1 — set-based writes in `update osm`

### 5.1 Design

Keep every *decision* in Rust exactly where it is; batch only the SQL.
`apply_collapsed_phases` already has a phase structure. Inside the node,
way-rebuild and relation-rebuild phases, replace "for each object:
read → delete → insert → read" with "for the whole set, in chunks:
read all → delete all → insert all → read all":

1. **Collect** the object ids to rebuild (already `affected_way_ids` /
   `affected_relation_ids`), skipping those `kvstore::get_way` says are gone,
   as today.
2. **Determine tags** per object: from the collapsed change if present,
   otherwise from stored rows. `stored_rebuild_tags` becomes one batched
   query over the chunk; it **must still run before any DELETE** and must
   return stored values, never placeholders (CLAUDE.md,
   "`update osm` must maintain `osm_former_buildings`…", point 4).
3. **Note old cells**: batch `note_existing` into one query per table
   returning `(osm_id, x_lo, x_hi, y_lo, y_hi)` per row, then run the
   existing per-row Rust logic (the `MAX_ENQUEUE_CELLS_PER_ROW` guard and its
   warning) on each returned row. Keep one row per stored row — no
   `DISTINCT`, no aggregation.
4. **Delete**: one `DELETE … WHERE osm_type = ? AND osm_id IN (…)` per
   table per chunk.
5. **Unresolved check** in Rust: `unresolved_way_members(kv, &ids)` already
   takes a slice. Keep the per-object `WARN Ignoring way/…` messages naming
   every missing id, and a relation is skipped whole.
6. **Insert**: one `INSERT … SELECT` per table per chunk, from a
   parameterized multi-row `VALUES` list of `(id, tag columns…)`, keeping
   the exact same select expressions and WHERE guards per row
   (`resolve_way_coords`, `ST_NPoints >= 4`, `ST_IsClosed`,
   `repaired_geom_sql` paired with `has_polygon_sql`). Sketch:

   ```sql
   INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
   SELECT s.id, 'way', s.building, <repaired_geom_sql(ring of s.id)>
   FROM (VALUES (?::BIGINT, ?::VARCHAR), (?::BIGINT, ?::VARCHAR), …) s(id, building)
   WHERE resolve_way_coords(s.id) IS NOT NULL AND … AND <has_polygon_sql(…)>
   ```

   Extend `way_ring_polygon_sql` / the geometry builders to accept a column
   reference (`s.id`) instead of a literal or `?` — they already take a
   `way_ref` string for exactly this kind of reuse.
7. **Note new cells** with the batched query from step 3.
8. Chunk size: a named constant (start with 1,000 objects per statement).
   Any chunk size gives the memory win; it only bounds statement size.

Recommended input form: **parameterized multi-row `VALUES`** (as sketched),
not interpolated literals and not a temp table:
- it is typed, and removes the `building.replace('\'', "''")`
  interpolation the way path uses today;
- a temp table filled by per-row INSERTs has the same bug (measured), and
  one filled by the `Appender` works but adds per-pooled-connection
  lifecycle (it outlives the job on that connection) and the question of
  how the Appender participates in the open transaction — avoid unless
  VALUES proves inadequate.

### 5.2 Gotchas specific to P1

- **CLAUDE.md invariants that the rewrite must keep**, all under
  *"`update osm` applies each object's last version only"*, *"an OSM object
  at the extract's edge is ignored"*, *"invalid OSM geometry is repaired"*,
  and the former-buildings gotcha. Re-read those sections before starting.
  In particular: the inferred-rebuild early return must consider the former
  key; a de-tagged object still deletes and notes its old cell; an object
  that becomes unresolvable is removed and its cell enqueued.
- **Order across phases is unchanged**: all node KV writes happen before
  the way rebuild; way KV writes before relation rebuild. Relation rebuild
  reads member geometry via `resolve_way_coords` (KV), not from
  `osm_buildings`, so batching the way phase does not change what the
  relation phase sees. Verify this is still true when you get there.
- **`resolve_way_coords` will now run on many rows in one statement, so
  DuckDB may call it from several threads at once.** It is a `VScalar`
  holding `Arc<RocksDB>` (`src/osm/udf.rs`); confirm it has no
  `RefCell`/thread-local assumptions. RocksDB reads are thread-safe.
- **NULL typing in `VALUES`**: cast every placeholder (`?::VARCHAR`), and
  test a chunk where a column is NULL in every row (e.g. no `addr:street`
  anywhere).
- **Empty geometry**: an inline `repaired_geom_sql` without `has_polygon_sql`
  in the same statement's WHERE yields `MULTIPOLYGON EMPTY`, whose
  `ST_XMin` is NULL and breaks the next `note_existing`. Keep the pairing.
- **Error context**: today a failure says `way {id}`. A chunk failure should
  name the chunk's first and last id and the phase. The phase wrapper in
  `apply_collapsed` stays.
- **Collapse guarantees one change per id**; a duplicate id inside a chunk
  would be a bug upstream of this code. Keep the `debug_assert`s.
- **Crash/replay safety** is unchanged as long as the KV calls stay the
  idempotent get-modify-put ones (`apply_batch`'s doc comment). Don't
  introduce merge operators.
- **Prepared statements**: the text varies with chunk length; use plain
  `prepare`, or `prepare_cached` only for the full-chunk size.
- **Test chunk boundaries**: sets of `CHUNK-1`, `CHUNK`, `CHUNK+1` objects.

### 5.3 Validation for P1

1. **Existing tests pass unchanged.** `cargo test --bin osmpbudynkiv2
   update::osm` first (the crate has no lib target, so `cargo test --lib`
   fails), then the full `cargo test`. The `update::osm` module has the
   guards named in CLAUDE.md (`a_batch_collapses_positions_that_were_never_committed`,
   `a_way_edited_twice_in_one_diff_is_served_with_its_last_version_s_tags`,
   `a_way_whose_missing_node_arrives_in_a_later_diff_stays_ignored`,
   `osc_xml_straddling_cell_boundary_updates_the_neighbouring_cells_serving_table`,
   `replaying_a_batch_over_a_partially_written_kv_store_converges_to_the_golden_state`,
   …). A test you had to edit is a behaviour change: stop and justify it.
2. **Memory regression test** (harness from §4): a batch modifying ~300
   addressed building ways plus some address nodes and a relation, applied
   inside an open transaction, `IN_MEMORY_TABLE` under a wide bound. Check
   it fails against the pre-change code (stash, run, unstash) — a memory
   test that never failed proves nothing.
3. **Differential replay on real data** (the strongest correctness check).
   Goal: old and new binaries apply the same real diffs to identical
   starting states and produce identical tables.
   - Build the pre-change binary from `HEAD` before editing
     (`cargo build --release`, copy it to `target/memexp/before`), and the
     new one after.
   - Two starting copies of **both** stores (`osmpbudynkiv2.duckdb` and
     `osmpbudynkiv2.rocksdb/`, ~16 GB per copy; ~286 GB free on
     `/mnt/nvme`). Copy only while no process has them open. The local DB is
     at sequence 7263772 (2026-09-03).
   - A local replication mirror: download sequences 7263773..7264772
     (1,000 sequences; each `NNN.osc.gz` plus `NNN.state.txt`, paths per
     `osm::replication::sequence_to_path`/`sequence_state_path`) from
     `https://download.openstreetmap.fr/replication/europe/poland/minute/`,
     write a `state.txt` pinned to 7264772, serve the directory with
     `python3 -m http.server`, and point `[download_urls] osm_replication` in a
     scratch config at it. Pinning the head makes both runs stop at the
     same sequence.
   - Scratch configs identical except paths, with production-like
     `duckdb_init_commands`: `memory_limit 6GB`, `threads 8`,
     `preserve_insertion_order = false` (otherwise the old binary won't
     show the memory difference). Run `update osm` with each binary under
     `/usr/bin/time -f "wall=%es peak_rss=%MKB"`.
   - Compare with the DuckDB CLI: `ATTACH` both, and for `osm_buildings`,
     `osm_addresses` and `osm_former_buildings` require both
     `A EXCEPT ALL B` and `B EXCEPT ALL A` to be empty (compare `geom` via
     `ST_AsWKB`). For `match_dirty_cells`, compare the sets of
     `(source, cell_z, cell_x, cell_y)` (`enqueued_at` differs by design).
     `metadata`'s sequence must match.
   - Report wall time and peak RSS for both runs.
4. **Heavy-batch check (optional but recommended).** An `#[ignore]`d test,
   driven by env vars pointing at a *copy* of the local stores, that builds a
   synthetic collapsed change set modifying ~8,500 addressed building ways
   (tags from `osm_buildings`/`osm_addresses`, node refs from KV), runs it
   through `apply_collapsed` inside a transaction, reads `IN_MEMORY_TABLE`,
   then rolls back. Expect single-digit MiB versus ~2.8 GB before.

## 6. Task P2 — the match drain: measure, then decide

### 6.1 Measure first

Harness test with the §4.2 settings: seed `bdot10k`/`prg` data over ~512
cells (or use an `#[ignore]`d test on a copy of the real DB), open a
transaction, run `drain_one_cell` for 512 queued cells, read
`IN_MEMORY_TABLE`, roll back. Record the number in the commit message and
in `docs/`. If it is under ~64 MiB, stop and just document it.

### 6.2 If it matters: commit in sub-batches, don't rewrite the queries

Set-based multi-cell recompute is the wrong fix: the per-cell SQL is
carefully shaped around RTREE plans (CLAUDE.md, *"the same index loss has a
second, unrelated trigger"*, and the `MATERIALIZED` gotcha), and a
multi-cell rewrite would put all of that at risk. Instead, commit every K
cells (e.g. 64) inside `drain_batch`. CLAUDE.md already establishes this as
correct: *"Cancellation commits rather than rolling back … committing at any
cell boundary is correct."*

Gotchas for this:
- `batch_start` stays a single value for the whole batch, used for both
  the read and every queue-delete (*"the drain's cutoff is load-bearing"*).
- `enqueue_tiles_for_cells` must run **in each sub-transaction for exactly
  that sub-transaction's cells** — a committed recompute whose tile enqueue
  was lost is a permanently stale tile.
- The failure path replays one cell at a time. With sub-commits, only the
  failed sub-batch's cells need replaying; earlier sub-batches are already
  committed and must not be replayed or double-counted.
- `compare::drain_refresh_concurrency` and
  `osm_apply_batch_and_match_refresh_drain_do_not_collide` must still pass.
- Make K a named constant, not config, unless there's a measured reason.

## 7. Task P3 — `reports::import_rows`

Offline CLI (`reports import`), one transaction for the whole file, two
INSERTs per report. Convert to chunked multi-row `VALUES` for
`object_reports`, and a single set-based `INSERT INTO match_dirty_cells …
SELECT` over the chunk. Validation: the existing export→import round trip
(`server::jobs::backup::tests::a_dump_round_trips_back_through_reports_import`)
and the `reports` tests pass unchanged; there is no dedicated `import_rows`
test yet, so add one that imports rows with NULL optional fields and checks
content and enqueued cells, plus a memory-bound test with ~300 reports; ids are
still reallocated from the current maximum (a documented property).

## 8. Documentation to update

- `CLAUDE.md`: one concise gotcha — "In DuckDB 1.5.x each INSERT statement
  inside a transaction holds ~18–29 KiB per column until commit (parallel
  insert path with `preserve_insertion_order = false`); inside a
  transaction, write sets with one statement per table (chunked), never one
  per row. Tests need `SET GLOBAL preserve_insertion_order = false` and
  `threads > 1` to see it." Point at the doc below and the negative control.
- `docs/duckdb_per_statement_insert_memory.md`: the measurements from §1,
  the source references (`plan_insert.cpp`, `PhysicalInsert::Combine`, the
  upstream fix commit), and the before/after numbers you measured.
- Doc comments at the new batched helpers, in the repo's style (the reason
  and the measurement, not a restatement of the code).

## 9. Working rules for this session

- Commit only your own changes. `example_config.toml` has the owner's
  uncommitted edits (`enabled = false` on several jobs) — never stage it
  wholesale; if you need a hunk there, stage it alone
  (`git apply --cached`). Don't push. End commit messages with the
  attribution line from the session's system reminder.
- Don't touch the production server. Production keeps its temporary
  `[jobs.osm_update] batch_size = 1` until a release with P1 is deployed.
- The machine has 31 GB RAM. Don't run the differential replay, a release
  build and the full test suite at the same time. Kill processes by PID,
  never `pkill -f` with a pattern that appears in your own command line.
- `cargo fmt -- --check` and `cargo clippy` clean before each commit.
- One commit per task (P1, P2, P3), each with its measurements in the message.

## 10. Definition of done

- P1 merged: existing tests green; memory regression test present, and
  shown to fail on the old code; negative control present; the differential
  replay shows identical tables and lower peak RSS; docs updated.
- P2: measured and either documented as not needed, or sub-commits
  implemented with the gotchas above covered by tests.
- P3 done or explicitly deferred with a reason.
- Summary for the owner: the numbers, and anything that behaved differently
  than this plan predicts.

## Appendix — minimal repro (no spatial needed)

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
                "FROM duckdb_memory()").fetchone())   # 1.5.5: 254 ; 2.0.0.dev2609250715: 1
c.execute("ROLLBACK")
```
