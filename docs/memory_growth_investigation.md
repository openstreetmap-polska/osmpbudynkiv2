# Production memory growth: ~9.7 GB RSS + 6 GB swap after 4 days

Written 2026-09-09. Open investigation — the cause is narrowed but not proven.
This file records the measurements so the next round starts from evidence
instead of from the same three guesses.

Deployment: v0.1.2 (commit `5cfabfc`), `run` under systemd, started
`Sat Sep 5 17:30:30 2026`, sampled ~4 days 4 hours later.

---

## The numbers

```
VmHWM      9,993,168 kB      Threads: 34
VmRSS      9,740,356 kB
Anonymous  9,744,220 kB      Pss_File: 24,338 kB
Private_Dirty 9,738,324 kB
Referenced 6,347,892 kB
Swap       6,043,504 kB      (SwapPss identical -- all private)
```

Total anonymous commitment is therefore **~15.8 GB**, against a DuckDB
`memory_limit` of 4 GB. RSS sits just under `VmHWM`, so this is the high-water
mark, not a post-spike decline. The box is swapping 6 GB, which is its own
operational problem independent of the growth.

`Pss_File` of 24 MB rules out anything mmap-shaped — no file-backed growth, no
mapped database. It is all private anonymous heap.

## Ruled out: RocksDB

Near-idle across the entire ~20 h `LOG` window: 1.09M cumulative writes,
0.06 GB ingested, 0.02 GB compacted, 3.6 s of compaction CPU, zero write stalls.
Three block caches at 32 MB each — and those are RocksDB's silent defaults, not
the configured 512 MB, which is a separate bug written up in
`docs/rocksdb_block_cache_not_applied.md`. Total RocksDB footprint is on the
order of 100 MB of a 9.7 GB RSS.

This kills the "table-reader memory grows with SST count" hypothesis that the
investigation opened with: there are barely any SST files.

## Ruled out: DuckDB spilling

`max_temp_directory_size = '12GB'`, and the temp directory is **4.0 K**. DuckDB
is staying inside its 4 GB limit and not spilling, so the buffer manager is not
in a thrashing state that would explain untracked overhead. (Database file:
15 G. WAL: 15 M.)

## The signature: jemalloc arena retention

`pmap -x` is the informative measurement. Only **298 mappings total**, and
~9.5 GB of the 9.74 GB RSS lives in about **21 anonymous regions**:

```
     VIRT (kB)    RSS (kB)   DIRTY (kB)
       7371264      404260      398980
       4271104     2278764     2278764
       3845120     2645676     2645676
       3049472      815816      815816
       1384448      375344      375344
       1338368      502368      500920
        906240      620032      616948
        685568      516776      516480
        462336      360104      357716
        ...
```

Two things to read off this:

- **Huge virtual extents with partial residency.** 7.4 GB of address space
  holding 404 MB of pages; 3.0 GB holding 816 MB. That is what a jemalloc arena
  looks like — a large reserved extent, sparsely dirty — and not what a single
  runaway allocation or a leaking `Vec` looks like.
- **The memory is spread across ~21 of them, not concentrated in one.**
  Individual arenas are carrying 2.6 GB and 2.3 GB of dirty pages. jemalloc
  never migrates memory between arenas, so a thread that frees into arena *n*
  cannot satisfy an allocation bound to arena *m*; each arena's high-water mark
  is retained independently.

`Referenced` (6.3 GB) sitting well below `Rss` (9.7 GB), together with 6 GB
swapped out, says a large fraction of those pages have not been touched
recently — consistent with retained-but-free arena pages rather than a live
working set.

**There are two jemallocs in this process** and both contribute arenas: DuckDB's
prefixed `duckdb_je_*` build, and `rust-rocksdb`'s unprefixed one whose
`malloc`/`free` override glibc's and therefore back the entire Rust heap. See
CLAUDE.md's jemalloc gotcha. Neither is currently configured, so both run at
jemalloc's defaults, including a default `narenas` of 4x the CPU count.

Contributing to arena spread: 34 threads, `db_pool_size = 8` (eight `try_clone`d
DuckDB connections that genuinely run spatial queries in parallel), DuckDB
`threads = 3`, and RocksDB's `max_background_jobs` at `available_parallelism()`.

## What this does *not* yet establish

Arena retention and a genuine slow leak look identical in `pmap` — a leak's
allocations live in arenas too. The measurements above establish *where* the
memory is held and rule out the storage engines as the holder; they do not
establish whether the underlying allocations are live or freed.

The discriminator is behavioural, not another snapshot: constrain the allocator
and see whether growth flattens.

## Next step (in progress)

Set in the systemd unit (see `example.service`, which now carries this):

```ini
Environment=MALLOC_CONF=narenas:4,background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:0
```

- `narenas:4` caps arena count, directly attacking per-arena high-water
  retention.
- `background_thread:true` purges asynchronously instead of on the allocating
  thread, so the decay settings do not land as request-path latency.
- `dirty_decay_ms:5000` halves jemalloc's 10 s default so dirty pages return to
  the OS sooner. `muzzy_decay_ms:0` is jemalloc's own default (purge
  immediately) and is stated explicitly only so a later edit does not quietly
  relax it -- raising it would *delay* returning muzzy pages, the opposite of
  what is wanted here.

`MALLOC_CONF` is read from the environment by both jemalloc builds, so one
variable configures both.

**If growth flattens over the following days, it was retention** and the config
line is the fix. **If it does not, it is a real leak** and the next step is
`heaptrack` against a staging copy of the database — note that profiling the
Rust-side allocations needs a build with the RocksDB `jemalloc` feature off (or
jemalloc rebuilt with `--enable-prof`), since the shipped one has no profiling
compiled in.

The systemd unit also gained `MemoryAccounting` so the next occurrence is
graphed rather than reconstructed from `/proc`. On the ceiling settings, read
"What the first restart taught us" below **before** copying anything: the first
attempt at those took the site down.

## Leading hypothesis for *what* allocates

Unconfirmed, listed so the heaptrack run knows where to look first. `/package`
is the one endpoint that builds a large GeoJSON body wholly in memory, is
`no-store` so no cache ever absorbs a repeat, and is bounded only by
`max_area_sq_deg = 0.04` (~14 x 22 km). Repeated multi-hundred-megabyte
transient allocations across many threads is precisely the workload that
ratchets jemalloc RSS upward and never returns it. `/tiles` at z14 allocates far
less per request but far more often, and its 256 MB in-process cache is
bounded and accounted for.


---

## What the first restart taught us (2026-09-09, ~20:06)

Two separate things went wrong on the restart that applied the `MALLOC_CONF`
and cgroup settings. Neither is the memory leak, and both are worth not
rediscovering.

### 1. A dirty WAL broke every index-bound query -- duckdb-spatial #861

Startup was clean (all tables present, sane row counts, HTTP listening), then
the first z14 tile query failed:

```
ERROR osmpbudynkiv2::server::tiles: tile query failed
  error=INTERNAL Error: Operation requires a flat vector but a non-flat vector
  was encountered
  ... entire stack inside spatial.duckdb_extension ...
  z=14 x=8952 y=5285
```

This is **[duckdb-spatial#861](https://github.com/duckdb/duckdb-spatial/issues/861)**,
fixed by **[PR#862](https://github.com/duckdb/duckdb-spatial/pull/862)**. The
match is exact on all four discriminators: the error string, DuckDB v1.5.5 with
the spatial extension, failure on index-bound queries after a reopen, and a
stack in `RTreeIndex::Append` -> `Insert` -> `VerifyFlatVector`. The bug is in
`ConvertToEntries`, which reads row IDs as a flat vector when buffered WAL
replay hands it a dictionary vector built over a selection vector.

**This is not WAL corruption.** The on-disk data is fine; the replay *code path*
mishandles a vector layout. Do not reach for `import full` on seeing it -- and
if you ever do, run `reports export` first, since `object_reports` is the one
table no import can rebuild (CLAUDE.md).

**Why this deployment is unusually exposed.** The bug needs a WAL containing
inserts into a *pre-existing* RTREE index at an uncheckpointed close. This
server produces that continuously: `osm_buildings`, `osm_addresses`,
`osm_former_buildings` and all three `*_unmatched` serving tables carry RTREE
indexes on `geom`, and two enabled jobs write to them constantly -- `osm_update`
inserting replication diffs every 60 s, and `match_refresh` doing
DELETE-then-INSERT per drained cell every 30 s. The WAL measured 15 MB.

**The causal chain back to the memory problem is the point.** The old process
was at 9.7 GB RSS with 6 GB swapped out. Shutting that down means faulting
pages back in, which took longer than `TimeoutStopSec=60`, so systemd
`SIGKILL`ed it -- skipping the checkpoint, leaving the dirty WAL, and arming
#861 for the next open. The memory growth and the broken tiles are one incident,
not two. `TimeoutStopSec` is now 300 for exactly this reason.

`CHECKPOINT` was then confirmed to fail the same way -- #861's other
manifestation -- so the WAL cannot be drained on the current stack:

```
FATAL Error: Failed to create checkpoint because of error:
Operation requires a flat vector but a non-flat vector was encountered
```

**The obvious remedy does not work here.** `FORCE INSTALL spatial FROM
core_nightly` has no build for DuckDB v1.5.5: `core_nightly` publishes against
the current development version, and this server is pinned to v1.5.5 by
`cmake/duckdb_version.cmake`. Picking up #862 therefore means a DuckDB version
bump, not an extension swap.

The working recovery is the table copy in `docs/duckdb_checkpoint_failure.md`,
already validated on this dataset at 62 seconds for ~60M rows -- **and this is
a recurrence of that document's July incident**, whose "precise trigger was not
identified" is what #861 now answers. Read it before acting; it carries the
exact-DDL requirement, the file-size surprise, and the `reports export`
precondition. It also now carries the CLI version-skew hazard: the September
session reached this error through a v2.1.0-alpha CLI against a v1.5.5
database, and a *successful* checkpoint there could have upgraded the storage
format beyond what the production binary can open.

Worth knowing: the tile symptom is indistinguishable from
`docs/invalid_geometry_tile_500s.md` from the outside -- both are a 500 that
loses one whole z14 tile, both layers included. That doc's existence is not
evidence that a given tile 500 is that cause.

### 2. `MemoryHigh=6G` exhausted the connection pool in 40 seconds

The rest of the log is `Failed to acquire pool connection` on every endpoint --
`/tiles`, `/updates`, `/status` -- starting 36 s after startup and not stopping.
That is r2d2's 30 s timeout, meaning all `db_pool_size = 8` connections stayed
busy continuously. A failing query cannot cause this; it returns its connection.

Cause was the `MemoryHigh=6G` added in the same change, against a process whose
observed working set was 9.7 GB. `MemoryHigh` does not cap -- it throttles the
cgroup and reclaims hard once crossed. Every DuckDB query slowed enough to hold
its pool connection past 30 s, and `MemorySwapMax=0` removed the release valve.

**The lesson worth keeping: a too-low `MemoryHigh` does not produce a memory
error.** It produces a pool-exhaustion error with nothing anywhere naming memory
as the cause, which is a genuinely misleading failure to debug from scratch.
Confirm or exclude it with:

```bash
systemctl show osmpbudynkiv2 -p MemoryCurrent -p MemoryHigh -p MemoryMax
cat /sys/fs/cgroup/system.slice/osmpbudynkiv2.service/memory.events   # climbing `high` = throttling
cat /sys/fs/cgroup/system.slice/osmpbudynkiv2.service/memory.pressure
```

`example.service` now ships `MemoryHigh` commented out, `MemoryMax` as a pure
backstop with instructions to size it from a measured `MemoryCurrent` rather
than a guess, and no `MemorySwapMax` -- swap slack is what lets a shutdown
finish, which is what avoids re-entering #861 on every restart.

### Consequence for the leak investigation

The `MALLOC_CONF` experiment did not get a clean run: the cgroup throttling
confounds any RSS reading from this window. It needs restarting from a healthy
baseline once the spatial extension is patched and the ceilings are re-sized.
Nothing above changes the arena-retention finding, which stands on the `pmap`
data from the *previous* four-day run.

---

## Resolution (2026-09-13)

It was a genuine leak, and the refresh-driven hypothesis was right about
*when* but not *which*. `update prg` retained every parsed Arrow batch in
duckdb-rs's never-freed ArrowVTab store, about 2.8 GiB per daily run. The
building refreshes were innocent. Measurements, profile diffs and the fix:
`docs/prg_arrow_batch_leak.md`.
