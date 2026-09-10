# RocksDB tuning: what was measured, what landed, what was rejected

Written 2026-09-10. Ten tuning recommendations were proposed after the
block-cache fix (`docs/rocksdb_block_cache_not_applied.md`), then each was
measured against the real store before anything was implemented. Two
survived. This file exists mainly for the eight that did not: every one of
them sounds right from first principles, and several were wrong for reasons
only the real data shows.

Store measured: full Poland, as built by `import osm` plus a full-country
`tiles warm` — 5.6 GB. Unless noted, workloads are the `update osm` read shape
(4,000 real ways → their node refs via batched MultiGet, plus the
`node_to_ways` / `way_to_relations` reads `apply_changes` does per changed
object) and 26k tile-store gets, repeated with the page cache warm. Latency on
this NVMe box understates production's slower disk; block-read and ticker
counts do not, so the verdicts below lean on those.

| CF | keys | live |
|---|---|---|
| `nodes` | 244,457,258 | 2,330 MB |
| `node_to_ways` | 237,293,094 | 1,287 MB |
| `ways` | 33,847,368 | 659 MB |
| `relations` | 286,905 | 19.6 MB |
| `way_to_relations` | 1,745,496 | 18.9 MB |
| `tiles` | 182,503 | 1,070 MB |

## Landed

### Bloom filter on `CF_TILES` (10 bits/key)

`TileStore::may_exist` is `key_may_exist_cf`, which answers "yes" whenever it
cannot decide without I/O. With no filter, that is every key inside an SST's
key range. Probing every absent z14 tile inside the warmed bbox (sea, border,
uncovered cells), cold, production cache sizes:

| | false positives |
|---|---|
| no filter | 58,816 of 58,946 — **99.78%** |
| bloom, 10 bits/key | 590 of 58,967 — **1.00%** |

Cost: ~1.25 bytes per tile (`table_readers_mem` for 182k tiles: 1.98 →
2.19 MB). The probe also got 2× faster, since the filter answers without
touching data blocks.

Two consumers were broken by the missing filter, and neither failed loudly:
`tiles warm`'s resume filter skipped the tiles it had not rendered yet, and
`tile_refresh` re-rendered tiles that had never been stored — undoing the
"re-render only resident tiles" load bound. End to end with the filter: a warm
interrupted at 112,337 of 182,503 resumed with 70,145 of the 70,166 missing
tiles (21 skipped, 0.03%).

**It does not retrofit.** A filter is written into an SST when the SST is
written. Existing files keep none, and a full `compact_range_cf` does not
rewrite them either: files already at the bottommost level with no overlap are
skipped (a 137 MB tiles family "compacted" in 42 ms; `way_to_relations` in
6.5 µs). A store written before the filter needs `tiles clear` + `tiles warm`.

Guard: `tile_store::tests::may_exist_rejects_absent_tiles_once_they_are_on_disk`
(1,999 of 2,000 absent tiles say yes without the filter).

### Statistics, exposed on `/status`

`StatsLevel::ExceptHistogramOrTimers` — counters only. Overhead on the real
workload, two interleaved pairs each: `update osm` reads +1–2%, tile gets
+2–4% (~0.15 µs per get). Everything in this file was measured with these
counters; without them the block cache and the filter are unobservable in
production. `/status` carries `rocksdb.*` — see `kvstore::KvStats`.

Two things to know. The counters live on the `Options` object, not the
database, so `RocksDB` owns its `Options` for the same reason it owns its
caches. And they are database-wide: the hit/miss pair mixes tile and OSM
reads, so per-tier saturation is read off each `Cache` directly
(`*_usage_bytes` vs `*_capacity_bytes`).

## Rejected

### Bloom filters on the OSM families

The idea: every point lookup probes several levels, and a filter kills the
negative probes. Measured on a copy compacted with filters on `nodes` and
`ways`, identical workload:

- block reads 14,793 → 14,790
- `BloomFilterUseful` = **0** — no read avoided
- `table_readers_mem` 60.6 → **113.5 MB**

Lookups here are hits, not misses. 237.3M `node_to_ways` entries against
244.5M nodes means 97% of nodes are in a way (a 3% negative rate — the premise
that `node_to_ways` is negative-dominated was simply false), and every changed
way in a real `.osc` diff existed. `way_to_relations` really is
negative-dominated (95% of ways are in no relation; 41% of changed ways on the
real diff) but the whole family is 18.9 MB and lives in cache, so there is no
I/O to save. A flat 10-bit filter on the two big families alone would cost
588 MB.

### `cache_index_and_filter_blocks(true)`

The idea: index blocks sit outside the block cache, unaccounted. They do —
62.4 MB of them — and moving them in works (`table_readers_mem` → 0.57 MB,
shared cache usage 110 → 167 MB, data-block misses unchanged). But it costs:
warm reads +2.5% (OSM) and +2–3% (tiles), the first cold pass +11%, and once
the cache is full those 57 MB displace data blocks. The 62 MB is a stable
overhead proportional to store size, not implicated in the production memory
growth (`docs/memory_growth_investigation.md`: RocksDB was ~100 MB of a 9.7 GB
RSS). Accounting tidiness is not worth a measured slowdown.

### Capping total memtable memory (`db_write_buffer_size`)

The idea: 64 MB × 4 buffers × 5 families is ~1.4 GB of reachable, unaccounted
memtable. The `import osm` LOG (446 flushes) says otherwise: `num_memtables`
never exceeded 1 on any family, each ~64 MB, so the real peak is ~5 × 64 MB.
All 28 write stalls were `node_to_ways` hitting 20–22 **L0 files** — compaction
behind, not memtable pressure. A full-country `tiles warm`: 22 flushes, one
memtable at a time, zero stalls. A cap tight enough to bind would mean smaller,
earlier flushes, i.e. more L0 files — worse on the stall the import actually
has. The companion idea (a 16 MB tiles write buffer) had no measured need
either.

### Higher bottommost zstd level

Level 3 vs 9, both force-compacted so the comparison is fair: `nodes` −1.1%,
`ways` −3.1%, `node_to_ways` −13.9% — −218 MB overall — for 2.4–3.2× the
compaction time and +1.5% on warm reads. Now that import ends with a forced
compaction (below) the level would reach every family, but the two big wins it
would buy there are the smallest (−1.1%, −3.1%), and 2.4–3.2× compaction time
would put back most of the minute subcompactions took off the import.

### Fewer background jobs (12 → 4)

RocksDB spawns its pool eagerly at open: 12 jobs = 12 threads (3 high, 9 low),
4 = 4. The motivation was jemalloc arena spread, but production's
`MALLOC_CONF=narenas:4` already caps arenas regardless of thread count, and
idle threads touch almost none of their stacks. Meanwhile the import is
compaction-bound (the L0 stalls above) at 12, and `import osm` and `run` get
the same `max_background_jobs` from `kvstore::open_with`.

### Cache sizes

Left at 512 MB shared / 256 MB tiles. Not validatable here: whether 256 MB
covers the tile working set depends on production's access skew (the full
tiles family is 1,070 MB, and the bench's uniform-ish tile sweep saturated the
cache at 255 MB, which says nothing about real traffic). The `/status`
counters above are what will answer it.

## Found along the way, and landed: a forced compaction after `import osm`

Not one of the ten, and worth more than any of them.

**`nodes` and `ways` are never compacted by `import osm`.** They are written in
id order, so every flush is non-overlapping and gets trivially *moved* down to
L6: the import LOG shows ~320 `nodes` and ~33 `ways` files moved and zero real
compactions (vs 68 for `node_to_ways`, whose merge operands overlap).
`live_files()` on the real store shows the consequence directly — **all 179
`nodes` files and all 30 `ways` files carry nonzero sequence numbers**, spread
over three levels, while 20 of 21 `node_to_ways` files are already at zero.

Forcing a bottommost compaction, on copies of the real store:

- size: `nodes` 2,340 → 1,959 MB (−16%), `ways` 664 → 608 MB (−8.5%),
  `node_to_ways` 1,289 → 1,287 (already compacted); every family ends in one
  level with every sequence number zero — which is where the size comes from:
  a bottommost compaction zeroes seqnos, and on a 16-byte node record the
  8-byte trailer is most of what zstd cannot squeeze;
- `update osm` read workload: **126–129 → 85 ms warm (−33%)**, 272 → 213–219 ms
  cold (−20%), reproducible across interleaved runs; `BlockCacheDataHit`
  281,645 → 162,710 with misses unchanged, i.e. ~119k fewer (cached)
  multi-level probes;
- transient disk: +1.36 GB at peak (sampled every 2 s), net −439 MB.

It is also the real fix for the multi-level probing that made OSM filters look
attractive, and it leaves them pointless (`BloomFilterUseful = 0` on the
compacted copy).

**Subcompactions make it cheap.** Single-threaded, the forced `nodes` + `ways`
compaction took 218 s on a SATA SSD; with `max_subcompactions = 12`, 45 s, same
output. A full Poland `import osm`, both runs from scratch on the same disk:

| | subcompactions 1 | subcompactions 12 |
|---|---|---|
| whole import | 10m57s | **8m04s** |
| streaming pass | 4m22s | 4m22s |
| compaction step | 4m07s | **1m02s** |
| final store | 3,888 MB | 3,888 MB |

The old import already paid ~155 s for the plain `node_to_ways` compaction
alone, so the new step is faster than the one it replaced.

What landed (`kvstore::compact_osm_families`): a forced bottommost compaction of
`nodes`, `ways`, `relations`; a plain one of the two reverse indexes (forcing
`node_to_ways` measured −0.2% for 143 s). `import osm` ends with it, and
`kv compact` runs it by hand — 50 s on the real store. The commands that run it
open RocksDB with `max_subcompactions` = cores (`kvstore::open_for_bulk_load`);
everything else, `run` included, keeps the default of 1 it was measured with.
It has to be set at open: `rust-rocksdb` wraps neither
`CompactRangeOptions::max_subcompactions` nor `SetDBOptions`.
