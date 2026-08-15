# RocksDB encoding + single-pass PBF import — measured

Measured 2026-08-15 on `example_data/OSM/poland-2026-08-11.osm.pbf` (2.39 GB),
12-core machine, `--release`. Baseline figures are the 2026-08-05 run recorded
in `docs/import_time5.md` on the same machine with a 2.38 GB extract.

The import ran against an isolated `db_path`/`rocksdb_path` so the live
database was untouched.

## Result

| | baseline | after | change |
|---|---|---|---|
| `import osm` wall clock | 22m 58s | **9m 42s** | **2.4× faster** |
| RocksDB on disk | 8–9 GB | **4.22 GiB** (4,527,180,611 B) | **~2× smaller** |

("after" is the shipped sequential build. The rayon build measured 8m 57s /
4,527,204,789 B — see the variant comparison below for why it was dropped.)

## What changed

Four things landed together (see the two CLAUDE.md gotchas for the reasoning):

1. Node values became two `i32` decimicrodegrees instead of two `f64`.
2. Keys became big-endian, so numerically adjacent ids are lexicographically
   adjacent and land in the same SST block.
3. Way node refs became delta + zigzag varint.
4. The three `ST_ReadOSM` streaming passes (nodes, ways, relations) collapsed
   into one sequential `osmpbf` `BlobReader` pass.

## Per-step timing

| step | baseline | after |
|---|---|---|
| stream nodes to RocksDB | 3m 26s | — |
| stream ways to RocksDB | 5m 19s | — |
| stream relations to RocksDB | 14.5s | — |
| **stream PBF to RocksDB (one pass)** | — | **5m 00s** |
| import address nodes | 10.7s | 6.9s |
| import way buildings and addresses | **11m 48s** | **1m 46s** |
| import way former buildings | — | 5.9s |
| import relation buildings and addresses | ~11s | 9.9s |
| import relation former buildings | — | 5.8s |
| compact reverse indexes | 1m 42s | 2m 20s |
| create spatial indexes | ~4s | 5.8s |

Two results are worth calling out.

**The three streaming passes (9m 00s combined) became one 5m 00s pass.** This
is entirely from decompressing the file once instead of three times — the pass
is single-threaded.

**`import way buildings and addresses` went from 11m 48s to 1m 45s (6.7×), and
that pass was not modified at all.** It still uses `ST_ReadOSM` and the
`resolve_node_coords` UDF. The speedup is entirely from lever 2: the pass does
~18M × ~5 random node lookups, and under little-endian keys a building's
consecutive node ids were scattered across the whole keyspace, costing roughly
one SST block read per node. Big-endian keys put them in the same block. Lever
1 compounds it by halving the bytes each lookup pulls through the block cache.

This is the measured confirmation of the prediction that key locality was the
lever attacking both size and time at once.

## Per-column-family sizes after

Via `cargo run --release --example kv_sizes -- <store>`
(`rocksdb.total-sst-files-size`):

| column family | bytes | GiB | encoding |
|---|---|---|---|
| `nodes` | 2,446,887,051 | 2.28 | `i32` pair (lever 1) |
| `node_to_ways` | 1,345,805,202 | 1.25 | fixed-width — **lever 3 not applied** |
| `ways` | 693,598,321 | 0.65 | delta+varint (lever 3) |
| `relations` | 21,030,882 | 0.02 | fixed-width |
| `way_to_relations` | 19,858,001 | 0.02 | fixed-width |
| `meta` | 1,154 | 0.00 | version stamp |
| **TOTAL** | **4,527,180,611** | **4.22** | |

## Variant comparison: threading and the zlib backend

Three full national imports on the same machine, same PBF, same isolated
`db_path`/`rocksdb_path`. Only the `stream PBF to RocksDB` pass can be affected
by either variable, so that is the column to read.

| variant | stream PBF pass | RocksDB total | shipped |
|---|---|---|---|
| rayon `par_bridge`, default backend | 4m 19s | 4,527,204,789 B | no |
| **sequential, default backend** | **5m 00s** | **4,527,180,611 B** | **yes** |
| sequential, `osmpbf/zlib-ng` | 4m 46s | 4,527,186,652 B | no |

All three produced identical output — `buildings=17985399 addresses=8688325
former_buildings=15378` in every run — and stores within 24 KB of each other
(0.0005%), confirming that neither parallel blob commits nor the zlib backend
changes what lands on disk.

**Threading was dropped.** 12 cores bought 41s (13.7%) of one pass, ~7% of the
import. The pass is bound by RocksDB write throughput and the sequential blob
read, not decode CPU, so the cores have little to do. That is not worth a
`rayon` dependency plus nondeterministic `node_to_ways` ordering. Note the
`par_bridge` version is *correct*, just not worth it — see the doc comment on
`stream_pbf_to_rocksdb` for why it is safe to parallelize.

**`zlib-ng` was dropped**, and the reason it gains so little is the part worth
remembering: **the default build is already on a fast zlib, not miniz_oxide.**
`flate2`'s backend priority is C zlib (`any_c_zlib`) > `zlib-rs` >
`rust_backend`, and `zip` (via `prg_convert`) turns on `flate2/zlib-rs` for the
whole dependency graph. Cargo unifies features, so `osmpbf`'s declared
`rust-zlib` default never actually selects miniz_oxide in this binary — the
"default" run above is really zlib-rs, a Rust port of zlib-ng, which is why
swapping in C zlib-ng moved only 14s (4.7%). Paying a cmake + C build for that
is a bad trade.

The lesson generalizes: **when benchmarking a `flate2` feature in this repo,
check the resolved feature set, never the crate's declared default.**

```
cargo tree -f "{p} [{f}]" | grep "flate2 v"
```

Two numbers in the sequential-`zlib-ng` run are environmental noise, not
results: `compact reverse indexes` at 4m 08s (vs 2m 20s / 2m 14s) and
`create spatial indexes` at 20.5s (vs 5.8s). Both are RocksDB/DuckDB work that
never touches `flate2` — RocksDB compaction uses its own bundled zstd — and
that run's total (11m 40s) is inflated accordingly. Editing `Cargo.toml`
immediately beforehand triggers a rust-analyzer re-index, the likely culprit.
`import way buildings and addresses` also swung 1m 45s / 1m 46s / 1m 55s across
the three runs; it reads via DuckDB's `ST_ReadOSM`, so it too is untouchable by
these variables.

## Correctness verification on real data

Element counts match the baseline run's magnitudes (nodes 243,833,227; ways
33,759,796; relations 291,063; buildings 17,985,399; addresses 8,688,325),
confirming the `osmpbf` reader sees everything `ST_ReadOSM` did.

Two checks on the resulting geometry, both against the whole national dataset:

- **0 of 26,689,102 rows** (buildings + addresses + former buildings) fall
  outside Poland's bounding box. This is the check for the failure mode the
  format version guards against — misreading the value layout produces
  real-looking coordinates in the wrong hemisphere, not an error.
- **0 of 2,000,000 sampled building vertices** are off OSM's exact 1e-7 degree
  grid; max deviation 6e-8 in scaled units (6e-15 degrees), which is f64
  representation noise many orders of magnitude below the grid spacing. The
  `i32` round trip is lossless on real data, matching
  `encoding::tests::coordinate_roundtrip_is_exact_on_the_osm_grid`.

## Obvious next lever

`node_to_ways` is now 30% of the store (1.25 GiB) and is the one large CF still
on the fixed-width encoding, because it carries a merge operator whose partial
merge concatenates bare 8-byte operands without decoding them
(`kvstore::id_list_partial_merge`). Applying delta+varint there means reworking
that operator — the full merge would decode, append and re-encode instead of
memcpy'ing. Average list length is close to 1, so the win is smaller per entry
than it was for `ways`, but at 1.25 GiB the absolute number is no longer small.
