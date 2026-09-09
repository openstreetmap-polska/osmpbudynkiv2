# `rocksdb_block_cache_mb` is silently discarded — every CF runs on RocksDB's default 32 MB cache

Written 2026-09-09. Found while investigating unbounded memory growth in the
production deployment (v0.1.2, ~4 days uptime). The
memory growth turned out to be unrelated — see the "Not the memory bug" section
at the bottom — but the block cache the investigation went looking for was not
there.

Not fixed. The fix is small and mechanical (below); it is recorded here rather
than applied blind because it *raises* the process's steady-state footprint by
roughly 640 MB, and that wants to land alongside the memory-growth work rather
than in the middle of it.

---

## Symptom

`config.toml` sets `rocksdb_block_cache_mb = 512`. The running process has no
512 MB cache. Its RocksDB `LOG` — 123 consecutive `DUMPING STATS` blocks,
covering roughly the last 20 hours — reports only this, and nothing else:

```
Block cache AutoHyperClockCache@0x7f998502e750#3995943 capacity: 32.00 MB ...
Block cache AutoHyperClockCache@0x7f998502e810#3995943 capacity: 32.00 MB ...
Block cache AutoHyperClockCache@0x7f998502e990#3995943 capacity: 32.00 MB ...
```

Three caches, 32.00 MB each, `AutoHyperClockCache`. Two independent tells that
these are not ours:

1. **The size.** 32 MB is RocksDB's built-in default, not any value this
   codebase can produce.
2. **The type.** `kvstore::open` builds its cache with
   `rocksdb::Cache::new_lru_cache`, which is an `LRUCache`. The string
   `LRUCache` does not appear anywhere in the log. `AutoHyperClockCache` is
   what RocksDB's default `BlockBasedTableFactory` constructs for itself when
   no cache is supplied.

Each of the three sits saturated at ~31.2 MB of `DataBlock` entries:

```
Block cache entry stats(count,size,portion): DataBlock(7880,31.20 MB,97.49%) Misc(1,0.00 KB,0%)
```

So the effective block cache is **96 MB across the whole store, not 512 MB** —
and it is fully saturated, i.e. actively evicting, which is exactly the
condition the 512 MB setting exists to avoid.

## Cause

`kvstore::open` does construct the configured cache and does install it — on
the wrong options object.

```rust
// src/osm/kvstore.rs, open()
let mut bbt = BlockBasedOptions::default();
let cache = rocksdb::Cache::new_lru_cache(block_cache_mb * 1024 * 1024);
bbt.set_block_cache(&cache);
db_opts.set_block_based_table_factory(&bbt);   // <-- DB-level options

let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
    .iter()
    .map(|name| ColumnFamilyDescriptor::new(*name, make_cf_opts(name, ...)))
    .collect();

DBWithThreadMode::open_cf_descriptors(&db_opts, path, cfs)
```

and `make_cf_opts` starts from a blank slate:

```rust
fn make_cf_opts(name: &str, write_buffer_bytes: usize) -> Options {
    let mut cf_opts = Options::default();
    cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    // ... write buffers, dynamic levels, merge operators ...
    cf_opts                         // no set_block_based_table_factory
}
```

**A `ColumnFamilyDescriptor`'s options fully replace the DB-level options for
every column-family-scoped setting, and the block-based table factory — which
is what carries `block_cache` — is one of those.** It is not merged, and there
is no warning. Every CF therefore opens with `Options::default()`'s table
factory, and that factory lazily creates its own default 32 MB cache.

The DB-level factory `open` painstakingly configured is used by exactly nothing.
The `cache` local is dropped at the end of `open`, and the only reason the
memory is reclaimed rather than leaked is that `Cache` is refcounted and nothing
else ever held a reference.

### Why three caches and not five

`ALL_CFS` has more entries than that, but a default `BlockBasedTableFactory`
only materialises its cache once it actually opens a table, and only CFs holding
SST files ever get there. The count is incidental; the sizes are the finding.

## Fix

Set the factory inside `make_cf_opts`, where the per-CF options are actually
built, and pass the already-constructed `Cache` down so all non-tile CFs keep
*sharing* one budget rather than getting 512 MB each:

```rust
fn make_cf_opts(name: &str, write_buffer_bytes: usize, shared_cache: &rocksdb::Cache) -> Options {
    let mut cf_opts = Options::default();
    let mut bbt = BlockBasedOptions::default();
    bbt.set_block_cache(shared_cache);
    cf_opts.set_block_based_table_factory(&bbt);
    // ... existing settings ...
}
```

Sharing matters: `Cache` is refcounted, so handing the same handle to every CF
gives one 512 MB pool with `ALL_CFS` competing inside it, which is what
`rocksdb_block_cache_mb`'s doc comment ("shared across all column families")
already promises. Constructing a fresh `new_lru_cache` per CF would instead
multiply the budget by the CF count.

**The same bug is already present, unlanded, in the working tree's tile-store
version of `make_cf_opts`** — the `CF_TILES` branch is the only one that sets a
table factory, so it will be the only CF whose configured cache takes effect,
and the other four will keep their silent 32 MB defaults. Fix both branches
together.

### Budget after the fix

**+416 MB** for the shared OSM cache (96 MB of accidental defaults becomes the
configured 512 MB), plus a further **+224 MB** on the unlanded tile-store branch
once `CF_TILES` starts honouring `rocksdb_tile_block_cache_mb = 256` instead of
its own 32 MB default. About **+640 MB** in total. That is the intended cost of
the setting working, not a regression — but confirm it against `MemoryCurrent`
before and after rather than trusting the arithmetic.

## Guard

There is no test. A unit test can assert the property directly without opening a
real store of any size:

```rust
db.property_int_value_cf(&cf, "rocksdb.block-cache-capacity")
```

should equal the configured byte count for every name in `ALL_CFS`, and — the
half that catches the *sharing* regression, which a per-CF capacity check alone
would not — `rocksdb.block-cache-usage` should read identically across all of
them, since a shared cache reports one common usage figure.

## Not the memory bug

Recorded so nobody re-runs this investigation: this defect makes RocksDB's
resident footprint **smaller** than configured, not larger, so it cannot explain
memory growth. In the same production process RocksDB was near-idle overall —
1.09M cumulative writes, 0.06 GB ingested, 0.02 GB compacted and 3.6 s of
compaction CPU across the entire log — for a total footprint around 100 MB
against a 9.7 GB RSS. See `docs/memory_growth_investigation.md`.
