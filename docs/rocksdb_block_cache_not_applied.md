# `rocksdb_block_cache_mb` is silently discarded — every CF runs on RocksDB's default 32 MB cache

Written 2026-09-09. Found while investigating unbounded memory growth in the
production deployment (v0.1.2, ~4 days uptime). The
memory growth turned out to be unrelated — see the "Not the memory bug" section
at the bottom — but the block cache the investigation went looking for was not
there.

**Fixed 2026-09-09.** The fix went in as described below, plus one case the
original write-up did not anticipate — see *What actually landed*. Expect the
steady-state footprint to rise by roughly 640 MB; that is the setting working,
not a regression, and it wants watching against `MemoryCurrent` alongside the
`MALLOC_CONF` experiment in `docs/memory_growth_investigation.md`.

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


---

## What actually landed

Two things beyond the sketch above.

### The recreate path needed the cache too

`clear` and `clear_tiles` drop and recreate a column family on a **live**
database, and `create_cf` takes a fresh `Options`. Passing `make_cf_opts` a
cache it can only get from `open`'s locals meant a recreated family fell back to
RocksDB's default — so the defect would have come straight back after every
`tiles clear`, on the one family that had been working. The fix above alone does
not cover this.

`RocksDB` is therefore no longer a bare type alias for
`DBWithThreadMode<MultiThreaded>`. It is a struct owning the database plus the
two `Cache` handles it was opened with, and it `Deref`s to the database so every
existing `db.get_cf(...)` call site is unchanged. `Cache` is refcounted, so
holding them costs a pointer each.

Two caches, not one: the OSM families share `rocksdb_block_cache_mb` (the
"shared across all column families" its doc comment already promised), and
`CF_TILES` keeps its own `rocksdb_tile_block_cache_mb` so a browsing session's
tile blocks can never evict the OSM node blocks `update osm` reads on every
minutely diff.

### The guard, and the trap in writing it

`every_column_family_opens_on_its_configured_block_cache` asserts both halves,
because they catch different regressions:

- **capacity per family** — catches the factory going missing again;
- **`rocksdb.block-cache-usage` identical across the OSM families** — catches a
  future edit handing each family its own `new_lru_cache`, which would multiply
  the configured budget by the family count while every *capacity* still read
  correctly.

Two things make the test non-obvious, both verified by reintroducing the bug and
watching it fail:

1. **The configured size must not be 32 MB.** That is RocksDB's own default, so
   a family that silently fell back to it still reports the "configured"
   capacity and the assertion passes for the wrong reason. The test uses 48 MB.
   The first draft used 32 and was blind to exactly the symptom this document
   opens with.
2. **Usage only says anything after an SST read.** A memtable read never
   consults the block cache, so the test writes a node, `flush_cf`s it to an
   SST, and only then reads it back. Without the flush every family reports the
   same near-zero usage and the sharing half passes vacuously.

An empty family reports a few dozen bytes rather than zero (cache bookkeeping),
so the tiles-are-separate assertion is `usage(CF_TILES) < shared`, not
`== 0`.