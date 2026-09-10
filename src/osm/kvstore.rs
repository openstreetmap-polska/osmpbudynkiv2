use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::statistics::{StatsLevel, Ticker};
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor, DBWithThreadMode,
    MergeOperands, MultiThreaded, Options, WriteBatch, WriteOptions,
};
use serde::Serialize;

use super::encoding;

/// Column family names for the 5 OSM key spaces, a tiny `meta` space holding
/// the on-disk format version, and the rendered-tile cache.
pub const CF_NODES: &str = "nodes";
pub const CF_WAYS: &str = "ways";
pub const CF_RELATIONS: &str = "relations";
pub const CF_NODE_TO_WAYS: &str = "node_to_ways";
pub const CF_WAY_TO_RELATIONS: &str = "way_to_relations";
pub const CF_META: &str = "meta";

/// Rendered z12-z14 `/tiles` bodies, gzipped. A pure cache: every entry has an
/// authoritative source (DuckDB) behind it, which is what licenses every
/// "degrade, don't fail" choice in `server::tile_store` -- a miss costs a
/// re-render, never a wrong answer.
///
/// Adding this CF deliberately does NOT bump [`KV_FORMAT_VERSION`]: it changes
/// no existing key or value layout, `create_missing_column_families(true)` is
/// already set, and bumping would force a ~12-minute `import osm` on deploy for
/// nothing. Its own guard is `tile_store::TILE_VALUE_FORMAT`, whose mismatch
/// decodes as a *miss* rather than a hard bail -- the opposite policy to the
/// one below, because the recovery costs differ by four orders of magnitude.
pub const CF_TILES: &str = "tiles";

const ALL_CFS: &[&str] = &[
    CF_NODES,
    CF_WAYS,
    CF_RELATIONS,
    CF_NODE_TO_WAYS,
    CF_WAY_TO_RELATIONS,
    CF_META,
    CF_TILES,
];

/// On-disk format version for this store.
///
/// Bump this whenever the byte layout of any key or value changes. Nothing
/// about the layout is self-describing, so without this stamp an old store
/// read by a new binary decodes to plausible-looking garbage rather than
/// failing: an 8-byte `i32` coordinate pair read out of a 16-byte `f64` value
/// yields real numbers in the wrong place, and every building silently lands
/// somewhere in the Gulf of Guinea. There is no in-place migration — the store
/// is rebuilt wholesale by `import osm` — so the only job here is to make the
/// mismatch loud.
pub const KV_FORMAT_VERSION: u32 = 1;

const FORMAT_VERSION_KEY: &[u8] = b"format_version";

/// Message used when the store's stamp disagrees with [`KV_FORMAT_VERSION`].
/// Named so tests can assert on it exactly rather than on a substring.
pub const FORMAT_MISMATCH_MESSAGE: &str = "RocksDB store was built by an incompatible version — re-run `import osm` \
     to rebuild it (there is no in-place migration)";

/// The block caches the store's column families were opened with.
///
/// These have to be owned by the handle rather than dropped at the end of
/// [`open`], because [`clear`] and [`clear_tiles`] recreate a column family on
/// a *live* database and `create_cf` takes a fresh `Options`. Without the
/// original cache in hand, a recreated family silently falls back to RocksDB's
/// own default -- which is the whole defect recorded in
/// `docs/rocksdb_block_cache_not_applied.md`, reintroduced after every
/// `tiles clear`. `Cache` is refcounted, so holding one here costs a pointer.
struct Caches {
    /// Shared by every column family except `CF_TILES`, which is what
    /// `rocksdb_block_cache_mb`'s "shared across all column families" promise
    /// means. Handing each family its own `new_lru_cache` would instead
    /// multiply the configured budget by the family count.
    shared: Cache,
    /// `CF_TILES` alone, so a browsing session's tile blocks can never evict
    /// the OSM node blocks `update osm` reads on every minutely diff.
    tiles: Cache,
}

/// The store handle: a RocksDB database plus the caches and DB-level options
/// it was opened with.
///
/// Derefs to the underlying database, so every `db.get_cf(...)` call site reads
/// exactly as it did when this was a bare type alias.
pub struct RocksDB {
    db: DBWithThreadMode<MultiThreaded>,
    caches: Caches,
    /// Kept for the same reason as `caches`: RocksDB's statistics live on the
    /// `Options` object, not on the database, so [`RocksDB::stats`] can only
    /// read a ticker back while the options `open` enabled them on are still
    /// alive. Dropping them at the end of `open` would leave counters that are
    /// collected but unreadable.
    opts: Options,
}

/// Cumulative RocksDB counters since the store was opened, for `/status`.
///
/// **The tickers are database-wide, not per column family**: there is one
/// `Statistics` object per database, so the block-cache hit/miss pair mixes
/// tile reads with OSM reads even though the two sit on separate caches.
/// Per-tier saturation is what the `*_block_cache_*` usage/capacity pairs are
/// for -- they are read off each `Cache` directly. The bloom counters, by
/// contrast, are effectively tiles-only, because `CF_TILES` is the only family
/// with a filter (see [`make_cf_opts`]).
///
/// Collected at `StatsLevel::ExceptHistogramOrTimers` -- counters only.
/// Measured against the real store: +1-2% on the `update osm` read path and
/// +2-4% on tile-store gets (~0.15 us per get), negligible next to an HTTP
/// response. Histograms or timers would cost more and nothing reads them.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct KvStats {
    pub block_cache_data_hit: u64,
    pub block_cache_data_miss: u64,
    /// Lookups a filter answered "definitely absent", i.e. reads avoided.
    pub bloom_filter_useful: u64,
    /// Lookups a filter answered "may be present"...
    pub bloom_filter_full_positive: u64,
    /// ...and of those, how many really were. The gap between the two is the
    /// false-positive count; at 10 bits per key it should sit near 1%.
    pub bloom_filter_full_true_positive: u64,
    pub shared_block_cache_usage_bytes: u64,
    pub shared_block_cache_capacity_bytes: u64,
    pub tiles_block_cache_usage_bytes: u64,
    pub tiles_block_cache_capacity_bytes: u64,
}

impl RocksDB {
    pub fn stats(&self) -> KvStats {
        let t = |ticker| self.opts.get_ticker_count(ticker);
        KvStats {
            block_cache_data_hit: t(Ticker::BlockCacheDataHit),
            block_cache_data_miss: t(Ticker::BlockCacheDataMiss),
            bloom_filter_useful: t(Ticker::BloomFilterUseful),
            bloom_filter_full_positive: t(Ticker::BloomFilterFullPositive),
            bloom_filter_full_true_positive: t(Ticker::BloomFilterFullTruePositive),
            shared_block_cache_usage_bytes: self.caches.shared.get_usage() as u64,
            shared_block_cache_capacity_bytes: self.caches.shared.get_capacity() as u64,
            tiles_block_cache_usage_bytes: self.caches.tiles.get_usage() as u64,
            tiles_block_cache_capacity_bytes: self.caches.tiles.get_capacity() as u64,
        }
    }
}

impl std::ops::Deref for RocksDB {
    type Target = DBWithThreadMode<MultiThreaded>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

/// Hand-written because `Cache` is not `Debug`, and only so `Result<RocksDB>`
/// keeps working with `unwrap_err`. Prints nothing about the store's contents.
impl std::fmt::Debug for RocksDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDB")
            .field("path", &self.db.path())
            .finish()
    }
}

/// Full merge for reverse-index CFs.
/// Existing value (if any) is an encoded id-list: 4-byte LE count + N * 8-byte LE i64s.
/// Each operand is a single 8-byte LE i64 to append.
fn id_list_full_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut ids: Vec<u8> = match existing {
        Some(val) if val.len() >= 4 => val[4..].to_vec(),
        _ => Vec::new(),
    };

    for operand in operands {
        ids.extend_from_slice(operand);
    }

    let count = (ids.len() / 8) as u32;
    let mut result = Vec::with_capacity(4 + ids.len());
    result.extend_from_slice(&count.to_le_bytes());
    result.extend_from_slice(&ids);
    Some(result)
}

/// Partial merge: operands are bare 8-byte i64s. Just concatenate.
fn id_list_partial_merge(
    _key: &[u8],
    _existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let total_len: usize = operands.iter().map(|op| op.len()).sum();
    let mut result = Vec::with_capacity(total_len);
    for operand in operands {
        result.extend_from_slice(operand);
    }
    Some(result)
}

/// Per-column-family options.
///
/// **Every family must set a block-based table factory here, and the block
/// cache rides on that factory.** A `ColumnFamilyDescriptor`'s options fully
/// *replace* the DB-level options for every column-family-scoped setting rather
/// than merging with them, and the table factory is one of those -- so a family
/// that sets no factory silently gets `Options::default()`'s, which lazily
/// builds its own 32 MB cache the first time it opens an SST. That is not a
/// warning, a log line or an error; the only symptom is
/// `Block cache ... capacity: 32.00 MB` in RocksDB's `LOG` and a cache that
/// evicts constantly. Setting the factory on `db_opts` in [`open`] looks like
/// it should work and does nothing at all. Measured in production against
/// `rocksdb_block_cache_mb = 512`: three families on 32 MB apiece, all
/// saturated. Full write-up in `docs/rocksdb_block_cache_not_applied.md`;
/// guard is `every_column_family_opens_on_its_configured_block_cache`.
///
/// **`CF_TILES` is the only family with a bloom filter, and both halves of
/// that are measured.** `TileStore::may_exist` is `key_may_exist_cf`, which
/// answers "yes" whenever it cannot decide without I/O -- so with no filter it
/// is "yes" for every key inside an SST's key range. On the fully warmed
/// store (182,503 tiles) it said yes for 58,816 of 58,946 absent z14 tiles,
/// 99.78%, which made `tiles warm`'s resume skip the tiles it was meant to
/// render and `tile_refresh` re-render tiles that were never stored. At 10
/// bits per key that drops to ~1% for ~1.25 bytes a tile. The OSM families
/// were measured too and deliberately get none: after `import osm` their
/// lookups are overwhelmingly hits (97% of nodes are in a way; changed ways
/// all exist), so on a compacted copy of the real store a filter avoided
/// **zero** reads while adding 53 MB of filter memory. See
/// `docs/rocksdb_tuning_measured.md`.
fn make_cf_opts(name: &str, write_buffer_bytes: usize, caches: &Caches) -> Options {
    let mut cf_opts = Options::default();
    let mut bbt = BlockBasedOptions::default();
    bbt.set_block_cache(if name == CF_TILES {
        &caches.tiles
    } else {
        &caches.shared
    });
    if name == CF_TILES {
        // Written into each SST as it is written, so it does NOT retrofit an
        // existing store: older files answer "may exist" for everything until
        // they are rewritten, and a full `compact_range_cf` leaves
        // already-bottommost files alone. `tiles clear` + `tiles warm` is the
        // way to get accurate probes on a store written before this.
        bbt.set_bloom_filter(10.0, false);
    }
    cf_opts.set_block_based_table_factory(&bbt);
    if name == CF_TILES {
        // Values arrive already gzipped by `server::tile_store`, so compressing
        // them again would burn CPU on every write and every compaction for
        // nothing. This is the one CF that opts out of the blanket Zstd below,
        // and it reads as an oversight without this comment.
        cf_opts.set_compression_type(rocksdb::DBCompressionType::None);
        // Steady small request-path writes, not a bulk-load burst.
        cf_opts.set_max_write_buffer_number(2);
        if write_buffer_bytes > 0 {
            cf_opts.set_write_buffer_size(write_buffer_bytes);
        }
        cf_opts.set_level_compaction_dynamic_level_bytes(true);
        return cf_opts;
    }
    cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    if write_buffer_bytes > 0 {
        cf_opts.set_write_buffer_size(write_buffer_bytes);
    }
    // Allow several memtables to exist at once so writers don't block while an
    // earlier memtable is still being flushed to L0.
    cf_opts.set_max_write_buffer_number(4);
    cf_opts.set_min_write_buffer_number_to_merge(1);
    // Use dynamic level sizing so the LSM tree stays well-shaped under bulk
    // inserts instead of needing a huge pre-known key count.
    cf_opts.set_level_compaction_dynamic_level_bytes(true);
    if name == CF_NODE_TO_WAYS || name == CF_WAY_TO_RELATIONS {
        cf_opts.set_merge_operator("id_list_merge", id_list_full_merge, id_list_partial_merge);
    }
    cf_opts
}

pub fn open(
    path: &Path,
    block_cache_mb: u64,
    write_buffer_mb: u64,
    tile_block_cache_mb: u64,
) -> Result<RocksDB> {
    open_with(
        path,
        block_cache_mb,
        write_buffer_mb,
        tile_block_cache_mb,
        1,
    )
}

/// [`open`], for the offline commands that end in [`compact_osm_families`]
/// (`import osm`/`full`, `init`, `kv compact`): the same store with
/// compactions allowed to split across every core.
///
/// Measured on a full Poland `import osm`, both from scratch on the same disk:
/// 10m57s -> **8m04s**, all of it in the final compaction (4m07s -> 1m02s;
/// `nodes` 76 -> 12 s, `node_to_ways` 154 -> 44 s), with the streaming pass
/// identical and the store byte-for-byte the same size. `run` and everything
/// else keep RocksDB's default of 1, which is what they were measured with --
/// their compactions are small, and `run` shares its cores with requests.
///
/// It has to be set here, at open: RocksDB's per-call override
/// (`CompactRangeOptions::max_subcompactions`) is not wrapped by
/// `rust-rocksdb`, and neither is `SetDBOptions`.
pub fn open_for_bulk_load(
    path: &Path,
    block_cache_mb: u64,
    write_buffer_mb: u64,
    tile_block_cache_mb: u64,
) -> Result<RocksDB> {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(4);
    open_with(
        path,
        block_cache_mb,
        write_buffer_mb,
        tile_block_cache_mb,
        cores,
    )
}

fn open_with(
    path: &Path,
    block_cache_mb: u64,
    write_buffer_mb: u64,
    tile_block_cache_mb: u64,
    max_subcompactions: u32,
) -> Result<RocksDB> {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    // Bulk-load tuning: give RocksDB enough background threads to flush
    // memtables and run compactions in parallel with foreground writes, and
    // hint async fsync of SST data as it's written so the final flush is
    // cheap. None of this changes correctness — only throughput.
    let bg_jobs = std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(4)
        .max(4);
    db_opts.set_max_background_jobs(bg_jobs);
    db_opts.set_max_subcompactions(max_subcompactions);
    db_opts.set_bytes_per_sync(1 << 20);
    db_opts.set_wal_bytes_per_sync(1 << 20);
    // A DB-level setting, so unlike the table factory below it is NOT
    // replaced by the per-family descriptors. Read back via `RocksDB::stats`.
    db_opts.enable_statistics();
    db_opts.set_statistics_level(StatsLevel::ExceptHistogramOrTimers);

    // Deliberately NOT `db_opts.set_block_based_table_factory(...)`: the
    // per-family options below replace it wholesale, so a factory set here
    // would be used by nothing. See `make_cf_opts`.
    let write_buffer_bytes: usize = (write_buffer_mb * 1024 * 1024)
        .try_into()
        .context("write_buffer_mb overflow")?;
    let caches = Caches {
        shared: Cache::new_lru_cache(
            (block_cache_mb * 1024 * 1024)
                .try_into()
                .context("block_cache_mb overflow")?,
        ),
        tiles: Cache::new_lru_cache(
            (tile_block_cache_mb * 1024 * 1024)
                .try_into()
                .context("tile_block_cache_mb overflow")?,
        ),
    };

    let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
        .iter()
        .map(|name| {
            ColumnFamilyDescriptor::new(*name, make_cf_opts(name, write_buffer_bytes, &caches))
        })
        .collect();

    let db = RocksDB {
        db: DBWithThreadMode::open_cf_descriptors(&db_opts, path, cfs)
            .context("Failed to open RocksDB")?,
        caches,
        opts: db_opts,
    };

    check_or_stamp_format_version(&db)?;

    Ok(db)
}

/// Verify the store's format version, stamping it if it carries no stamp yet.
///
/// A fresh directory, or one just cleared by [`clear`], is stamped with the
/// current version. A stamp that disagrees with [`KV_FORMAT_VERSION`] is
/// rejected -- see that constant for why silence here is dangerous.
fn check_or_stamp_format_version(db: &RocksDB) -> Result<()> {
    let stored = db
        .get_cf(&cf(db, CF_META), FORMAT_VERSION_KEY)
        .context("Failed to read RocksDB format version")?;

    match stored {
        Some(bytes) => {
            let found = u32::from_le_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!(FORMAT_MISMATCH_MESSAGE))?,
            );
            if found != KV_FORMAT_VERSION {
                anyhow::bail!(
                    "{FORMAT_MISMATCH_MESSAGE} (found version {found}, expected {KV_FORMAT_VERSION})"
                );
            }
            Ok(())
        }
        None => stamp_format_version(db),
    }
}

fn stamp_format_version(db: &RocksDB) -> Result<()> {
    db.put_cf(
        &cf(db, CF_META),
        FORMAT_VERSION_KEY,
        KV_FORMAT_VERSION.to_le_bytes(),
    )
    .context("Failed to stamp RocksDB format version")?;
    Ok(())
}

/// Cheap "is there anything in here" probe — the nodes CF is populated first
/// by every import, so one key there is enough to call the store non-empty.
/// Drop and recreate all column families, effectively clearing all data.
pub fn clear(db: &RocksDB) -> Result<()> {
    for name in ALL_CFS {
        db.drop_cf(name)
            .with_context(|| format!("Failed to drop CF {name}"))?;
        db.create_cf(*name, &make_cf_opts(name, 0, &db.caches))
            .with_context(|| format!("Failed to recreate CF {name}"))?;
    }
    // The meta CF was just dropped along with the rest, so the version stamp
    // has to be rewritten or the next `open` would see a store with data and
    // no stamp, and reject it.
    stamp_format_version(db)?;
    Ok(())
}

/// Drop and recreate just the tiles column family, emptying it without
/// touching the OSM key spaces or the format stamp.
///
/// The whole-store [`clear`] is `import osm`'s tool; this is
/// `server::tile_store::clear`'s, behind the `tiles clear` CLI verb. Kept here
/// rather than in `tile_store` so column-family options stay in one place --
/// a recreate that forgot `make_cf_opts` would silently give the family
/// RocksDB's defaults, re-enabling Zstd over already-gzipped values.
pub fn clear_tiles(db: &RocksDB) -> Result<()> {
    db.drop_cf(CF_TILES)
        .with_context(|| format!("Failed to drop CF {CF_TILES}"))?;
    db.create_cf(CF_TILES, &make_cf_opts(CF_TILES, 0, &db.caches))
        .with_context(|| format!("Failed to recreate CF {CF_TILES}"))?;
    Ok(())
}

fn cf<'a>(db: &'a RocksDB, name: &str) -> Arc<BoundColumnFamily<'a>> {
    db.cf_handle(name)
        .unwrap_or_else(|| panic!("missing column family: {name}"))
}

// --- Node operations ---

/// Store a node's coordinates, given in decimicrodegrees. Callers holding
/// degrees (the `.osc` replication path) convert with
/// `encoding::f64_to_decimicro`.
pub fn put_node(db: &RocksDB, node_id: i64, lon_dm: i32, lat_dm: i32) -> Result<()> {
    db.put_cf(
        &cf(db, CF_NODES),
        encoding::encode_key(node_id),
        encoding::encode_node(lon_dm, lat_dm),
    )?;
    Ok(())
}

/// Read a node's coordinates back, in decimicrodegrees.
///
/// Test-only: production reads coordinates in bulk through
/// [`multi_get_nodes_wkb_coords`], never one node at a time. Kept because it is
/// the natural way for a test to assert on what a write actually stored.
#[cfg(test)]
pub fn get_node(db: &RocksDB, node_id: i64) -> Result<Option<(i32, i32)>> {
    if let Some(value) = db.get_cf(&cf(db, CF_NODES), encoding::encode_key(node_id))? {
        let coords = encoding::decode_node(&value);
        Ok(Some(coords))
    } else {
        Ok(None)
    }
}

/// Batch-look-up node coordinates for many node IDs, widening them into a
/// single buffer of WKB coordinate pairs (two LE f64 each).
/// Returns `Ok(None)` if *any* node is missing (callers treat missing refs
/// as "cannot build geometry").
///
/// **Widens, never copies.** Node values are stored as `i32` decimicrodegrees
/// — half the bytes of WKB's `f64` pairs, and not byte-compatible with them —
/// so there is no memcpy shortcut from stored bytes into a geometry buffer.
/// The name says `wkb_coords` rather than anything shorter for that reason.
/// See `encoding::push_wkb_coords`.
pub fn multi_get_nodes_wkb_coords(db: &RocksDB, node_ids: &[i64]) -> Result<Option<Vec<u8>>> {
    let keys: Vec<[u8; 8]> = node_ids
        .iter()
        .map(|id| encoding::encode_key(*id))
        .collect();
    let handle = cf(db, CF_NODES);
    // `sorted_input: false` -- a way's refs are near-consecutive but not
    // guaranteed sorted, and `true` on unsorted keys is incorrect, not merely
    // slower.
    let batch = db
        .batched_multi_get_pinned_batch_cf(&handle, &keys, false)
        .context("batched MultiGet for nodes failed")?;
    let mut out: Vec<u8> = Vec::with_capacity(node_ids.len() * encoding::WKB_COORD_BYTE_LEN);
    for r in batch.iter() {
        match r.context("batched MultiGet for nodes failed")? {
            Some(bytes) => encoding::push_wkb_coords(&mut out, bytes),
            None => return Ok(None),
        }
    }
    Ok(Some(out))
}

pub fn delete_node(db: &RocksDB, node_id: i64) -> Result<()> {
    db.delete_cf(&cf(db, CF_NODES), encoding::encode_key(node_id))?;
    Ok(())
}

// --- Way operations ---

pub fn put_way(db: &RocksDB, way_id: i64, node_ids: &[i64]) -> Result<()> {
    db.put_cf(
        &cf(db, CF_WAYS),
        encoding::encode_key(way_id),
        encoding::encode_delta_id_list(node_ids),
    )?;
    Ok(())
}

pub fn get_way(db: &RocksDB, way_id: i64) -> Result<Option<Vec<i64>>> {
    if let Some(value) = db.get_cf(&cf(db, CF_WAYS), encoding::encode_key(way_id))? {
        Ok(Some(encoding::decode_delta_id_list(&value)))
    } else {
        Ok(None)
    }
}

pub fn delete_way(db: &RocksDB, way_id: i64) -> Result<()> {
    db.delete_cf(&cf(db, CF_WAYS), encoding::encode_key(way_id))?;
    Ok(())
}

// --- Relation operations ---

pub fn put_relation(db: &RocksDB, relation_id: i64, members: &[(i64, u8, u8)]) -> Result<()> {
    db.put_cf(
        &cf(db, CF_RELATIONS),
        encoding::encode_key(relation_id),
        encoding::encode_relation_members(members),
    )?;
    Ok(())
}

pub fn get_relation(db: &RocksDB, relation_id: i64) -> Result<Option<Vec<(i64, u8, u8)>>> {
    if let Some(value) = db.get_cf(&cf(db, CF_RELATIONS), encoding::encode_key(relation_id))? {
        Ok(Some(encoding::decode_relation_members(&value)))
    } else {
        Ok(None)
    }
}

pub fn delete_relation(db: &RocksDB, relation_id: i64) -> Result<()> {
    db.delete_cf(&cf(db, CF_RELATIONS), encoding::encode_key(relation_id))?;
    Ok(())
}

// --- Reverse index: node -> ways ---

pub fn get_node_to_ways(db: &RocksDB, node_id: i64) -> Result<Vec<i64>> {
    if let Some(value) = db.get_cf(&cf(db, CF_NODE_TO_WAYS), encoding::encode_key(node_id))? {
        Ok(encoding::decode_fixed_id_list(&value))
    } else {
        Ok(vec![])
    }
}

pub fn put_node_to_ways(db: &RocksDB, node_id: i64, way_ids: &[i64]) -> Result<()> {
    if way_ids.is_empty() {
        db.delete_cf(&cf(db, CF_NODE_TO_WAYS), encoding::encode_key(node_id))?;
        return Ok(());
    }
    db.put_cf(
        &cf(db, CF_NODE_TO_WAYS),
        encoding::encode_key(node_id),
        encoding::encode_fixed_id_list(way_ids),
    )?;
    Ok(())
}

pub fn add_node_to_ways(db: &RocksDB, node_id: i64, way_id: i64) -> Result<()> {
    let mut way_ids = get_node_to_ways(db, node_id)?;
    if !way_ids.contains(&way_id) {
        way_ids.push(way_id);
        put_node_to_ways(db, node_id, &way_ids)?;
    }
    Ok(())
}

pub fn remove_node_to_ways(db: &RocksDB, node_id: i64, way_id: i64) -> Result<()> {
    let mut way_ids = get_node_to_ways(db, node_id)?;
    way_ids.retain(|&id| id != way_id);
    put_node_to_ways(db, node_id, &way_ids)?;
    Ok(())
}

// --- Reverse index: way -> relations ---

pub fn get_way_to_relations(db: &RocksDB, way_id: i64) -> Result<Vec<i64>> {
    if let Some(value) = db.get_cf(&cf(db, CF_WAY_TO_RELATIONS), encoding::encode_key(way_id))? {
        Ok(encoding::decode_fixed_id_list(&value))
    } else {
        Ok(vec![])
    }
}

pub fn put_way_to_relations(db: &RocksDB, way_id: i64, relation_ids: &[i64]) -> Result<()> {
    if relation_ids.is_empty() {
        db.delete_cf(&cf(db, CF_WAY_TO_RELATIONS), encoding::encode_key(way_id))?;
        return Ok(());
    }
    db.put_cf(
        &cf(db, CF_WAY_TO_RELATIONS),
        encoding::encode_key(way_id),
        encoding::encode_fixed_id_list(relation_ids),
    )?;
    Ok(())
}

pub fn add_way_to_relations(db: &RocksDB, way_id: i64, relation_id: i64) -> Result<()> {
    let mut relation_ids = get_way_to_relations(db, way_id)?;
    if !relation_ids.contains(&relation_id) {
        relation_ids.push(relation_id);
        put_way_to_relations(db, way_id, &relation_ids)?;
    }
    Ok(())
}

pub fn remove_way_to_relations(db: &RocksDB, way_id: i64, relation_id: i64) -> Result<()> {
    let mut relation_ids = get_way_to_relations(db, way_id)?;
    relation_ids.retain(|&id| id != relation_id);
    put_way_to_relations(db, way_id, &relation_ids)?;
    Ok(())
}

// The single-item `merge_*` counterparts of `batch_merge_node_to_way` /
// `batch_merge_way_to_relation` are deliberately absent: nothing writes a
// reverse-index entry outside a `WriteBatch`, so the batch pair is the whole
// public surface and the merge operator has exactly one caller per CF.

// --- WriteBatch for atomic operations ---

pub fn new_batch() -> WriteBatch {
    WriteBatch::default()
}

pub fn batch_put_node(
    db: &RocksDB,
    batch: &mut WriteBatch,
    node_id: i64,
    lon_dm: i32,
    lat_dm: i32,
) {
    batch.put_cf(
        &cf(db, CF_NODES),
        encoding::encode_key(node_id),
        encoding::encode_node(lon_dm, lat_dm),
    );
}

pub fn batch_put_way(db: &RocksDB, batch: &mut WriteBatch, way_id: i64, node_ids: &[i64]) {
    batch.put_cf(
        &cf(db, CF_WAYS),
        encoding::encode_key(way_id),
        encoding::encode_delta_id_list(node_ids),
    );
}

pub fn batch_put_relation(
    db: &RocksDB,
    batch: &mut WriteBatch,
    relation_id: i64,
    members: &[(i64, u8, u8)],
) {
    batch.put_cf(
        &cf(db, CF_RELATIONS),
        encoding::encode_key(relation_id),
        encoding::encode_relation_members(members),
    );
}

pub fn batch_merge_node_to_way(db: &RocksDB, batch: &mut WriteBatch, node_id: i64, way_id: i64) {
    batch.merge_cf(
        &cf(db, CF_NODE_TO_WAYS),
        encoding::encode_key(node_id),
        way_id.to_le_bytes(),
    );
}

pub fn batch_merge_way_to_relation(
    db: &RocksDB,
    batch: &mut WriteBatch,
    way_id: i64,
    relation_id: i64,
) {
    batch.merge_cf(
        &cf(db, CF_WAY_TO_RELATIONS),
        encoding::encode_key(way_id),
        relation_id.to_le_bytes(),
    );
}

pub fn write_batch(db: &RocksDB, batch: &WriteBatch) -> Result<()> {
    // Bulk import: skip the WAL. If the process dies mid-import the DB is
    // thrown away and restarted from the PBF anyway.
    let mut wo = WriteOptions::new();
    wo.disable_wal(true);
    db.write_opt(batch, &wo)
        .context("Failed to write RocksDB batch")?;
    Ok(())
}

/// Families `import osm` writes in id order. Their flushes never overlap, so
/// RocksDB *trivially moves* each file down to the bottommost level instead of
/// compacting it -- the import LOG shows ~320 `nodes` files moved and zero
/// real compactions -- and nothing ever rewrites them afterwards. They need a
/// forced bottommost compaction; see [`compact_osm_families`].
const ID_ORDERED_FAMILIES: &[&str] = &[CF_NODES, CF_WAYS, CF_RELATIONS];

/// Families written through the merge operator. Their operands overlap, so
/// ordinary compaction already rewrites them; a plain `compact_range` is what
/// collapses the remaining operands into single values.
const REVERSE_INDEX_FAMILIES: &[&str] = &[CF_NODE_TO_WAYS, CF_WAY_TO_RELATIONS];

/// Compact every OSM column family into its final shape. `import osm` runs
/// this as its last RocksDB step; `kv compact` runs it by hand, which is how a
/// store imported before this existed gets the benefit.
///
/// **The id-ordered families must be compacted with
/// `BottommostLevelCompaction::Force`, and that is the whole point.** A plain
/// `compact_range` skips files already sitting at the bottommost level with
/// nothing above them to merge -- which, after import's trivial moves, is most
/// of `nodes`. Forced, measured against the real Poland store:
///
/// - `nodes` 2,340 -> 1,959 MB (-16%), `ways` 664 -> 608 MB (-8.5%);
/// - the `update osm` read path (way -> refs -> batched node coordinates)
///   126-129 -> 85 ms warm (-33%), 272 -> 213 ms cold (-20%), because a lookup
///   stops probing several levels;
/// - about a minute inside a full import, opened with
///   [`open_for_bulk_load`] (4 minutes without subcompactions).
///
/// The size drop comes from sequence numbers: a bottommost compaction zeroes
/// them, a trivial move keeps each key's 8-byte trailer as written, and on a
/// 16-byte node record that trailer is most of what zstd cannot squeeze.
///
/// The reverse indexes deliberately stay on a plain `compact_range`: forcing
/// `node_to_ways` measured 1,289 -> 1,287 MB for 143 s, because the merge
/// operator had already made import compact it for real.
///
/// Cancellation is checked **between** families only. RocksDB offers no way to
/// abort a manual compaction in flight, so a Ctrl+C lets the current family
/// finish (minutes, for `nodes`) -- which is also why stopping is safe: every
/// family is either fully rewritten or untouched, never half-way.
pub fn compact_osm_families(db: &RocksDB) -> Result<()> {
    let sst_bytes = |name: &str| {
        db.property_int_value_cf(&cf(db, name), "rocksdb.total-sst-files-size")
            .ok()
            .flatten()
            .unwrap_or(0)
    };
    let mut forced = rocksdb::CompactOptions::default();
    forced.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
    let plain = rocksdb::CompactOptions::default();

    let families = ID_ORDERED_FAMILIES
        .iter()
        .map(|n| (*n, &forced))
        .chain(REVERSE_INDEX_FAMILIES.iter().map(|n| (*n, &plain)));
    for (name, opts) in families {
        crate::shutdown::check_requested()?;
        let before = sst_bytes(name);
        let t = std::time::Instant::now();
        db.compact_range_cf_opt(&cf(db, name), None::<&[u8]>, None::<&[u8]>, opts);
        tracing::info!(
            family = name,
            before_mb = before / (1 << 20),
            after_mb = sst_bytes(name) / (1 << 20),
            elapsed_s = t.elapsed().as_secs(),
            "compacted RocksDB column family"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_tmp_db() -> (TempDir, RocksDB) {
        let tmp = TempDir::new().unwrap();
        let db = open(tmp.path(), 32, 4, 8).unwrap();
        (tmp, db)
    }

    fn dm(v: f64) -> i32 {
        encoding::f64_to_decimicro(v)
    }

    #[test]
    fn test_node_roundtrip() {
        let (_tmp, db) = open_tmp_db();
        put_node(&db, 1, dm(20.0), dm(50.0)).unwrap();
        assert_eq!(get_node(&db, 1).unwrap(), Some((dm(20.0), dm(50.0))));
        delete_node(&db, 1).unwrap();
        assert_eq!(get_node(&db, 1).unwrap(), None);
    }

    #[test]
    fn test_multi_get_nodes_wkb_coords() {
        let (_tmp, db) = open_tmp_db();
        put_node(&db, 1, dm(20.0), dm(50.0)).unwrap();
        put_node(&db, 2, dm(21.0), dm(51.0)).unwrap();

        // The buffer is widened to WKB coordinate pairs, so it is twice the
        // stored size, not equal to it.
        let raw = multi_get_nodes_wkb_coords(&db, &[1, 2]).unwrap().unwrap();
        assert_eq!(raw.len(), 2 * encoding::WKB_COORD_BYTE_LEN);
        let lon = f64::from_le_bytes(raw[..8].try_into().unwrap());
        let lat = f64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert!((lon - 20.0).abs() < 1e-9);
        assert!((lat - 50.0).abs() < 1e-9);

        // A missing node anywhere in the list yields None.
        let res = multi_get_nodes_wkb_coords(&db, &[1, 999]).unwrap();
        assert!(res.is_none());
    }

    /// A store written by a *different* version must be rejected too — this is
    /// the case that would otherwise decode to plausible-looking garbage.
    #[test]
    fn store_with_a_different_format_version_is_rejected_on_open() {
        let tmp = TempDir::new().unwrap();
        {
            let db = open(tmp.path(), 32, 4, 8).unwrap();
            put_node(&db, 1, dm(20.0), dm(50.0)).unwrap();
            db.put_cf(
                &cf(&db, CF_META),
                FORMAT_VERSION_KEY,
                (KV_FORMAT_VERSION + 1).to_le_bytes(),
            )
            .unwrap();
        }

        let err = open(tmp.path(), 32, 4, 8).unwrap_err();
        assert!(
            format!("{err:#}").contains(FORMAT_MISMATCH_MESSAGE),
            "got: {err:#}"
        );
    }

    /// `clear` drops every CF including `meta`, so it must re-stamp. Asserted
    /// on the stamp itself, not on a later `open` succeeding: `open` stamps an
    /// unstamped store on its own, so a reopen would pass either way.
    /// Every family opens on the cache it was configured with, and the non-tile
    /// families **share one pool**.
    ///
    /// Both halves are needed and they catch different regressions. Capacity
    /// alone would pass if each family were handed its own
    /// `Cache::new_lru_cache(block_cache_mb)` -- the budget would then be
    /// multiplied by the family count while every capacity still read
    /// correctly. Usage is what distinguishes one shared pool from N private
    /// ones, and it only says anything once a read has actually populated a
    /// block, which is why this flushes an SST first: a memtable read never
    /// touches the block cache at all.
    #[test]
    fn every_column_family_opens_on_its_configured_block_cache() {
        let tmp = tempfile::tempdir().unwrap();
        // Neither size may be 32 MB: that is RocksDB's own default, so a
        // family that silently fell back to it would still report the
        // "configured" capacity and the check would pass for the wrong reason.
        let db = open(tmp.path(), 48, 4, 8).unwrap();

        for name in ALL_CFS {
            let expected = if *name == CF_TILES { 8 } else { 48 } * 1024 * 1024;
            let capacity = db
                .property_int_value_cf(&cf(&db, name), "rocksdb.block-cache-capacity")
                .unwrap()
                .unwrap();
            assert_eq!(
                capacity, expected,
                "{name} must open on the configured cache, not RocksDB's silent 32 MB default"
            );
        }

        // Populate the shared pool through one family: write, flush to an SST
        // (the block cache is only consulted for SST reads), then read back.
        put_node(&db, 1, 210_000_000, 520_000_000).unwrap();
        db.flush_cf(&cf(&db, CF_NODES)).unwrap();
        assert!(get_node(&db, 1).unwrap().is_some());

        let usage = |name: &str| {
            db.property_int_value_cf(&cf(&db, name), "rocksdb.block-cache-usage")
                .unwrap()
                .unwrap()
        };
        let shared = usage(CF_NODES);
        assert!(shared > 0, "the read should have cached a block");
        for name in ALL_CFS.iter().filter(|n| **n != CF_TILES) {
            assert_eq!(
                usage(name),
                shared,
                "{name} reports its own usage, so it is not sharing the OSM pool"
            );
        }
        // Not zero: an empty family still reports a few dozen bytes of the
        // cache's own bookkeeping. What matters is that it is a *different*
        // pool, so a browsing session's tile blocks can never evict the OSM
        // node blocks `update osm` reads on every minutely diff.
        assert!(
            usage(CF_TILES) < shared,
            "tiles must be a separate pool from the OSM families"
        );
    }

    /// Statistics live on the `Options` object rather than the database, so
    /// this pins both halves of making them usable: that `open` enables them,
    /// and that the handle still holds the options once `open` has returned.
    /// A read has to come off an SST for the block-cache counters to move --
    /// a memtable read never consults the cache -- hence the flush.
    #[test]
    fn stats_count_block_cache_reads_after_open_returns() {
        let tmp = tempfile::tempdir().unwrap();
        let db = open(tmp.path(), 48, 4, 8).unwrap();
        let before = db.stats();
        assert_eq!(before.shared_block_cache_capacity_bytes, 48 * 1024 * 1024);
        assert_eq!(before.tiles_block_cache_capacity_bytes, 8 * 1024 * 1024);

        put_node(&db, 1, 210_000_000, 520_000_000).unwrap();
        db.flush_cf(&cf(&db, CF_NODES)).unwrap();
        assert!(get_node(&db, 1).unwrap().is_some());

        let after = db.stats();
        assert!(
            after.block_cache_data_miss > before.block_cache_data_miss,
            "an SST read should register as a block-cache miss -- are statistics enabled?"
        );
        assert!(after.shared_block_cache_usage_bytes > 0);
    }

    /// Recreates the shape `import osm` leaves `nodes` in: id-ordered writes,
    /// flushed, then moved to the bottommost level by a plain `compact_range`
    /// -- which, with nothing overlapping, is a trivial move that rewrites
    /// nothing and keeps every key's sequence number. That is the state the
    /// real store was measured in, and the one a plain compaction cannot fix.
    fn a_trivially_moved_nodes_family() -> (TempDir, RocksDB) {
        let (tmp, db) = open_tmp_db();
        let mut batch = new_batch();
        for id in 1..=5_000 {
            batch_put_node(&db, &mut batch, id, 210_000_000 + id as i32, 520_000_000);
        }
        write_batch(&db, &batch).unwrap();
        db.flush_cf(&cf(&db, CF_NODES)).unwrap();
        db.compact_range_cf(&cf(&db, CF_NODES), None::<&[u8]>, None::<&[u8]>);
        (tmp, db)
    }

    fn node_files(db: &RocksDB) -> Vec<rocksdb::LiveFile> {
        db.live_files()
            .unwrap()
            .into_iter()
            .filter(|f| f.column_family_name == CF_NODES)
            .collect()
    }

    /// The id-ordered families must be compacted with
    /// `BottommostLevelCompaction::Force`. A plain compaction leaves a
    /// trivially moved file exactly as written -- same file, sequence numbers
    /// intact -- and that file is where the measured 16% size and 33% read
    /// cost come from. Asserted on the file itself rather than on its size,
    /// which depends on how well a test fixture happens to compress.
    #[test]
    fn compaction_rewrites_id_ordered_data_a_plain_compaction_leaves_alone() {
        let (_tmp, db) = a_trivially_moved_nodes_family();
        let before = node_files(&db);
        assert!(
            before.iter().any(|f| f.largest_seqno > 0),
            "fixture should start with sequence numbers intact, as import leaves them"
        );

        // The trap: a second plain compaction is a no-op on this shape.
        db.compact_range_cf(&cf(&db, CF_NODES), None::<&[u8]>, None::<&[u8]>);
        let names =
            |files: &[rocksdb::LiveFile]| files.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
        assert_eq!(names(&node_files(&db)), names(&before));

        compact_osm_families(&db).unwrap();
        let after = node_files(&db);
        assert!(
            after.iter().all(|f| !names(&before).contains(&f.name)),
            "every nodes file should have been rewritten"
        );
        assert!(
            after.iter().all(|f| f.largest_seqno == 0),
            "a bottommost rewrite zeroes sequence numbers: {:?}",
            after.iter().map(|f| f.largest_seqno).collect::<Vec<_>>()
        );
        assert_eq!(
            get_node(&db, 4_321).unwrap(),
            Some((210_004_321, 520_000_000))
        );
    }

    /// The reverse indexes are still compacted -- that is what collapses the
    /// merge operands `import osm` writes -- even though they are not forced.
    #[test]
    fn compaction_still_collapses_reverse_index_merge_operands() {
        let (_tmp, db) = open_tmp_db();
        let mut batch = new_batch();
        for way in [100, 101, 102] {
            batch_merge_node_to_way(&db, &mut batch, 10, way);
        }
        write_batch(&db, &batch).unwrap();
        db.flush_cf(&cf(&db, CF_NODE_TO_WAYS)).unwrap();

        compact_osm_families(&db).unwrap();
        let files: Vec<_> = db
            .live_files()
            .unwrap()
            .into_iter()
            .filter(|f| f.column_family_name == CF_NODE_TO_WAYS)
            .collect();
        assert_eq!(files.iter().map(|f| f.num_entries).sum::<u64>(), 1);
        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![100, 101, 102]);
    }

    #[test]
    fn clear_restamps_the_format_version() {
        let tmp = TempDir::new().unwrap();
        let db = open(tmp.path(), 32, 4, 8).unwrap();
        put_node(&db, 1, dm(20.0), dm(50.0)).unwrap();
        clear(&db).unwrap();

        let stamp = db.get_cf(&cf(&db, CF_META), FORMAT_VERSION_KEY).unwrap();
        assert_eq!(
            stamp.as_deref(),
            Some(&KV_FORMAT_VERSION.to_le_bytes()[..]),
            "clear must leave the current format stamp in place"
        );
    }

    #[test]
    fn test_way_roundtrip() {
        let (_tmp, db) = open_tmp_db();
        put_way(&db, 2, &[1, 2, 3]).unwrap();
        assert_eq!(get_way(&db, 2).unwrap(), Some(vec![1, 2, 3]));
        delete_way(&db, 2).unwrap();
        assert_eq!(get_way(&db, 2).unwrap(), None);
    }

    #[test]
    fn test_relation_roundtrip() {
        let (_tmp, db) = open_tmp_db();
        put_relation(&db, 3, &[(1, 1, 0), (2, 1, 1)]).unwrap();
        assert_eq!(
            get_relation(&db, 3).unwrap(),
            Some(vec![(1, 1, 0), (2, 1, 1)])
        );
        delete_relation(&db, 3).unwrap();
        assert_eq!(get_relation(&db, 3).unwrap(), None);
    }

    #[test]
    fn test_reverse_index_node_to_ways() {
        let (_tmp, db) = open_tmp_db();
        add_node_to_ways(&db, 10, 100).unwrap();
        add_node_to_ways(&db, 10, 101).unwrap();
        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![100, 101]);
        remove_node_to_ways(&db, 10, 100).unwrap();
        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![101]);
    }

    #[test]
    fn test_reverse_index_way_to_relations() {
        let (_tmp, db) = open_tmp_db();
        add_way_to_relations(&db, 20, 200).unwrap();
        add_way_to_relations(&db, 20, 201).unwrap();
        assert_eq!(get_way_to_relations(&db, 20).unwrap(), vec![200, 201]);
        remove_way_to_relations(&db, 20, 200).unwrap();
        assert_eq!(get_way_to_relations(&db, 20).unwrap(), vec![201]);
    }

    #[test]
    fn test_merge_node_to_ways() {
        let (_tmp, db) = open_tmp_db();
        let mut batch = new_batch();
        batch_merge_node_to_way(&db, &mut batch, 10, 100);
        batch_merge_node_to_way(&db, &mut batch, 10, 101);
        batch_merge_node_to_way(&db, &mut batch, 10, 102);
        write_batch(&db, &batch).unwrap();
        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![100, 101, 102]);
    }

    #[test]
    fn test_merge_way_to_relation() {
        let (_tmp, db) = open_tmp_db();
        let mut batch = new_batch();
        batch_merge_way_to_relation(&db, &mut batch, 20, 200);
        batch_merge_way_to_relation(&db, &mut batch, 20, 201);
        write_batch(&db, &batch).unwrap();
        assert_eq!(get_way_to_relations(&db, 20).unwrap(), vec![200, 201]);
    }

    #[test]
    fn test_merge_on_top_of_existing_put() {
        let (_tmp, db) = open_tmp_db();
        put_node_to_ways(&db, 10, &[100, 101]).unwrap();
        let mut batch = new_batch();
        batch_merge_node_to_way(&db, &mut batch, 10, 102);
        batch_merge_node_to_way(&db, &mut batch, 10, 103);
        write_batch(&db, &batch).unwrap();
        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![100, 101, 102, 103]);
    }
}
