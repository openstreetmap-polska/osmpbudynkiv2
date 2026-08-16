use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MergeOperands,
    MultiThreaded, Options, WriteBatch, WriteOptions,
};

use super::encoding;

/// Column family names for the 5 key spaces, plus a tiny `meta` space holding
/// the on-disk format version.
pub const CF_NODES: &str = "nodes";
pub const CF_WAYS: &str = "ways";
pub const CF_RELATIONS: &str = "relations";
pub const CF_NODE_TO_WAYS: &str = "node_to_ways";
pub const CF_WAY_TO_RELATIONS: &str = "way_to_relations";
pub const CF_META: &str = "meta";

const ALL_CFS: &[&str] = &[
    CF_NODES,
    CF_WAYS,
    CF_RELATIONS,
    CF_NODE_TO_WAYS,
    CF_WAY_TO_RELATIONS,
    CF_META,
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
///
/// Version 2: `i32` decimicrodegree node values (was two `f64`), big-endian
/// keys (was little-endian), delta+varint way ref lists (was fixed-width).
pub const KV_FORMAT_VERSION: u32 = 2;

const FORMAT_VERSION_KEY: &[u8] = b"format_version";

/// Message used when the store predates [`KV_FORMAT_VERSION`]. Named so tests
/// can assert on it exactly rather than on a substring.
pub const FORMAT_MISMATCH_MESSAGE: &str = "RocksDB store was built by an incompatible version — re-run `import osm` \
     to rebuild it (there is no in-place migration)";

pub type RocksDB = DBWithThreadMode<MultiThreaded>;

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

fn make_cf_opts(name: &str, write_buffer_bytes: usize) -> Options {
    let mut cf_opts = Options::default();
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

pub fn open(path: &Path, block_cache_mb: u64, write_buffer_mb: u64) -> Result<RocksDB> {
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
    db_opts.set_bytes_per_sync(1 << 20);
    db_opts.set_wal_bytes_per_sync(1 << 20);

    let mut bbt = BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(
        (block_cache_mb * 1024 * 1024)
            .try_into()
            .context("block_cache_mb overflow")?,
    );
    bbt.set_block_cache(&cache);
    db_opts.set_block_based_table_factory(&bbt);

    let write_buffer_bytes: usize = (write_buffer_mb * 1024 * 1024)
        .try_into()
        .context("write_buffer_mb overflow")?;

    let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, make_cf_opts(name, write_buffer_bytes)))
        .collect();

    let db = DBWithThreadMode::open_cf_descriptors(&db_opts, path, cfs)
        .context("Failed to open RocksDB")?;

    check_or_stamp_format_version(&db)?;

    Ok(db)
}

/// Verify the store's format version, stamping it if the store is empty.
///
/// An empty store (fresh directory, or one just cleared by [`clear`]) is
/// stamped with the current version. A store carrying data but no stamp was
/// built before versioning existed, so it is by definition an older layout and
/// is rejected. See [`KV_FORMAT_VERSION`] for why silence here is dangerous.
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
        None => {
            if store_has_data(db) {
                anyhow::bail!("{FORMAT_MISMATCH_MESSAGE} (found an unversioned store)");
            }
            stamp_format_version(db)
        }
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
fn store_has_data(db: &RocksDB) -> bool {
    db.iterator_cf(&cf(db, CF_NODES), rocksdb::IteratorMode::Start)
        .next()
        .is_some()
}

/// Drop and recreate all column families, effectively clearing all data.
pub fn clear(db: &RocksDB) -> Result<()> {
    for name in ALL_CFS {
        db.drop_cf(name)
            .with_context(|| format!("Failed to drop CF {name}"))?;
        db.create_cf(*name, &make_cf_opts(name, 0))
            .with_context(|| format!("Failed to recreate CF {name}"))?;
    }
    // The meta CF was just dropped along with the rest, so the version stamp
    // has to be rewritten or the next `open` would see a store with data and
    // no stamp, and reject it.
    stamp_format_version(db)?;
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
#[allow(dead_code)]
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
/// Note this widens rather than copies: node values are stored as `i32`
/// decimicrodegrees, which is half the bytes but no longer byte-identical to
/// WKB's layout. See `encoding::push_wkb_coords`.
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

#[allow(dead_code)]
pub fn merge_node_to_way(db: &RocksDB, node_id: i64, way_id: i64) -> Result<()> {
    db.merge_cf(
        &cf(db, CF_NODE_TO_WAYS),
        encoding::encode_key(node_id),
        way_id.to_le_bytes(),
    )?;
    Ok(())
}

#[allow(dead_code)]
pub fn merge_way_to_relation(db: &RocksDB, way_id: i64, relation_id: i64) -> Result<()> {
    db.merge_cf(
        &cf(db, CF_WAY_TO_RELATIONS),
        encoding::encode_key(way_id),
        relation_id.to_le_bytes(),
    )?;
    Ok(())
}

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

/// Compact the reverse-index column families to collapse merge operands.
/// Call after bulk import to optimize read performance.
pub fn compact_reverse_indexes(db: &RocksDB) {
    db.compact_range_cf(&cf(db, CF_NODE_TO_WAYS), None::<&[u8]>, None::<&[u8]>);
    db.compact_range_cf(&cf(db, CF_WAY_TO_RELATIONS), None::<&[u8]>, None::<&[u8]>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_tmp_db() -> (TempDir, RocksDB) {
        let tmp = TempDir::new().unwrap();
        let db = open(tmp.path(), 32, 4).unwrap();
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

    /// A store carrying data but no version stamp predates versioning, so it
    /// must be rejected rather than decoded as if it were the current layout.
    #[test]
    fn unversioned_store_with_data_is_rejected_on_open() {
        let tmp = TempDir::new().unwrap();
        {
            let db = open(tmp.path(), 32, 4).unwrap();
            put_node(&db, 1, dm(20.0), dm(50.0)).unwrap();
            // Simulate a pre-versioning store: data present, stamp absent.
            db.delete_cf(&cf(&db, CF_META), FORMAT_VERSION_KEY).unwrap();
        }

        let err = open(tmp.path(), 32, 4).unwrap_err();
        assert!(
            format!("{err:#}").contains(FORMAT_MISMATCH_MESSAGE),
            "got: {err:#}"
        );
    }

    /// A store written by a *different* version must be rejected too — this is
    /// the case that would otherwise decode to plausible-looking garbage.
    #[test]
    fn store_with_a_different_format_version_is_rejected_on_open() {
        let tmp = TempDir::new().unwrap();
        {
            let db = open(tmp.path(), 32, 4).unwrap();
            put_node(&db, 1, dm(20.0), dm(50.0)).unwrap();
            db.put_cf(
                &cf(&db, CF_META),
                FORMAT_VERSION_KEY,
                (KV_FORMAT_VERSION + 1).to_le_bytes(),
            )
            .unwrap();
        }

        let err = open(tmp.path(), 32, 4).unwrap_err();
        assert!(
            format!("{err:#}").contains(FORMAT_MISMATCH_MESSAGE),
            "got: {err:#}"
        );
    }

    /// `clear` drops every CF including `meta`, so it must re-stamp — otherwise
    /// the next open sees data without a stamp and refuses to start.
    #[test]
    fn clear_restamps_the_format_version_so_reopen_succeeds() {
        let tmp = TempDir::new().unwrap();
        {
            let db = open(tmp.path(), 32, 4).unwrap();
            put_node(&db, 1, dm(20.0), dm(50.0)).unwrap();
            clear(&db).unwrap();
            put_node(&db, 2, dm(21.0), dm(51.0)).unwrap();
        }
        let db = open(tmp.path(), 32, 4).unwrap();
        assert_eq!(get_node(&db, 2).unwrap(), Some((dm(21.0), dm(51.0))));
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
        merge_node_to_way(&db, 10, 100).unwrap();
        merge_node_to_way(&db, 10, 101).unwrap();
        merge_node_to_way(&db, 10, 102).unwrap();
        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![100, 101, 102]);
    }

    #[test]
    fn test_merge_way_to_relation() {
        let (_tmp, db) = open_tmp_db();
        merge_way_to_relation(&db, 20, 200).unwrap();
        merge_way_to_relation(&db, 20, 201).unwrap();
        assert_eq!(get_way_to_relations(&db, 20).unwrap(), vec![200, 201]);
    }

    #[test]
    fn test_merge_on_top_of_existing_put() {
        let (_tmp, db) = open_tmp_db();
        put_node_to_ways(&db, 10, &[100, 101]).unwrap();
        merge_node_to_way(&db, 10, 102).unwrap();
        merge_node_to_way(&db, 10, 103).unwrap();
        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![100, 101, 102, 103]);
    }
}
