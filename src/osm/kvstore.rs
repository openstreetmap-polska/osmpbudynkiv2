use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded,
    Options, WriteBatch,
};

use super::encoding;

/// Column family names for the 5 key spaces.
pub const CF_NODES: &str = "nodes";
pub const CF_WAYS: &str = "ways";
pub const CF_RELATIONS: &str = "relations";
pub const CF_NODE_TO_WAYS: &str = "node_to_ways";
pub const CF_WAY_TO_RELATIONS: &str = "way_to_relations";

const ALL_CFS: &[&str] = &[
    CF_NODES,
    CF_WAYS,
    CF_RELATIONS,
    CF_NODE_TO_WAYS,
    CF_WAY_TO_RELATIONS,
];

pub type RocksDB = DBWithThreadMode<MultiThreaded>;

pub fn open(path: &Path, block_cache_mb: u64, write_buffer_mb: u64) -> Result<RocksDB> {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

    let mut bbt = BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(
        (block_cache_mb * 1024 * 1024)
            .try_into()
            .context("block_cache_mb overflow")?,
    );
    bbt.set_block_cache(&cache);
    db_opts.set_block_based_table_factory(&bbt);
    db_opts.set_write_buffer_size(
        (write_buffer_mb * 1024 * 1024)
            .try_into()
            .context("write_buffer_mb overflow")?,
    );

    let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect();

    let db = DBWithThreadMode::open_cf_descriptors(&db_opts, path, cfs)
        .context("Failed to open RocksDB")?;

    Ok(db)
}

fn cf<'a>(db: &'a RocksDB, name: &str) -> Arc<BoundColumnFamily<'a>> {
    db.cf_handle(name)
        .unwrap_or_else(|| panic!("missing column family: {name}"))
}

// --- Node operations ---

pub fn put_node(db: &RocksDB, node_id: i64, lon: f64, lat: f64) -> Result<()> {
    db.put_cf(
        &cf(db, CF_NODES),
        encoding::encode_key(node_id),
        encoding::encode_node(lon, lat),
    )?;
    Ok(())
}

pub fn get_node(db: &RocksDB, node_id: i64) -> Result<Option<(f64, f64)>> {
    if let Some(value) = db.get_cf(&cf(db, CF_NODES), encoding::encode_key(node_id))? {
        let coords = encoding::decode_node(&value);
        Ok(Some(coords))
    } else {
        Ok(None)
    }
}

/// Get raw encoded node bytes (16 bytes: lon LE f64 || lat LE f64).
/// Returns the raw bytes without decoding — useful for building WKB directly.
pub fn get_node_raw(db: &RocksDB, node_id: i64) -> Result<Option<Vec<u8>>> {
    match db.get_cf(&cf(db, CF_NODES), encoding::encode_key(node_id))? {
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
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
        encoding::encode_id_list(node_ids),
    )?;
    Ok(())
}

pub fn get_way(db: &RocksDB, way_id: i64) -> Result<Option<Vec<i64>>> {
    if let Some(value) = db.get_cf(&cf(db, CF_WAYS), encoding::encode_key(way_id))? {
        Ok(Some(encoding::decode_id_list(&value)))
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
        Ok(encoding::decode_id_list(&value))
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
        encoding::encode_id_list(way_ids),
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
        Ok(encoding::decode_id_list(&value))
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
        encoding::encode_id_list(relation_ids),
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

// --- WriteBatch for atomic operations ---

pub fn new_batch() -> WriteBatch {
    WriteBatch::default()
}

pub fn batch_put_node(db: &RocksDB, batch: &mut WriteBatch, node_id: i64, lon: f64, lat: f64) {
    batch.put_cf(
        &cf(db, CF_NODES),
        encoding::encode_key(node_id),
        encoding::encode_node(lon, lat),
    );
}

pub fn batch_put_way(db: &RocksDB, batch: &mut WriteBatch, way_id: i64, node_ids: &[i64]) {
    batch.put_cf(
        &cf(db, CF_WAYS),
        encoding::encode_key(way_id),
        encoding::encode_id_list(node_ids),
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

pub fn write_batch(db: &RocksDB, batch: WriteBatch) -> Result<()> {
    db.write(batch).context("Failed to write RocksDB batch")?;
    Ok(())
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

    #[test]
    fn test_node_roundtrip() {
        let (_tmp, db) = open_tmp_db();
        put_node(&db, 1, 20.0, 50.0).unwrap();
        assert_eq!(get_node(&db, 1).unwrap(), Some((20.0, 50.0)));
        delete_node(&db, 1).unwrap();
        assert_eq!(get_node(&db, 1).unwrap(), None);
    }

    #[test]
    fn test_node_raw_roundtrip() {
        let (_tmp, db) = open_tmp_db();
        put_node(&db, 1, 20.0, 50.0).unwrap();
        let raw = get_node_raw(&db, 1).unwrap().unwrap();
        assert_eq!(raw.len(), encoding::NODE_BYTE_LEN);
        let lon = f64::from_le_bytes(raw[..8].try_into().unwrap());
        let lat = f64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert!((lon - 20.0).abs() < 1e-15);
        assert!((lat - 50.0).abs() < 1e-15);
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
}
