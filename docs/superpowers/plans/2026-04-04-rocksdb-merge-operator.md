# RocksDB Merge Operator for Reverse Indexes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate read-modify-write during reverse index population by using RocksDB merge operators, and compact after import.

**Architecture:** Register an associative merge operator on the `node_to_ways` and `way_to_relations` column families that concatenates id-list operands. During import, use `WriteBatch::merge_cf` to append single i64 values — no reads needed. On `get`, RocksDB applies the merge function transparently. The existing `encode_id_list`/`decode_id_list` format is preserved for the merged result. After import completes, run manual compaction on the two reverse-index CFs to collapse merge operands into final values.

**Tech Stack:** Rust, rocksdb 0.24 (`MergeOperands`, `set_merge_operator_associative`, `WriteBatch::merge_cf`, `compact_range_cf`)

---

## File Structure

- `src/osm/kvstore.rs` — register merge operator on reverse-index CFs in `open()`, add `merge_node_to_way`/`merge_way_to_relation` helpers, add `compact_reverse_indexes`
- `src/import/osm.rs` — rewrite `stream_ways_to_rocksdb` and `stream_relations_to_rocksdb` to use merge instead of HashMap flush; call `compact_reverse_indexes` at end of import
- `src/osm/encoding.rs` — no changes (existing `encode_id_list`/`decode_id_list` reused)

---

### Task 1: Register merge operator on reverse-index CFs

The merge function for our id-list CFs is simple: concatenate operands. Each merge operand is a raw 8-byte LE i64. The full merge combines an optional existing value (already an encoded id-list: 4-byte count + N*8-byte ids) with one or more 8-byte operands by appending the new ids and updating the count. Partial merge (operand + operand without existing value) just concatenates the raw 8-byte values.

We use `set_merge_operator` (not `set_merge_operator_associative`) because full merge and partial merge have different logic:
- **Full merge**: existing value is an `encode_id_list` (4-byte count prefix + ids) — append new ids, update count
- **Partial merge**: operands are bare 8-byte i64s — just concatenate them

**Files:**
- Modify: `src/osm/kvstore.rs`

- [ ] **Step 1: Write the test**

Add to the `tests` module in `src/osm/kvstore.rs`:

```rust
    #[test]
    fn test_merge_node_to_ways() {
        let (_tmp, db) = open_tmp_db();

        // Use merge to add way_ids one at a time
        merge_node_to_way(&db, 10, 100).unwrap();
        merge_node_to_way(&db, 10, 101).unwrap();
        merge_node_to_way(&db, 10, 102).unwrap();

        // Read should return all three
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

        // First put a value the normal way
        put_node_to_ways(&db, 10, &[100, 101]).unwrap();

        // Then merge additional values
        merge_node_to_way(&db, 10, 102).unwrap();
        merge_node_to_way(&db, 10, 103).unwrap();

        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![100, 101, 102, 103]);
    }
```

- [ ] **Step 2: Run tests to see them fail**

Run: `cargo test osm::kvstore`
Expected: Compile error — `merge_node_to_way` and `merge_way_to_relation` don't exist.

- [ ] **Step 3: Implement merge operator and helpers**

In `src/osm/kvstore.rs`, make the following changes:

**Add import** at the top, to the existing `rocksdb` use block:

```rust
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode,
    MergeOperands, MultiThreaded, Options, WriteBatch,
};
```

**Add the merge function** before `open()`:

```rust
/// Merge function for reverse-index CFs (node_to_ways, way_to_relations).
///
/// Each merge operand is a single 8-byte LE i64 (an id to append).
/// The existing value (if any) is an encoded id-list: 4-byte LE count + N * 8-byte LE i64s.
/// Full merge: append all operand ids to the existing list, update count.
/// If no existing value, build a new id-list from all operands.
fn id_list_full_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut ids: Vec<u8> = match existing {
        Some(val) if val.len() >= 4 => {
            // Skip the 4-byte count prefix, keep the raw id bytes
            val[4..].to_vec()
        }
        _ => Vec::new(),
    };

    for operand in operands {
        ids.extend_from_slice(operand);
    }

    // Build the encoded id-list: 4-byte count + raw id bytes
    let count = (ids.len() / 8) as u32;
    let mut result = Vec::with_capacity(4 + ids.len());
    result.extend_from_slice(&count.to_le_bytes());
    result.extend_from_slice(&ids);
    Some(result)
}

/// Partial merge: operands are bare 8-byte i64s. Just concatenate them.
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
```

**Update `open()`** to set the merge operator on the two reverse-index CFs. Replace the CF descriptor creation:

```rust
    let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
        .iter()
        .map(|name| {
            let mut cf_opts = Options::default();
            cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
            cf_opts.set_write_buffer_size(write_buffer_bytes);
            ColumnFamilyDescriptor::new(*name, cf_opts)
        })
        .collect();
```

With:

```rust
    let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
        .iter()
        .map(|name| {
            let mut cf_opts = Options::default();
            cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
            cf_opts.set_write_buffer_size(write_buffer_bytes);
            if *name == CF_NODE_TO_WAYS || *name == CF_WAY_TO_RELATIONS {
                cf_opts.set_merge_operator(
                    "id_list_merge",
                    id_list_full_merge,
                    id_list_partial_merge,
                );
            }
            ColumnFamilyDescriptor::new(*name, cf_opts)
        })
        .collect();
```

**Add merge helper functions** after the `add_way_to_relations` function:

```rust
pub fn merge_node_to_way(db: &RocksDB, node_id: i64, way_id: i64) -> Result<()> {
    db.merge_cf(
        &cf(db, CF_NODE_TO_WAYS),
        encoding::encode_key(node_id),
        way_id.to_le_bytes(),
    )?;
    Ok(())
}

pub fn merge_way_to_relation(db: &RocksDB, way_id: i64, relation_id: i64) -> Result<()> {
    db.merge_cf(
        &cf(db, CF_WAY_TO_RELATIONS),
        encoding::encode_key(way_id),
        relation_id.to_le_bytes(),
    )?;
    Ok(())
}
```

**Add batch merge helpers** after the existing `batch_put_way_to_relations` function:

```rust
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
```

- [ ] **Step 4: Run tests**

Run: `cargo test osm::kvstore`
Expected: All 11 tests pass (8 old + 3 new).

- [ ] **Step 5: Commit**

```bash
git add src/osm/kvstore.rs
git commit -m "feat: add merge operator for reverse-index CFs to eliminate read-modify-write"
```

---

### Task 2: Rewrite stream_ways_to_rocksdb to use merge

Replace the chunked HashMap approach with simple `batch_merge_node_to_way` calls inside the existing WriteBatch loop. No HashMap, no flush function, no reads during write — just append merge operands.

**Files:**
- Modify: `src/import/osm.rs`

- [ ] **Step 1: Rewrite stream_ways_to_rocksdb**

Replace the entire `stream_ways_to_rocksdb` function and remove the `flush_node_to_ways` helper.

Current `stream_ways_to_rocksdb` (lines 146-191) and `flush_node_to_ways` (lines 194-209) should be replaced with:

```rust
fn stream_ways_to_rocksdb(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Pass 2: Streaming ways to RocksDB");

    let sql = format!(
        "SELECT id, refs FROM ST_ReadOSM('{pbf_path}') WHERE kind = 'way' AND refs IS NOT NULL AND len(refs) > 0"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut batch = kvstore::new_batch();
    let mut count = 0u64;

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let refs_value: Value = row.get(1)?;
        let refs = value_to_i64_list(refs_value)?;
        kvstore::batch_put_way(kv, &mut batch, id, &refs);

        for &node_id in &refs {
            kvstore::batch_merge_node_to_way(kv, &mut batch, node_id, id);
        }

        count += 1;
        if count % 10000 == 0 {
            kvstore::write_batch(kv, batch)?;
            batch = kvstore::new_batch();
        }
    }

    if count % 10000 != 0 {
        kvstore::write_batch(kv, batch)?;
    }

    info!(count, "Ways streamed to RocksDB");
    Ok(())
}
```

Also remove the `use std::collections::HashMap;` import at the top if it becomes unused (check after Task 3 — `stream_relations_to_rocksdb` might still use it. If both are rewritten, remove it).

- [ ] **Step 2: Run tests**

Run: `cargo test import::osm`
Expected: All 4 pass.

- [ ] **Step 3: Commit**

```bash
git add src/import/osm.rs
git commit -m "perf: use merge operator in stream_ways_to_rocksdb, eliminate all reads during way import"
```

---

### Task 3: Rewrite stream_relations_to_rocksdb to use merge

Same pattern: replace the chunked HashMap with `batch_merge_way_to_relation`.

**Files:**
- Modify: `src/import/osm.rs`

- [ ] **Step 1: Rewrite stream_relations_to_rocksdb**

Replace the entire `stream_relations_to_rocksdb` function and remove `flush_way_to_relations`.

Current `stream_relations_to_rocksdb` and `flush_way_to_relations` should be replaced with:

```rust
fn stream_relations_to_rocksdb(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Pass 3: Streaming relations to RocksDB");

    let sql = format!(
        "SELECT id, refs, ref_types, ref_roles FROM ST_ReadOSM('{pbf_path}') WHERE kind = 'relation' AND refs IS NOT NULL AND len(refs) > 0"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut batch = kvstore::new_batch();
    let mut count = 0u64;

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let refs = value_to_i64_list(row.get::<_, Value>(1)?)?;
        let ref_types = value_to_string_list(row.get::<_, Value>(2)?)?;
        let ref_roles = value_to_string_list(row.get::<_, Value>(3)?)?;

        let members: Vec<(i64, u8, u8)> = refs
            .into_iter()
            .zip(ref_types.into_iter())
            .zip(ref_roles.into_iter())
            .map(|((ref_id, ref_type), ref_role)| {
                (
                    ref_id,
                    string_to_member_type(&ref_type),
                    string_to_member_role(&ref_role),
                )
            })
            .collect();

        kvstore::batch_put_relation(kv, &mut batch, id, &members);

        for &(way_id, ref_type, _) in &members {
            if ref_type == 1 {
                kvstore::batch_merge_way_to_relation(kv, &mut batch, way_id, id);
            }
        }

        count += 1;
        if count % 1000 == 0 {
            kvstore::write_batch(kv, batch)?;
            batch = kvstore::new_batch();
        }
    }

    if count % 1000 != 0 {
        kvstore::write_batch(kv, batch)?;
    }

    info!(count, "Relations streamed to RocksDB");
    Ok(())
}
```

- [ ] **Step 2: Remove unused HashMap import**

Remove from the top of `src/import/osm.rs`:
```rust
use std::collections::HashMap;
```

- [ ] **Step 3: Run tests**

Run: `cargo test import::osm`
Expected: All 4 pass.

- [ ] **Step 4: Commit**

```bash
git add src/import/osm.rs
git commit -m "perf: use merge operator in stream_relations_to_rocksdb, eliminate all reads during relation import"
```

---

### Task 4: Add post-import compaction

After import, the reverse-index CFs have many unmerged operands in the LSM tree. A manual compaction collapses them into final values, which:
- Reduces read amplification for subsequent lookups (UDFs, update path)
- Frees disk space (removes tombstones and redundant operands)
- Makes the subsequent update path faster (fewer merge operands to process on read)

**Files:**
- Modify: `src/osm/kvstore.rs` — add `compact_reverse_indexes`
- Modify: `src/import/osm.rs` — call it after import

- [ ] **Step 1: Add compact_reverse_indexes to kvstore**

Add after the `write_batch` function in `src/osm/kvstore.rs`:

```rust
/// Compact the reverse-index column families to collapse merge operands.
/// Call after bulk import to optimize read performance.
pub fn compact_reverse_indexes(db: &RocksDB) {
    db.compact_range_cf(&cf(db, CF_NODE_TO_WAYS), None::<&[u8]>, None::<&[u8]>);
    db.compact_range_cf(&cf(db, CF_WAY_TO_RELATIONS), None::<&[u8]>, None::<&[u8]>);
}
```

- [ ] **Step 2: Call compact_reverse_indexes after import**

In `src/import/osm.rs`, in the `import` function, add a call after `stream_relations_to_rocksdb` and before `import_way_buildings_and_addresses`. The compaction must happen before UDF queries read from the reverse indexes. Actually, the UDFs read from `nodes` and `ways` CFs, not the reverse indexes. But the update path reads reverse indexes. The best place is after all three streaming passes, before spatial index creation:

In the `import` function, after `import_relation_buildings_and_addresses(conn, pbf_str)?;` and before `create_spatial_indexes(conn)?;`, add:

```rust
    info!("Compacting reverse indexes");
    kvstore::compact_reverse_indexes(kv);
```

- [ ] **Step 3: Run full test suite**

Run: `cargo test`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add src/osm/kvstore.rs src/import/osm.rs
git commit -m "perf: compact reverse-index CFs after import to collapse merge operands"
```

---

### Task 5: Clean up now-unused code

The chunked HashMap approach from the previous plan is now fully replaced by merge operators. Remove dead code.

**Files:**
- Modify: `src/osm/kvstore.rs` — remove `batch_put_node_to_ways` and `batch_put_way_to_relations` and their tests (replaced by `batch_merge_*`)

- [ ] **Step 1: Remove batch_put_node_to_ways and batch_put_way_to_relations**

In `src/osm/kvstore.rs`, delete the `batch_put_node_to_ways` function (lines 263-273) and the `batch_put_way_to_relations` function (lines 275-290).

Also delete their tests: `test_batch_put_node_to_ways` and `test_batch_put_way_to_relations`.

- [ ] **Step 2: Check for remaining usages**

Run: `cargo build 2>&1`
Expected: Clean build. If anything still references the removed functions, fix it.

- [ ] **Step 3: Run full test suite**

Run: `cargo test`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add src/osm/kvstore.rs
git commit -m "refactor: remove batch_put reverse-index helpers, replaced by merge operators"
```
