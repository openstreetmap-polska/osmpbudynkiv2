# RocksDB Write Optimization Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize RocksDB population during OSM import by batching reverse index writes and tuning per-column-family options.

**Architecture:** Three fixes: (1) accumulate reverse index updates in a chunked `HashMap` and flush as `WriteBatch` instead of individual read-modify-write per node ref, (2) include reverse index writes in the same `WriteBatch` as primary writes during import, (3) propagate compression and write buffer settings to per-column-family options instead of only setting them at the DB level.

**Tech Stack:** Rust, rocksdb 0.24, HashMap for chunked accumulation

---

## File Structure

- `src/osm/kvstore.rs` — add `batch_put_node_to_ways`, `batch_put_way_to_relations` helpers; propagate CF options in `open`
- `src/import/osm.rs` — rewrite `stream_ways_to_rocksdb` and `stream_relations_to_rocksdb` to use chunked HashMap + WriteBatch for reverse indexes
- `src/osm/kvstore.rs` tests — test the new batch helpers

---

### Task 1: Propagate compression and write buffer to column family options

Currently `open()` sets `set_compression_type` and `set_write_buffer_size` on `db_opts`, but creates each CF with `Options::default()`. Per-CF options don't inherit from DB-level options in RocksDB — each CF gets the default 4MB write buffer and no compression. Fix by applying the same settings to each CF descriptor.

**Files:**
- Modify: `src/osm/kvstore.rs:49-53`

- [ ] **Step 1: Run existing tests to establish baseline**

Run: `cargo test osm::kvstore`
Expected: All pass.

- [ ] **Step 2: Update CF descriptor creation to use tuned options**

Replace lines 49-53 in `src/osm/kvstore.rs`:

```rust
    let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect();
```

With:

```rust
    let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
        .iter()
        .map(|name| {
            let mut cf_opts = Options::default();
            cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
            cf_opts.set_write_buffer_size(
                (write_buffer_mb * 1024 * 1024)
                    .try_into()
                    .expect("write_buffer_mb overflow"),
            );
            ColumnFamilyDescriptor::new(*name, cf_opts)
        })
        .collect();
```

- [ ] **Step 3: Run tests**

Run: `cargo test osm::kvstore`
Expected: All pass. Behavior is identical — just better tuned under the hood.

- [ ] **Step 4: Commit**

```bash
git add src/osm/kvstore.rs
git commit -m "perf: propagate compression and write buffer settings to RocksDB column families"
```

---

### Task 2: Add batch helpers for reverse index writes

Add `batch_put_node_to_ways` and `batch_put_way_to_relations` that write to a `WriteBatch` instead of calling `db.put_cf()` directly. These will be used by the chunked import in Task 3.

**Files:**
- Modify: `src/osm/kvstore.rs`

- [ ] **Step 1: Write the test for batch_put_node_to_ways**

Add to the `tests` module in `src/osm/kvstore.rs`:

```rust
    #[test]
    fn test_batch_put_node_to_ways() {
        let (_tmp, db) = open_tmp_db();
        let mut batch = new_batch();
        batch_put_node_to_ways(&db, &mut batch, 10, &[100, 101]);
        batch_put_node_to_ways(&db, &mut batch, 11, &[200]);
        write_batch(&db, batch).unwrap();

        assert_eq!(get_node_to_ways(&db, 10).unwrap(), vec![100, 101]);
        assert_eq!(get_node_to_ways(&db, 11).unwrap(), vec![200]);
    }

    #[test]
    fn test_batch_put_way_to_relations() {
        let (_tmp, db) = open_tmp_db();
        let mut batch = new_batch();
        batch_put_way_to_relations(&db, &mut batch, 20, &[300, 301]);
        write_batch(&db, batch).unwrap();

        assert_eq!(get_way_to_relations(&db, 20).unwrap(), vec![300, 301]);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test osm::kvstore`
Expected: Compilation error — `batch_put_node_to_ways` and `batch_put_way_to_relations` don't exist yet.

- [ ] **Step 3: Implement the batch helpers**

Add after the `batch_put_relation` function (around line 258) in `src/osm/kvstore.rs`:

```rust
pub fn batch_put_node_to_ways(
    db: &RocksDB,
    batch: &mut WriteBatch,
    node_id: i64,
    way_ids: &[i64],
) {
    if way_ids.is_empty() {
        batch.delete_cf(&cf(db, CF_NODE_TO_WAYS), encoding::encode_key(node_id));
    } else {
        batch.put_cf(
            &cf(db, CF_NODE_TO_WAYS),
            encoding::encode_key(node_id),
            encoding::encode_id_list(way_ids),
        );
    }
}

pub fn batch_put_way_to_relations(
    db: &RocksDB,
    batch: &mut WriteBatch,
    way_id: i64,
    relation_ids: &[i64],
) {
    if relation_ids.is_empty() {
        batch.delete_cf(&cf(db, CF_WAY_TO_RELATIONS), encoding::encode_key(way_id));
    } else {
        batch.put_cf(
            &cf(db, CF_WAY_TO_RELATIONS),
            encoding::encode_key(way_id),
            encoding::encode_id_list(relation_ids),
        );
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test osm::kvstore`
Expected: All pass, including the two new tests.

- [ ] **Step 5: Commit**

```bash
git add src/osm/kvstore.rs
git commit -m "feat: add WriteBatch helpers for reverse index writes"
```

---

### Task 3: Rewrite stream_ways_to_rocksdb with chunked reverse index batching

Replace the per-node-ref read-modify-write with a chunked `HashMap<i64, Vec<i64>>` that accumulates `node_id → [way_ids]` mappings in memory, then flushes them as a single `WriteBatch`. The chunk size limits memory usage.

**Problem being fixed:** For each of ~300M node refs, the old code does: `GET node_to_ways` → decode → `contains()` linear scan → push → encode → `PUT node_to_ways`. That's 300M individual RocksDB round-trips outside any batch.

**New approach:** Accumulate in a `HashMap<i64, Vec<i64>>`. When the map exceeds `CHUNK_SIZE` entries, merge each entry with what's already in RocksDB (one GET per unique node), write the merged result as a `WriteBatch` (one write for the whole chunk), then clear the map. The merge-on-flush handles the case where the same node appears in chunks processed at different times.

**Files:**
- Modify: `src/import/osm.rs:145-178`

- [ ] **Step 1: Add HashMap import at the top of import/osm.rs**

Add to the imports at the top of `src/import/osm.rs`:

```rust
use std::collections::HashMap;
```

- [ ] **Step 2: Rewrite stream_ways_to_rocksdb**

Replace the entire `stream_ways_to_rocksdb` function (lines 145-178) with:

```rust
fn stream_ways_to_rocksdb(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Pass 2: Streaming ways to RocksDB");

    const CHUNK_SIZE: usize = 500_000;

    let sql = format!(
        "SELECT id, refs FROM ST_ReadOSM('{pbf_path}') WHERE kind = 'way' AND refs IS NOT NULL AND len(refs) > 0"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut batch = kvstore::new_batch();
    let mut count = 0u64;

    // Accumulate node_id → [way_ids] in memory, flush in chunks.
    let mut node_to_ways: HashMap<i64, Vec<i64>> = HashMap::with_capacity(CHUNK_SIZE);

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let refs_value: Value = row.get(1)?;
        let refs = value_to_i64_list(refs_value)?;
        kvstore::batch_put_way(kv, &mut batch, id, &refs);

        for &node_id in &refs {
            node_to_ways.entry(node_id).or_default().push(id);
        }

        count += 1;
        if count % 10000 == 0 {
            kvstore::write_batch(kv, batch)?;
            batch = kvstore::new_batch();
        }

        if node_to_ways.len() >= CHUNK_SIZE {
            flush_node_to_ways(kv, &mut node_to_ways)?;
        }
    }

    if count % 10000 != 0 {
        kvstore::write_batch(kv, batch)?;
    }
    if !node_to_ways.is_empty() {
        flush_node_to_ways(kv, &mut node_to_ways)?;
    }

    info!(count, "Ways streamed to RocksDB");
    Ok(())
}

/// Merge accumulated node→ways mappings with existing RocksDB data and write as a single batch.
fn flush_node_to_ways(kv: &RocksDB, map: &mut HashMap<i64, Vec<i64>>) -> Result<()> {
    let mut batch = kvstore::new_batch();
    for (&node_id, new_way_ids) in map.iter() {
        let mut existing = kvstore::get_node_to_ways(kv, node_id)?;
        for &wid in new_way_ids {
            if !existing.contains(&wid) {
                existing.push(wid);
            }
        }
        kvstore::batch_put_node_to_ways(kv, &mut batch, node_id, &existing);
    }
    kvstore::write_batch(kv, batch)?;
    map.clear();
    Ok(())
}
```

**Memory analysis:** Each `HashMap` entry is ~(8 key + 24 Vec header + 8*avg_ways_per_node) bytes. At `CHUNK_SIZE = 500_000` unique nodes with ~2 way_ids each: 500k * (8 + 24 + 16) ≈ 24MB — well within budget.

- [ ] **Step 3: Run import tests**

Run: `cargo test import::osm`
Expected: All pass. The fixture PBF tests exercise the full import path.

- [ ] **Step 4: Commit**

```bash
git add src/import/osm.rs
git commit -m "perf: batch reverse index writes in stream_ways_to_rocksdb using chunked HashMap"
```

---

### Task 4: Rewrite stream_relations_to_rocksdb with chunked reverse index batching

Same pattern as Task 3, but for the `way_id → [relation_ids]` reverse index.

**Files:**
- Modify: `src/import/osm.rs:351-403`

- [ ] **Step 1: Rewrite stream_relations_to_rocksdb**

Replace the entire `stream_relations_to_rocksdb` function (lines 351-403) with:

```rust
fn stream_relations_to_rocksdb(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Pass 3: Streaming relations to RocksDB");

    const CHUNK_SIZE: usize = 100_000;

    let sql = format!(
        "SELECT id, refs, ref_types, ref_roles FROM ST_ReadOSM('{pbf_path}') WHERE kind = 'relation' AND refs IS NOT NULL AND len(refs) > 0"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut batch = kvstore::new_batch();
    let mut count = 0u64;

    // Accumulate way_id → [relation_ids] in memory, flush in chunks.
    let mut way_to_relations: HashMap<i64, Vec<i64>> = HashMap::with_capacity(CHUNK_SIZE);

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
                way_to_relations.entry(way_id).or_default().push(id);
            }
        }

        count += 1;
        if count % 1000 == 0 {
            kvstore::write_batch(kv, batch)?;
            batch = kvstore::new_batch();
        }

        if way_to_relations.len() >= CHUNK_SIZE {
            flush_way_to_relations(kv, &mut way_to_relations)?;
        }
    }

    if count % 1000 != 0 {
        kvstore::write_batch(kv, batch)?;
    }
    if !way_to_relations.is_empty() {
        flush_way_to_relations(kv, &mut way_to_relations)?;
    }

    info!(count, "Relations streamed to RocksDB");
    Ok(())
}

/// Merge accumulated way→relations mappings with existing RocksDB data and write as a single batch.
fn flush_way_to_relations(kv: &RocksDB, map: &mut HashMap<i64, Vec<i64>>) -> Result<()> {
    let mut batch = kvstore::new_batch();
    for (&way_id, new_rel_ids) in map.iter() {
        let mut existing = kvstore::get_way_to_relations(kv, way_id)?;
        for &rid in new_rel_ids {
            if !existing.contains(&rid) {
                existing.push(rid);
            }
        }
        kvstore::batch_put_way_to_relations(kv, &mut batch, way_id, &existing);
    }
    kvstore::write_batch(kv, batch)?;
    map.clear();
    Ok(())
}
```

**Memory analysis:** Relations are far fewer (~50k for Poland vs ~30M ways). `CHUNK_SIZE = 100_000` unique ways with ~1-2 relation_ids each: ~100k * 40 ≈ 4MB.

- [ ] **Step 2: Run import tests**

Run: `cargo test import::osm`
Expected: All pass.

- [ ] **Step 3: Run full test suite**

Run: `cargo test`
Expected: All pass. The update tests are unaffected — they use the per-item `add_node_to_ways`/`add_way_to_relations` which are correct for small incremental updates.

- [ ] **Step 4: Commit**

```bash
git add src/import/osm.rs
git commit -m "perf: batch reverse index writes in stream_relations_to_rocksdb using chunked HashMap"
```
