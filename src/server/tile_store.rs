//! Persistent storage for rendered z12..=z14 `/tiles` bodies, in the `tiles`
//! RocksDB column family.
//!
//! # Why this exists at all
//!
//! A z14 tile costs a median 49 ms to render and up to 709 ms in dense cities,
//! against a ~1 ms point lookup here -- and unlike `tile_cache` this survives a
//! restart, so a deploy no longer throws away every tile the country has ever
//! looked at. On the production disk (slower than the dev NVMe those numbers
//! come from) the gap is wider still.
//!
//! # This store is authoritative, and nothing verifies it at read time
//!
//! There is no version, no epoch and no per-cell freshness check on the read
//! path: a hit is served as-is. What keeps it correct is that every site which
//! changes what a tile renders *pushes* the affected cell into
//! `tile_dirty_cells`, and `jobs::tile_refresh` deletes and re-renders from
//! there. See `server::tile_dirty` for the expansion rule and CLAUDE.md for the
//! producer list -- a producer that forgets to enqueue leaves a permanently
//! stale tile, and `tiles clear` is the only backstop.
//!
//! # Degrade, never fail
//!
//! Every entry has an authoritative source (DuckDB) behind it, so a miss costs
//! a re-render and never a wrong answer. That licenses the policy throughout
//! this module: a corrupt, truncated, or wrong-format value decodes as a
//! **miss**, a write that RocksDB declines is dropped and counted, and no
//! operation here returns an error onto the request path. Contrast
//! `kvstore::KV_FORMAT_VERSION`, which hard-bails on a mismatch: there the
//! recovery is a 12-minute re-import, here it is one tile.
//!
//! # Stored in the form it is served
//!
//! Values hold **gzip** bytes, not raw MVT, and the serving path hands them to
//! the response body verbatim. The server does no other response compression
//! (`tower-http` is built with `["fs", "set-header"]`), so before this the
//! reverse proxy gzipped every tile on every request; compressing once at
//! level 9 removes that per-request cost and shrinks the store at the same
//! time. Two consequences that are easy to get wrong:
//!
//! - RocksDB compression is **off** for this column family (see
//!   `kvstore::make_cf_opts`). Zstd over gzip output is pure waste.
//! - The `ETag` hashes the **uncompressed** bytes, so it identifies content
//!   rather than encoding -- which is what makes the gzip and identity
//!   responses honestly interchangeable under one weak validator, and what
//!   stops a `flate2` version bump from invalidating every client's cache.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use axum::body::Bytes;
use rocksdb::{WriteBatch, WriteOptions};

use crate::osm::kvstore::{self, RocksDB};

/// `(z, x, y)` -- Web-Mercator XYZ, the same numbering `/tiles/{z}/{x}/{y}`
/// uses.
pub type TileKey = (u32, u32, u32);

/// Bumped when the key or value BYTE LAYOUT here changes.
///
/// Never bump this for a change in what a tile *contains* --
/// `tiles::TILE_FORMAT_VERSION` covers that, and it is stored alongside. A
/// mismatch in either is a miss, so both self-heal on the next request.
const TILE_VALUE_FORMAT: u8 = 1;

/// Fixed part of a value: format tag, tile format version, uncompressed
/// length, ETag length.
const VALUE_HEADER_LEN: usize = 1 + 4 + 4 + 1;

/// Flush a bulk batch once it holds this many payload bytes. Byte-bounded
/// rather than count-bounded on purpose: tiles run from 4 KB to 463 KB, so a
/// fixed key count is either a pointlessly small batch or an unbounded memory
/// spike.
const BATCH_FLUSH_BYTES: usize = 32 * 1024 * 1024;

/// gzip level. Paid once per render and read back many times, so the slowest
/// level is the right one -- ~3 minutes of CPU to compress all of Poland,
/// against hours to render it.
const GZIP_LEVEL: u32 = 9;

/// A tile body in the form it is both stored and served.
#[derive(Clone)]
pub struct TileBody {
    /// Length of the *uncompressed* MVT. Sizes the identity fallback's
    /// decompress buffer exactly, and distinguishes "stored, and the render
    /// produced nothing" from "not stored" -- a gzip of zero bytes is ~20
    /// bytes, not 0, so the payload length cannot answer that.
    pub orig_len: u32,
    /// gzip stream. `Bytes` so a hit clones a refcount rather than copying a
    /// tile that can run several hundred KB.
    pub gzip: Bytes,
}

/// What a store hit yields: the body plus the `ETag` that was computed over
/// its uncompressed bytes at render time.
pub struct StoredTile {
    pub etag: String,
    pub body: TileBody,
}

/// Compress a freshly rendered tile into the representation everything
/// downstream uses, and hash it.
///
/// The one home for "render output -> what we store and send". The hash is
/// taken over `raw`, before compression -- see the module doc.
pub fn prepare(raw: &[u8]) -> Result<(String, TileBody)> {
    let etag = content_etag(raw);
    let gzip = gzip_compress(raw).context("gzip a rendered tile")?;
    Ok((
        etag,
        TileBody {
            orig_len: raw.len() as u32,
            gzip: Bytes::from(gzip),
        },
    ))
}

impl TileBody {
    /// Undo the compression, for a client that did not offer `gzip`.
    pub fn decompress(&self) -> Result<Vec<u8>> {
        use std::io::Read;
        let mut out = Vec::with_capacity(self.orig_len as usize);
        flate2::read::GzDecoder::new(&self.gzip[..])
            .read_to_end(&mut out)
            .context("gunzip a stored tile")?;
        Ok(out)
    }

    /// Bytes this body occupies in memory, for `tile_cache`'s byte budget.
    pub fn size(&self) -> u64 {
        self.gzip.len() as u64
    }
}

fn gzip_compress(raw: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(GZIP_LEVEL));
    enc.write_all(raw)?;
    enc.finish()
}

/// FNV-1a 64, hand-written on purpose.
///
/// `DefaultHasher` is explicitly not guaranteed stable across Rust releases,
/// and an `ETag` scheme that shifts on a toolchain bump would flush every
/// client's cache for a build-system detail. 64 bits over ~260k tiles is a
/// ~1e-9 collision probability, whose cost is one stale tile for one
/// `max-age` window.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// The `ETag` payload for a tile: a hash of its uncompressed bytes.
/// `http_cache::weak_etag` wraps this into the header value.
pub fn content_etag(raw: &[u8]) -> String {
    format!("{:016x}", fnv1a64(raw))
}

/// 9 bytes: `[z][x BE][y BE]`.
///
/// Big-endian for the reason `osm::encoding::encode_key` is: RocksDB sorts
/// lexicographically and delta-encodes within a block, and a block is the unit
/// of I/O, so numerically adjacent keys must be lexicographically adjacent.
/// `z` leads so each tier occupies one contiguous range and a z13 read never
/// walks z14 blocks; `(x, y)` then puts a viewport column into one block.
fn encode_key((z, x, y): TileKey) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = z as u8;
    key[1..5].copy_from_slice(&x.to_be_bytes());
    key[5..9].copy_from_slice(&y.to_be_bytes());
    key
}

/// `[TILE_VALUE_FORMAT][u32 LE tile_format_version][u32 LE orig_len][u8
/// etag_len][etag][gzip payload]`.
///
/// Little-endian inside the value while the key is big-endian matches
/// `osm::encoding`, which already draws exactly this line: only ordering wants
/// big-endian. The payload goes last so decoding can hand out a zero-copy
/// slice of the buffer that was just read.
fn encode_value(tile_format_version: u32, etag: &str, body: &TileBody) -> Vec<u8> {
    let etag = etag.as_bytes();
    let mut out = Vec::with_capacity(VALUE_HEADER_LEN + etag.len() + body.gzip.len());
    out.push(TILE_VALUE_FORMAT);
    out.extend_from_slice(&tile_format_version.to_le_bytes());
    out.extend_from_slice(&body.orig_len.to_le_bytes());
    out.push(etag.len() as u8);
    out.extend_from_slice(etag);
    out.extend_from_slice(&body.gzip);
    out
}

/// Total decode: every malformed shape returns `None`, which the caller treats
/// as a miss. Nothing here may panic -- this runs on the request path against
/// bytes that could have been written by an older binary.
fn decode_value(raw: Vec<u8>, tile_format_version: u32) -> Option<StoredTile> {
    if raw.len() < VALUE_HEADER_LEN || raw[0] != TILE_VALUE_FORMAT {
        return None;
    }
    let stored_version = u32::from_le_bytes(raw[1..5].try_into().ok()?);
    if stored_version != tile_format_version {
        return None;
    }
    let orig_len = u32::from_le_bytes(raw[5..9].try_into().ok()?);
    let etag_len = raw[9] as usize;
    let etag_end = VALUE_HEADER_LEN + etag_len;
    if raw.len() < etag_end {
        return None;
    }
    let etag = std::str::from_utf8(&raw[VALUE_HEADER_LEN..etag_end])
        .ok()?
        .to_string();
    // Zero-copy: the payload is a refcounted view into the buffer just read,
    // so a 463 KB tile is never memcpy'd on its way to the response body.
    let gzip = Bytes::from(raw).slice(etag_end..);
    Some(StoredTile {
        etag,
        body: TileBody { orig_len, gzip },
    })
}

/// A pending group of writes. See [`BATCH_FLUSH_BYTES`] for why it is bounded
/// by bytes.
pub struct TileBatch {
    batch: WriteBatch,
    bytes: usize,
}

impl TileBatch {
    fn new() -> Self {
        Self {
            batch: WriteBatch::default(),
            bytes: 0,
        }
    }
}

/// See the module doc. Cheap to clone-wrap in an `Arc` and share across the
/// request path, the refresh job and the warm command.
pub struct TileStore {
    /// `None` when persistence is disabled -- a genuine no-op store rather
    /// than an `Option<TileStore>` every call site has to unwrap, mirroring
    /// `TileCache::new(0)`.
    db: Option<Arc<RocksDB>>,
    /// `tiles::TILE_FORMAT_VERSION`, injected rather than imported so this
    /// module stays free of MVT concerns and tests can vary it.
    tile_format_version: u32,
    hits: AtomicU64,
    misses: AtomicU64,
    write_skips: AtomicU64,
}

impl TileStore {
    pub fn new(kv: Arc<RocksDB>, tile_format_version: u32, enabled: bool) -> Self {
        Self {
            db: enabled.then_some(kv),
            tile_format_version,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            write_skips: AtomicU64::new(0),
        }
    }

    /// A working no-op. Test-only: production reaches the same state through
    /// `new(kv, _, false)` when `cache.persist_tiles` is off, so this exists
    /// purely so an `AppState` can be built without a RocksDB handle.
    #[cfg(test)]
    pub fn disabled() -> Self {
        Self {
            db: None,
            tile_format_version: 0,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            write_skips: AtomicU64::new(0),
        }
    }

    /// The column family handle, refetched per operation rather than cached in
    /// the struct: [`TileStore::clear`] drops and recreates the family, which
    /// invalidates any handle held across it.
    fn cf(&self) -> Option<(&Arc<RocksDB>, Arc<rocksdb::BoundColumnFamily<'_>>)> {
        let db = self.db.as_ref()?;
        let cf = db.cf_handle(kvstore::CF_TILES)?;
        Some((db, cf))
    }

    /// `None` on a miss, a bad format tag, a `TILE_FORMAT_VERSION` mismatch, a
    /// truncated value, or a read error. Never errors, never panics.
    pub fn get(&self, key: TileKey) -> Option<StoredTile> {
        let (db, cf) = self.cf()?;
        // `get_cf`, not `get_pinned_cf`: a pinnable slice borrows the DB, so
        // handing it to a response body would force a copy, whereas the owned
        // buffer becomes a refcounted `Bytes` view in `decode_value`.
        let raw = match db.get_cf(&cf, encode_key(key)) {
            Ok(Some(raw)) => raw,
            Ok(None) => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            Err(e) => {
                tracing::warn!(error = %e, z = key.0, x = key.1, y = key.2, "tile store read failed");
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        match decode_value(raw, self.tile_format_version) {
            Some(tile) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(tile)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Request-path write. Dropped and counted rather than retried if RocksDB
    /// declines it -- see [`TileStore::request_write_opts`].
    pub fn put(&self, key: TileKey, etag: &str, body: &TileBody) {
        let Some((db, cf)) = self.cf() else {
            return;
        };
        let value = encode_value(self.tile_format_version, etag, body);
        if let Err(e) = db.put_cf_opt(&cf, encode_key(key), value, &Self::request_write_opts()) {
            self.write_skips.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(error = %e, z = key.0, x = key.1, y = key.2, "tile store write skipped");
        }
    }

    /// Bloom-filter probe: no disk I/O, and **false positives are expected**
    /// -- ~1% at the 10 bits per key `kvstore::make_cf_opts` gives `CF_TILES`.
    /// That filter is load-bearing, not an optimisation: `key_may_exist_cf`
    /// says "yes" whenever it cannot decide without I/O, so without one this
    /// was "yes" for ~100% of absent tiles once they were flushed to disk.
    /// `jobs::tile_refresh` uses it to decide residency, where a false positive
    /// costs a tombstone for a key that was not there plus one tile rendered
    /// that was not resident -- which merely warms it. Do not "fix" this into
    /// a real read: answering it with `multi_get_cf` would materialise
    /// thousands of tile *values* to answer a yes/no.
    pub fn may_exist(&self, key: TileKey) -> bool {
        match self.cf() {
            Some((db, cf)) => db.key_may_exist_cf(&cf, encode_key(key)),
            None => false,
        }
    }

    /// Drop and recreate the column family -- the same shape as
    /// `kvstore::clear`, and the operator escape hatch behind `tiles clear`.
    pub fn clear(&self) -> Result<()> {
        let Some(db) = self.db.as_ref() else {
            return Ok(());
        };
        kvstore::clear_tiles(db)
    }

    // --- Bulk path: `tiles warm` and both `tile_refresh` passes -------------

    pub fn batch(&self) -> TileBatch {
        TileBatch::new()
    }

    pub fn batch_put(&self, b: &mut TileBatch, key: TileKey, etag: &str, body: &TileBody) {
        let Some((_, cf)) = self.cf() else {
            return;
        };
        let value = encode_value(self.tile_format_version, etag, body);
        b.bytes += value.len();
        b.batch.put_cf(&cf, encode_key(key), value);
    }

    pub fn batch_delete(&self, b: &mut TileBatch, key: TileKey) {
        let Some((_, cf)) = self.cf() else {
            return;
        };
        b.batch.delete_cf(&cf, encode_key(key));
    }

    /// Write and reset the batch if it has grown past [`BATCH_FLUSH_BYTES`].
    pub fn flush_if_full(&self, b: &mut TileBatch) -> Result<()> {
        if b.bytes < BATCH_FLUSH_BYTES {
            return Ok(());
        }
        self.write_batch(std::mem::replace(b, TileBatch::new()))
    }

    /// Write a batch, whatever its size.
    pub fn write_batch(&self, b: TileBatch) -> Result<()> {
        let Some(db) = self.db.as_ref() else {
            return Ok(());
        };
        if b.batch.is_empty() {
            return Ok(());
        }
        db.write_opt(&b.batch, &Self::bulk_write_opts())
            .context("tile store batch write")
    }

    /// Request-path write options.
    ///
    /// Three flags, each load-bearing (note `disable_wal` has no `set_`
    /// prefix, unlike its neighbours):
    ///
    /// - **WAL off**: a lost memtable costs a re-render, and on a slow disk
    ///   skipping the fsync is most of the point. `WriteOptions` is per-call,
    ///   so OSM writes keep their WAL.
    /// - **`low_pri`**: tile writes yield to real work under pressure.
    /// - **`no_slowdown`**: on a compaction backlog the write is declined
    ///   immediately rather than stalling a tile response. This is the one
    ///   flag the bulk options drop.
    fn request_write_opts() -> WriteOptions {
        let mut wo = WriteOptions::new();
        wo.disable_wal(true);
        wo.set_low_pri(true);
        wo.set_no_slowdown(true);
        wo
    }

    /// Bulk write options: the same, minus `no_slowdown`.
    ///
    /// `tiles warm` and `tile_refresh` have no client waiting on them, and a
    /// declined batch throws away up to 32 MB of rendering -- so waiting for
    /// compaction is strictly better than dropping the work. That flag is
    /// precisely what separates "a request is blocked on this" from "a
    /// background job is doing bulk work".
    fn bulk_write_opts() -> WriteOptions {
        let mut wo = WriteOptions::new();
        wo.disable_wal(true);
        wo.set_low_pri(true);
        wo
    }

    // --- Diagnostics, read by /status --------------------------------------

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Writes RocksDB declined because compaction was behind. A growing value
    /// means the disk cannot keep up with the tile write rate.
    pub fn write_skips(&self) -> u64 {
        self.write_skips.load(Ordering::Relaxed)
    }

    /// `rocksdb.estimate-live-data-size` for the tiles family. Diagnostic
    /// only, and an estimate by name -- nothing decides anything on it.
    pub fn live_bytes(&self) -> Option<u64> {
        let (db, cf) = self.cf()?;
        db.property_int_value_cf(&cf, "rocksdb.estimate-live-data-size")
            .ok()
            .flatten()
    }

    /// Whether persistence is on at all, so `/status` can distinguish "no
    /// hits because nothing is warm" from "no hits because it is disabled".
    pub fn enabled(&self) -> bool {
        self.db.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const TFV: u32 = 7;

    fn store(dir: &TempDir) -> TileStore {
        let kv = Arc::new(kvstore::open(dir.path(), 8, 4, 8).unwrap());
        TileStore::new(kv, TFV, true)
    }

    fn body(raw: &[u8]) -> (String, TileBody) {
        prepare(raw).unwrap()
    }

    #[test]
    fn a_stored_tile_reads_back_byte_identical() {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);
        let raw = b"an mvt payload, more or less".repeat(40);
        let (etag, b) = body(&raw);
        s.put((14, 9148, 5394), &etag, &b);

        let got = s.get((14, 9148, 5394)).expect("must hit");
        assert_eq!(got.etag, etag);
        assert_eq!(got.body.orig_len as usize, raw.len());
        assert_eq!(got.body.decompress().unwrap(), raw);
        assert_eq!(s.hits(), 1);
    }

    /// The store's size converges to (tiles ever rendered) x (current tile
    /// size) only if a re-render overwrites in place. If `put` accumulated,
    /// nothing would bound it.
    #[test]
    fn put_replaces_rather_than_accumulating() {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);
        let key = (14, 1, 1);
        let (e1, b1) = body(b"first");
        let (e2, b2) = body(b"second, and different");
        s.put(key, &e1, &b1);
        s.put(key, &e2, &b2);

        let got = s.get(key).expect("must hit");
        assert_eq!(got.etag, e2);
        assert_eq!(got.body.decompress().unwrap(), b"second, and different");
    }

    #[test]
    fn a_tile_format_version_mismatch_is_a_miss_not_garbage() {
        let dir = TempDir::new().unwrap();
        let kv = Arc::new(kvstore::open(dir.path(), 8, 4, 8).unwrap());
        let old = TileStore::new(kv.clone(), TFV, true);
        let (etag, b) = body(b"rendered by the old binary");
        old.put((14, 2, 2), &etag, &b);

        let new = TileStore::new(kv, TFV + 1, true);
        assert!(
            new.get((14, 2, 2)).is_none(),
            "a tile whose MVT shape predates this binary must not be served"
        );
        assert_eq!(new.misses(), 1);
    }

    #[test]
    fn an_unknown_value_format_tag_is_a_miss() {
        let (etag, b) = body(b"x");
        let mut value = encode_value(TFV, &etag, &b);
        value[0] = TILE_VALUE_FORMAT.wrapping_add(1);
        assert!(decode_value(value, TFV).is_none());
    }

    #[test]
    fn a_truncated_value_is_a_miss_rather_than_a_panic() {
        let (etag, b) = body(b"a payload long enough to slice into");
        let value = encode_value(TFV, &etag, &b);
        for cut in 0..value.len().min(VALUE_HEADER_LEN + etag.len()) {
            assert!(
                decode_value(value[..cut].to_vec(), TFV).is_none(),
                "a value truncated to {cut} bytes must decode as a miss"
            );
        }
    }

    /// Unreachable from a real render (`ST_AsMVT` always emits a layer
    /// header), but if it ever happened, a zero-length payload reading back as
    /// a miss would put that tile into an endless re-render loop.
    #[test]
    fn a_zero_length_payload_reads_back_as_a_hit_not_a_miss() {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);
        let (etag, b) = body(b"");
        assert_eq!(b.orig_len, 0);
        s.put((13, 5, 5), &etag, &b);

        let got = s.get((13, 5, 5)).expect("an empty tile is still stored");
        assert_eq!(got.body.orig_len, 0);
        assert!(got.body.decompress().unwrap().is_empty());
    }

    /// `may_exist` is only a residency probe because `CF_TILES` carries a
    /// bloom filter (`kvstore::make_cf_opts`). Without one, `key_may_exist_cf`
    /// answers "yes" for every key inside an SST's range: measured on the
    /// fully warmed store, 99.78% of absent z14 tiles, which made `tiles
    /// warm`'s resume skip the very tiles it had not rendered yet.
    ///
    /// The flush is the whole point. A memtable lookup is exact, so without it
    /// this passes with or without the filter; and stored and absent keys are
    /// interleaved so every absent key sits inside the SST's key range, where
    /// only the filter can say no.
    #[test]
    fn may_exist_rejects_absent_tiles_once_they_are_on_disk() {
        let dir = TempDir::new().unwrap();
        let kv = Arc::new(kvstore::open(dir.path(), 8, 4, 8).unwrap());
        let s = TileStore::new(kv.clone(), TFV, true);
        let (etag, b) = body(b"tile");
        for x in (0..4000u32).step_by(2) {
            s.put((14, x, 5300), &etag, &b);
        }
        kv.flush_cf(&kv.cf_handle(kvstore::CF_TILES).unwrap())
            .unwrap();

        assert!(
            (0..4000u32).step_by(2).all(|x| s.may_exist((14, x, 5300))),
            "a filter has no false negatives: every stored tile must say yes"
        );
        let absent = 2000;
        let false_positives = (1..4000u32)
            .step_by(2)
            .filter(|x| s.may_exist((14, *x, 5300)))
            .count();
        // ~1% at 10 bits per key; ~100% with no filter at all.
        assert!(
            false_positives < absent / 20,
            "{false_positives} of {absent} absent tiles said \"may exist\" -- \
             has CF_TILES lost its bloom filter?"
        );
        // And the filter is what answered, as `/status` would report it.
        assert!(kv.stats().bloom_filter_useful >= (absent - false_positives) as u64);
    }

    #[test]
    fn a_disabled_store_is_a_genuine_no_op() {
        let s = TileStore::disabled();
        let (etag, b) = body(b"anything");
        s.put((14, 1, 1), &etag, &b);
        assert!(s.get((14, 1, 1)).is_none());
        assert!(!s.may_exist((14, 1, 1)));
        assert!(s.clear().is_ok());
        assert!(!s.enabled());
    }

    /// Big-endian keys exist so numerically adjacent tiles are
    /// lexicographically adjacent -- that is what puts a viewport column into
    /// one RocksDB block. Little-endian would scatter them by their least
    /// significant byte.
    #[test]
    fn keys_sort_so_adjacent_tiles_are_adjacent_in_the_keyspace() {
        assert!(encode_key((14, 100, 200)) < encode_key((14, 100, 201)));
        assert!(encode_key((14, 100, 255)) < encode_key((14, 100, 256)));
        assert!(encode_key((14, 100, 999)) < encode_key((14, 101, 0)));
        // And each zoom occupies its own contiguous range.
        assert!(encode_key((13, u32::MAX, u32::MAX)) < encode_key((14, 0, 0)));
    }

    #[test]
    fn a_batched_write_and_a_single_put_produce_the_same_value() {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);
        let (etag, b) = body(b"identical either way");
        s.put((14, 1, 1), &etag, &b);

        let mut batch = s.batch();
        s.batch_put(&mut batch, (14, 2, 2), &etag, &b);
        s.write_batch(batch).unwrap();

        let via_put = s.get((14, 1, 1)).unwrap();
        let via_batch = s.get((14, 2, 2)).unwrap();
        assert_eq!(via_put.etag, via_batch.etag);
        assert_eq!(via_put.body.orig_len, via_batch.body.orig_len);
        assert_eq!(via_put.body.gzip, via_batch.body.gzip);
    }

    #[test]
    fn a_batch_flushes_on_its_byte_budget_not_its_key_count() {
        let dir = TempDir::new().unwrap();
        let s = store(&dir);
        // Incompressible payloads, so the stored size tracks the input size.
        let big: Vec<u8> = (0..3_000_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        let (etag, b) = body(&big);

        let mut batch = s.batch();
        for i in 0..8u32 {
            s.batch_put(&mut batch, (14, i, 0), &etag, &b);
            s.flush_if_full(&mut batch).unwrap();
        }
        assert!(
            batch.bytes < BATCH_FLUSH_BYTES,
            "the batch must have been flushed before exceeding its budget"
        );
        s.write_batch(batch).unwrap();
        for i in 0..8u32 {
            assert!(
                s.get((14, i, 0)).is_some(),
                "tile {i} must have been written"
            );
        }
    }

    #[test]
    fn clear_empties_the_store_but_leaves_the_osm_families_alone() {
        let dir = TempDir::new().unwrap();
        let kv = Arc::new(kvstore::open(dir.path(), 8, 4, 8).unwrap());
        kvstore::put_node(&kv, 42, 210_000_000, 520_000_000).unwrap();
        let s = TileStore::new(kv.clone(), TFV, true);
        let (etag, b) = body(b"warm");
        s.put((14, 3, 3), &etag, &b);
        assert!(s.get((14, 3, 3)).is_some());

        s.clear().unwrap();

        assert!(s.get((14, 3, 3)).is_none(), "clear must empty the tiles CF");
        assert_eq!(
            kvstore::get_node(&kv, 42).unwrap(),
            Some((210_000_000, 520_000_000)),
            "clear must not touch the OSM key spaces"
        );
    }

    /// The ETag identifies content, not encoding -- so two renders producing
    /// the same MVT are interchangeable regardless of how they were stored,
    /// and a client revalidating after a re-render that changed nothing gets
    /// its 304.
    #[test]
    fn the_etag_is_stable_across_identical_renders_and_moves_when_bytes_do() {
        assert_eq!(content_etag(b"same bytes"), content_etag(b"same bytes"));
        assert_ne!(content_etag(b"same bytes"), content_etag(b"other bytes"));
    }
}
