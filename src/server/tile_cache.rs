//! A bounded in-process byte cache for served z14 `/tiles` responses.
//!
//! z14 only -- see `tiles::serve_tile`'s comment on why z5..=z13 carry no
//! `ETag` at all, which is exactly the signal this cache invalidates against.
//! A viewport refresh in MapLibre requests ~16 adjacent tiles at once; without
//! this, that's ~16 full MVT query rounds even when nothing in the covered
//! cells changed since the same viewport was requested a few seconds earlier.
//! This trades a bounded slice of process memory for skipping those.
//!
//! # Disabling
//!
//! [`TileCache::new(0)`] is a genuine working no-op, not a special-cased
//! `Option<TileCache>` at the call site: every [`TileCache::get`] misses
//! (`max_bytes == 0` short-circuits before touching the lock) and every
//! [`TileCache::insert`] is a no-op for the same reason. Setting
//! `tile_cache_max_bytes = 0` in config therefore reverts to the pre-cache
//! behaviour with no code path change and no redeploy beyond the config edit.
//!
//! # Design: two generations, not an LRU
//!
//! Hand-rolled rather than pulling in the `lru` crate. Two reasons:
//!
//! - Exact recency ordering buys nothing here. Requests arrive in viewport
//!   bursts of ~16 adjacent tiles; within a burst, "least recently used" is
//!   meaningless noise, not a useful signal -- what actually matters is that
//!   the last couple of *viewports* (not individual tiles within one) survive
//!   between bursts.
//! - `lru` bounds by entry count, not bytes. This cache has to be
//!   byte-bounded (tile sizes vary a lot -- a real Kraków z14 tile measured
//!   650 KB), which means writing the eviction accounting by hand regardless
//!   of whether an LRU crate is doing the list-splicing underneath.
//!
//! So instead: a `hot` generation and a `cold` generation. Everything new
//! lands in `hot`. A read that only finds the key in `cold` *promotes* it
//! into `hot` (see [`TileCache::get`]) -- that's what gives a tile that
//! survived one viewport a chance to survive the next one too, instead of
//! being evicted purely by exact-LRU order within a single burst. When `hot`
//! would grow past half the budget, `cold` is dropped wholesale and `hot`
//! takes its place (see [`place_in_hot`]) -- O(1) amortised, never a scan
//! over entries to decide what to evict.
//!
//! **The half-budget check runs before adding the new entry, not after.**
//! Checking after would let `hot` briefly overshoot to `max_bytes/2 +
//! max_bytes/4` (the largest a single accepted entry can be) before the swap
//! fires -- and since `cold` can independently be sitting at that same
//! high-water mark left over from the *previous* swap, resident bytes at
//! that instant could reach `1.5 * max_bytes`. Checking first instead keeps
//! `hot_bytes <= max_bytes / 2` a standing invariant after every insert, so
//! `cold` (always a past snapshot of `hot` at swap time) obeys the same
//! bound, and `hot + cold <= max_bytes` holds at every instant, not just
//! most of them.
//!
//! Refusing any single entry bigger than `max_bytes / 4` is what makes that
//! invariant reachable at all: without a cap, one huge tile could jump `hot`
//! from just under half straight past the full budget in a single insert.
//!
//! # Key and invalidation
//!
//! Keyed on `(z, x, y)`, with the version string that produced the entry's
//! bytes stored *inside* the entry rather than folded into the key. A
//! recompute of the tile's cell then makes a `get` at the new version a
//! **miss** against the stale entry, and the `insert` that follows
//! *replaces* it in place -- there is never a dead, superseded-version entry
//! sitting in the map, still counted against the budget, waiting for byte
//! eviction to notice nobody wants it. Folding the version into the key
//! instead would leave exactly that dead entry resident until eviction got
//! around to it.
//!
//! # No single-flight
//!
//! Deliberately absent. The real workload this cache serves is a viewport
//! burst of ~16 *different* tiles, not N concurrent requests for the *same*
//! tile -- MapLibre does not re-request a tile it already has an outstanding
//! request for. Don't add a request-coalescing layer here on spec; it would
//! be solving a collision this workload doesn't produce.
//!
//! # Locking
//!
//! `std::sync::Mutex`, not `tokio::sync::Mutex`. Every access happens inside
//! a `spawn_blocking` closure (see `tiles::serve_tile`) alongside the
//! blocking DuckDB calls already made there, so nothing here is ever held
//! across an `.await` point -- the one situation a std mutex is the wrong
//! tool for. Reaching for the async mutex anyway would only add executor
//! overhead for no correctness benefit.
//!
//! Bytes are stored as [`axum::body::Bytes`] rather than `Vec<u8>` so a hit
//! clones a refcounted handle (a few atomic increments) instead of copying a
//! tile that can run several hundred KB.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use axum::body::Bytes;

/// `(z, x, y)`. `z` is always 14 from today's one caller (`tiles::serve_tile`),
/// but keeping it in the key rather than assuming z14 costs nothing and
/// avoids a silent collision if this cache is ever reused for another tier.
type Key = (u32, u32, u32);

struct Entry {
    version: String,
    bytes: Bytes,
}

impl Entry {
    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// The two generations, plus `hot`'s running byte total. `cold`'s total is
/// never tracked separately -- nothing ever needs it live: `cold` only ever
/// shrinks (an entry is promoted out) or is replaced wholesale on a swap,
/// never grown in place, so there is nothing to keep a running count of.
struct Generations {
    hot: HashMap<Key, Entry>,
    hot_bytes: u64,
    cold: HashMap<Key, Entry>,
}

/// See the module doc for the full design rationale.
pub struct TileCache {
    max_bytes: u64,
    state: Mutex<Generations>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl TileCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(Generations {
                hot: HashMap::new(),
                hot_bytes: 0,
                cold: HashMap::new(),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Take the state lock, recovering the guard if a previous holder panicked.
    ///
    /// Never `unwrap`: this mutex is taken on the request path, so propagating
    /// poison would turn one panicking request into a permanently 500ing tile
    /// endpoint. Nothing here can be left half-updated in a way the next caller
    /// misreads -- the worst a poisoned guard carries is a byte total that
    /// over- or under-counts one entry, which eviction corrects on its own.
    fn state(&self) -> MutexGuard<'_, Generations> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A hit requires both the key and the stored version to match -- see
    /// the module doc's "Key and invalidation" section. A version mismatch
    /// is a miss, exactly like the key being absent; the stale entry is left
    /// in place for the next [`TileCache::insert`] to overwrite rather than
    /// being cleaned up here.
    pub fn get(&self, key: Key, version: &str) -> Option<Bytes> {
        if self.max_bytes == 0 {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let mut state = self.state();

        if let Some(entry) = state.hot.get(&key)
            && entry.version == version
        {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.bytes.clone());
        }

        let cold_matches = state
            .cold
            .get(&key)
            .is_some_and(|entry| entry.version == version);
        if cold_matches {
            // Promote: a cold hit is exactly the case the two-generation
            // design exists for (see module doc) -- move it into hot so it
            // survives whatever swap `cold` itself next goes through.
            let entry = state.cold.remove(&key).expect("just matched above");
            self.hits.fetch_add(1, Ordering::Relaxed);
            let bytes = entry.bytes.clone();
            place_in_hot(&mut state, self.max_bytes, key, entry);
            return Some(bytes);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// A no-op for `max_bytes == 0` (see module doc) and for any single
    /// entry bigger than a quarter of the budget -- see the module doc's
    /// invariant explanation for why that cap is load-bearing, not merely
    /// defensive.
    pub fn insert(&self, key: Key, version: String, bytes: Bytes) {
        if self.max_bytes == 0 {
            return;
        }
        let entry = Entry { version, bytes };
        if entry.size() > self.max_bytes / 4 {
            return;
        }
        let mut state = self.state();
        place_in_hot(&mut state, self.max_bytes, key, entry);
    }

    /// Cache hits since process start. Diagnostic only -- surfaced as
    /// `/status`'s `tile_cache_hits` so an operator can answer "is this cache
    /// doing anything", and asserted on by `tiles`' tests in place of timing.
    /// Nothing reads it to make a decision.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cache misses since process start (including every lookup while the
    /// cache is disabled via `max_bytes == 0`).
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

/// Insert (or replace) `entry` under `key` in `hot`, first ageing the
/// current `hot` into `cold` if the addition would push `hot_bytes` past
/// half the budget. Shared by [`TileCache::insert`] and the promotion path
/// in [`TileCache::get`] -- both are "this entry belongs in hot now", they
/// just arrive from different callers.
fn place_in_hot(state: &mut Generations, max_bytes: u64, key: Key, entry: Entry) {
    let size = entry.size();
    let existing = state.hot.get(&key).map(Entry::size).unwrap_or(0);
    let bytes_after = state.hot_bytes - existing + size;

    if bytes_after > max_bytes / 2 {
        // Drop cold wholesale and swap hot into its place -- see the module
        // doc for why this check runs before the insert below, not after.
        state.cold = std::mem::take(&mut state.hot);
        state.hot_bytes = 0;
    }

    // A stale copy of this key must not go on lingering in cold once a
    // current copy lives in hot -- `get` should never be able to find two
    // different bytes for the same key across the two generations.
    state.cold.remove(&key);

    let previous = state.hot.insert(key, entry).map(|e| e.size()).unwrap_or(0);
    state.hot_bytes = state.hot_bytes - previous + size;
}

#[cfg(test)]
impl TileCache {
    /// Test-only: `hot`'s tracked total plus `cold`'s actual summed size, so
    /// tests can pin the "peak resident never exceeds `max_bytes`" invariant
    /// directly instead of inferring it indirectly from behaviour.
    fn resident_bytes(&self) -> u64 {
        let state = self.state();
        let cold_bytes: u64 = state.cold.values().map(Entry::size).sum();
        state.hot_bytes + cold_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(n: usize) -> Bytes {
        Bytes::from(vec![0u8; n])
    }

    #[test]
    fn new_with_zero_budget_disables_cleanly() {
        let cache = TileCache::new(0);
        // Must not panic.
        cache.insert((14, 1, 1), "v1".to_string(), bytes(10));
        assert!(cache.get((14, 1, 1), "v1").is_none());
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn get_with_a_version_mismatch_is_a_miss() {
        let cache = TileCache::new(1_000);
        cache.insert((14, 1, 1), "v1".to_string(), bytes(10));

        assert!(cache.get((14, 1, 1), "v2").is_none());
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 0);

        // The current, matching version is still there afterwards -- a
        // mismatched lookup must not have disturbed the real entry.
        assert!(cache.get((14, 1, 1), "v1").is_some());
    }

    #[test]
    fn entry_larger_than_a_quarter_of_the_budget_is_refused() {
        let cache = TileCache::new(400); // quarter = 100
        cache.insert((14, 1, 1), "v1".to_string(), bytes(101));
        assert!(cache.get((14, 1, 1), "v1").is_none());
    }

    #[test]
    fn insert_with_a_new_version_replaces_rather_than_duplicates_the_entry() {
        let cache = TileCache::new(10_000);
        cache.insert((14, 1, 1), "v1".to_string(), bytes(50));
        cache.insert((14, 1, 1), "v2".to_string(), bytes(80));

        assert!(
            cache.get((14, 1, 1), "v1").is_none(),
            "the superseded version must no longer be servable"
        );
        let got = cache
            .get((14, 1, 1), "v2")
            .expect("the new version must hit");
        assert_eq!(got.len(), 80);
        assert_eq!(
            cache.resident_bytes(),
            80,
            "must not still be counting the superseded version's bytes -- \
             that would mean a second entry, not a replacement"
        );
    }

    #[test]
    fn stays_under_budget_and_peak_resident_never_exceeds_max_bytes_across_many_inserts() {
        let max_bytes = 10_000u64;
        let cache = TileCache::new(max_bytes);
        let mut peak = 0u64;
        for i in 0..500u32 {
            // Sizes vary but always stay comfortably under the quarter-budget
            // refusal threshold (2_500), so every insert here is accepted.
            let size = 100 + (i % 20) as usize * 50;
            cache.insert((14, i, i), format!("v{i}"), bytes(size));
            peak = peak.max(cache.resident_bytes());
        }
        assert!(
            peak <= max_bytes,
            "peak resident {peak} exceeded the {max_bytes} budget"
        );
    }

    /// Pins the promotion behaviour end to end: a key evicted into `cold` by
    /// one swap, then promoted back into `hot` by a `get`, must ride along
    /// into the *new* `cold` at the next swap instead of being dropped
    /// wholesale with the rest of the old `cold` it originally shared.
    #[test]
    fn cold_hit_is_promoted_to_hot_and_survives_a_swap_that_would_otherwise_drop_it() {
        // max_bytes = 40: quarter = 10 (every entry below is exactly that,
        // the largest size this budget accepts), half = 20 (the hot->cold
        // swap threshold).
        let cache = TileCache::new(40);
        let k1 = (14, 1, 1);
        let k2 = (14, 2, 2);
        let k3 = (14, 3, 3);
        let k4 = (14, 4, 4);

        cache.insert(k1, "v1".to_string(), bytes(10)); // hot: {k1}=10
        cache.insert(k2, "v1".to_string(), bytes(10)); // hot: {k1,k2}=20
        cache.insert(k3, "v1".to_string(), bytes(10)); // 20+10>20 -> swap: cold={k1,k2}, hot={k3}=10

        // k1 now lives only in cold.
        assert_eq!(cache.hits(), 0);
        let got = cache
            .get(k1, "v1")
            .expect("k1 must still be cached, in cold");
        assert_eq!(got.len(), 10);
        assert_eq!(cache.hits(), 1);
        // Promotion moved it: hot: {k3,k1}=20, cold: {k2}.

        cache.insert(k4, "v1".to_string(), bytes(10)); // 20+10>20 -> swap: cold={k3,k1}, hot={k4}=10

        // Without the promotion above, k1 would have been dropped wholesale
        // along with the rest of the OLD cold ({k1,k2}) by this second swap.
        // It wasn't -- it rode along inside hot and is now in the NEW cold.
        assert!(
            cache.get(k1, "v1").is_some(),
            "a promoted cold hit must survive the next generation swap"
        );
    }
}
