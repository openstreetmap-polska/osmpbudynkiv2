//! A bounded in-process byte cache for served z5..=z11 `/tiles` responses.
//!
//! This tier only. z12..=z14 are persisted in `tile_store` and
//! push-invalidated by every write path that changes what one of those tiles
//! renders -- see that module's doc and CLAUDE.md's producer list. z5..=z11
//! cannot use that scheme: `server::tiles::agg_bin_ctes` bounds each tile's
//! `ts_*` attributes by `now() - [changes] max_age_days`, so a tile's content
//! moves with the wall clock even when not one row underneath it has changed.
//! There is no write that could push-invalidate that -- the only thing that
//! changed is what time it is -- so a TTL is not a simpler substitute for a
//! push signal here, it is the only correct tool. A viewport refresh in
//! MapLibre requests ~16 adjacent tiles at once; without this, that's ~16 full
//! aggregate-query rounds even when nothing this tier reads has changed since
//! the same viewport was requested a few seconds earlier. This trades a
//! bounded slice of process memory for skipping those.
//!
//! # Disabling
//!
//! [`TileCache::new(0, ttl)`] is a genuine working no-op, not a
//! special-cased `Option<TileCache>` at the call site: every
//! [`TileCache::get`] misses (`max_bytes == 0` short-circuits before touching
//! the lock) and every [`TileCache::insert`] is a no-op for the same reason.
//! Setting `tile_cache_max_bytes = 0` in config therefore reverts to the
//! pre-cache behaviour with no code path change and no redeploy beyond the
//! config edit.
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
//!   byte-bounded (tile sizes vary a lot), which means writing the eviction
//!   accounting by hand regardless of whether an LRU crate is doing the
//!   list-splicing underneath.
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
//! # TTL
//!
//! Keyed on `(z, x, y)` alone -- there is no version to fold in, because
//! there is nothing that changes to version against (see above). Freshness is
//! instead an [`std::time::Instant`] stamped on each entry at insert time,
//! checked against `self.ttl` on every [`TileCache::get`]. The TTL is not an
//! independent tuning knob: it is set from `cache.agg_tile_max_age_seconds`,
//! the exact same config field `http_cache` uses to build this tier's
//! `Cache-Control: max-age`. That equality is the whole soundness argument --
//! it guarantees this cache can never hand back a copy staler than one every
//! browser talking to this server is already entitled to hold under the
//! header we ourselves send, so the two must never be split into separate
//! knobs that could drift apart. (The same field also supplies z12..=z13's
//! max-age, where it carries no such proof -- those tiers read `tile_store`,
//! not this cache, and the field is merely a shared default there. If the
//! field is ever split in two, this tier's half is the one the proof is
//! about.)
//!
//! An entry past its TTL is a miss, exactly like the key being absent; the
//! stale entry is left in place for the next [`TileCache::insert`] to
//! overwrite rather than being cleaned up on the read path. An expired entry
//! therefore keeps counting against the byte budget until it is either
//! overwritten or aged out by a generation swap -- there is no eager sweep.
//! That's sufficient: the budget is sized for residency, not for freshness,
//! and a swap already happens on the same rhythm viewport bursts do, so a
//! stale entry cannot occupy space indefinitely any more than a live one can.
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
//! Bodies are stored as [`TileBody`], i.e. gzip bytes plus the uncompressed
//! length, so the byte budget below counts compressed bytes -- the same form
//! `tile_store` persists and the same form the response body sends verbatim.
//! `TileBody::gzip` is `axum::body::Bytes`, so a hit clones a refcounted
//! handle (a few atomic increments) instead of copying a tile that can run
//! several hundred KB.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::tile_store::TileBody;

/// `(z, x, y)`. `z` is always in `5..=11` from today's one caller
/// (`tiles::serve_tile`), but keeping it in the key rather than assuming a
/// fixed zoom costs nothing and avoids a silent collision if this cache is
/// ever reused for another tier.
type Key = (u32, u32, u32);

struct Entry {
    inserted: Instant,
    body: TileBody,
}

impl Entry {
    fn size(&self) -> u64 {
        self.body.size()
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
    ttl: Duration,
    state: Mutex<Generations>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl TileCache {
    pub fn new(max_bytes: u64, ttl: Duration) -> Self {
        Self {
            max_bytes,
            ttl,
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

    /// A hit requires the key to be present with an entry that has not yet
    /// aged past `self.ttl` -- see the module doc's "TTL" section. An expired
    /// entry is a miss, exactly like the key being absent; it is left in
    /// place for the next [`TileCache::insert`] to overwrite rather than
    /// being cleaned up here.
    pub fn get(&self, key: Key) -> Option<TileBody> {
        if self.max_bytes == 0 {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let mut state = self.state();

        if let Some(entry) = state.hot.get(&key)
            && entry.inserted.elapsed() < self.ttl
        {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.body.clone());
        }

        let cold_fresh = state
            .cold
            .get(&key)
            .is_some_and(|entry| entry.inserted.elapsed() < self.ttl);
        if cold_fresh {
            // Promote: a cold hit is exactly the case the two-generation
            // design exists for (see module doc) -- move it into hot so it
            // survives whatever swap `cold` itself next goes through.
            let entry = state.cold.remove(&key).expect("just matched above");
            self.hits.fetch_add(1, Ordering::Relaxed);
            let body = entry.body.clone();
            place_in_hot(&mut state, self.max_bytes, key, entry);
            return Some(body);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// A no-op for `max_bytes == 0` (see module doc) and for any single
    /// entry bigger than a quarter of the budget -- see the module doc's
    /// invariant explanation for why that cap is load-bearing, not merely
    /// defensive.
    pub fn insert(&self, key: Key, body: TileBody) {
        if self.max_bytes == 0 {
            return;
        }
        let entry = Entry {
            inserted: Instant::now(),
            body,
        };
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
    /// cache is disabled via `max_bytes == 0`, and every lookup of an
    /// entry past its TTL).
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
    use axum::body::Bytes;

    /// An exact-size body: `tile_store::prepare` gzips its input, so its
    /// output size does not track the input size -- the budget tests here
    /// depend on exact sizes, so this builds a `TileBody` directly instead.
    fn body(n: usize) -> TileBody {
        TileBody {
            orig_len: n as u32,
            gzip: Bytes::from(vec![0u8; n]),
        }
    }

    const LONG_TTL: Duration = Duration::from_secs(300);

    #[test]
    fn new_with_zero_budget_disables_cleanly() {
        let cache = TileCache::new(0, LONG_TTL);
        // Must not panic.
        cache.insert((7, 1, 1), body(10));
        assert!(cache.get((7, 1, 1)).is_none());
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn an_entry_past_its_ttl_is_a_miss() {
        let cache = TileCache::new(1_000, Duration::from_millis(1));
        cache.insert((7, 1, 1), body(10));
        std::thread::sleep(Duration::from_millis(20));

        assert!(cache.get((7, 1, 1)).is_none());
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 0);
    }

    #[test]
    fn an_entry_within_its_ttl_is_a_hit() {
        let cache = TileCache::new(1_000, LONG_TTL);
        cache.insert((7, 1, 1), body(10));

        let got = cache.get((7, 1, 1)).expect("must hit within the TTL");
        assert_eq!(got.gzip.len(), 10);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 0);
    }

    #[test]
    fn entry_larger_than_a_quarter_of_the_budget_is_refused() {
        let cache = TileCache::new(400, LONG_TTL); // quarter = 100
        cache.insert((7, 1, 1), body(101));
        assert!(cache.get((7, 1, 1)).is_none());
    }

    #[test]
    fn insert_replaces_rather_than_duplicating_the_entry() {
        let cache = TileCache::new(10_000, LONG_TTL);
        cache.insert((7, 1, 1), body(50));
        cache.insert((7, 1, 1), body(80));

        let got = cache.get((7, 1, 1)).expect("must hit");
        assert_eq!(got.gzip.len(), 80);
        assert_eq!(
            cache.resident_bytes(),
            80,
            "must not still be counting the replaced entry's bytes -- that \
             would mean a second entry, not a replacement"
        );
    }

    #[test]
    fn stays_under_budget_and_peak_resident_never_exceeds_max_bytes_across_many_inserts() {
        let max_bytes = 10_000u64;
        let cache = TileCache::new(max_bytes, LONG_TTL);
        let mut peak = 0u64;
        for i in 0..500u32 {
            // Sizes vary but always stay comfortably under the quarter-budget
            // refusal threshold (2_500), so every insert here is accepted.
            let size = 100 + (i % 20) as usize * 50;
            cache.insert((7, i, i), body(size));
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
        let cache = TileCache::new(40, LONG_TTL);
        let k1 = (7, 1, 1);
        let k2 = (7, 2, 2);
        let k3 = (7, 3, 3);
        let k4 = (7, 4, 4);

        cache.insert(k1, body(10)); // hot: {k1}=10
        cache.insert(k2, body(10)); // hot: {k1,k2}=20
        cache.insert(k3, body(10)); // 20+10>20 -> swap: cold={k1,k2}, hot={k3}=10

        // k1 now lives only in cold.
        assert_eq!(cache.hits(), 0);
        let got = cache.get(k1).expect("k1 must still be cached, in cold");
        assert_eq!(got.gzip.len(), 10);
        assert_eq!(cache.hits(), 1);
        // Promotion moved it: hot: {k3,k1}=20, cold: {k2}.

        cache.insert(k4, body(10)); // 20+10>20 -> swap: cold={k3,k1}, hot={k4}=10

        // Without the promotion above, k1 would have been dropped wholesale
        // along with the rest of the OLD cold ({k1,k2}) by this second swap.
        // It wasn't -- it rode along inside hot and is now in the NEW cold.
        assert!(
            cache.get(k1).is_some(),
            "a promoted cold hit must survive the next generation swap"
        );
    }
}
