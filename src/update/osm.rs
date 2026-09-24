use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use duckdb::{Connection, OptionalExt};
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::download::{download_file_as_quiet, download_file_quiet};
use crate::osm::geometry;
use crate::osm::kvstore::RocksDB;
use crate::osm::lifecycle;
use crate::osm::replication::{
    ChangeAction, OsmChange, RelationChange, WayChange, parse_osc, parse_state_txt,
    sequence_to_path,
};
use crate::osm::{encoding, kvstore};
use crate::update::dirty_cells::{DirtyCells, Layer};

/// `job_run_log` key this function reports under (see `Job::log_keys` on
/// `server::jobs::osm_update::OsmUpdateJob`). Self-reported here rather than
/// by the job wrapper, same as `import::osm::import` reports "import:osm"
/// itself -- this function is also reachable straight from the CLI
/// (`update::run`'s `Osm` arm), not just through the background job.
pub const OSM_UPDATE_JOB_LOG_KEY: &str = "update:osm";

/// Apply pending OSM replication sequences.
///
/// `show_progress` gates a single overall progress bar covering the whole
/// run (as opposed to one per downloaded `.osc.gz`, which would just be
/// noise). Pass `true` only from an interactive CLI invocation -- a
/// background job renders no terminal, and a progress bar's carriage-return
/// redraws would otherwise pollute its log output. Individual sequence
/// downloads never get their own bar either way; see `download_file_quiet`.
///
/// `is_cancelled` is polled between batches, never mid-batch -- mirrors
/// `compare::drain::drain_batch`'s "never mid-transaction" rule, since a
/// batch's DuckDB transaction (`apply_batch`) is already its own atomic unit
/// -- see `apply_batch`'s doc comment for why a batch, rather than a single
/// sequence, is what one transaction covers. On cancellation this returns
/// `Ok(())` early: the remaining sequences are simply resumed on the next
/// call, from the `metadata` stamp the last committed batch left behind. The
/// background job (`server::jobs::osm_update`) passes `&|| ctx.is_cancelled()`
/// so the supervisor's timeout actually shortens a run instead of only being
/// recorded after the fact; the CLI path (`update::run`'s `Osm` arm) passes
/// `&|| false` since it has no supervisor and should run to completion.
///
/// Downloads are prefetched ahead of the sequence currently being applied
/// (see [`spawn_prefetcher`]) and, during catch-up, several sequences share
/// one DuckDB transaction (see [`apply_batch`]). Both are read from
/// `config.jobs.osm_update`.
pub fn update(
    conn: &Connection,
    kv: &RocksDB,
    config: &Config,
    replication_base_url: &str,
    show_progress: bool,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<()> {
    let download_dir = config.download_dir();
    let current_seq = get_current_sequence(conn)?;
    info!(current_seq, "Current replication sequence");

    let (latest_seq, latest_timestamp) =
        fetch_latest_sequence(replication_base_url, &download_dir)?;
    info!(latest_seq, "Latest available sequence");

    if current_seq >= latest_seq {
        info!("Database is up to date");
        let _ = crate::job_log::record(
            conn,
            OSM_UPDATE_JOB_LOG_KEY,
            "Success",
            Some(&format!("already up to date at sequence {current_seq}")),
        );
        return Ok(());
    }

    let pending = latest_seq - current_seq;
    info!(pending, "Sequences to apply");

    let osm_update_cfg = &config.jobs.osm_update;
    let chunk_size = catch_up_chunk_size(
        pending,
        osm_update_cfg.batch_commit_threshold,
        osm_update_cfg.batch_size,
    );

    let pb = if show_progress {
        let pb = ProgressBar::new(pending);
        pb.set_style(
            ProgressStyle::with_template(
                "{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("=>-"),
        );
        pb.set_message("Applying OSM replication sequences");
        Some(pb)
    } else {
        None
    };

    // `fetch_frontier` is the highest sequence the apply loop has *claimed*
    // -- the last sequence of the batch it is downloading right now, or of
    // the last batch it finished. The prefetch thread reads it as both the
    // floor it must stay above and the base of its window, and BOTH of those
    // have to be a fetch-time quantity rather than an apply-time one.
    //
    // It used to be `last_applied`, advanced inside `apply_batch`, and that
    // was a file-leaking bug rather than a cosmetic difference. A sequence's
    // `.osc.gz` is deleted by `decompress_and_remove` the moment it is
    // *fetched*, which is before the whole batch is even assembled and long
    // before `apply_batch` runs -- so throughout the apply loop's fetch phase
    // an apply-time floor still pointed at sequences the apply loop was
    // downloading, consuming and deleting right then. The prefetcher
    // therefore downloaded exactly those sequences concurrently, and since
    // `do_download` renames its temp file onto the destination unconditionally
    // (see the comment there about losing the rename race), the prefetcher's
    // rename re-created a file the apply loop had already unlinked. Nothing
    // ever looked at that sequence again, so it sat in `download_dir`
    // forever: measured at exactly one orphaned file per steady-state tick,
    // every tick, plus a 100% duplicate download rate against the
    // replication server for the one sequence a steady-state tick needs.
    //
    // Claiming the batch up front fixes both at once: the prefetcher skips
    // over anything at or below the frontier, so it never *starts* on a
    // sequence the apply loop owns (and `PrefetchInFlight` covers the one it
    // started just before the claim), and at steady state (one pending sequence, wholly inside
    // the first batch) it issues no requests at all -- there is by definition
    // nothing to prefetch ahead of.
    //
    // Initialised to the *first* batch's end rather than `current_seq`,
    // because the thread is spawned below before the loop runs: leaving the
    // first batch unclaimed would let the prefetcher read a stale frontier
    // and race the apply loop for batch 1 exactly as before.
    //
    // `stop` is how the main thread tells the prefetcher to give up promptly
    // on any exit path below, so `update()` never blocks its `join()` on a
    // full backoff wait.
    let fetch_frontier = Arc::new(AtomicU64::new(batch_end_for(
        current_seq + 1,
        chunk_size,
        latest_seq,
    )));
    let stop = Arc::new(AtomicBool::new(false));
    let in_flight = Arc::new(PrefetchInFlight::default());
    // `prefetch_ahead == 0` disables prefetching outright (no thread spawned
    // at all), the same "0 means off, via config alone" idiom as
    // `TileCache::new(0)`.
    let prefetch_handle = (osm_update_cfg.prefetch_ahead > 0).then(|| {
        spawn_prefetcher(
            replication_base_url.to_string(),
            download_dir.clone(),
            current_seq,
            latest_seq,
            osm_update_cfg.prefetch_ahead,
            Arc::clone(&fetch_frontier),
            Arc::clone(&in_flight),
            Arc::clone(&stop),
        )
    });

    // The whole catch-up loop lives in this closure so that, however it
    // exits (falls through to completion, an early `return Ok(())` on
    // shutdown/cancellation, or an `Err` via `?`), the `stop.store` + `join`
    // below always runs exactly once on the way out. A `Drop` guard would
    // work too, but would need to reach back into `pb`/`stop` from a
    // separate type; a closure keeps everything in this function's scope.
    let result = (|| -> Result<u64> {
        let mut seq = current_seq + 1;
        let mut applied_so_far: u64 = 0;
        let mut last_logged_bucket: u64 = 0;

        while seq <= latest_seq {
            if crate::shutdown::is_requested() {
                info!("Shutdown requested, stopping update");
                if let Some(pb) = &pb {
                    pb.abandon_with_message("Shutdown requested");
                }
                return Ok(applied_so_far);
            }

            // Polled between batches only -- see the doc comment above for
            // why mid-batch cancellation would be wrong (the transaction
            // inside apply_batch is the atomic unit).
            if is_cancelled() {
                info!("Cancellation requested, stopping update");
                if let Some(pb) = &pb {
                    pb.abandon_with_message("Cancellation requested");
                }
                return Ok(applied_so_far);
            }

            let batch_end = batch_end_for(seq, chunk_size, latest_seq);
            // Claim the whole batch before fetching any of it, so the
            // prefetch thread stays off every sequence this iteration is
            // about to download and delete -- see `fetch_frontier`'s
            // declaration above for the leak this ordering closes. Storing
            // it after the fetch loop, or per sequence inside it, would
            // reopen that window.
            fetch_frontier.store(batch_end, Ordering::SeqCst);

            // Fetch and parse the whole batch BEFORE opening the DuckDB
            // transaction in apply_batch -- see apply_batch's doc comment for
            // why that bound matters.
            let mut batch = Vec::with_capacity((batch_end - seq + 1) as usize);
            for s in seq..=batch_end {
                // The frontier above stops the prefetcher *starting* any of
                // this batch, but not a download it started just before;
                // wait that one out rather than fetching it a second time.
                // See `PrefetchInFlight`.
                in_flight.wait_while_downloading(s);
                batch.push(fetch_and_parse_sequence(
                    s,
                    replication_base_url,
                    &download_dir,
                )?);
            }

            apply_batch(conn, kv, &batch, &latest_timestamp)?;

            let applied_count = batch.len() as u64;
            applied_so_far += applied_count;
            if let Some(pb) = &pb {
                pb.inc(applied_count);
            } else {
                // Log once per PROGRESS_LOG_INTERVAL sequences *crossed*, not
                // when the running total happens to land exactly on a
                // multiple of it. Before batching, `applied_so_far` advanced
                // one at a time and hit every multiple, so an equality test
                // was equivalent; with a batch of `chunk_size` it steps over
                // them, and any `batch_size` that doesn't divide the interval
                // (7, 30, 40, ...) would silently produce NO progress logs at
                // all for the whole run -- exactly the multi-hour catch-up
                // this batching exists for, and the one place the operator
                // has no progress bar to fall back on (`show_progress` is
                // false for the background job).
                let bucket = applied_so_far / PROGRESS_LOG_INTERVAL;
                if bucket > last_logged_bucket {
                    last_logged_bucket = bucket;
                    info!(
                        seq = batch_end,
                        progress = format!("{applied_so_far}/{pending}"),
                        "Progress"
                    );
                }
            }

            seq = batch_end + 1;
        }

        if let Some(pb) = &pb {
            pb.finish_with_message("OSM update complete");
        }
        info!(final_seq = latest_seq, "OSM update complete");
        Ok(applied_so_far)
    })();

    stop.store(true, Ordering::SeqCst);
    if let Some(handle) = prefetch_handle {
        // Best-effort: a panicked prefetch thread must not mask the real
        // result of the catch-up loop above.
        let _ = handle.join();
    }

    match &result {
        Ok(applied) => {
            // `applied < pending` means a shutdown/cancellation `return`
            // above cut the loop short -- still "Success" (see this
            // function's doc comment on why that return is `Ok`, not
            // `Err`: the metadata stamp only advances per committed batch,
            // so this is real, resumable progress), but the message says so
            // rather than implying every pending sequence landed.
            let msg = if *applied < pending {
                format!(
                    "applied {applied} of {pending} pending sequences (stopped early), now at sequence {}",
                    current_seq + applied
                )
            } else {
                format!("applied {applied} sequences, now at sequence {latest_seq}")
            };
            let _ = crate::job_log::record(conn, OSM_UPDATE_JOB_LOG_KEY, "Success", Some(&msg));
        }
        Err(e) => {
            let _ = crate::job_log::record(
                conn,
                OSM_UPDATE_JOB_LOG_KEY,
                "Error",
                Some(&format!("{e:#}")),
            );
        }
    }

    result.map(|_| ())
}

/// Batch size for one DuckDB transaction during catch-up.
///
/// Batching only engages when `pending` (computed once, at the start of
/// `update()`, from the sequence range the whole run needs to cover) exceeds
/// `batch_commit_threshold` -- otherwise `chunk_size` is `1`, which must
/// stay byte-for-byte today's one-sequence-per-transaction path. Steady
/// state (one pending sequence per tick, the overwhelmingly common case
/// outside a cold-start catch-up) never crosses the threshold, so it is
/// untouched by this change.
///
/// `pending` is deliberately not recomputed per batch: the threshold decides
/// the *mode* for the whole run, not each chunk, so a catch-up that starts
/// just over the threshold stays batched all the way to its last few
/// sequences rather than dropping back to `chunk_size = 1` right at the end.
///
/// `.max(1)` guards a misconfigured `batch_size = 0`, which would otherwise
/// make the `while seq <= latest_seq` loop in `update()` build a batch of
/// zero sequences per iteration and never advance `seq`.
fn catch_up_chunk_size(pending: u64, batch_commit_threshold: u64, batch_size: usize) -> usize {
    if pending > batch_commit_threshold {
        batch_size.max(1)
    } else {
        1
    }
}

/// Last sequence of the batch that starts at `seq`.
///
/// One home for the expression because `update()` needs it in two places
/// that must not drift: once per loop iteration, and once up front to
/// initialise `fetch_frontier` *before* the prefetch thread is spawned. If
/// those two disagreed, the first batch would be fetched without having been
/// claimed, which is precisely the duplicate-download-and-orphan window the
/// frontier exists to close.
fn batch_end_for(seq: u64, chunk_size: usize, latest_seq: u64) -> u64 {
    (seq + chunk_size as u64 - 1).min(latest_seq)
}

/// How often the prefetch thread rechecks `stop` while waiting for its
/// window to reopen (see [`spawn_prefetcher`]). Deliberately short and fixed
/// rather than growing like `download_with_retry`'s backoff: there is no
/// "increasing cost" to justify growth here, since the window reopens the
/// moment the fetch frontier advances -- a short fixed poll just bounds how long
/// `update()`'s `join()` can be kept waiting once it sets `stop`.
const PREFETCH_WINDOW_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// The one sequence the prefetch thread is downloading right now, so the
/// apply loop waits for that download instead of racing it.
///
/// `fetch_frontier` alone cannot close this: the prefetcher reads the
/// frontier, finds `next` unclaimed and starts downloading it, and if the
/// apply loop then claims `next` before that download lands, it finds no
/// file on disk and downloads the sequence itself. Idle, the prefetcher is
/// far enough ahead that this almost never happens; under CPU load the
/// prefetch thread is starved, the apply loop catches up, and the two run
/// in lockstep with a duplicate on nearly every step (measured: 11 of 100
/// sequences under 12-way synthetic load, which made the duplicate-bound
/// tests flaky). Each duplicate also re-created, via `do_download`'s
/// unconditional rename, a file the apply loop had already consumed.
///
/// The handshake is a store-then-load on each side, serialised by the
/// mutex: the prefetcher records `next` and re-reads the frontier in one
/// critical section ([`Self::claim`]), while the apply loop stores the
/// frontier *before* taking the lock to check this slot
/// ([`Self::wait_while_downloading`]). Whichever critical section runs
/// second sees the other side's write, so either the prefetcher backs off
/// or the apply loop waits -- never neither.
#[derive(Default)]
struct PrefetchInFlight {
    seq: std::sync::Mutex<Option<u64>>,
    done: std::sync::Condvar,
}

impl PrefetchInFlight {
    /// Record `seq` as in flight, unless the apply loop has already claimed
    /// it. The frontier must be read under the lock -- reading it before
    /// would reopen exactly the window this type exists to close.
    ///
    /// Released when the returned guard drops, so a panicking download
    /// cannot leave the apply loop waiting forever.
    fn claim<'a>(&'a self, seq: u64, fetch_frontier: &AtomicU64) -> Option<InFlightGuard<'a>> {
        let mut slot = self.seq.lock().unwrap();
        if seq <= fetch_frontier.load(Ordering::SeqCst) {
            return None;
        }
        *slot = Some(seq);
        Some(InFlightGuard(self))
    }

    /// Block while the prefetcher is downloading `seq`. The caller must have
    /// already stored a frontier covering `seq`. Once this returns the file
    /// is either on disk or the prefetch failed, and either way the caller's
    /// own `download_file_as_quiet` does the right thing.
    fn wait_while_downloading(&self, seq: u64) {
        let slot = self.seq.lock().unwrap();
        let _unused = self.done.wait_while(slot, |s| *s == Some(seq)).unwrap();
    }
}

struct InFlightGuard<'a>(&'a PrefetchInFlight);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        // `unwrap_or_else(into_inner)`: this can run during a panic unwind,
        // and a poisoned lock must still be cleared rather than double-panic.
        *self.0.seq.lock().unwrap_or_else(|e| e.into_inner()) = None;
        self.0.done.notify_all();
    }
}

/// How many applied sequences between "Progress" log lines when there is no
/// progress bar (i.e. the background job). See the call site in `update()`
/// for why this is compared as a bucket rather than by exact divisibility.
const PROGRESS_LOG_INTERVAL: u64 = 100;

/// Spawn the bounded-window prefetch thread for sequences
/// `current_seq+1..=latest_seq`.
///
/// Deliberately minimal: no channel plumbing, no change to apply ordering.
/// It downloads into the same `download_dir`, under the same
/// `osc_local_file_name(seq)` filenames the apply loop's own
/// `fetch_and_parse_sequence` uses, so that call's `download_file_as_quiet`
/// simply finds the file already there and returns immediately --
/// `download::tests::existing_file_skips_download_entirely` pins that
/// "already exists, skip" branch. This function does no DB access at all;
/// the apply loop remains the sole source of truth for what actually lands
/// in the database.
///
/// Bounded by `fetch_frontier` so it never runs more than `prefetch_ahead`
/// sequences ahead of the apply loop (memory/disk for `prefetch_ahead`
/// buffered `.osc.gz` files, not the whole backlog), and by `latest_seq` so
/// it never prefetches a sequence that doesn't exist yet. Also bounded
/// *behind*, and that bound is the load-bearing one: a `next` at or below
/// the frontier is skipped straight to `frontier + 1` rather than
/// downloaded, because the apply loop owns everything up to the frontier --
/// it has either already consumed and deleted that sequence's file or is
/// downloading it synchronously right now.
///
/// **Both bounds must read a fetch-time counter, never an apply-time one.**
/// This thread and the apply loop share one filename per sequence
/// (`osc_local_file_name`), which is what makes the exists-check dedup work;
/// the flip side is that both targeting one sequence at once is not a benign
/// duplicate. `do_download` renames its temp file onto the destination
/// unconditionally, so whichever finishes second re-creates the file --
/// and if the apply loop finished first it has already run
/// `decompress_and_remove`, leaving an orphan nothing will ever read.
/// Against the old apply-time floor that happened on every steady-state
/// tick, so `download_dir` grew by one file per tick forever. See
/// `fetch_frontier`'s declaration in [`update`].
///
/// Whatever this thread downloads and the apply loop never reaches is
/// unlinked by a cleanup pass after the loop -- see the comment there for
/// why it is driven by this thread's own record rather than a directory
/// scan.
///
/// A download failure here is non-fatal and is logged at `debug!` rather
/// than retried: `download_file_as_quiet` (via `download_with_retry`)
/// already gave the sequence three attempts with exponential backoff, and
/// retrying again on top of that would let the prefetcher fall behind its
/// window chasing one stubborn sequence for no benefit -- the apply loop's
/// own download call is the authority regardless, and just downloads the
/// sequence itself if the prefetch never landed.
#[allow(clippy::too_many_arguments)]
fn spawn_prefetcher(
    replication_base_url: String,
    download_dir: PathBuf,
    current_seq: u64,
    latest_seq: u64,
    prefetch_ahead: usize,
    fetch_frontier: Arc<AtomicU64>,
    in_flight: Arc<PrefetchInFlight>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut next = current_seq + 1;
        // Every sequence this thread successfully downloaded, so the cleanup
        // pass after the loop can unlink whatever the apply loop never got
        // to. Bounded by `prefetch_ahead` live entries in practice and by
        // the backlog length in the worst case, which is `u64`s either way.
        let mut downloaded: Vec<u64> = Vec::new();

        while next <= latest_seq {
            if stop.load(Ordering::SeqCst) {
                // `break`, not `return` -- the cleanup below must run on
                // this exit path too, and it is the *common* one for a
                // cancelled or failed `update()`.
                break;
            }

            let frontier = fetch_frontier.load(Ordering::SeqCst);
            if next <= frontier {
                // At or below the frontier the apply loop owns this
                // sequence: it has either already fetched, consumed and
                // deleted it, or it is downloading it synchronously right
                // now as part of the batch it has claimed. Either way this
                // thread must not touch it. Downloading it anyway is not
                // merely wasted bytes -- `do_download` renames its temp file
                // onto the destination unconditionally, so a download
                // finishing after the apply loop's `decompress_and_remove`
                // re-creates a file nothing will ever read again. Skip
                // straight past the whole claimed range rather than
                // stepping through it one sequence at a time, since the
                // frontier can jump by a whole batch between two of this
                // thread's checks.
                next = frontier + 1;
                continue;
            }

            let window_ceiling = frontier + prefetch_ahead as u64;
            if next > window_ceiling {
                // Window full: wait for the apply loop to claim its next
                // batch, checking `stop` between short sleeps rather than
                // one long one, so a cancelled or failed update() doesn't
                // block its join() for the whole wait.
                std::thread::sleep(PREFETCH_WINDOW_POLL_INTERVAL);
                continue;
            }

            // The frontier may have moved past `next` since it was read
            // above; `claim` re-checks it under the lock the apply loop
            // waits on. On refusal, loop round so the skip above applies.
            let Some(guard) = in_flight.claim(next, &fetch_frontier) else {
                continue;
            };
            let path = sequence_to_path(next);
            let url = format!("{replication_base_url}/{path}");
            let result = download_file_as_quiet(&url, &download_dir, &osc_local_file_name(next));
            drop(guard);
            match result {
                Ok(_) => downloaded.push(next),
                Err(e) => {
                    debug!(
                        seq = next,
                        error = %e,
                        "prefetch download failed; the apply loop will download it synchronously instead"
                    );
                }
            }

            next += 1;
        }

        // Wait until `update()` is finished before touching the directory.
        //
        // Reaching this point does NOT mean the apply loop is done: the
        // download loop above also ends on its own once `next` passes
        // `latest_seq`, which happens as soon as the frontier gets within
        // `prefetch_ahead` of the head -- with the apply loop still working
        // through the backlog behind it. Every entry in `downloaded` is
        // then still wanted, and deleting it there is not a harmless
        // no-op: at best the apply loop re-downloads the sequence, and at
        // worst the unlink lands between its exists-check and
        // `decompress_gz`'s open, failing the whole run with "Failed to
        // open". Measured on a backlog of 100 with `prefetch_ahead` above
        // it: 63 of 100 sequences re-downloaded.
        //
        // `update()` sets `stop` unconditionally on every exit path before
        // it joins this thread, so this wait always ends, and once it does
        // the apply loop has provably stopped fetching. Polled on the same
        // short interval as the window wait, so the join it gates is never
        // held up for long.
        while !stop.load(Ordering::SeqCst) {
            std::thread::sleep(PREFETCH_WINDOW_POLL_INTERVAL);
        }

        // Remove every prefetched file the apply loop never consumed.
        //
        // Whatever it did consume is already gone (`decompress_and_remove`),
        // so those unlinks are no-ops; what is left are sequences ahead of
        // the frontier when `update()` stopped -- a shutdown, a supervisor
        // timeout, or a failed batch. Without this they sit in
        // `download_dir` until some later run happens to resume onto them,
        // which for a run that stops for good is never, and the documented
        // contract (`example_config.toml` on `cleanup_downloaded_files`) is
        // that replication diffs are always cleaned up regardless of that
        // setting.
        //
        // Deliberately driven by this thread's own record of what it wrote
        // rather than by scanning `download_dir` for `*.osc.gz`: the
        // directory is shared (with the apply loop, and potentially with
        // another instance configured onto the same `download_dir`), and a
        // scan cannot tell a file this run abandoned from one another run is
        // about to consume. Running here rather than in `update()` after the
        // join is also what covers the download still in flight when `stop`
        // was set -- its rename lands before the loop breaks, so the
        // sequence is already in `downloaded` by the time this runs.
        for seq in downloaded {
            let _ = std::fs::remove_file(download_dir.join(osc_local_file_name(seq)));
        }
    })
}

fn get_current_sequence(conn: &Connection) -> Result<u64> {
    let result: Result<String, _> = conn.query_row(
        "SELECT value FROM metadata WHERE key = 'osm_replication_sequence'",
        [],
        |row| row.get(0),
    );

    match result {
        Ok(val) => val.parse().context("Invalid sequence number in metadata"),
        Err(_) => {
            bail!("No replication sequence number found in metadata. Run 'import osm' first.")
        }
    }
}

fn fetch_latest_sequence(replication_base_url: &str, download_dir: &Path) -> Result<(u64, String)> {
    let url = format!("{replication_base_url}/state.txt");
    let state_path = download_file_quiet(&url, download_dir)?;
    let text = std::fs::read_to_string(&state_path).context("Failed to read state.txt")?;
    let _ = std::fs::remove_file(&state_path);
    parse_state_txt(&text)
}

/// One downloaded, decompressed, and parsed replication sequence, ready to
/// apply. Produced by [`fetch_and_parse_sequence`] and consumed by
/// [`apply_batch`] -- kept as its own type (rather than, say, a tuple) so a
/// batch's `Vec<FetchedSequence>` reads clearly as "everything needed to
/// apply N sequences", already fetched, with no DB handle in sight.
struct FetchedSequence {
    seq: u64,
    changes: OsmChange,
}

/// Download and parse one replication sequence. No DB access -- deliberately
/// the network+parsing half only, so a whole batch can be fetched before any
/// of it is applied; see [`apply_batch`] for what that split buys.
fn fetch_and_parse_sequence(
    seq: u64,
    replication_base_url: &str,
    download_dir: &Path,
) -> Result<FetchedSequence> {
    let path = sequence_to_path(seq);
    let url = format!("{replication_base_url}/{path}");

    // `sequence_to_path` is only used to build the URL. The local filename
    // must be derived from `seq` directly, not (as `download_file_quiet`
    // would do) from the URL's last path segment: `sequence_to_path` nests
    // three directory levels of the zero-padded sequence number
    // (`007/237/736.osc.gz`), so its *last segment alone* repeats every
    // 1000 sequences -- sequence 7237736 and sequence 7236736 both end in
    // `736.osc.gz`. Reusing that segment as the on-disk filename would let
    // `download_file_as_impl`'s exists-check silently hand back a different
    // sequence's stale file without downloading anything.
    let osc_gz_path = download_file_as_quiet(&url, download_dir, &osc_local_file_name(seq))?;
    let osc_xml = decompress_and_remove(&osc_gz_path)?;
    let changes = parse_osc(&osc_xml)?;

    Ok(FetchedSequence { seq, changes })
}

/// Apply a batch of already-fetched sequences inside a single DuckDB
/// transaction, stamping `metadata` once with the batch's *last* sequence.
///
/// `batch` must be non-empty and sorted ascending by `seq` -- `update()`'s
/// caller loop guarantees both. The order is load-bearing: `OsmChange::collapse`
/// breaks a version tie in favour of the later sequence. Steady state and any
/// catch-up small enough
/// to stay under `batch_commit_threshold` always call this with a
/// single-element batch (`catch_up_chunk_size` returns `1`), which is
/// exactly the pre-batching `apply_sequence` behaviour: one BEGIN, one
/// `apply_collapsed`, one metadata stamp, one COMMIT.
///
/// **Why fetching happens before `BEGIN`.** `update()`'s caller loop fetches
/// and parses the whole batch (network + gzip + XML) *before* calling this
/// function, so nothing in here ever blocks on the network while the
/// transaction is open. That bound is the direct mitigation for the
/// concurrency risk below: the longer this transaction is held, the more it
/// overlaps the `match_refresh` drain, so keeping network/parsing strictly
/// outside it is what keeps that overlap bounded by DB work alone. Do not
/// "simplify" this by having `apply_batch` itself download each sequence
/// inside the loop below.
///
/// **Resume correctness needs no new bookkeeping.** The metadata stamp is
/// written and committed together with every other write in this
/// transaction, so a crash (or any error) partway through leaves it at
/// whatever it was before this call -- `get_current_sequence` then resumes
/// at the batch's *first* sequence on the next `update()` call, replaying
/// the whole batch from scratch rather than a partial one. There is no
/// "resume from sequence N of this batch" state to maintain.
///
/// This function no longer advances any prefetch-window counter. It used to
/// bump a `last_applied` atomic per sequence so the prefetcher's window slid
/// forward during a large batch instead of stalling until the commit; the
/// window now rides on `update()`'s `fetch_frontier`, which is claimed
/// *before* the batch is fetched and so slides forward strictly earlier. See
/// that declaration for why an apply-time counter was not merely late but
/// orphaned a downloaded file per tick.
///
/// **Crash-safety argument, and the one thing it rests on.** Every RocksDB
/// primitive `apply_collapsed` calls is either an unconditional upsert/delete
/// (`put_node`, `delete_way`, ...) or a read-modify-write set toggle
/// (`add_node_to_ways`/`remove_node_to_ways`, `src/osm/kvstore.rs:260-313`).
/// All of those are idempotent, so replaying the *entire* batch on top of
/// whatever prefix a crash left in RocksDB converges to the same state as
/// applying it once cleanly -- including at every intermediate statement,
/// which matters because `resolve_way_coords` reads RocksDB live during each
/// `osm_buildings`/`osm_former_buildings` INSERT. `rebuild_way_geometry`'s
/// inferred arm reads *DuckDB* instead, which rolled back cleanly with the
/// rest of this transaction, so it re-derives the same tags on replay.
///
/// This is NOT because "there are no merge operators here" or because the
/// merge functions are dead code -- a merge operator genuinely is registered
/// for the reverse-index column families (`src/osm/kvstore.rs:81`), and
/// `batch_merge_node_to_way`/`batch_merge_way_to_relation` are live callers
/// of it in `src/import/osm.rs` (~lines 303 and 636, the *import* path's
/// bulk-load). A list-append merge is NOT idempotent -- replaying one would
/// duplicate ids in the reverse index. The argument above holds only because
/// `apply_collapsed` (this *update* path) exclusively uses the get-modify-put
/// functions and never a merge; `replaying_a_batch_over_a_partially_written_kv_store_converges_to_the_golden_state`
/// (this file's test module) pins the resulting convergence directly against
/// `apply_collapsed` (through the test-only `apply_changes` wrapper), not a
/// description of it.
///
/// **Concurrency risk.** Committing a whole batch at once holds the write
/// transaction long enough to overlap the `match_refresh` drain.
/// `match_dirty_cells` is the only table both sides write, and
/// append-vs-delete-of-different-rows is what
/// `compare::drain_refresh_concurrency` already establishes as safe under
/// DuckDB's optimistic concurrency control -- but that test exercises a
/// *government-refresh*-shaped writer, not this batch shape; see this file's
/// `osm_apply_batch_and_match_refresh_drain_do_not_collide` for the
/// OSM-shaped analogue. Separately, because `DirtyCells::flush`'s `now()` is
/// transaction-start-scoped (see the CLAUDE.md gotcha of the same name),
/// every cell a batch dirties is stamped with the *batch's* start time and
/// stays invisible to the drain until the whole batch commits -- the same
/// cosmetic staleness already accepted for government refreshes, just now
/// also bounded by batch duration for OSM. And a failed batch re-downloads
/// every sequence in it, since `.osc.gz` files are deleted right after
/// decompression (`decompress_and_remove`) and there is nothing on disk to
/// resume from. These three are why the defaults
/// (`batch_commit_threshold`/`batch_size` = 20) are modest, not e.g. 200.
fn apply_batch(
    conn: &Connection,
    kv: &RocksDB,
    batch: &[FetchedSequence],
    timestamp: &str,
) -> Result<()> {
    let last_seq = batch
        .last()
        .map(|f| f.seq)
        .expect("apply_batch must not be called with an empty batch");
    let first_seq = batch[0].seq;
    // Every error out of here names the range, so a job that keeps failing
    // shows in the journal whether it is retrying the same batch.
    let range = || format!("applying OSM sequences {first_seq}..={last_seq}");

    conn.execute_batch("BEGIN TRANSACTION")
        .with_context(range)?;

    let result = (|| -> Result<()> {
        // One collapsed change set for the whole batch, not one per
        // sequence. See `OsmChange::collapse` for why that is equivalent.
        apply_collapsed(
            conn,
            kv,
            &OsmChange::collapse(batch.iter().map(|f| &f.changes)),
        )?;

        conn.execute_batch(&format!(
            "DELETE FROM metadata WHERE key IN ('osm_replication_sequence', 'osm_replication_timestamp');
             INSERT INTO metadata VALUES ('osm_replication_sequence', '{last_seq}');
             INSERT INTO metadata VALUES ('osm_replication_timestamp', '{timestamp}');"
        ))
        .context("stamping the replication sequence")?;

        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT")
                .context("commit")
                .with_context(range)?;
            Ok(())
        }
        Err(e) => {
            // A rollback that fails part-way (DuckDB pins each undo-buffer
            // block, which can itself hit the memory limit) leaves the rows
            // it had not reached marked as deleted by a transaction that no
            // longer exists. Every later batch touching them then fails with
            // `Conflict on tuple deletion!` until the process restarts, so
            // this is the line that explains that error.
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                warn!(
                    error = %rb,
                    first_seq,
                    last_seq,
                    "failed to roll back OSM batch; rows it deleted may stay locked until restart"
                );
            }
            Err(e.context(range()))
        }
    }
}

/// On-disk filename for a downloaded replication diff. Deliberately built
/// from `seq` directly rather than reusing `sequence_to_path(seq)`'s last
/// path segment as a filename -- see the comment at its call site in
/// `fetch_and_parse_sequence` for why that collides across sequences 1000
/// apart. Also the filename the prefetch thread (`spawn_prefetcher`) uses,
/// which is exactly what lets the apply loop's own download call find a
/// prefetched file already there.
fn osc_local_file_name(seq: u64) -> String {
    format!("{seq}.osc.gz")
}

/// Decompress a downloaded `.osc.gz` and delete it, **whether or not**
/// decompression succeeded.
///
/// The cleanup must not be skipped on failure. Since the local filename is
/// now stable per sequence (see [`osc_local_file_name`]), a corrupt leftover
/// would otherwise be handed straight back by `download_file_as_impl`'s
/// exists-check on every subsequent attempt, and that sequence could never
/// make progress again — the update job would wedge permanently on one bad
/// download.
///
/// This exists as its own function rather than three lines inline in
/// `fetch_and_parse_sequence` so the regression test can pin the *production*
/// ordering. A test that merely re-executed the same three statements would
/// still pass if `fetch_and_parse_sequence` were reverted to
/// `decompress_gz(&path)?` followed by the removal, which is exactly the bug.
///
/// Note the removal here is unconditional and is *not* gated on
/// `config.cleanup_downloaded_files` — that setting governs only the
/// dataset/PBF paths, never replication diffs.
fn decompress_and_remove(path: &Path) -> Result<String> {
    let decompressed = decompress_gz(path);
    let _ = std::fs::remove_file(path);
    decompressed
}

fn decompress_gz(path: &Path) -> Result<String> {
    let file = std::fs::File::open(path).with_context(|| format!("Failed to open {path:?}"))?;
    let mut decoder = GzDecoder::new(file);
    let mut xml = String::new();
    decoder
        .read_to_string(&mut xml)
        .context("Failed to decompress gzip")?;
    Ok(xml)
}

/// Apply one diff on its own: the unit tests' entry point. Production goes
/// through `apply_batch`, which collapses a whole batch at once.
#[cfg(test)]
fn apply_changes(conn: &Connection, kv: &RocksDB, changes: &OsmChange) -> Result<()> {
    apply_collapsed(conn, kv, &OsmChange::collapse([changes]))
}

/// Apply a change set that holds **at most one change per object**, i.e. the
/// output of `OsmChange::collapse`. The rebuilds look an object's tags up by
/// id, so a second version of the same id would be ambiguous. That was the
/// "first version wins" bug, when this was a `find` over an uncollapsed diff.
///
/// A failure names the phase it happened in. Without it the job's error was a
/// bare `Out of Memory Error`, with nothing saying which of ~a dozen
/// statement kinds hit the limit.
fn apply_collapsed(conn: &Connection, kv: &RocksDB, changes: &OsmChange) -> Result<()> {
    let mut phase = "node changes";
    apply_collapsed_phases(conn, kv, changes, &mut phase).with_context(|| phase)
}

/// [`apply_collapsed`]'s body. Sets `phase` at the start of each section so
/// the wrapper can name it in the error.
fn apply_collapsed_phases(
    conn: &Connection,
    kv: &RocksDB,
    changes: &OsmChange,
    phase: &mut &'static str,
) -> Result<()> {
    let way_changes: HashMap<i64, &WayChange> = changes.ways.iter().map(|w| (w.id, w)).collect();
    let relation_changes: HashMap<i64, &RelationChange> =
        changes.relations.iter().map(|r| (r.id, r)).collect();
    debug_assert_eq!(way_changes.len(), changes.ways.len(), "uncollapsed ways");
    debug_assert_eq!(
        relation_changes.len(),
        changes.relations.len(),
        "uncollapsed relations"
    );

    let mut affected_way_ids: HashSet<i64> = HashSet::new();
    let mut affected_relation_ids: HashSet<i64> = HashSet::new();
    let mut dirty = DirtyCells::new();

    // --- Apply node changes ---
    for node in &changes.nodes {
        match node.action {
            ChangeAction::Delete => {
                let way_ids = kvstore::get_node_to_ways(kv, node.id)?;
                affected_way_ids.extend(&way_ids);
                for &wid in &way_ids {
                    kvstore::remove_node_to_ways(kv, node.id, wid)?;
                }
                kvstore::delete_node(kv, node.id)?;
                dirty.note_existing(conn, Layer::Addresses, "osm_addresses", node.id, "node")?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'node'",
                    [node.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                // The `.osc` carries degrees as decimal text; the store keeps
                // decimicrodegrees. `f64_to_decimicro` rounds rather than
                // truncates -- see its doc comment.
                kvstore::put_node(
                    kv,
                    node.id,
                    encoding::f64_to_decimicro(node.lon),
                    encoding::f64_to_decimicro(node.lat),
                )?;
                let way_ids = kvstore::get_node_to_ways(kv, node.id)?;
                affected_way_ids.extend(&way_ids);
                dirty.note_existing(conn, Layer::Addresses, "osm_addresses", node.id, "node")?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'node'",
                    [node.id],
                )?;
                if let Some(hn) = tag_value(&node.tags, "addr:housenumber") {
                    let street = tag_value(&node.tags, "addr:street");
                    let city = tag_value(&node.tags, "addr:city")
                        .or_else(|| tag_value(&node.tags, "addr:place"));
                    let postcode = tag_value(&node.tags, "addr:postcode");
                    conn.execute(
                        "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
                         VALUES (?, 'node', ?, ?, ?, ?, ST_Point(?, ?))",
                        duckdb::params![node.id, hn, street, city, postcode, node.lon, node.lat],
                    )?;
                    dirty.note_point(Layer::Addresses, node.lon, node.lat);
                }
            }
        }
    }

    // --- Apply way changes ---
    *phase = "way changes";
    for way in &changes.ways {
        match way.action {
            ChangeAction::Delete => {
                if let Some(old_node_ids) = kvstore::get_way(kv, way.id)? {
                    for &nid in &old_node_ids {
                        kvstore::remove_node_to_ways(kv, nid, way.id)?;
                    }
                }
                let rel_ids = kvstore::get_way_to_relations(kv, way.id)?;
                affected_relation_ids.extend(&rel_ids);
                kvstore::delete_way(kv, way.id)?;
                dirty.note_existing(conn, Layer::Buildings, "osm_buildings", way.id, "way")?;
                dirty.note_existing(conn, Layer::Addresses, "osm_addresses", way.id, "way")?;
                dirty.note_existing(
                    conn,
                    Layer::Buildings,
                    "osm_former_buildings",
                    way.id,
                    "way",
                )?;
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_former_buildings WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                if let Some(old_node_ids) = kvstore::get_way(kv, way.id)? {
                    for &nid in &old_node_ids {
                        kvstore::remove_node_to_ways(kv, nid, way.id)?;
                    }
                }
                kvstore::put_way(kv, way.id, &way.node_refs)?;
                for &nid in &way.node_refs {
                    kvstore::add_node_to_ways(kv, nid, way.id)?;
                }
                let rel_ids = kvstore::get_way_to_relations(kv, way.id)?;
                affected_relation_ids.extend(&rel_ids);
                affected_way_ids.insert(way.id);
            }
        }
    }

    // --- Apply relation changes ---
    *phase = "relation changes";
    for rel in &changes.relations {
        match rel.action {
            ChangeAction::Delete => {
                if let Some(old_members) = kvstore::get_relation(kv, rel.id)? {
                    for (ref_id, member_type, _) in &old_members {
                        if *member_type == encoding::encode_member_type("way") {
                            kvstore::remove_way_to_relations(kv, *ref_id, rel.id)?;
                        }
                    }
                }
                kvstore::delete_relation(kv, rel.id)?;
                dirty.note_existing(conn, Layer::Buildings, "osm_buildings", rel.id, "relation")?;
                dirty.note_existing(conn, Layer::Addresses, "osm_addresses", rel.id, "relation")?;
                dirty.note_existing(
                    conn,
                    Layer::Buildings,
                    "osm_former_buildings",
                    rel.id,
                    "relation",
                )?;
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_former_buildings WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                if let Some(old_members) = kvstore::get_relation(kv, rel.id)? {
                    for (ref_id, member_type, _) in &old_members {
                        if *member_type == encoding::encode_member_type("way") {
                            kvstore::remove_way_to_relations(kv, *ref_id, rel.id)?;
                        }
                    }
                }
                let members: Vec<(i64, u8, u8)> = rel
                    .members
                    .iter()
                    .map(|m| {
                        (
                            m.member_ref,
                            encoding::encode_member_type(&m.member_type),
                            encoding::encode_member_role(&m.role),
                        )
                    })
                    .collect();
                kvstore::put_relation(kv, rel.id, &members)?;
                for m in &rel.members {
                    if m.member_type == "way" {
                        kvstore::add_way_to_relations(kv, m.member_ref, rel.id)?;
                    }
                }
                affected_relation_ids.insert(rel.id);
            }
        }
    }

    // --- Rebuild affected way geometries ---
    *phase = "way geometry rebuild";
    for &way_id in &affected_way_ids {
        rebuild_way_geometry(conn, kv, way_id, &way_changes, &mut dirty)
            .with_context(|| format!("way {way_id}"))?;
    }

    // Cascade way changes to relations
    *phase = "way-to-relation cascade";
    for &way_id in &affected_way_ids {
        let rel_ids = kvstore::get_way_to_relations(kv, way_id)?;
        affected_relation_ids.extend(&rel_ids);
    }

    // --- Rebuild affected relation geometries ---
    *phase = "relation geometry rebuild";
    for &relation_id in &affected_relation_ids {
        rebuild_relation_geometry(conn, kv, relation_id, &relation_changes, &mut dirty)
            .with_context(|| format!("relation {relation_id}"))?;
    }

    *phase = "dirty-cell flush";
    dirty.flush(conn)?;

    Ok(())
}

fn tag_value(tags: &[(String, String)], key: &str) -> Option<String> {
    tags.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}

/// What a rebuild re-inserts: `(building, housenumber, street, city, postcode,
/// former)`, where `city` is already `COALESCE(addr:city, addr:place)` and
/// `former` is the lifecycle `(key, value)`.
type RebuildTags = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<(String, String)>,
);

/// A stored `osm_addresses` row's `(housenumber, street, city, postcode)`.
type StoredAddress = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The tags of a way/relation that is being rebuilt only because a node or
/// member way under it moved, so the changeset carries no tags for it: read
/// back from the rows it already has, which must be done BEFORE the rebuild
/// deletes them. `None` means it has no row at all, so there is nothing to
/// rebuild.
///
/// **Every value is the stored one, never a placeholder.** The rebuild deletes
/// and re-inserts, so whatever this returns *is* the new row. This used to
/// return only existence -- `building = 'yes'` and an address of
/// `housenumber = ''` with NULL street/city/postcode -- so a node nudged by
/// 3 cm blanked the address of every building sharing it, and the PRG address
/// it was matching reappeared as unmatched (Niezapominajki 11, Pruszków, OSM
/// way 1081685388). An existing database keeps the blanked rows until
/// `import osm` is re-run; the tag values exist nowhere else in this system.
fn stored_rebuild_tags(
    conn: &Connection,
    osm_id: i64,
    osm_type: &str,
) -> Result<Option<RebuildTags>> {
    let building: Option<String> = conn
        .query_row(
            "SELECT COALESCE(building, 'yes') FROM osm_buildings WHERE osm_id = ? AND osm_type = ?",
            duckdb::params![osm_id, osm_type],
            |row| row.get(0),
        )
        .optional()?;
    let address: Option<StoredAddress> = conn
        .query_row(
            "SELECT housenumber, street, city, postcode FROM osm_addresses
             WHERE osm_id = ? AND osm_type = ?",
            duckdb::params![osm_id, osm_type],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    // Keep the stored key/value so the re-insert uses the SAME lifecycle key.
    let former: Option<(String, String)> = conn
        .query_row(
            "SELECT lifecycle_key, lifecycle_value FROM osm_former_buildings
             WHERE osm_id = ? AND osm_type = ?",
            duckdb::params![osm_id, osm_type],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    // Load-bearing: without `&& former.is_none()`, a former-building object
    // whose geometry moved would return here before the delete/re-insert, so
    // its row would keep stale pre-move geometry.
    if building.is_none() && address.is_none() && former.is_none() {
        return Ok(None);
    }
    // A stored address row always has a housenumber (every insert site
    // requires one), so `Some` here is what re-inserts it.
    let (housenumber, street, city, postcode) = match address {
        Some((hn, street, city, postcode)) => {
            (hn.or_else(|| Some(String::new())), street, city, postcode)
        }
        None => (None, None, None, None),
    };
    Ok(Some((
        building,
        housenumber,
        street,
        city,
        postcode,
        former,
    )))
}

/// A way that cannot be turned into a geometry: absent from the store
/// (`missing_nodes` is `None`), or present but referencing nodes that are not
/// (`Some(ids)`).
#[derive(Debug, PartialEq)]
struct UnresolvedWay {
    way_id: i64,
    missing_nodes: Option<Vec<i64>>,
}

/// The ways among `way_ids` that cannot be resolved to coordinates, with the
/// node ids each one is missing.
///
/// This is the extract's edge. The feed is a diff that has already been
/// filtered to Poland, so a way crossing the border can reference nodes that
/// never reached us, either in the PBF or in any diff. Such an object is
/// **ignored with a warning, never built from whatever part is present**: a
/// partial multipolygon would be a wrong footprint, and it would suppress or
/// un-suppress government buildings on the strength of that. A node arriving
/// later does not bring the object back, because only its node refs are
/// stored and not its tags. It returns on its next direct edit, or on
/// `import osm`.
fn unresolved_way_members(kv: &RocksDB, way_ids: &[i64]) -> Result<Vec<UnresolvedWay>> {
    let mut unresolved = Vec::new();
    for &way_id in way_ids {
        let Some(refs) = kvstore::get_way(kv, way_id)? else {
            unresolved.push(UnresolvedWay {
                way_id,
                missing_nodes: None,
            });
            continue;
        };
        // One multi-get answers the common case; per-node lookups only run
        // to name the missing ids once something is known to be missing.
        if kvstore::multi_get_nodes_wkb_coords(kv, &refs)?.is_some() {
            continue;
        }
        let mut missing = Vec::new();
        for &node_id in &refs {
            if !missing.contains(&node_id)
                && kvstore::multi_get_nodes_wkb_coords(kv, &[node_id])?.is_none()
            {
                missing.push(node_id);
            }
        }
        unresolved.push(UnresolvedWay {
            way_id,
            missing_nodes: Some(missing),
        });
    }
    Ok(unresolved)
}

/// The log line naming what could not be resolved, e.g. `way/7 is missing
/// node/1, node/2; way/8 is not in the store`. Ids are listed in full, so the
/// operator can look each one up.
fn describe_unresolved(unresolved: &[UnresolvedWay]) -> String {
    let parts: Vec<String> = unresolved
        .iter()
        .map(|u| match &u.missing_nodes {
            None => format!("way/{} is not in the store", u.way_id),
            Some(nodes) => format!(
                "way/{} is missing {}",
                u.way_id,
                nodes
                    .iter()
                    .map(|n| format!("node/{n}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        })
        .collect();
    format!("{} (outside the extract?)", parts.join("; "))
}

fn rebuild_way_geometry(
    conn: &Connection,
    kv: &RocksDB,
    way_id: i64,
    way_changes: &HashMap<i64, &WayChange>,
    dirty: &mut DirtyCells,
) -> Result<()> {
    if kvstore::get_way(kv, way_id)?.is_none() {
        return Ok(());
    }

    // Determine tags: from the change if directly affected, else from the rows it
    // already has (`stored_rebuild_tags`), read BEFORE the delete below.
    let way_change = way_changes.get(&way_id);
    let (building_tag, housenumber, street, city, postcode, former) = match way_change {
        Some(wc) => (
            tag_value(&wc.tags, "building"),
            tag_value(&wc.tags, "addr:housenumber"),
            tag_value(&wc.tags, "addr:street"),
            tag_value(&wc.tags, "addr:city").or_else(|| tag_value(&wc.tags, "addr:place")),
            tag_value(&wc.tags, "addr:postcode"),
            lifecycle::key_of(&wc.tags).map(|key| {
                (
                    key.to_string(),
                    tag_value(&wc.tags, key).unwrap_or_default(),
                )
            }),
        ),
        None => match stored_rebuild_tags(conn, way_id, "way")? {
            Some(tags) => tags,
            None => return Ok(()),
        },
    };

    // No early return when all of building/address/former are absent: that is
    // the de-tag case (a Modify stripped building/addr:housenumber/a lifecycle
    // key off a way we serve), and it still has to delete the base row and
    // note the cell it left -- otherwise the government object this way was
    // matching (or vetoing) stays wrong until the next full compare. The
    // re-inserts below are already guarded by their own is_some() checks, so
    // falling through simply deletes and stops.
    dirty.note_existing(conn, Layer::Buildings, "osm_buildings", way_id, "way")?;
    dirty.note_existing(conn, Layer::Addresses, "osm_addresses", way_id, "way")?;
    dirty.note_existing(
        conn,
        Layer::Buildings,
        "osm_former_buildings",
        way_id,
        "way",
    )?;

    conn.execute(
        "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;
    conn.execute(
        "DELETE FROM osm_former_buildings WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;

    if building_tag.is_some() || former.is_some() || housenumber.is_some() {
        let unresolved = unresolved_way_members(kv, &[way_id])?;
        if !unresolved.is_empty() {
            warn!(
                "Ignoring way/{way_id}: {}",
                describe_unresolved(&unresolved)
            );
            return Ok(());
        }
    }

    if building_tag.is_some() {
        let building = building_tag.as_deref().unwrap_or("yes");
        let building_sql = building.replace('\'', "''");
        let ring = way_ring_polygon_sql(&way_id.to_string());
        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
             SELECT {way_id}, 'way', '{building_sql}',
                    {geom}
             WHERE resolve_way_coords({way_id}) IS NOT NULL
               AND ST_NPoints(ST_GeomFromWKB(resolve_way_coords({way_id}))) >= 4
               AND ST_IsClosed(ST_GeomFromWKB(resolve_way_coords({way_id})))
               AND {guard}",
            geom = geometry::repaired_geom_sql(&ring),
            guard = geometry::has_polygon_sql(&ring),
        ))?;
        dirty.note_existing(conn, Layer::Buildings, "osm_buildings", way_id, "way")?;
    }

    if let Some((lifecycle_key, lifecycle_value)) = &former {
        let ring = way_ring_polygon_sql("?");
        conn.execute(
            &format!(
                "INSERT INTO osm_former_buildings (osm_id, osm_type, lifecycle_key, lifecycle_value, geom)
                 SELECT ?, 'way', ?, ?,
                        {geom}
                 WHERE resolve_way_coords(?) IS NOT NULL
                   AND ST_NPoints(ST_GeomFromWKB(resolve_way_coords(?))) >= 4
                   AND ST_IsClosed(ST_GeomFromWKB(resolve_way_coords(?)))
                   AND {guard}",
                geom = geometry::repaired_geom_sql(&ring),
                guard = geometry::has_polygon_sql(&ring),
            ),
            duckdb::params![
                way_id,
                lifecycle_key,
                lifecycle_value,
                way_id,
                way_id,
                way_id,
                way_id,
                way_id
            ],
        )?;
        dirty.note_existing(
            conn,
            Layer::Buildings,
            "osm_former_buildings",
            way_id,
            "way",
        )?;
    }

    if housenumber.is_some() {
        conn.execute(
            "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
             SELECT ?, 'way', ?, ?, ?, ?,
                    ST_Centroid(ST_GeomFromWKB(resolve_way_coords(?)))
             WHERE resolve_way_coords(?) IS NOT NULL",
            duckdb::params![way_id, housenumber, street, city, postcode, way_id, way_id],
        )?;
        dirty.note_existing(conn, Layer::Addresses, "osm_addresses", way_id, "way")?;
    }

    Ok(())
}

/// The raw polygon a closed way's node coordinates describe, shared by
/// `osm_buildings`' and `osm_former_buildings`' way inserts. `way_ref` is
/// whatever the caller's statement uses to name the way -- an interpolated id
/// for the `execute_batch` call site, a literal `?` for the parameterized one.
///
/// Deliberately *unrepaired*: the caller wraps it in
/// `osm::geometry::repaired_geom_sql` for its select list and in
/// `osm::geometry::has_polygon_sql` for its WHERE, so both see the identical
/// inner expression. Building the repair in here instead would leave the guard
/// with no way to ask about the same geometry without spelling it out again.
fn way_ring_polygon_sql(way_ref: &str) -> String {
    format!("ST_MakePolygon(ST_GeomFromWKB(resolve_way_coords({way_ref})))")
}

/// The assembled relation polygon (outer ways unioned, inner ways
/// differenced), reading the CTE columns `relation_multipolygon_geom_sql`
/// below produces. Same split as `way_ring_polygon_sql`: unrepaired here, so
/// the select list and the WHERE guard can both wrap one expression.
///
/// This exists because that expression is long enough that spelling it twice
/// at each of the two relation call sites -- four copies of a nested CASE --
/// would be exactly the kind of drift the shared CTE builder below already
/// avoids for the rest of the statement.
fn relation_polygon_sql() -> String {
    "CASE
                     WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                     ELSE o.outer_geom
                 END"
    .to_string()
}

/// The multipolygon assembly CTE chain shared by every relation geometry
/// INSERT that reconstructs a polygon from way members by unioning the
/// 'outer' ways, unioning the 'inner' ways, and differencing them --
/// `osm_buildings` and `osm_former_buildings`' relation inserts both build
/// this way. `osm_addresses`' relation insert does not: it wants a centroid,
/// not a polygon, so it is deliberately left out of this shared home.
/// `values_sql` is the `(way_id, role)` VALUES list built from the relation's
/// way members. Callers append their own final `SELECT ... FROM outer_polys o
/// LEFT JOIN inner_polys i ON true WHERE o.outer_geom IS NOT NULL`, since the
/// non-geometry columns (and whether they come from a literal or a bind
/// parameter) differ per caller.
fn relation_multipolygon_geom_sql(values_sql: &str) -> String {
    format!(
        "WITH way_members(way_id, member_role) AS (VALUES {values_sql}),
         way_geoms AS (
             SELECT way_id, member_role,
                    ST_GeomFromWKB(resolve_way_coords(way_id)) AS line_geom
             FROM way_members
             WHERE resolve_way_coords(way_id) IS NOT NULL
         ),
         outer_polys AS (
             SELECT ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
             FROM way_geoms
             WHERE (member_role = 'outer' OR member_role = '')
               AND ST_NPoints(line_geom) >= 4
               AND ST_IsClosed(line_geom)
         ),
         inner_polys AS (
             SELECT ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
             FROM way_geoms
             WHERE member_role = 'inner'
               AND ST_NPoints(line_geom) >= 4
               AND ST_IsClosed(line_geom)
         )"
    )
}

fn rebuild_relation_geometry(
    conn: &Connection,
    kv: &RocksDB,
    relation_id: i64,
    relation_changes: &HashMap<i64, &RelationChange>,
    dirty: &mut DirtyCells,
) -> Result<()> {
    let members = match kvstore::get_relation(kv, relation_id)? {
        Some(m) => m,
        None => return Ok(()),
    };

    // Determine tags: from the change if directly affected, else from the rows it
    // already has (`stored_rebuild_tags`), read BEFORE the delete below.
    let rel_change = relation_changes.get(&relation_id);
    let (building_tag, housenumber, street, city, postcode, former) = match rel_change {
        Some(rc) => (
            tag_value(&rc.tags, "building"),
            tag_value(&rc.tags, "addr:housenumber"),
            tag_value(&rc.tags, "addr:street"),
            tag_value(&rc.tags, "addr:city").or_else(|| tag_value(&rc.tags, "addr:place")),
            tag_value(&rc.tags, "addr:postcode"),
            lifecycle::key_of(&rc.tags).map(|key| {
                (
                    key.to_string(),
                    tag_value(&rc.tags, key).unwrap_or_default(),
                )
            }),
        ),
        None => match stored_rebuild_tags(conn, relation_id, "relation")? {
            Some(tags) => tags,
            None => return Ok(()),
        },
    };

    // No early return when all of building/address/former are absent -- the
    // de-tag case still has to delete and note the vacated cell. See
    // rebuild_way_geometry.
    dirty.note_existing(
        conn,
        Layer::Buildings,
        "osm_buildings",
        relation_id,
        "relation",
    )?;
    dirty.note_existing(
        conn,
        Layer::Addresses,
        "osm_addresses",
        relation_id,
        "relation",
    )?;
    dirty.note_existing(
        conn,
        Layer::Buildings,
        "osm_former_buildings",
        relation_id,
        "relation",
    )?;

    conn.execute(
        "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
        [relation_id],
    )?;
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
        [relation_id],
    )?;
    conn.execute(
        "DELETE FROM osm_former_buildings WHERE osm_id = ? AND osm_type = 'relation'",
        [relation_id],
    )?;

    // Build a VALUES list of way members: (way_id, role)
    let way_members: Vec<(i64, &str)> = members
        .iter()
        .filter(|(_, member_type, _)| *member_type == encoding::encode_member_type("way"))
        .map(|(ref_id, _, role)| (*ref_id, encoding::decode_member_role(*role)))
        .collect();

    if way_members.is_empty() {
        return Ok(());
    }

    if building_tag.is_some() || former.is_some() || housenumber.is_some() {
        let way_ids: Vec<i64> = way_members.iter().map(|(wid, _)| *wid).collect();
        let unresolved = unresolved_way_members(kv, &way_ids)?;
        if !unresolved.is_empty() {
            warn!(
                "Ignoring relation/{relation_id}: {}",
                describe_unresolved(&unresolved)
            );
            return Ok(());
        }
    }

    let values_sql: String = way_members
        .iter()
        .map(|(wid, role)| format!("({wid}, '{role}')"))
        .collect::<Vec<_>>()
        .join(", ");

    if building_tag.is_some() {
        let building = building_tag.as_deref().unwrap_or("yes");
        let building_sql = building.replace('\'', "''");
        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
             {cte}
             SELECT
                 {relation_id}, 'relation', '{building_sql}',
                 {geom}
             FROM outer_polys o
             LEFT JOIN inner_polys i ON true
             WHERE o.outer_geom IS NOT NULL
               AND {guard}",
            cte = relation_multipolygon_geom_sql(&values_sql),
            geom = geometry::repaired_geom_sql(&relation_polygon_sql()),
            guard = geometry::has_polygon_sql(&relation_polygon_sql()),
        ))?;
        dirty.note_existing(
            conn,
            Layer::Buildings,
            "osm_buildings",
            relation_id,
            "relation",
        )?;
    }

    if let Some((lifecycle_key, lifecycle_value)) = &former {
        let sql = format!(
            "INSERT INTO osm_former_buildings (osm_id, osm_type, lifecycle_key, lifecycle_value, geom)
             {cte}
             SELECT
                 ?, 'relation', ?, ?,
                 {geom}
             FROM outer_polys o
             LEFT JOIN inner_polys i ON true
             WHERE o.outer_geom IS NOT NULL
               AND {guard}",
            cte = relation_multipolygon_geom_sql(&values_sql),
            geom = geometry::repaired_geom_sql(&relation_polygon_sql()),
            guard = geometry::has_polygon_sql(&relation_polygon_sql()),
        );
        conn.execute(
            &sql,
            duckdb::params![relation_id, lifecycle_key, lifecycle_value],
        )?;
        dirty.note_existing(
            conn,
            Layer::Buildings,
            "osm_former_buildings",
            relation_id,
            "relation",
        )?;
    }

    if housenumber.is_some() {
        let hn_sql = housenumber
            .as_deref()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());
        let street_sql = street
            .as_deref()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());
        let city_sql = city
            .as_deref()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());
        let postcode_sql = postcode
            .as_deref()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());

        conn.execute_batch(&format!(
            "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
             WITH way_members(way_id, member_role) AS (VALUES {values_sql}),
             way_geoms AS (
                 SELECT ST_GeomFromWKB(resolve_way_coords(way_id)) AS line_geom
                 FROM way_members
                 WHERE resolve_way_coords(way_id) IS NOT NULL
             )
             SELECT {relation_id}, 'relation', {hn_sql}, {street_sql}, {city_sql}, {postcode_sql},
                    ST_Centroid(ST_Collect(list(line_geom)))
             FROM way_geoms"
        ))?;
        dirty.note_existing(
            conn,
            Layer::Addresses,
            "osm_addresses",
            relation_id,
            "relation",
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::db::init_db;
    use crate::osm::kvstore;
    use crate::osm::replication::{NodeChange, RelationMember};

    /// Test coordinates are written in degrees for readability; the store
    /// keeps decimicrodegrees.
    fn dm(v: f64) -> i32 {
        encoding::f64_to_decimicro(v)
    }

    /// The KV half of the shared test fixture: nodes 1-4 forming a square,
    /// and way 100 (referencing them) with its reverse index. Split out from
    /// [`setup_test_db_and_kv`] so `replaying_a_batch_over_a_partially_written_kv_store_converges_to_the_golden_state`
    /// can build a DuckDB connection bound to an ALREADY-seeded KV store
    /// (one that also carries a "crash" prefix's writes) without re-seeding
    /// the KV a second time.
    fn seed_kv(kv: &RocksDB) -> Result<()> {
        kvstore::put_node(kv, 1, dm(20.0), dm(50.0))?;
        kvstore::put_node(kv, 2, dm(20.001), dm(50.0))?;
        kvstore::put_node(kv, 3, dm(20.001), dm(50.001))?;
        kvstore::put_node(kv, 4, dm(20.0), dm(50.001))?;

        kvstore::put_way(kv, 100, &[1, 2, 3, 4, 1])?;
        for &nid in &[1i64, 2, 3, 4] {
            kvstore::add_node_to_ways(kv, nid, 100)?;
        }
        Ok(())
    }

    /// The DuckDB half of the shared test fixture: way 100's existing
    /// building geometry (matching `seed_kv`'s square) and the pre-batch
    /// `metadata` stamp. Split out for the same reason as `seed_kv`.
    fn seed_duckdb(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "INSERT INTO osm_buildings VALUES (100, 'way', 'yes', ST_MakePolygon(ST_MakeLine(
                list_value(ST_Point(20.0, 50.0), ST_Point(20.001, 50.0),
                           ST_Point(20.001, 50.001), ST_Point(20.0, 50.001),
                           ST_Point(20.0, 50.0))
            )));
            INSERT INTO metadata VALUES ('osm_replication_sequence', '1000');",
        )?;
        Ok(())
    }

    /// Seed `count` nodes starting at `first_id` from a lon/lat ring and
    /// return the closed ref list for a way built from them.
    fn seed_ring(kv: &RocksDB, first_id: i64, ring: &[(f64, f64)]) -> Result<Vec<i64>> {
        for (i, (lon, lat)) in ring.iter().enumerate() {
            kvstore::put_node(kv, first_id + i as i64, dm(*lon), dm(*lat))?;
        }
        let mut refs: Vec<i64> = (first_id..first_id + ring.len() as i64).collect();
        refs.push(first_id);
        Ok(refs)
    }

    /// The `update osm` half of `osm::geometry`'s repair (the import half is
    /// pinned by that module's own tests). An incoming `.osc` creating a
    /// self-intersecting building way must land as valid geometry — otherwise
    /// the next per-cell recompute throws inside `drain_batch`, which rolls
    /// the cell back and leaves it queued, so that cell fails on every tick
    /// forever while serving stale tiles.
    ///
    /// The ring is OSM way 229993348's real coordinates: the bowtie that
    /// actually threw `side location conflict` and rolled back a national
    /// `compare full`.
    #[test]
    fn way_create_repairs_self_intersecting_geometry() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let refs = seed_ring(
            &kv,
            900,
            &[
                (15.4182745, 53.1661674),
                (15.4182624, 53.1661753),
                (15.41827, 53.1661467),
                (15.4182855, 53.166089),
                (15.4182344, 53.1660838),
                (15.4182263, 53.1661127),
                (15.4182028, 53.1661973),
            ],
        )?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                ways: vec![WayChange {
                    action: ChangeAction::Create,
                    version: 1,
                    id: 900,
                    node_refs: refs,
                    tags: vec![("building".into(), "service".into())],
                }],
                ..Default::default()
            },
        )?;

        let (valid, area): (bool, f64) = conn.query_row(
            "SELECT ST_IsValid(geom), ST_Area(geom) FROM osm_buildings WHERE osm_id = 900",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert!(
            valid,
            "an invalid way arriving via replication must be repaired on the way in"
        );
        assert!(area > 0.0, "the repair must keep the building's footprint");
        Ok(())
    }

    /// The other side of the update path's guard: a way with no area at all
    /// repairs to a linestring, so `has_polygon_sql` must keep it out of the
    /// table entirely. Storing `MULTIPOLYGON EMPTY` instead would make
    /// `note_existing`'s `ST_XMin` read NULL and fail the next edit to this
    /// object. Coordinates are eighths so the points are exactly collinear in
    /// f64 (see the CLAUDE.md fixture gotcha).
    #[test]
    fn way_create_skips_a_geometry_with_no_polygonal_part() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let refs = seed_ring(&kv, 910, &[(21.0, 52.0), (21.0625, 52.0), (21.125, 52.0)])?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                ways: vec![WayChange {
                    action: ChangeAction::Create,
                    version: 1,
                    id: 910,
                    node_refs: refs,
                    tags: vec![("building".into(), "yes".into())],
                }],
                ..Default::default()
            },
        )?;

        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 910",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(n, 0, "a zero-area way must not be stored as an empty row");
        Ok(())
    }

    /// The relation arm of the former-building insert, which had no test of
    /// its own — every other former-building test drives the *way* arm, so
    /// the relation INSERT's SQL (a different statement, built from
    /// `relation_multipolygon_geom_sql` plus the repair wrapper and its
    /// `has_polygon_sql` guard) was only ever exercised in production.
    #[test]
    fn relation_tagged_demolished_creates_a_former_building_row() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        kvstore::put_relation(
            &kv,
            210,
            &[(
                100,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 100, 210)?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                relations: vec![RelationChange {
                    action: ChangeAction::Create,
                    version: 1,
                    id: 210,
                    members: vec![RelationMember {
                        member_type: "way".into(),
                        member_ref: 100,
                        role: "outer".into(),
                    }],
                    tags: vec![
                        ("type".into(), "multipolygon".into()),
                        ("demolished:building".into(), "yes".into()),
                    ],
                }],
                ..Default::default()
            },
        )?;

        let (key, value, valid, area): (String, String, bool, f64) = conn.query_row(
            "SELECT lifecycle_key, lifecycle_value, ST_IsValid(geom), ST_Area(geom)
             FROM osm_former_buildings WHERE osm_id = 210 AND osm_type = 'relation'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        assert_eq!(
            (key.as_str(), value.as_str()),
            ("demolished:building", "yes")
        );
        assert!(valid && area > 0.0, "relation geometry must survive intact");

        // The relation is not a live building, so it must not also land in
        // osm_buildings (the disjointness rule in osm::lifecycle).
        let buildings: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 210",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(buildings, 0);
        Ok(())
    }

    fn setup_test_db_and_kv() -> Result<(Connection, Arc<RocksDB>, tempfile::TempDir)> {
        let tmpdir = tempfile::tempdir()?;
        let kv = Arc::new(kvstore::open(tmpdir.path(), 8, 4, 8)?);
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, Some(kv.clone()))?;

        seed_kv(&kv)?;
        seed_duckdb(&conn)?;

        Ok((conn, kv, tmpdir))
    }

    #[test]
    fn test_apply_node_create() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: 10,
                lon: 21.0,
                lat: 51.0,
                tags: vec![
                    ("addr:housenumber".into(), "5".into()),
                    ("addr:street".into(), "Nowa".into()),
                ],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        // Node should be in RocksDB
        let coords = kvstore::get_node(&kv, 10)?.unwrap();
        assert!((encoding::decimicro_to_f64(coords.0) - 21.0).abs() < 1e-9);

        // Address should be in DuckDB
        let hn: String = conn.query_row(
            "SELECT housenumber FROM osm_addresses WHERE osm_id = 10 AND osm_type = 'node'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(hn, "5");

        Ok(())
    }

    #[test]
    fn test_apply_node_delete() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let create = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: 20,
                lon: 21.0,
                lat: 51.0,
                tags: vec![("addr:housenumber".into(), "10".into())],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &create)?;

        let delete = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Delete,
                version: 1,
                id: 20,
                lon: 0.0,
                lat: 0.0,
                tags: vec![],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &delete)?;

        assert!(kvstore::get_node(&kv, 20)?.is_none());

        let addr_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_addresses WHERE osm_id = 20",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(addr_count, 0);

        Ok(())
    }

    #[test]
    fn test_apply_node_modify_cascades_to_way() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Modify,
                version: 1,
                id: 1,
                lon: 20.0005,
                lat: 50.0005,
                tags: vec![],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        // Node should be updated in RocksDB
        let (lon, lat) = kvstore::get_node(&kv, 1)?.unwrap();
        assert!((encoding::decimicro_to_f64(lon) - 20.0005).abs() < 1e-9);
        assert!((encoding::decimicro_to_f64(lat) - 50.0005).abs() < 1e-9);

        // Building geometry should have been rebuilt
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1, "Building should still exist after node modify");

        Ok(())
    }

    /// Read back `(building, housenumber, street, city, postcode)` for one
    /// object, NULL-safe on every column.
    fn stored_building_and_address(
        conn: &Connection,
        osm_id: i64,
        osm_type: &str,
    ) -> Result<(Option<String>, StoredAddress)> {
        Ok(conn.query_row(
            "SELECT (SELECT building FROM osm_buildings WHERE osm_id = ?1 AND osm_type = ?2),
                    a.housenumber, a.street, a.city, a.postcode
             FROM (SELECT 1) LEFT JOIN osm_addresses a ON a.osm_id = ?1 AND a.osm_type = ?2",
            duckdb::params![osm_id, osm_type],
            |row| {
                Ok((
                    row.get(0)?,
                    (row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?),
                ))
            },
        )?)
    }

    /// The Niezapominajki 11 regression: a node shared by an addressed
    /// building moves, the way itself is not in the changeset, so
    /// `rebuild_way_geometry` takes the inferred arm -- which used to
    /// re-insert the address as `housenumber = ''` with NULL street/city/
    /// postcode and the building as `'yes'`, silently un-matching the PRG
    /// address next to it.
    #[test]
    fn test_apply_node_move_keeps_the_way_s_stored_tags() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        conn.execute_batch(
            "UPDATE osm_buildings SET building = 'house' WHERE osm_id = 100 AND osm_type = 'way';
             INSERT INTO osm_addresses VALUES
                 (100, 'way', '11', 'Niezapominajki', 'Pruszków', '05-800', ST_Point(20.0005, 50.0005));",
        )?;

        let changes = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Modify,
                version: 1,
                id: 1,
                lon: 19.9999,
                lat: 49.9999,
                tags: vec![],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &changes)?;

        assert_eq!(
            stored_building_and_address(&conn, 100, "way")?,
            (
                Some("house".into()),
                (
                    Some("11".into()),
                    Some("Niezapominajki".into()),
                    Some("Pruszków".into()),
                    Some("05-800".into())
                ),
            )
        );
        let x_min: f64 = conn.query_row(
            "SELECT ST_XMin(geom) FROM osm_buildings WHERE osm_id = 100 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert!(
            x_min < 20.0,
            "geometry must still be rebuilt, got xmin={x_min}"
        );
        Ok(())
    }

    /// The relation twin of `test_apply_node_move_keeps_the_way_s_stored_tags`:
    /// the node moves, member way 100 is rebuilt, and the cascade rebuilds
    /// relation 200 through `rebuild_relation_geometry`'s inferred arm.
    #[test]
    fn test_apply_node_move_keeps_the_relation_s_stored_tags() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        kvstore::put_relation(
            &kv,
            200,
            &[(
                100,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 100, 200)?;
        conn.execute_batch(
            "INSERT INTO osm_buildings
                 SELECT 200, 'relation', 'apartments', geom FROM osm_buildings WHERE osm_id = 100;
             INSERT INTO osm_addresses VALUES
                 (200, 'relation', '7A', 'Lipowa', 'Reguły', NULL, ST_Point(20.0005, 50.0005));",
        )?;

        let changes = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Modify,
                version: 1,
                id: 1,
                lon: 19.9999,
                lat: 49.9999,
                tags: vec![],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &changes)?;

        assert_eq!(
            stored_building_and_address(&conn, 200, "relation")?,
            (
                Some("apartments".into()),
                (
                    Some("7A".into()),
                    Some("Lipowa".into()),
                    Some("Reguły".into()),
                    None
                ),
            )
        );
        Ok(())
    }

    #[test]
    fn test_apply_way_delete() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Delete,
                version: 1,
                id: 100,
                node_refs: vec![],
                tags: vec![],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        let building_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(building_count, 0);

        assert!(kvstore::get_way(&kv, 100)?.is_none());

        Ok(())
    }

    /// An OSM diff that adds a served address node, well inside a single z14
    /// cell (further from every edge than `OSM_MATCH_BUFFER_DEG`), must
    /// enqueue exactly that one cell under source 'prg', and must NOT touch
    /// the building sources.
    ///
    /// A `.osc.gz`-driven CLI test was tried first: `fixtures/osm.osc.gz`
    /// (a real minutely diff) does not touch any node/way/relation id
    /// present in the imported `fixtures/osm.pbf` extract, so it never
    /// exercises the served-object note sites. Crafting a synthetic
    /// `.osc.gz` would just re-encode this same `OsmChange` value in XML+gz
    /// for no added assurance, so this unit test exercises `apply_changes`
    /// directly instead (per the task's documented fallback).
    ///
    /// The fixture point is deliberately NOT the original (21.0, 51.0):
    /// verified that point sits at z14 cell (9147, 5484), but the buffered
    /// read at 51.0 - OSM_MATCH_BUFFER_DEG lands in cell_y 5485 -- a real
    /// latitude boundary sits only a small fraction of a degree south of
    /// 51.0, well within reach of the buffer whatever `OSM_MATCH_BUFFER_DEG`'s
    /// exact value is. That would make this test assert 2 for reasons that
    /// have nothing to do with the layer-gating it's meant to cover, and
    /// everything to do with an accident of the fixture's position.
    /// Repositioning to the interior of the same cell (its `tile_to_bbox`
    /// midpoint, the technique `compare::incremental`'s tests already use)
    /// keeps the assertion about layer gating rather than boundary geometry.
    #[test]
    fn test_apply_node_create_enqueues_prg_dirty_cells() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let (cx, cy) =
            crate::tile_math::lonlat_to_tile(21.0, 51.0, crate::tile_math::CHANGE_CELL_ZOOM);
        let (min_lon, min_lat, max_lon, max_lat) =
            crate::tile_math::tile_to_bbox(crate::tile_math::CHANGE_CELL_ZOOM, cx, cy);
        let lon = (min_lon + max_lon) / 2.0;
        let lat = (min_lat + max_lat) / 2.0;

        let changes = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: 10,
                lon,
                lat,
                tags: vec![
                    ("addr:housenumber".into(), "5".into()),
                    ("addr:street".into(), "Nowa".into()),
                ],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        let (px, py) =
            crate::tile_math::lonlat_to_tile(lon, lat, crate::tile_math::CHANGE_CELL_ZOOM);
        assert_eq!(
            (px, py),
            (cx, cy),
            "sanity: the cell midpoint must remain in the same cell as the original fixture point"
        );
        let prg: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'prg'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            prg, 1,
            "an address well inside a single cell must enqueue only that cell"
        );
        let center: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells
             WHERE source = 'prg' AND cell_x = ? AND cell_y = ?",
            duckdb::params![px as i32, py as i32],
            |row| row.get(0),
        )?;
        assert_eq!(center, 1, "center cell of the new address must be enqueued");

        let building_sources: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source IN ('bdot10k', 'egib')",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            building_sources, 0,
            "an address-only edit must not enqueue building sources"
        );

        Ok(())
    }

    /// Deleting a served building way must enqueue exactly the cell it left
    /// (way 100's fixture square sits well inside a single z14 cell) under
    /// BOTH building sources (bdot10k + egib), and must NOT touch prg (the
    /// way carries no address in this fixture).
    #[test]
    fn test_apply_way_delete_enqueues_building_dirty_cells() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Delete,
                version: 1,
                id: 100,
                node_refs: vec![],
                tags: vec![],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        for source in ["bdot10k", "egib"] {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = ?",
                duckdb::params![source],
                |row| row.get(0),
            )?;
            assert_eq!(
                n, 1,
                "exactly the vacated cell should be enqueued for {source}"
            );
        }
        let prg: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'prg'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            prg, 0,
            "a building-only edit must not enqueue the address source"
        );

        Ok(())
    }

    /// The OSM producer leg, end to end: raw `.osc` XML through `parse_osc`,
    /// `apply_changes` (which enqueues dirty cells), and `drain_batch` into the
    /// `*_unmatched` serving table an editor actually sees.
    ///
    /// Every other test here stops at `apply_changes` and asserts on
    /// `match_dirty_cells`, and the branch's smoke test substituted `reconcile`
    /// for the `update osm` leg because the checked-in fixture touches no id
    /// present in the fixture PBF. So nothing covered the whole chain: an OSM
    /// edit arriving as XML and changing what is served. The scenario is the
    /// one that matters most -- an editor deletes an OSM building, so the
    /// government building it was matching must come *back* as unmatched.
    #[test]
    fn osc_xml_flows_through_parse_apply_drain_into_the_serving_table() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 rodzaj_kod VARCHAR, kondygnacje_nadziemne INTEGER,
                 kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, geom GEOMETRY);
             -- Sits inside way 100's footprint, so OSM currently covers it.
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('gov1', ST_MakeEnvelope(20.0002, 50.0002, 20.0008, 50.0008));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )?;

        // Baseline: the government building is matched, so it is NOT served.
        crate::compare::buildings::compare_bdot10k(&conn)?;
        let served: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bdot10k_unmatched WHERE LOKALNYID = 'gov1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            served, 0,
            "precondition: gov1 is covered by OSM way 100, so it must not be served"
        );

        // An editor deletes the OSM building, arriving as replication XML.
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <delete>
    <way id="100" version="2"/>
  </delete>
</osmChange>"#;
        let changes = parse_osc(osc)?;
        assert_eq!(changes.ways.len(), 1, "parse_osc must see the deleted way");

        apply_changes(&conn, &kv, &changes)?;

        // apply_changes only enqueues; the drain is what rebuilds the cell.
        let queued: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'bdot10k'",
            [],
            |r| r.get(0),
        )?;
        assert!(queued > 0, "the delete must enqueue the vacated cell");

        let stats = crate::compare::drain::drain_batch(&conn, 100, &|| false)?;
        assert_eq!(stats.failed, 0, "no cell may fail to recompute");
        assert!(stats.cells > 0, "the drain must have recomputed something");

        // The government building is now uncovered, so it must be served.
        let served_after: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bdot10k_unmatched WHERE LOKALNYID = 'gov1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            served_after, 1,
            "after the OSM building was deleted, gov1 must reappear as unmatched"
        );

        Ok(())
    }

    /// The real gap the fixed-3x3-removal left uncovered: every other test
    /// in this file that exercises the *serving* consequence of an edit
    /// (`osc_xml_flows_through_parse_apply_drain_into_the_serving_table`
    /// above, plus `compare::full_vs_incremental_equivalence` and
    /// `compare::drain_refresh_concurrency`) seeds `match_dirty_cells` either
    /// with a single-cell fixture or via `reconcile::enqueue_all`, which
    /// builds cells straight in SQL and never calls `DirtyCells` at all. None
    /// of them would notice if `note_existing` regressed to recording only a
    /// row's centroid cell instead of its whole (buffered) bbox range.
    ///
    /// Here way 300's bbox straddles the boundary between z14 cells A (its
    /// own home cell, where its centroid lands) and B (a neighbour it only
    /// barely pokes into), while the government building it matches, gov1,
    /// sits entirely inside B. Deleting way 300 must enqueue BOTH cells: A
    /// (empty, a no-op recompute) and B, where gov1 must come back as
    /// unmatched. Under a hypothetical regression that recorded only way
    /// 300's centroid cell (A), B would never be enqueued and the drain would
    /// never touch it -- confirmed by temporarily reverting `note_existing`
    /// to the old `ST_Centroid`-based single-cell query and re-running this
    /// test: it fails at the `queued == 2` sanity check first (1, not 2 --
    /// that assertion alone already pins the regression), and would fail at
    /// the final `served_after` assertion too (`0`, not `1`) if the earlier
    /// one were removed.
    ///
    /// Boundary coordinates come from `tile_to_bbox`'s own computed f64
    /// output, offset by a dyadic (exact in f64) fraction, per the CLAUDE.md
    /// gotcha on hand-written geometry fixtures -- same technique as
    /// `dirty_cells::tests::note_existing_records_both_cells_a_straddling_bbox_touches`.
    #[test]
    fn osc_xml_straddling_cell_boundary_updates_the_neighbouring_cells_serving_table() -> Result<()>
    {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 rodzaj_kod VARCHAR, kondygnacje_nadziemne INTEGER,
                 kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, geom GEOMETRY);",
        )?;

        let (min_lon_a, min_lat, max_lon_a, max_lat) =
            crate::tile_math::tile_to_bbox(crate::tile_math::CHANGE_CELL_ZOOM, 9147, 5411);
        let mid_lat = (min_lat + max_lat) / 2.0;
        let shift = 1.0 / 8192.0; // ~13.7m at this latitude; exact in f64.

        // Way 300: deep inside cell A on its west side, poking just past the
        // A/B boundary (max_lon_a) to the east.
        let way_min_lon = min_lon_a + 0.002;
        let way_max_lon = max_lon_a + shift;
        let way_min_lat = mid_lat - 0.001;
        let way_max_lat = mid_lat + 0.001;

        // gov1: entirely inside B, inside the sliver way 300 pokes into, so
        // it starts out fully covered (matched).
        let gov_min_lon = max_lon_a + shift / 4.0;
        let gov_max_lon = max_lon_a + shift / 2.0;
        let gov_min_lat = mid_lat - shift / 4.0;
        let gov_max_lat = mid_lat + shift / 4.0;

        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings VALUES (300, 'way', 'yes', ST_MakeEnvelope(
                 {way_min_lon}, {way_min_lat}, {way_max_lon}, {way_max_lat}));
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('gov1', ST_MakeEnvelope({gov_min_lon}, {gov_min_lat}, {gov_max_lon}, {gov_max_lat}));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);"
        ))?;

        // Sanity check the fixture: way 300's bbox really does straddle
        // cells 9147/9148, and gov1 really does sit in the neighbouring
        // cell B (9148), not way 300's own home cell A (9147).
        let (way_cx_west, way_cy) = crate::tile_math::lonlat_to_tile(
            way_min_lon,
            mid_lat,
            crate::tile_math::CHANGE_CELL_ZOOM,
        );
        let (way_cx_east, _) = crate::tile_math::lonlat_to_tile(
            way_max_lon,
            mid_lat,
            crate::tile_math::CHANGE_CELL_ZOOM,
        );
        assert_eq!(
            (way_cx_west, way_cy, way_cx_east),
            (9147, 5411, 9148),
            "sanity: way 300's bbox must straddle cells 9147 and 9148"
        );
        let (gov_cx, gov_cy) = crate::tile_math::lonlat_to_tile(
            (gov_min_lon + gov_max_lon) / 2.0,
            (gov_min_lat + gov_max_lat) / 2.0,
            crate::tile_math::CHANGE_CELL_ZOOM,
        );
        assert_eq!(
            (gov_cx, gov_cy),
            (9148, 5411),
            "sanity: gov1 must sit in the neighbouring cell B, not way 300's home cell A"
        );

        // Baseline: gov1 is covered by way 300, so it is matched and not served.
        crate::compare::buildings::compare_bdot10k(&conn)?;
        let served: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bdot10k_unmatched WHERE LOKALNYID = 'gov1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            served, 0,
            "precondition: gov1 is covered by way 300, so it must not be served"
        );

        // An editor deletes the OSM way, arriving as replication XML.
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <delete>
    <way id="300" version="2"/>
  </delete>
</osmChange>"#;
        let changes = parse_osc(osc)?;
        apply_changes(&conn, &kv, &changes)?;

        // Both cells must be enqueued: way 300's own bbox spans exactly 2
        // cells and buildings carry no OSM read buffer (layer_buffer_deg).
        let queued: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'bdot10k'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            queued, 2,
            "the delete must enqueue exactly the 2 cells way 300's bbox touched"
        );

        let stats = crate::compare::drain::drain_batch(&conn, 100, &|| false)?;
        assert_eq!(stats.failed, 0, "no cell may fail to recompute");

        // gov1, in the NEIGHBOURING cell, must reappear as unmatched.
        let served_after: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bdot10k_unmatched WHERE LOKALNYID = 'gov1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            served_after, 1,
            "after way 300 was deleted, gov1 in the neighbouring cell must reappear as unmatched"
        );

        Ok(())
    }

    /// A Modify that strips every building/address tag off a served way is a
    /// de-tag: the OSM building is gone even though the way still exists. The
    /// base row must go with it, and the cell must be enqueued so the
    /// government building it was matching reappears as unmatched.
    #[test]
    fn test_apply_way_modify_stripping_tags_removes_row_and_enqueues() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Modify,
                version: 1,
                id: 100,
                node_refs: vec![1, 2, 3, 4, 1],
                // building=yes removed by the editor; nothing served left.
                tags: vec![("note".into(), "not a building any more".into())],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        let building_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            building_count, 0,
            "de-tagged way must not leave a stale osm_buildings row"
        );

        for source in ["bdot10k", "egib"] {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = ?",
                duckdb::params![source],
                |row| row.get(0),
            )?;
            assert_eq!(
                n, 1,
                "de-tagged way must enqueue the cell it left for {source}"
            );
        }

        Ok(())
    }

    /// Same de-tag, but on a relation: `rebuild_relation_geometry` has the
    /// identical early return, so it needs its own coverage.
    #[test]
    fn test_apply_relation_modify_stripping_tags_removes_row_and_enqueues() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        // Seed a served multipolygon relation 200 built from way 100.
        kvstore::put_relation(
            &kv,
            200,
            &[(
                100,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 100, 200)?;
        conn.execute_batch(
            "INSERT INTO osm_buildings VALUES (200, 'relation', 'yes', ST_MakePolygon(ST_MakeLine(
                list_value(ST_Point(20.0, 50.0), ST_Point(20.001, 50.0),
                           ST_Point(20.001, 50.001), ST_Point(20.0, 50.001),
                           ST_Point(20.0, 50.0))
            )));",
        )?;

        let changes = OsmChange {
            relations: vec![RelationChange {
                action: ChangeAction::Modify,
                version: 1,
                id: 200,
                members: vec![RelationMember {
                    member_type: "way".into(),
                    member_ref: 100,
                    role: "outer".into(),
                }],
                tags: vec![("type".into(), "multipolygon".into())],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        let building_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 200 AND osm_type = 'relation'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            building_count, 0,
            "de-tagged relation must not leave a stale osm_buildings row"
        );

        for source in ["bdot10k", "egib"] {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = ?",
                duckdb::params![source],
                |row| row.get(0),
            )?;
            assert_eq!(
                n, 1,
                "de-tagged relation must enqueue the cell it left for {source}, got {n}"
            );
        }

        Ok(())
    }

    /// Retagging `building=yes` -> `demolished:building=yes` via replication
    /// XML: the OSM building disappears and a former-building row takes its
    /// place. Modelled on
    /// `osc_xml_flows_through_parse_apply_drain_into_the_serving_table`, which
    /// seeds `gov1` inside way 100's footprint.
    ///
    /// Stops at what `update osm` itself is responsible for: the building row
    /// is gone, the former-building row exists with the right lifecycle key,
    /// and the vacated cell got enqueued. The suppression half -- that `gov1`
    /// must stay OUT of `bdot10k_unmatched` once the veto (Step 5 of the plan)
    /// sees the new `osm_former_buildings` row -- is its own end-to-end test
    /// below, `test_apply_way_retag_building_to_demolished_suppresses_the_government_building`,
    /// the single most valuable assertion in the whole change.
    #[test]
    fn test_apply_way_retag_building_to_demolished_creates_former_row() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <modify>
    <way id="100" version="2">
      <nd ref="1"/>
      <nd ref="2"/>
      <nd ref="3"/>
      <nd ref="4"/>
      <nd ref="1"/>
      <tag k="demolished:building" v="yes"/>
    </way>
  </modify>
</osmChange>"#;
        let changes = parse_osc(osc)?;
        assert_eq!(changes.ways.len(), 1, "parse_osc must see the modified way");

        apply_changes(&conn, &kv, &changes)?;

        let building_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(building_count, 0, "retag must remove the osm_buildings row");

        let (lifecycle_key, lifecycle_value): (String, String) = conn.query_row(
            "SELECT lifecycle_key, lifecycle_value FROM osm_former_buildings
             WHERE osm_id = 100 AND osm_type = 'way'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(lifecycle_key, "demolished:building");
        assert_eq!(lifecycle_value, "yes");

        for source in ["bdot10k", "egib"] {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = ?",
                duckdb::params![source],
                |row| row.get(0),
            )?;
            assert_eq!(n, 1, "retag must enqueue the vacated cell for {source}");
        }

        Ok(())
    }

    /// The suppression half of the retag scenario above, run end to end
    /// through `compare` + the drain: once `update osm` turns way 100 into a
    /// former-building row, the government building it used to match must
    /// stay OUT of `bdot10k_unmatched`, not reappear as unmatched the way a
    /// plain OSM deletion would (see
    /// `osc_xml_flows_through_parse_apply_drain_into_the_serving_table`, whose
    /// `gov1` fixture this reuses). Under pre-veto code this assertion would
    /// fail with `served_after == 1` -- this is the single most valuable test
    /// in the whole change.
    #[test]
    fn test_apply_way_retag_building_to_demolished_suppresses_the_government_building() -> Result<()>
    {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 rodzaj_kod VARCHAR, kondygnacje_nadziemne INTEGER,
                 kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, geom GEOMETRY);
             -- Sits inside way 100's footprint, so OSM currently covers it.
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('gov1', ST_MakeEnvelope(20.0002, 50.0002, 20.0008, 50.0008));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )?;

        // Baseline: the government building is matched by the live way 100, so it is NOT served.
        crate::compare::buildings::compare_bdot10k(&conn)?;
        let served: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bdot10k_unmatched WHERE LOKALNYID = 'gov1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            served, 0,
            "precondition: gov1 is covered by OSM way 100, so it must not be served"
        );

        // An editor retags the OSM building as demolished, arriving as replication XML.
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <modify>
    <way id="100" version="2">
      <nd ref="1"/>
      <nd ref="2"/>
      <nd ref="3"/>
      <nd ref="4"/>
      <nd ref="1"/>
      <tag k="demolished:building" v="yes"/>
    </way>
  </modify>
</osmChange>"#;
        let changes = parse_osc(osc)?;
        assert_eq!(changes.ways.len(), 1, "parse_osc must see the modified way");

        apply_changes(&conn, &kv, &changes)?;

        let building_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            building_count, 0,
            "the retag must remove the osm_buildings row"
        );

        let former_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_former_buildings WHERE osm_id = 100 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            former_count, 1,
            "the retag must create the former-building row"
        );

        // apply_changes only enqueues; the drain is what rebuilds the cell.
        let queued: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'bdot10k'",
            [],
            |r| r.get(0),
        )?;
        assert!(queued > 0, "the retag must enqueue the vacated cell");

        let stats = crate::compare::drain::drain_batch(&conn, 100, &|| false)?;
        assert_eq!(stats.failed, 0, "no cell may fail to recompute");
        assert!(stats.cells > 0, "the drain must have recomputed something");

        // gov1 is now covered by a former-building polygon instead of a live
        // OSM building -- the veto must keep it suppressed, not let it
        // reappear as unmatched.
        let served_after: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bdot10k_unmatched WHERE LOKALNYID = 'gov1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            served_after, 0,
            "after the retag, gov1 must stay suppressed rather than reappear as unmatched"
        );

        Ok(())
    }

    /// Tagging a plain (previously untagged) way `demolished:building` must
    /// create the `osm_former_buildings` row and must NOT also land in
    /// `osm_buildings` -- the disjointness decision from Step 3 of the plan
    /// applies identically on the `update osm` side.
    #[test]
    fn test_apply_way_create_with_demolished_building_tag_creates_former_row() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Create,
                version: 1,
                id: 300,
                node_refs: vec![1, 2, 3, 4, 1],
                tags: vec![("demolished:building".into(), "house".into())],
            }],
            ..Default::default()
        };

        apply_changes(&conn, &kv, &changes)?;

        let (lifecycle_key, lifecycle_value): (String, String) = conn.query_row(
            "SELECT lifecycle_key, lifecycle_value FROM osm_former_buildings
             WHERE osm_id = 300 AND osm_type = 'way'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(lifecycle_key, "demolished:building");
        assert_eq!(lifecycle_value, "house");

        let building_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 300 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            building_count, 0,
            "a former-building way must not also land in osm_buildings"
        );

        Ok(())
    }

    /// A node move on a former-building way must go through
    /// `rebuild_way_geometry`'s INFERRED arm (the way itself is not directly
    /// in the changeset), and the row must survive with its lifecycle_key
    /// intact and its geometry reflecting the move. This is the direct guard
    /// for edit 3's early-return extension: without `&& former.is_none()`,
    /// the function returns before the delete/re-insert, so the geometry
    /// would stay stale at the pre-move position.
    #[test]
    fn test_apply_node_move_on_former_building_way_keeps_row_with_moved_geometry() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        // A separate former-building way (150), independent from way 100's square.
        kvstore::put_node(&kv, 11, dm(21.0), dm(51.0))?;
        kvstore::put_node(&kv, 12, dm(21.001), dm(51.0))?;
        kvstore::put_node(&kv, 13, dm(21.001), dm(51.001))?;
        kvstore::put_node(&kv, 14, dm(21.0), dm(51.001))?;
        kvstore::put_way(&kv, 150, &[11, 12, 13, 14, 11])?;
        for &nid in &[11i64, 12, 13, 14] {
            kvstore::add_node_to_ways(&kv, nid, 150)?;
        }
        conn.execute_batch(
            "INSERT INTO osm_former_buildings (osm_id, osm_type, lifecycle_key, lifecycle_value, geom)
             VALUES (150, 'way', 'demolished:building', 'yes', ST_MakePolygon(ST_MakeLine(
                 list_value(ST_Point(21.0, 51.0), ST_Point(21.001, 51.0),
                            ST_Point(21.001, 51.001), ST_Point(21.0, 51.001),
                            ST_Point(21.0, 51.0))
             )));",
        )?;

        // Move node 11 far east -- the way itself is not in the changeset, so
        // rebuild_way_geometry takes the INFERRED (None) arm for way 150.
        let changes = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Modify,
                version: 1,
                id: 11,
                lon: 22.5,
                lat: 51.0,
                tags: vec![],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &changes)?;

        let (lifecycle_key, count): (String, i64) = conn.query_row(
            "SELECT lifecycle_key, COUNT(*) OVER ()
             FROM osm_former_buildings WHERE osm_id = 150 AND osm_type = 'way'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(count, 1, "row must survive the node move");
        assert_eq!(
            lifecycle_key, "demolished:building",
            "lifecycle_key must not be rewritten to a default"
        );

        let after_xmax: f64 = conn.query_row(
            "SELECT ST_XMax(geom) FROM osm_former_buildings
             WHERE osm_id = 150 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert!(
            after_xmax > 22.0,
            "geometry must reflect the moved node, got xmax={after_xmax}"
        );

        Ok(())
    }

    /// Deleting a former-building way must remove its `osm_former_buildings`
    /// row and enqueue exactly the cell it left (fixture square sits well
    /// inside a single z14 cell) under both building sources, mirroring
    /// `test_apply_way_delete_enqueues_building_dirty_cells`.
    #[test]
    fn test_apply_way_delete_removes_former_building_row_and_enqueues_dirty_cells() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        kvstore::put_node(&kv, 21, dm(22.0), dm(52.0))?;
        kvstore::put_node(&kv, 22, dm(22.001), dm(52.0))?;
        kvstore::put_node(&kv, 23, dm(22.001), dm(52.001))?;
        kvstore::put_node(&kv, 24, dm(22.0), dm(52.001))?;
        kvstore::put_way(&kv, 160, &[21, 22, 23, 24, 21])?;
        for &nid in &[21i64, 22, 23, 24] {
            kvstore::add_node_to_ways(&kv, nid, 160)?;
        }
        conn.execute_batch(
            "INSERT INTO osm_former_buildings (osm_id, osm_type, lifecycle_key, lifecycle_value, geom)
             VALUES (160, 'way', 'demolished:building', 'yes', ST_MakePolygon(ST_MakeLine(
                 list_value(ST_Point(22.0, 52.0), ST_Point(22.001, 52.0),
                            ST_Point(22.001, 52.001), ST_Point(22.0, 52.001),
                            ST_Point(22.0, 52.0))
             )));",
        )?;

        let changes = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Delete,
                version: 1,
                id: 160,
                node_refs: vec![],
                tags: vec![],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &changes)?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_former_buildings WHERE osm_id = 160",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            count, 0,
            "deleted former-building way must not leave a stale row"
        );

        for source in ["bdot10k", "egib"] {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = ?",
                duckdb::params![source],
                |row| row.get(0),
            )?;
            assert_eq!(
                n, 1,
                "deleting a former-building way must enqueue the cell it left for {source}"
            );
        }

        assert!(kvstore::get_way(&kv, 160)?.is_none());

        Ok(())
    }

    // --- Replication lifecycle cases ---
    //
    // One `.osc` can carry an object's whole history (create -> modify ->
    // delete -> undelete), a geometry can change without its owner being in
    // the diff at all, and the extract's edge means referenced nodes may be
    // absent. The tests below pin each of those shapes; the case list was
    // derived from praszuk/osm-replication-osc-poly-filter's integration tests.

    fn count(conn: &Connection, sql: &str) -> Result<i64> {
        Ok(conn.query_row(sql, [], |r| r.get(0))?)
    }

    fn z14_cell(lon: f64, lat: f64) -> (i32, i32) {
        let (x, y) = crate::tile_math::lonlat_to_tile(lon, lat, crate::tile_math::CHANGE_CELL_ZOOM);
        (x as i32, y as i32)
    }

    fn z14_cell_midpoint(cx: i32, cy: i32) -> (f64, f64) {
        let (min_lon, min_lat, max_lon, max_lat) = crate::tile_math::tile_to_bbox(
            crate::tile_math::CHANGE_CELL_ZOOM,
            cx as u32,
            cy as u32,
        );
        ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0)
    }

    fn queued_in_cell(conn: &Connection, source: &str, cell: (i32, i32)) -> Result<i64> {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = ? AND cell_x = ? AND cell_y = ?",
            duckdb::params![source, cell.0, cell.1],
            |r| r.get(0),
        )?)
    }

    /// Relation 200 = way 100 (the fixture square) plus way 101, a second
    /// square around (21.0, 51.0) built from nodes 11-14, both `outer`, with
    /// a served `building=yes` row covering both.
    fn seed_two_way_relation(conn: &Connection, kv: &RocksDB) -> Result<()> {
        kvstore::put_node(kv, 11, dm(21.0), dm(51.0))?;
        kvstore::put_node(kv, 12, dm(21.001), dm(51.0))?;
        kvstore::put_node(kv, 13, dm(21.001), dm(51.001))?;
        kvstore::put_node(kv, 14, dm(21.0), dm(51.001))?;
        kvstore::put_way(kv, 101, &[11, 12, 13, 14, 11])?;
        for &nid in &[11i64, 12, 13, 14] {
            kvstore::add_node_to_ways(kv, nid, 101)?;
        }
        let outer = encoding::encode_member_role("outer");
        let way = encoding::encode_member_type("way");
        kvstore::put_relation(kv, 200, &[(100, way, outer), (101, way, outer)])?;
        kvstore::add_way_to_relations(kv, 100, 200)?;
        kvstore::add_way_to_relations(kv, 101, 200)?;
        conn.execute_batch(
            "INSERT INTO osm_buildings VALUES (200, 'relation', 'yes', ST_Union(
                 ST_MakeEnvelope(20.0, 50.0, 20.001, 50.001),
                 ST_MakeEnvelope(21.0, 51.0, 21.001, 51.001)));",
        )?;
        Ok(())
    }

    /// Two versions of one way in a single diff (two uploads inside the same
    /// minute). The node list is written per version, so the KV store ends at
    /// the last one -- the tags must too. `rebuild_way_geometry` picking the
    /// *first* matching `WayChange` would serve v2's tags: `building=yes` and
    /// no address, so the PRG address this way now carries stays unmatched.
    #[test]
    fn a_way_edited_twice_in_one_diff_is_served_with_its_last_version_s_tags() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <modify>
    <way id="100" version="2">
      <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
      <tag k="building" v="yes"/>
    </way>
    <way id="100" version="3">
      <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
      <tag k="building" v="house"/>
      <tag k="addr:housenumber" v="11"/>
      <tag k="addr:street" v="Lipowa"/>
    </way>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert_eq!(
            stored_building_and_address(&conn, 100, "way")?,
            (
                Some("house".into()),
                (Some("11".into()), Some("Lipowa".into()), None, None)
            ),
            "the way must be served with its LAST version's tags"
        );
        Ok(())
    }

    /// The veto-side consequence of the same bug: v3 retags the way as
    /// demolished. Serving v2's tags would keep a live building that covers
    /// the government building and never create the former-building row.
    #[test]
    fn a_way_retagged_demolished_in_the_second_version_of_one_diff_leaves_no_live_building()
    -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <modify>
    <way id="100" version="2">
      <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
      <tag k="building" v="yes"/>
      <tag k="note" v="about to be demolished"/>
    </way>
    <way id="100" version="3">
      <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
      <tag k="demolished:building" v="yes"/>
    </way>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100"
            )?,
            0,
            "the final version is not a live building"
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_former_buildings WHERE osm_id = 100 AND osm_type = 'way'"
            )?,
            1,
            "the final version is a former building"
        );
        Ok(())
    }

    /// The feed does not promise versions in ascending file order. v3 comes
    /// first in the file here, so going by file position would end at v2. Both
    /// the tags and the node list (and with it the reverse index) must come
    /// from v3.
    #[test]
    fn a_way_s_versions_out_of_file_order_are_applied_by_version_number() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="5" version="1" lon="19.9995" lat="50.0012"/>
  </create>
  <modify>
    <way id="100" version="3">
      <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="5"/><nd ref="1"/>
      <tag k="building" v="house"/>
      <tag k="addr:housenumber" v="11"/>
    </way>
    <way id="100" version="2">
      <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
      <tag k="building" v="yes"/>
    </way>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert_eq!(
            stored_building_and_address(&conn, 100, "way")?,
            (Some("house".into()), (Some("11".into()), None, None, None))
        );
        assert_eq!(kvstore::get_way(&kv, 100)?, Some(vec![1, 2, 3, 5, 1]));
        assert!(kvstore::get_node_to_ways(&kv, 5)?.contains(&100));
        assert!(!kvstore::get_node_to_ways(&kv, 4)?.contains(&100));
        Ok(())
    }

    /// The relation twin: v3 strips `building`, so nothing may be served.
    #[test]
    fn a_relation_edited_twice_in_one_diff_is_served_with_its_last_version_s_tags() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        kvstore::put_relation(
            &kv,
            200,
            &[(
                100,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 100, 200)?;
        conn.execute_batch(
            "INSERT INTO osm_buildings
                 SELECT 200, 'relation', 'yes', geom FROM osm_buildings WHERE osm_id = 100;",
        )?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <modify>
    <relation id="200" version="2">
      <member type="way" ref="100" role="outer"/>
      <tag k="type" v="multipolygon"/>
      <tag k="building" v="yes"/>
    </relation>
    <relation id="200" version="3">
      <member type="way" ref="100" role="outer"/>
      <tag k="type" v="multipolygon"/>
    </relation>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 200 AND osm_type = 'relation'"
            )?,
            0,
            "the relation's final version carries no building tag"
        );
        Ok(())
    }

    /// Create and delete inside one diff: nothing may survive, including the
    /// reverse index -- a stale `node_to_ways` entry would make every later
    /// edit of those node ids try to rebuild a way that no longer exists.
    #[test]
    fn a_way_created_and_deleted_in_one_diff_leaves_nothing_behind() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="30" version="1" lon="22.0" lat="52.0"/>
    <node id="31" version="1" lon="22.001" lat="52.0"/>
    <node id="32" version="1" lon="22.001" lat="52.001"/>
    <way id="400" version="1">
      <nd ref="30"/><nd ref="31"/><nd ref="32"/><nd ref="30"/>
      <tag k="building" v="yes"/>
      <tag k="addr:housenumber" v="1"/>
    </way>
  </create>
  <delete>
    <way id="400" version="2"/>
    <node id="30" version="2"/>
    <node id="31" version="2"/>
    <node id="32" version="2"/>
  </delete>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert!(kvstore::get_way(&kv, 400)?.is_none());
        for nid in [30i64, 31, 32] {
            assert!(
                kvstore::get_node(&kv, nid)?.is_none(),
                "node {nid} survived"
            );
            assert!(
                !kvstore::get_node_to_ways(&kv, nid)?.contains(&400),
                "node {nid} still maps to the deleted way"
            );
        }
        for table in ["osm_buildings", "osm_addresses", "osm_former_buildings"] {
            assert_eq!(
                count(
                    &conn,
                    &format!("SELECT COUNT(*) FROM {table} WHERE osm_id = 400")
                )?,
                0,
                "{table} kept a row for a way that no longer exists"
            );
        }
        Ok(())
    }

    /// Create -> delete -> modify (an undelete) in one diff: the node exists
    /// at its final version's position with its final version's tags.
    #[test]
    fn a_node_undeleted_in_the_same_diff_is_served_at_its_final_state() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="40" version="1" lon="21.0" lat="51.0">
      <tag k="addr:housenumber" v="1"/>
    </node>
  </create>
  <delete>
    <node id="40" version="2"/>
  </delete>
  <modify>
    <node id="40" version="3" lon="21.5" lat="51.5">
      <tag k="addr:housenumber" v="2"/>
    </node>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        let (lon, _) = kvstore::get_node(&kv, 40)?.expect("the undeleted node must exist");
        assert!((encoding::decimicro_to_f64(lon) - 21.5).abs() < 1e-9);
        let (n, hn, x): (i64, String, f64) = conn.query_row(
            "SELECT COUNT(*) OVER (), housenumber, ST_X(geom) FROM osm_addresses WHERE osm_id = 40",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!((n, hn.as_str()), (1, "2"));
        assert!(
            (x - 21.5).abs() < 1e-9,
            "address must sit at the final position"
        );
        Ok(())
    }

    /// Create -> modify -> delete in one diff: nothing survives.
    #[test]
    fn a_node_created_modified_and_deleted_in_one_diff_leaves_nothing_behind() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="41" version="1" lon="21.0" lat="51.0">
      <tag k="addr:housenumber" v="1"/>
    </node>
  </create>
  <modify>
    <node id="41" version="2" lon="21.1" lat="51.1">
      <tag k="addr:housenumber" v="1"/>
    </node>
  </modify>
  <delete>
    <node id="41" version="3"/>
  </delete>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert!(kvstore::get_node(&kv, 41)?.is_none());
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_addresses WHERE osm_id = 41"
            )?,
            0
        );
        Ok(())
    }

    /// An address node stripped of `addr:housenumber` stops being an address,
    /// and the cell it left must be enqueued so the PRG address it matched
    /// comes back as unmatched.
    #[test]
    fn a_node_losing_its_address_tags_removes_the_row_and_enqueues_its_cell() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let (lon, lat) = z14_cell_midpoint(z14_cell(21.0, 51.0).0, z14_cell(21.0, 51.0).1);
        let node = |action, tags| OsmChange {
            nodes: vec![NodeChange {
                action,
                version: 1,
                id: 42,
                lon,
                lat,
                tags,
            }],
            ..Default::default()
        };
        apply_changes(
            &conn,
            &kv,
            &node(
                ChangeAction::Create,
                vec![("addr:housenumber".into(), "5".into())],
            ),
        )?;
        conn.execute_batch("DELETE FROM match_dirty_cells")?;

        apply_changes(
            &conn,
            &kv,
            &node(
                ChangeAction::Modify,
                vec![("amenity".into(), "bench".into())],
            ),
        )?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_addresses WHERE osm_id = 42"
            )?,
            0
        );
        assert_eq!(queued_in_cell(&conn, "prg", z14_cell(lon, lat))?, 1);
        Ok(())
    }

    /// An address node moved across cells dirties both: the one it left (its
    /// old match is gone) and the one it entered.
    #[test]
    fn an_address_node_moved_to_another_cell_enqueues_both_cells() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let from = z14_cell(21.0, 51.0);
        let to = (from.0 + 5, from.1);
        let (lon_a, lat_a) = z14_cell_midpoint(from.0, from.1);
        let (lon_b, lat_b) = z14_cell_midpoint(to.0, to.1);
        let node = |action, lon, lat| OsmChange {
            nodes: vec![NodeChange {
                action,
                version: 1,
                id: 43,
                lon,
                lat,
                tags: vec![("addr:housenumber".into(), "5".into())],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &node(ChangeAction::Create, lon_a, lat_a))?;
        conn.execute_batch("DELETE FROM match_dirty_cells")?;

        apply_changes(&conn, &kv, &node(ChangeAction::Modify, lon_b, lat_b))?;

        assert_eq!(queued_in_cell(&conn, "prg", from)?, 1, "the cell it left");
        assert_eq!(queued_in_cell(&conn, "prg", to)?, 1, "the cell it entered");
        Ok(())
    }

    /// A way whose node list changes (node 4 swapped for a node 5 created in
    /// the same diff) must end with a reverse index matching the new list:
    /// node 5 maps to the way, node 4 no longer does -- so a later edit of
    /// node 4 must not rebuild, or dirty cells for, a way that dropped it.
    #[test]
    fn a_node_removed_from_a_way_no_longer_rebuilds_it() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="5" version="1" lon="19.9995" lat="50.0012"/>
  </create>
  <modify>
    <way id="100" version="2">
      <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="5"/><nd ref="1"/>
      <tag k="building" v="yes"/>
    </way>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert!(kvstore::get_node_to_ways(&kv, 5)?.contains(&100));
        assert!(!kvstore::get_node_to_ways(&kv, 4)?.contains(&100));
        let x_min: f64 = conn.query_row(
            "SELECT ST_XMin(geom) FROM osm_buildings WHERE osm_id = 100",
            [],
            |r| r.get(0),
        )?;
        assert!(
            x_min < 20.0,
            "geometry must use the new node, got xmin={x_min}"
        );

        conn.execute_batch("DELETE FROM match_dirty_cells")?;
        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: vec![NodeChange {
                    action: ChangeAction::Modify,
                    version: 1,
                    id: 4,
                    lon: 20.0,
                    lat: 50.0011,
                    tags: vec![],
                }],
                ..Default::default()
            },
        )?;
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source IN ('bdot10k', 'egib')"
            )?,
            0,
            "moving a node the way no longer uses must not touch the way"
        );
        Ok(())
    }

    /// A node shared by two building ways (a party wall) moves: both ways
    /// are rebuilt, neither is in the diff.
    #[test]
    fn a_node_shared_by_two_building_ways_rebuilds_both() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        // Way 101 shares way 100's east edge (nodes 2 and 3).
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="5" version="1" lon="20.002" lat="50.0"/>
    <node id="6" version="1" lon="20.002" lat="50.001"/>
    <way id="101" version="1">
      <nd ref="2"/><nd ref="5"/><nd ref="6"/><nd ref="3"/><nd ref="2"/>
      <tag k="building" v="garage"/>
    </way>
  </create>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: vec![NodeChange {
                    action: ChangeAction::Modify,
                    version: 1,
                    id: 2,
                    lon: 20.001,
                    lat: 49.9995,
                    tags: vec![],
                }],
                ..Default::default()
            },
        )?;

        for (way_id, building) in [(100, "yes"), (101, "garage")] {
            let (b, y_min): (String, f64) = conn.query_row(
                "SELECT building, ST_YMin(geom) FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
                [way_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            assert_eq!(b, building, "way {way_id} must keep its tags");
            assert!(
                y_min < 50.0,
                "way {way_id} must reflect the moved shared node, got ymin={y_min}"
            );
        }
        Ok(())
    }

    /// A member way's node LIST changes (not just a node position) and the
    /// relation is not in the diff: the relation is rebuilt from the new
    /// list, keeping its stored tags.
    #[test]
    fn a_member_way_s_new_node_list_rebuilds_the_relation_with_stored_tags() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        kvstore::put_relation(
            &kv,
            200,
            &[(
                100,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 100, 200)?;
        conn.execute_batch(
            "DELETE FROM osm_buildings WHERE osm_id = 100;
             INSERT INTO osm_buildings VALUES (200, 'relation', 'apartments',
                 ST_MakeEnvelope(20.0, 50.0, 20.001, 50.001));",
        )?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="5" version="1" lon="19.999" lat="50.001"/>
  </create>
  <modify>
    <way id="100" version="2">
      <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="5"/><nd ref="1"/>
    </way>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        let (building, x_min): (String, f64) = conn.query_row(
            "SELECT building, ST_XMin(geom) FROM osm_buildings WHERE osm_id = 200 AND osm_type = 'relation'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(building, "apartments");
        assert!(x_min < 20.0, "relation must use the member's new node list");
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100"
            )?,
            0,
            "the untagged member way is not a building itself"
        );
        Ok(())
    }

    /// A relation that drops a member way: its geometry shrinks to the
    /// remaining member, and the dropped way no longer maps to it -- so a
    /// later edit of the dropped way must not rebuild (and dirty the far-away
    /// cell of) the relation.
    #[test]
    fn a_way_dropped_from_a_relation_no_longer_rebuilds_it() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        seed_two_way_relation(&conn, &kv)?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <modify>
    <relation id="200" version="2">
      <member type="way" ref="101" role="outer"/>
      <tag k="type" v="multipolygon"/>
      <tag k="building" v="yes"/>
    </relation>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert!(!kvstore::get_way_to_relations(&kv, 100)?.contains(&200));
        let x_min: f64 = conn.query_row(
            "SELECT ST_XMin(geom) FROM osm_buildings WHERE osm_id = 200 AND osm_type = 'relation'",
            [],
            |r| r.get(0),
        )?;
        assert!(x_min >= 21.0, "the dropped member must leave the geometry");

        conn.execute_batch("DELETE FROM match_dirty_cells")?;
        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: vec![NodeChange {
                    action: ChangeAction::Modify,
                    version: 1,
                    id: 1,
                    lon: 19.9999,
                    lat: 50.0,
                    tags: vec![],
                }],
                ..Default::default()
            },
        )?;
        assert_eq!(
            queued_in_cell(&conn, "bdot10k", z14_cell(21.0005, 51.0005))?,
            0,
            "editing the dropped way must not rebuild the relation"
        );
        Ok(())
    }

    /// Deleting a member way together with the relation edit that drops it
    /// (the API refuses to delete a way still in a relation, so they arrive
    /// together): the relation is rebuilt from the remaining member.
    #[test]
    fn a_member_way_deleted_with_its_relation_edit_leaves_the_rest_served() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        seed_two_way_relation(&conn, &kv)?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <modify>
    <relation id="200" version="2">
      <member type="way" ref="100" role="outer"/>
      <tag k="type" v="multipolygon"/>
      <tag k="building" v="yes"/>
    </relation>
  </modify>
  <delete>
    <way id="101" version="2"/>
  </delete>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        let x_max: f64 = conn.query_row(
            "SELECT ST_XMax(geom) FROM osm_buildings WHERE osm_id = 200 AND osm_type = 'relation'",
            [],
            |r| r.get(0),
        )?;
        assert!(
            x_max < 20.5,
            "the deleted member must leave the geometry, got xmax={x_max}"
        );
        assert!(kvstore::get_way(&kv, 101)?.is_none());
        assert!(kvstore::get_way_to_relations(&kv, 101)?.is_empty());
        Ok(())
    }

    /// Nodes, an untagged closed way and the multipolygon relation using it,
    /// all created in one diff: relations are applied after ways, so the
    /// relation must see its member.
    #[test]
    fn a_relation_built_from_ways_created_in_the_same_diff_is_served() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="50" version="1" lon="22.0" lat="52.0"/>
    <node id="51" version="1" lon="22.001" lat="52.0"/>
    <node id="52" version="1" lon="22.001" lat="52.001"/>
    <node id="53" version="1" lon="22.0" lat="52.001"/>
    <way id="500" version="1">
      <nd ref="50"/><nd ref="51"/><nd ref="52"/><nd ref="53"/><nd ref="50"/>
    </way>
    <relation id="600" version="1">
      <member type="way" ref="500" role="outer"/>
      <tag k="type" v="multipolygon"/>
      <tag k="building" v="school"/>
    </relation>
  </create>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        let (building, area): (String, f64) = conn.query_row(
            "SELECT building, ST_Area(geom) FROM osm_buildings WHERE osm_id = 600 AND osm_type = 'relation'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(building, "school");
        assert!(area > 0.0);
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 500"
            )?,
            0,
            "the untagged member way is not a building itself"
        );
        Ok(())
    }

    /// The extract's edge: a way referencing a node that is not in the store
    /// (never in the PBF, never in a diff) cannot be built. That must be a
    /// silent skip, not an error that fails the whole batch.
    #[test]
    fn a_way_referencing_a_node_outside_the_store_is_skipped_without_error() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                ways: vec![WayChange {
                    action: ChangeAction::Create,
                    version: 1,
                    id: 700,
                    node_refs: vec![1, 2, 3, 999, 1],
                    tags: vec![("building".into(), "yes".into())],
                }],
                ..Default::default()
            },
        )?;
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 700"
            )?,
            0
        );
        Ok(())
    }

    /// The accepted cost of ignoring border objects: once the missing node
    /// arrives in a later diff, the way does NOT come back. The way is not in
    /// that diff, and its tags were never stored because it had no row, so
    /// there is nothing to rebuild it from. It returns on its next direct
    /// edit. See `unresolved_way_members`.
    #[test]
    fn a_way_whose_missing_node_arrives_in_a_later_diff_stays_ignored() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                ways: vec![WayChange {
                    action: ChangeAction::Create,
                    version: 1,
                    id: 700,
                    node_refs: vec![1, 2, 3, 999, 1],
                    tags: vec![("building".into(), "yes".into())],
                }],
                ..Default::default()
            },
        )?;
        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: vec![NodeChange {
                    action: ChangeAction::Modify,
                    version: 1,
                    id: 999,
                    lon: 20.0,
                    lat: 50.0015,
                    tags: vec![],
                }],
                ..Default::default()
            },
        )?;
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 700"
            )?,
            0
        );
        Ok(())
    }

    /// A served way edited to reference a node outside the store is removed,
    /// not left at its old geometry, and the cell it left is enqueued, so the
    /// government building it matched is re-evaluated.
    #[test]
    fn a_served_way_that_becomes_unresolvable_is_removed_and_enqueued() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                ways: vec![WayChange {
                    action: ChangeAction::Modify,
                    version: 1,
                    id: 100,
                    node_refs: vec![1, 2, 3, 999, 1],
                    tags: vec![("building".into(), "yes".into())],
                }],
                ..Default::default()
            },
        )?;
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 100"
            )?,
            0
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'bdot10k'"
            )?,
            1
        );
        Ok(())
    }

    /// A multipolygon with one unresolvable member is ignored entirely,
    /// never built from the members that are present: the result would be a
    /// wrong footprint.
    #[test]
    fn a_relation_with_an_unresolvable_member_is_ignored_not_built_partially() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        seed_two_way_relation(&conn, &kv)?;
        // Way 101's node 13 is beyond the extract's edge.
        kvstore::delete_node(&kv, 13)?;

        let osc = r#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <modify>
    <relation id="200" version="2">
      <member type="way" ref="100" role="outer"/>
      <member type="way" ref="101" role="outer"/>
      <tag k="type" v="multipolygon"/>
      <tag k="building" v="yes"/>
    </relation>
  </modify>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 200 AND osm_type = 'relation'"
            )?,
            0
        );
        Ok(())
    }

    /// What the warning names: every unresolvable way, each missing node once
    /// in ref order, and a way absent from the store entirely. Resolvable
    /// ways are left out.
    #[test]
    fn unresolved_way_members_names_each_way_and_its_missing_nodes() -> Result<()> {
        let (_conn, kv, _dir) = setup_test_db_and_kv()?;
        kvstore::put_way(&kv, 700, &[1, 999, 2, 998, 999, 1])?;

        let unresolved = unresolved_way_members(&kv, &[100, 700, 12345])?;
        assert_eq!(
            unresolved,
            vec![
                UnresolvedWay {
                    way_id: 700,
                    missing_nodes: Some(vec![999, 998]),
                },
                UnresolvedWay {
                    way_id: 12345,
                    missing_nodes: None,
                },
            ]
        );
        assert_eq!(
            describe_unresolved(&unresolved),
            "way/700 is missing node/999, node/998; way/12345 is not in the store (outside the extract?)"
        );
        Ok(())
    }

    /// Several sequences in one batch are applied in order, so a later
    /// sequence's edit of an object wins over an earlier one's, and the
    /// stamp is the last sequence.
    #[test]
    fn a_batch_applies_its_sequences_in_order() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let way = |version, tags: Vec<(&str, &str)>| OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Modify,
                version,
                id: 100,
                node_refs: vec![1, 2, 3, 4, 1],
                tags: tags
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            }],
            ..Default::default()
        };
        apply_batch(
            &conn,
            &kv,
            &[
                FetchedSequence {
                    seq: 1001,
                    changes: way(2, vec![("building", "yes")]),
                },
                FetchedSequence {
                    seq: 1002,
                    changes: way(3, vec![("building", "house"), ("addr:housenumber", "3")]),
                },
            ],
            "2026-09-18T00:00:00Z",
        )?;

        assert_eq!(
            stored_building_and_address(&conn, 100, "way")?,
            (Some("house".into()), (Some("3".into()), None, None, None))
        );
        assert_eq!(get_current_sequence(&conn)?, 1002);
        Ok(())
    }

    /// The batch is collapsed as a whole, not sequence by sequence. An address
    /// served in cell A moves to B in one sequence and on to C in the next.
    /// B was never committed, so nothing was ever served there, and only A
    /// (what it left) and C (where it ended) need recomputing. A per-sequence
    /// apply would also enqueue B. That is harmless extra work, but seeing B
    /// here means the batch collapse is no longer in effect.
    #[test]
    fn a_batch_collapses_positions_that_were_never_committed() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let a = z14_cell(21.0, 51.0);
        let b = (a.0 + 5, a.1);
        let c = (a.0 + 10, a.1);
        let node_at = |version, cell: (i32, i32)| {
            let (lon, lat) = z14_cell_midpoint(cell.0, cell.1);
            OsmChange {
                nodes: vec![NodeChange {
                    action: ChangeAction::Modify,
                    version,
                    id: 44,
                    lon,
                    lat,
                    tags: vec![("addr:housenumber".into(), "5".into())],
                }],
                ..Default::default()
            }
        };
        apply_changes(&conn, &kv, &node_at(1, a))?;
        conn.execute_batch("DELETE FROM match_dirty_cells")?;

        apply_batch(
            &conn,
            &kv,
            &[
                FetchedSequence {
                    seq: 1001,
                    changes: node_at(2, b),
                },
                FetchedSequence {
                    seq: 1002,
                    changes: node_at(3, c),
                },
            ],
            "2026-09-18T00:00:00Z",
        )?;

        assert_eq!(queued_in_cell(&conn, "prg", a)?, 1, "the cell it left");
        assert_eq!(queued_in_cell(&conn, "prg", c)?, 1, "the cell it ended in");
        assert_eq!(
            queued_in_cell(&conn, "prg", b)?,
            0,
            "an intermediate position inside one batch was never served"
        );
        let x: f64 = conn.query_row(
            "SELECT ST_X(geom) FROM osm_addresses WHERE osm_id = 44",
            [],
            |r| r.get(0),
        )?;
        assert!((x - z14_cell_midpoint(c.0, c.1).0).abs() < 1e-9);
        Ok(())
    }

    // --- Type x step coverage gaps ---

    /// Relation 200 = way 100 as `outer`, served as a building with an
    /// address, reverse index in place.
    fn seed_served_relation(conn: &Connection, kv: &RocksDB) -> Result<()> {
        kvstore::put_relation(
            kv,
            200,
            &[(
                100,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(kv, 100, 200)?;
        conn.execute_batch(
            "INSERT INTO osm_buildings
                 SELECT 200, 'relation', 'yes', geom FROM osm_buildings WHERE osm_id = 100;
             INSERT INTO osm_addresses VALUES
                 (200, 'relation', '7', 'Lipowa', NULL, NULL, ST_Point(20.0005, 50.0005));",
        )?;
        Ok(())
    }

    /// Deleting an address node must enqueue the cell it left, so the PRG
    /// address it matched comes back. `test_apply_node_delete` only checks
    /// that the row is gone.
    #[test]
    fn a_deleted_address_node_enqueues_the_cell_it_left() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let cell = z14_cell(21.0, 51.0);
        let (lon, lat) = z14_cell_midpoint(cell.0, cell.1);
        let node = |action, version| OsmChange {
            nodes: vec![NodeChange {
                action,
                version,
                id: 45,
                lon,
                lat,
                tags: vec![("addr:housenumber".into(), "5".into())],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &node(ChangeAction::Create, 1))?;
        conn.execute_batch("DELETE FROM match_dirty_cells")?;

        apply_changes(&conn, &kv, &node(ChangeAction::Delete, 2))?;

        assert_eq!(queued_in_cell(&conn, "prg", cell)?, 1);
        Ok(())
    }

    /// Deleting an addressed building way removes the address row too, and
    /// enqueues the address source as well as the building ones.
    #[test]
    fn a_deleted_addressed_way_removes_its_address_and_enqueues_prg() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES
                 (100, 'way', '11', 'Lipowa', NULL, NULL, ST_Point(20.0005, 50.0005));",
        )?;
        let osc =
            r#"<osmChange version="0.6"><delete><way id="100" version="2"/></delete></osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_addresses WHERE osm_id = 100"
            )?,
            0
        );
        assert!(
            count(
                &conn,
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'prg'"
            )? > 0
        );
        Ok(())
    }

    /// The reverse of the demolished retag: `demolished:building` back to a
    /// live `building`. The former row must go, or it keeps suppressing the
    /// government building while OSM also counts as covering it.
    #[test]
    fn a_way_retagged_from_demolished_back_to_building_swaps_rows() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let way = |version, tag: (&str, &str)| OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Modify,
                version,
                id: 100,
                node_refs: vec![1, 2, 3, 4, 1],
                tags: vec![(tag.0.into(), tag.1.into())],
            }],
            ..Default::default()
        };
        apply_changes(&conn, &kv, &way(2, ("demolished:building", "yes")))?;
        conn.execute_batch("DELETE FROM match_dirty_cells")?;

        apply_changes(&conn, &kv, &way(3, ("building", "house")))?;

        assert_eq!(
            stored_building_and_address(&conn, 100, "way")?.0,
            Some("house".into())
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_former_buildings WHERE osm_id = 100"
            )?,
            0
        );
        assert!(
            count(
                &conn,
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'bdot10k'"
            )? > 0
        );
        Ok(())
    }

    /// Relation delete had no test at all: rows in every table, the stored
    /// members, the way -> relation reverse index, and the cells it left.
    #[test]
    fn a_relation_delete_removes_its_rows_reverse_index_and_enqueues() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        seed_served_relation(&conn, &kv)?;
        let osc = r#"<osmChange version="0.6"><delete><relation id="200" version="2"/></delete></osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert!(kvstore::get_relation(&kv, 200)?.is_none());
        assert!(!kvstore::get_way_to_relations(&kv, 100)?.contains(&200));
        for table in ["osm_buildings", "osm_addresses", "osm_former_buildings"] {
            assert_eq!(
                count(
                    &conn,
                    &format!(
                        "SELECT COUNT(*) FROM {table} WHERE osm_id = 200 AND osm_type = 'relation'"
                    )
                )?,
                0,
                "{table} kept a row for the deleted relation"
            );
        }
        for source in ["bdot10k", "egib", "prg"] {
            assert!(
                count(
                    &conn,
                    &format!("SELECT COUNT(*) FROM match_dirty_cells WHERE source = '{source}'")
                )? > 0,
                "the relation's cell must be enqueued for {source}"
            );
        }
        // The member way itself is untouched.
        assert_eq!(
            stored_building_and_address(&conn, 100, "way")?.0,
            Some("yes".into())
        );
        Ok(())
    }

    /// Relation create and delete in one diff: nothing survives, including the
    /// reverse index entry the create added.
    #[test]
    fn a_relation_created_and_deleted_in_one_diff_leaves_nothing_behind() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<osmChange version="0.6">
  <create>
    <relation id="210" version="1">
      <member type="way" ref="100" role="outer"/>
      <tag k="type" v="multipolygon"/>
      <tag k="building" v="yes"/>
    </relation>
  </create>
  <delete>
    <relation id="210" version="2"/>
  </delete>
</osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert!(kvstore::get_relation(&kv, 210)?.is_none());
        assert!(!kvstore::get_way_to_relations(&kv, 100)?.contains(&210));
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 210"
            )?,
            0
        );
        Ok(())
    }

    /// The relation arm of the live -> demolished retag.
    #[test]
    fn a_relation_retagged_demolished_swaps_rows() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        seed_served_relation(&conn, &kv)?;
        let osc = r#"<osmChange version="0.6"><modify>
    <relation id="200" version="2">
      <member type="way" ref="100" role="outer"/>
      <tag k="type" v="multipolygon"/>
      <tag k="demolished:building" v="yes"/>
    </relation>
</modify></osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 200 AND osm_type = 'relation'"
            )?,
            0
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_former_buildings WHERE osm_id = 200 AND osm_type = 'relation'"
            )?,
            1
        );
        Ok(())
    }

    /// Every other relation test is a single outer ring. An `inner` member
    /// must be cut out of the footprint, or a courtyard building covers (and
    /// matches) the government objects standing in its courtyard.
    #[test]
    fn a_relation_with_an_inner_ring_is_built_with_a_hole() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        // Eighths-of-a-thousandth coordinates, exact enough that the areas
        // compare cleanly.
        let osc = r#"<osmChange version="0.6"><create>
    <node id="60" version="1" lon="20.00025" lat="50.00025"/>
    <node id="61" version="1" lon="20.00075" lat="50.00025"/>
    <node id="62" version="1" lon="20.00075" lat="50.00075"/>
    <node id="63" version="1" lon="20.00025" lat="50.00075"/>
    <way id="102" version="1">
      <nd ref="60"/><nd ref="61"/><nd ref="62"/><nd ref="63"/><nd ref="60"/>
    </way>
    <relation id="220" version="1">
      <member type="way" ref="100" role="outer"/>
      <member type="way" ref="102" role="inner"/>
      <tag k="type" v="multipolygon"/>
      <tag k="building" v="yes"/>
    </relation>
</create></osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        let (area, courtyard_covered): (f64, bool) = conn.query_row(
            "SELECT ST_Area(geom), ST_Contains(geom, ST_Point(20.0005, 50.0005))
             FROM osm_buildings WHERE osm_id = 220 AND osm_type = 'relation'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert!(!courtyard_covered, "the courtyard must be a hole");
        assert!(
            (area - (1e-6 - 2.5e-7)).abs() < 1e-11,
            "area must be outer minus inner, got {area}"
        );
        Ok(())
    }

    /// The relation arm of the inferred former-building rebuild (the way arm
    /// is `test_apply_node_move_on_former_building_way_keeps_row_with_moved_geometry`).
    #[test]
    fn a_node_move_under_a_former_building_relation_keeps_its_row() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        kvstore::put_relation(
            &kv,
            230,
            &[(
                100,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 100, 230)?;
        conn.execute_batch(
            "INSERT INTO osm_former_buildings (osm_id, osm_type, lifecycle_key, lifecycle_value, geom)
             SELECT 230, 'relation', 'ruins:building', 'yes', geom FROM osm_buildings WHERE osm_id = 100;",
        )?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: vec![NodeChange {
                    action: ChangeAction::Modify,
                    version: 2,
                    id: 1,
                    lon: 19.999,
                    lat: 50.0,
                    tags: vec![],
                }],
                ..Default::default()
            },
        )?;

        let (key, x_min): (String, f64) = conn.query_row(
            "SELECT lifecycle_key, ST_XMin(geom) FROM osm_former_buildings
             WHERE osm_id = 230 AND osm_type = 'relation'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(key, "ruins:building");
        assert!(x_min < 20.0, "geometry must reflect the moved node");
        Ok(())
    }

    /// A building-tagged relation with no way members (only a node and a
    /// sub-relation; nested relations are not resolved) has no footprint to
    /// build. That is a skip, not an error failing the batch.
    #[test]
    fn a_building_relation_without_way_members_is_skipped_without_error() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let osc = r#"<osmChange version="0.6"><create>
    <relation id="240" version="1">
      <member type="node" ref="1" role=""/>
      <member type="relation" ref="200" role="outer"/>
      <tag k="type" v="multipolygon"/>
      <tag k="building" v="yes"/>
    </relation>
</create></osmChange>"#;
        apply_changes(&conn, &kv, &parse_osc(osc)?)?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 240"
            )?,
            0
        );
        Ok(())
    }

    // --- Post-insert enqueue: where an object ARRIVES ---------------------
    //
    // Both rebuild functions call `note_existing` twice per layer: once
    // before the DELETE (the cell the object is leaving) and once after each
    // INSERT (the cell it now occupies). Every test above that asserts an
    // enqueue happens to use a fixture where the object already had a row in
    // the same cell, so the pre-delete call alone produces the expected
    // number and the post-insert call is invisible -- deleting all six
    // post-insert calls left the whole suite green when this section was
    // written (mutation-checked, 2026-09-20).
    //
    // Only two shapes can see them, and both are real production cases:
    // a brand-new object (no prior row for the pre-delete call to find), and
    // an object whose geometry moved into a different cell (pre-delete
    // records the old cell, post-insert the new one). The create case is the
    // sharp one: a newly mapped OSM building that fails to dirty its cell
    // leaves the government building it now covers sitting in
    // `<source>_unmatched`, so the importer offers it again and a duplicate
    // lands in OSM. `queue reconcile`'s daily sweep bounds that to a day.
    //
    // The same fixtures pin the *pre-delete* calls for the address and
    // former-building layers, which were equally unpinned: only the
    // `osm_buildings` pre-delete call had a de-tag test covering it.

    /// Half-width, in degrees, of the square ways these tests build (~22 m).
    /// Small enough that the square plus `Layer::Addresses`' 0.003 deg read
    /// buffer still fits inside one z14 cell when centred on that cell's
    /// midpoint -- the property `dirty_cells::tests::
    /// note_point_address_at_cell_centre_stays_in_one_cell` pins directly.
    const SQUARE_HALF_DEG: f64 = 0.0002;

    /// A closed square way in the KV store, centred on (lon, lat), with its
    /// node -> way reverse index in place so a node move cascades to it.
    fn seed_square_way(
        kv: &RocksDB,
        way_id: i64,
        first_node: i64,
        lon: f64,
        lat: f64,
    ) -> Result<()> {
        let refs = seed_ring(kv, first_node, &square_corners(lon, lat))?;
        kvstore::put_way(kv, way_id, &refs)?;
        for &nid in &refs[..4] {
            kvstore::add_node_to_ways(kv, nid, way_id)?;
        }
        Ok(())
    }

    fn square_corners(lon: f64, lat: f64) -> [(f64, f64); 4] {
        let h = SQUARE_HALF_DEG;
        [
            (lon - h, lat - h),
            (lon + h, lat - h),
            (lon + h, lat + h),
            (lon - h, lat + h),
        ]
    }

    /// `NodeChange`s moving [`seed_square_way`]'s four nodes to a square
    /// around a new centre. The way itself stays out of the changeset, so
    /// the rebuild takes the INFERRED arm and reads its tags back out of the
    /// rows it already has -- the production-common shape.
    fn move_square_nodes(first_node: i64, lon: f64, lat: f64) -> Vec<NodeChange> {
        square_corners(lon, lat)
            .iter()
            .enumerate()
            .map(|(i, (x, y))| NodeChange {
                action: ChangeAction::Modify,
                version: 2,
                id: first_node + i as i64,
                lon: *x,
                lat: *y,
                tags: vec![],
            })
            .collect()
    }

    fn create_square_nodes(first_node: i64, lon: f64, lat: f64) -> Vec<NodeChange> {
        square_corners(lon, lat)
            .iter()
            .enumerate()
            .map(|(i, (x, y))| NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: first_node + i as i64,
                lon: *x,
                lat: *y,
                tags: vec![],
            })
            .collect()
    }

    fn square_refs(first_node: i64) -> Vec<i64> {
        vec![
            first_node,
            first_node + 1,
            first_node + 2,
            first_node + 3,
            first_node,
        ]
    }

    fn square_envelope_sql(lon: f64, lat: f64) -> String {
        let h = SQUARE_HALF_DEG;
        format!(
            "ST_MakeEnvelope({}, {}, {}, {})",
            lon - h,
            lat - h,
            lon + h,
            lat + h
        )
    }

    /// The distinct cells enqueued for `source`, in a stable order.
    /// `queued_in_cell` above answers "is this one cell queued"; these tests
    /// need the whole set, because the failure they guard against is a
    /// *missing* cell alongside a present one.
    fn queued_cells(conn: &Connection, source: &str) -> Result<Vec<(i32, i32)>> {
        let mut stmt = conn.prepare(
            "SELECT DISTINCT cell_x, cell_y FROM match_dirty_cells WHERE source = ?
             ORDER BY cell_x, cell_y",
        )?;
        let rows = stmt.query_map(duckdb::params![source], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// A z14 cell and the lon/lat a test square should be centred on to sit
    /// well inside it.
    struct Cell {
        cell: (i32, i32),
        lon: f64,
        lat: f64,
    }

    /// Two horizontally adjacent z14 cells. Adjacent in x only, so
    /// `queued_cells`' `ORDER BY cell_x, cell_y` puts `home` before `away`
    /// and the expected vectors below can be written literally.
    fn home_and_away() -> (Cell, Cell) {
        let at = |cell: (i32, i32)| {
            let (lon, lat) = z14_cell_midpoint(cell.0, cell.1);
            Cell { cell, lon, lat }
        };
        let home = z14_cell(21.0, 51.0);
        (at(home), at((home.0 + 1, home.1)))
    }

    /// A building way that did not exist before: nothing for the pre-delete
    /// `note_existing` to find, so the cell can only reach the queue through
    /// the call after the INSERT. Without it the government building this way
    /// now covers is never recomputed and keeps being offered for import.
    #[test]
    fn a_newly_created_building_way_enqueues_the_cell_it_arrived_in() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let (_home, away) = home_and_away();

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: create_square_nodes(900, away.lon, away.lat),
                ways: vec![WayChange {
                    action: ChangeAction::Create,
                    version: 1,
                    id: 300,
                    node_refs: square_refs(900),
                    tags: vec![("building".into(), "house".into())],
                }],
                ..Default::default()
            },
        )?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 300 AND osm_type = 'way'"
            )?,
            1,
            "precondition: the way must actually have been served"
        );
        for source in ["bdot10k", "egib"] {
            assert_eq!(
                queued_cells(&conn, source)?,
                vec![away.cell],
                "{source} must be told about the cell the new building landed in"
            );
        }
        assert_eq!(
            queued_cells(&conn, "prg")?,
            vec![],
            "a building-only create must not enqueue the address source"
        );
        Ok(())
    }

    /// A served, addressed building way whose nodes all move into the
    /// neighbouring cell. Both halves have to fire: the pre-delete call for
    /// the cell it left (whose government building becomes uncovered) and the
    /// post-insert call for the cell it entered (whose government building
    /// becomes covered). Covers the `osm_buildings` and `osm_addresses`
    /// layers of `rebuild_way_geometry` at once, since one way carries both.
    #[test]
    fn a_building_way_moved_to_another_cell_enqueues_both_cells() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let (home, away) = home_and_away();

        seed_square_way(&kv, 300, 900, home.lon, home.lat)?;
        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings VALUES (300, 'way', 'house', {env});
             INSERT INTO osm_addresses VALUES
                 (300, 'way', '7', 'Lipowa', NULL, NULL, ST_Point({hlon}, {hlat}));",
            hlon = home.lon,
            hlat = home.lat,
            env = square_envelope_sql(home.lon, home.lat),
        ))?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: move_square_nodes(900, away.lon, away.lat),
                ..Default::default()
            },
        )?;

        let moved: f64 = conn.query_row(
            "SELECT ST_XMin(geom) FROM osm_buildings WHERE osm_id = 300 AND osm_type = 'way'",
            [],
            |r| r.get(0),
        )?;
        assert!(
            moved > home.lon,
            "precondition: the rebuild must have moved the building east"
        );

        for source in ["bdot10k", "egib", "prg"] {
            assert_eq!(
                queued_cells(&conn, source)?,
                vec![home.cell, away.cell],
                "{source} must be told about the cell the building left AND the one it entered"
            );
        }
        Ok(())
    }

    /// The same move for a former-building way, which travels its own pair of
    /// `note_existing` calls against `osm_former_buildings`. A stale veto is
    /// the mirror failure of a stale building: the cell it left keeps
    /// suppressing a government building nothing covers any more.
    #[test]
    fn a_former_building_way_moved_to_another_cell_enqueues_both_cells() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let (home, away) = home_and_away();

        seed_square_way(&kv, 300, 900, home.lon, home.lat)?;
        conn.execute_batch(&format!(
            "INSERT INTO osm_former_buildings VALUES
                 (300, 'way', 'demolished:building', 'house', {env});",
            env = square_envelope_sql(home.lon, home.lat),
        ))?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: move_square_nodes(900, away.lon, away.lat),
                ..Default::default()
            },
        )?;

        let (key, x_min): (String, f64) = conn.query_row(
            "SELECT lifecycle_key, ST_XMin(geom) FROM osm_former_buildings
             WHERE osm_id = 300 AND osm_type = 'way'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(key, "demolished:building", "the row must survive the move");
        assert!(
            x_min > home.lon,
            "precondition: the veto must have moved east"
        );

        for source in ["bdot10k", "egib"] {
            assert_eq!(
                queued_cells(&conn, source)?,
                vec![home.cell, away.cell],
                "{source} must be told about both the vacated and the entered cell"
            );
        }
        assert_eq!(
            queued_cells(&conn, "prg")?,
            vec![],
            "a former-building move must not enqueue the address source"
        );
        Ok(())
    }

    /// `rebuild_relation_geometry`'s post-insert call, via the same "nothing
    /// existed before" shape as the way test above. The member way carries no
    /// tags of its own, so every queue row here comes from the relation.
    #[test]
    fn a_newly_created_building_relation_enqueues_the_cell_it_arrived_in() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let (_home, away) = home_and_away();

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: create_square_nodes(900, away.lon, away.lat),
                ways: vec![WayChange {
                    action: ChangeAction::Create,
                    version: 1,
                    id: 301,
                    node_refs: square_refs(900),
                    tags: vec![],
                }],
                relations: vec![RelationChange {
                    action: ChangeAction::Create,
                    version: 1,
                    id: 400,
                    members: vec![RelationMember {
                        member_type: "way".into(),
                        member_ref: 301,
                        role: "outer".into(),
                    }],
                    tags: vec![
                        ("type".into(), "multipolygon".into()),
                        ("building".into(), "house".into()),
                    ],
                }],
            },
        )?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 400 AND osm_type = 'relation'"
            )?,
            1,
            "precondition: the relation must actually have been served"
        );
        for source in ["bdot10k", "egib"] {
            assert_eq!(
                queued_cells(&conn, source)?,
                vec![away.cell],
                "{source} must be told about the cell the new relation landed in"
            );
        }
        Ok(())
    }

    /// The relation twin of `a_building_way_moved_to_another_cell_enqueues_
    /// both_cells`: the member way moves, the relation is reached through the
    /// way -> relation cascade, and both its building and address rows have to
    /// name the cell they left as well as the one they entered.
    #[test]
    fn a_building_relation_moved_to_another_cell_enqueues_both_cells() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let (home, away) = home_and_away();

        seed_square_way(&kv, 301, 900, home.lon, home.lat)?;
        kvstore::put_relation(
            &kv,
            400,
            &[(
                301,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 301, 400)?;
        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings VALUES (400, 'relation', 'house', {env});
             INSERT INTO osm_addresses VALUES
                 (400, 'relation', '7', 'Lipowa', NULL, NULL, ST_Point({hlon}, {hlat}));",
            hlon = home.lon,
            hlat = home.lat,
            env = square_envelope_sql(home.lon, home.lat),
        ))?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: move_square_nodes(900, away.lon, away.lat),
                ..Default::default()
            },
        )?;

        let moved: f64 = conn.query_row(
            "SELECT ST_XMin(geom) FROM osm_buildings WHERE osm_id = 400 AND osm_type = 'relation'",
            [],
            |r| r.get(0),
        )?;
        assert!(
            moved > home.lon,
            "precondition: the relation rebuild must have moved the building east"
        );

        for source in ["bdot10k", "egib", "prg"] {
            assert_eq!(
                queued_cells(&conn, source)?,
                vec![home.cell, away.cell],
                "{source} must be told about the cell the relation left AND the one it entered"
            );
        }
        Ok(())
    }

    /// The relation twin of the former-building way move.
    #[test]
    fn a_former_building_relation_moved_to_another_cell_enqueues_both_cells() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let (home, away) = home_and_away();

        seed_square_way(&kv, 301, 900, home.lon, home.lat)?;
        kvstore::put_relation(
            &kv,
            400,
            &[(
                301,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 301, 400)?;
        conn.execute_batch(&format!(
            "INSERT INTO osm_former_buildings VALUES
                 (400, 'relation', 'ruins:building', 'yes', {env});",
            env = square_envelope_sql(home.lon, home.lat),
        ))?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                nodes: move_square_nodes(900, away.lon, away.lat),
                ..Default::default()
            },
        )?;

        let x_min: f64 = conn.query_row(
            "SELECT ST_XMin(geom) FROM osm_former_buildings
             WHERE osm_id = 400 AND osm_type = 'relation'",
            [],
            |r| r.get(0),
        )?;
        assert!(
            x_min > home.lon,
            "precondition: the veto must have moved east"
        );

        for source in ["bdot10k", "egib"] {
            assert_eq!(
                queued_cells(&conn, source)?,
                vec![home.cell, away.cell],
                "{source} must be told about both the vacated and the entered cell"
            );
        }
        Ok(())
    }

    /// The relation delete arm notes three tables before deleting from them;
    /// `a_relation_delete_removes_its_rows_reverse_index_and_enqueues` covers
    /// the first two, but a relation carrying ONLY a former-building row
    /// leaves both of those empty, so the cell can only reach the queue
    /// through the `osm_former_buildings` call. Dropping a demolished-building
    /// relation lifts a veto: the government building it was suppressing has
    /// to be reconsidered.
    #[test]
    fn a_relation_delete_removes_its_former_building_row_and_enqueues() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;
        let (home, _away) = home_and_away();

        seed_square_way(&kv, 301, 900, home.lon, home.lat)?;
        kvstore::put_relation(
            &kv,
            400,
            &[(
                301,
                encoding::encode_member_type("way"),
                encoding::encode_member_role("outer"),
            )],
        )?;
        kvstore::add_way_to_relations(&kv, 301, 400)?;
        conn.execute_batch(&format!(
            "INSERT INTO osm_former_buildings VALUES
                 (400, 'relation', 'demolished:building', 'house', {env});",
            env = square_envelope_sql(home.lon, home.lat),
        ))?;

        apply_changes(
            &conn,
            &kv,
            &OsmChange {
                relations: vec![RelationChange {
                    action: ChangeAction::Delete,
                    version: 2,
                    id: 400,
                    members: vec![],
                    tags: vec![],
                }],
                ..Default::default()
            },
        )?;

        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM osm_former_buildings WHERE osm_id = 400"
            )?,
            0,
            "the former-building row must be gone"
        );
        for source in ["bdot10k", "egib"] {
            assert_eq!(
                queued_cells(&conn, source)?,
                vec![home.cell],
                "{source} must be told the veto was lifted here"
            );
        }
        Ok(())
    }

    #[test]
    fn osc_local_file_name_is_unique_per_sequence() {
        // Direct regression for the on-disk filename collision bug:
        // `sequence_to_path` nests the zero-padded sequence in three
        // directory levels (`007/237/736.osc.gz`), so its *last path
        // segment alone* repeats every 1000 sequences -- these two
        // sequences 1000 apart really do share it -- while
        // `osc_local_file_name` (used for the on-disk filename instead)
        // must not.
        let a = sequence_to_path(7_237_736);
        let b = sequence_to_path(7_236_736);
        assert_eq!(
            a.rsplit('/').next(),
            b.rsplit('/').next(),
            "sanity check: sequence_to_path's last segment really does collide for these two"
        );

        assert_ne!(
            osc_local_file_name(7_237_736),
            osc_local_file_name(7_236_736)
        );
    }

    #[test]
    fn corrupt_download_is_removed_after_decompress_failure() {
        // Direct regression for the cleanup-skipped-on-failure bug: the old
        // code was `let osc_xml = decompress_gz(&osc_gz_path)?; let _ =
        // std::fs::remove_file(&osc_gz_path);`, so the `?` short-circuited
        // past cleanup whenever decompression failed.
        //
        // This calls `decompress_and_remove` -- the function
        // `fetch_and_parse_sequence` actually uses -- rather than
        // re-executing a copy of its statements here. That distinction is
        // the whole point: a test that inlined the same three lines would
        // still pass after `fetch_and_parse_sequence` was reverted to
        // `decompress_gz(&path)?`, i.e. it could not fail on the bug it
        // names.
        //
        // It still does not call `fetch_and_parse_sequence` itself -- that
        // needs a mock server that also knows the URL shape -- so it does
        // not cover the DB transaction/rollback path (`apply_batch`), nor
        // that a retried sequence re-downloads cleanly afterwards. It does
        // download a real corrupt file over HTTP exactly the way
        // `fetch_and_parse_sequence` does (via `download_file_as_quiet` with
        // `osc_local_file_name`).
        let garbage: &'static [u8] = b"this is not a valid gzip stream, just garbage bytes";
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            use std::io::Write;
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                    garbage.len()
                );
                let _ = stream.write_all(headers.as_bytes());
                let _ = stream.write_all(garbage);
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let url = format!("http://{addr}/007/237/736.osc.gz");
        let seq = 7_237_736u64;
        let osc_gz_path =
            download_file_as_quiet(&url, tmp.path(), &osc_local_file_name(seq)).unwrap();
        assert!(osc_gz_path.exists());

        let osc_xml = decompress_and_remove(&osc_gz_path);
        assert!(
            osc_xml.is_err(),
            "garbage bytes must fail gzip decompression, or this test isn't exercising the failure path"
        );
        assert!(
            !osc_gz_path.exists(),
            "corrupt download must be cleaned up even though decompression failed"
        );
    }

    /// `update()` must stop before applying any sequence when `is_cancelled`
    /// already reports true, rather than grinding through the whole backlog
    /// first and only recording the cancellation afterwards.
    ///
    /// This pins the "stops before starting the next sequence" half of the
    /// contract: the mock `state.txt` server advertises exactly one pending
    /// sequence (1001, one past the metadata stamp of 1000 set up by
    /// `setup_test_db_and_kv`), and there is deliberately no mock server for
    /// that sequence's `.osc.gz` -- if `update` tried to download it despite
    /// `is_cancelled` returning true, the download would fail and this test
    /// would error out rather than merely pass for the wrong reason. What
    /// this does NOT cover: cancellation discovered partway through a
    /// multi-sequence catch-up (that would need a mock server answering
    /// several distinct sequence URLs, and the check's placement -- before
    /// `apply_batch`, at the top of the `while seq <= latest_seq` loop -- is
    /// a one-line diff verifiable by reading `update` itself, and is also
    /// covered end-to-end by `update_applies_in_batches_with_prefetch_and_stops_on_cancellation`
    /// below); nor does it cover the real background-job wiring in
    /// `server::jobs::osm_update::OsmUpdateJob::run`, which passes
    /// `&|| ctx.is_cancelled()` instead of a hardcoded closure.
    ///
    /// Prefetching is deliberately disabled (`prefetch_ahead: 0`) here: this
    /// test predates the prefetcher and pins something orthogonal to it
    /// (cancellation checked before the *first* sequence). With prefetching
    /// on, the one-shot mock server below would already have served its
    /// single request and exited by the time the prefetch thread started, so
    /// its download attempt would hit connection-refused and burn through
    /// `download_with_retry`'s several-second backoff before `update()`
    /// could `join()` it -- turning a near-instant test into a multi-second
    /// one for no added coverage. `prefetch_ahead: 0` keeps this test at its
    /// original speed; the prefetcher itself is covered by the batching test
    /// below instead.
    #[test]
    fn update_stops_before_applying_a_sequence_when_already_cancelled() -> Result<()> {
        let (conn, kv, _kv_dir) = setup_test_db_and_kv()?;

        // One-shot HTTP server standing in for `<replication_base_url>/state.txt`,
        // same raw-TcpListener style as `corrupt_download_is_removed_after_decompress_failure`
        // above and `update::mod`'s `serve_head_once`/`serve_body_once` test helpers.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            use std::io::Write;
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let body = "sequenceNumber=1001\ntimestamp=2024-01-01T00\\:00\\:00Z\n";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        let base_url = format!("http://{addr}");

        let download_dir = tempfile::tempdir().unwrap();
        let mut config = Config {
            download_dir: Some(download_dir.path().to_string_lossy().into_owned()),
            ..Config::default()
        };
        config.jobs.osm_update.prefetch_ahead = 0;

        update(&conn, &kv, &config, &base_url, false, &|| true)?;

        // Cancellation must have stopped the loop before `apply_batch`
        // ever ran, so the metadata stamp is untouched.
        let seq = get_current_sequence(&conn)?;
        assert_eq!(
            seq, 1000,
            "cancellation before the first sequence must leave the stamp unchanged"
        );

        // Still a Success row, with a message distinguishing "stopped early"
        // from "actually caught up" -- see `OSM_UPDATE_JOB_LOG_KEY`'s doc
        // comment for why this is Success rather than Error.
        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = &log[OSM_UPDATE_JOB_LOG_KEY];
        assert_eq!(entry.outcome, "Success");
        assert_eq!(
            entry.message.as_deref(),
            Some("applied 0 of 1 pending sequences (stopped early), now at sequence 1000")
        );

        Ok(())
    }

    /// When the local stamp already matches (or exceeds) the remote's latest
    /// sequence, `update()` returns before the catch-up loop even builds --
    /// this pins that the early return still writes a job_run_log row rather
    /// than leaving `/status` showing whatever the previous run left behind.
    #[test]
    fn update_logs_already_up_to_date() -> Result<()> {
        let (conn, kv, _kv_dir) = setup_test_db_and_kv()?;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            use std::io::Write;
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                // Same stamp `setup_test_db_and_kv` leaves in `metadata`
                // (see the sibling cancellation test above) -- current == latest.
                let body = "sequenceNumber=1000\ntimestamp=2024-01-01T00\\:00\\:00Z\n";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        let base_url = format!("http://{addr}");

        let download_dir = tempfile::tempdir().unwrap();
        let config = Config {
            download_dir: Some(download_dir.path().to_string_lossy().into_owned()),
            ..Config::default()
        };

        update(&conn, &kv, &config, &base_url, false, &|| false)?;

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = &log[OSM_UPDATE_JOB_LOG_KEY];
        assert_eq!(entry.outcome, "Success");
        assert_eq!(
            entry.message.as_deref(),
            Some("already up to date at sequence 1000")
        );

        Ok(())
    }

    // --- 2d: batched commits ---

    #[test]
    fn catch_up_chunk_size_batches_only_when_pending_exceeds_threshold() {
        // Steady state and any catch-up at or below the threshold: today's
        // one-sequence-per-transaction path, byte-for-byte -- chunk_size
        // must be exactly 1, not "close to 1".
        assert_eq!(catch_up_chunk_size(1, 20, 20), 1);
        assert_eq!(
            catch_up_chunk_size(20, 20, 20),
            1,
            "pending == threshold must NOT batch -- the rule is strictly greater-than"
        );

        // Once pending exceeds the threshold, batching engages at batch_size.
        assert_eq!(catch_up_chunk_size(21, 20, 20), 20);
        assert_eq!(catch_up_chunk_size(1440, 20, 20), 20);
        assert_eq!(
            catch_up_chunk_size(21, 20, 7),
            7,
            "chunk_size must track batch_size, not the threshold"
        );

        // A misconfigured batch_size = 0 must not produce a zero-length
        // chunk (which would spin update()'s while loop forever without
        // advancing seq).
        assert_eq!(catch_up_chunk_size(21, 20, 0), 1);
    }

    /// Crash-safety pin for 2d's batched commits -- see the extended comment
    /// on `apply_batch` for the full argument this test exercises. Short
    /// version: `apply_changes` writes to RocksDB immediately, outside any
    /// DuckDB transaction, but every DuckDB write happens inside the
    /// caller's transaction. A crash partway through a batch therefore
    /// leaves RocksDB reflecting a PREFIX of the batch's sequences while
    /// DuckDB rolls back to the pre-batch state entirely -- and resume
    /// always replays the WHOLE batch from its first sequence. This is only
    /// safe because every RocksDB primitive `apply_changes` uses is an
    /// idempotent upsert/delete or get-modify-put set toggle
    /// (`add_node_to_ways`/`remove_node_to_ways`), never the list-append
    /// RocksDB merge operator that's ALSO registered and live elsewhere
    /// (`import::osm`'s `batch_merge_node_to_way`/`batch_merge_way_to_relation`).
    ///
    /// This test simulates exactly that crash shape without needing
    /// `apply_batch`/a transaction at all, by calling the real
    /// `apply_changes` (never a copy of its logic) three times over:
    ///
    /// 1. **Golden**: apply three sequences, once each, cleanly.
    /// 2. **"Crash"**: apply the first two sequences to a KV store, through a
    ///    DuckDB connection that is then simply dropped -- standing in for a
    ///    rolled-back transaction, since RocksDB writes persist regardless
    ///    of what happens to DuckDB.
    /// 3. **Replay**: apply the FULL three sequences again, against a fresh
    ///    DuckDB connection (matching the golden run's starting point) but
    ///    the SAME KV store from step 2 (already carrying the first two
    ///    sequences' writes).
    ///
    /// The middle sequence -- a way reusing nodes already shared with the
    /// fixture's way 100 -- is what would expose a non-idempotent reverse
    /// index: if node-to-way association used the RocksDB merge operator
    /// instead of `add_node_to_ways`'s idempotent get-modify-put, replaying
    /// it would duplicate the way id in the shared nodes' reverse index, and
    /// the final snapshot would show three entries where the golden run has
    /// two.
    #[test]
    fn replaying_a_batch_over_a_partially_written_kv_store_converges_to_the_golden_state()
    -> Result<()> {
        let seq0 = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: 950,
                lon: 25.0,
                lat: 55.0,
                tags: vec![("addr:housenumber".into(), "3".into())],
            }],
            ..Default::default()
        };
        let seq1 = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Create,
                version: 1,
                id: 960,
                node_refs: vec![1, 2, 3, 4, 1],
                tags: vec![("building".into(), "yes".into())],
            }],
            ..Default::default()
        };
        let seq2 = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Modify,
                version: 1,
                id: 2,
                lon: 20.0015,
                lat: 50.0002,
                tags: vec![],
            }],
            ..Default::default()
        };
        let batch = [seq0, seq1, seq2];

        // --- Golden: apply all three, once each, cleanly.
        let (golden_conn, golden_kv, _d1) = setup_test_db_and_kv()?;
        for c in &batch {
            apply_changes(&golden_conn, &golden_kv, c)?;
        }
        let golden = snapshot_state(&golden_conn, &golden_kv)?;

        // --- "Crash": the first two sequences land in RocksDB, but the
        // DuckDB connection that received them is discarded before anything
        // reads it further -- standing in for a transaction that never
        // committed.
        let (discarded_conn, replay_kv, _d2) = setup_test_db_and_kv()?;
        apply_changes(&discarded_conn, &replay_kv, &batch[0])?;
        apply_changes(&discarded_conn, &replay_kv, &batch[1])?;
        drop(discarded_conn);

        // --- Replay: the WHOLE batch again, against a fresh DuckDB
        // connection (matching golden's starting point) but the SAME,
        // already-partially-written KV store from the "crash" above.
        //
        // Deliberately built by hand rather than via another
        // `setup_test_db_and_kv()` call: that would create its OWN fresh KV
        // store and bind the new connection's `resolve_way_coords` UDF to
        // THAT kv, not to `replay_kv` -- and the UDF binding is fixed at
        // connection creation, not reselected per call. A first attempt at
        // this test did exactly that and passed for the wrong reason at
        // first glance, then failed once way 960's geometry was checked: its
        // INSERT silently matched zero rows because `resolve_way_coords`
        // was resolving way 960 against a KV that had never heard of it.
        // Binding to `replay_kv` directly is what makes this test faithful
        // to production, where `conn` and `kv` are always the same pair.
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let replay_conn = init_db(
            Path::new(":memory:"),
            &init_commands,
            Some(replay_kv.clone()),
        )?;
        seed_duckdb(&replay_conn)?;
        for c in &batch {
            apply_changes(&replay_conn, &replay_kv, c)?;
        }
        let replayed = snapshot_state(&replay_conn, &replay_kv)?;

        assert_eq!(
            golden, replayed,
            "replaying a full batch over a partially-written KV store must converge \
             to the same state as applying it once cleanly"
        );

        Ok(())
    }

    /// Comparable snapshot of everything the scenario above touches.
    /// DuckDB side: every `osm_buildings`/`osm_addresses` row, geometry
    /// included as WKT so a stale-position bug shows up as a text diff.
    /// RocksDB side: the raw node/way state plus the reverse index for the
    /// nodes shared between way 100 (from the fixture) and the scenario's
    /// own way 960 -- the reverse index is exactly what a non-idempotent
    /// merge would duplicate.
    fn snapshot_state(conn: &Connection, kv: &RocksDB) -> Result<String> {
        let table_rows = |table: &str, tag_col: &str| -> Result<Vec<String>> {
            let sql = format!(
                "SELECT osm_id || '|' || osm_type || '|' || COALESCE({tag_col}, '') || '|' || ST_AsText(geom)
                 FROM {table} ORDER BY osm_id, osm_type"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        };

        let buildings = table_rows("osm_buildings", "building")?;
        let addresses = table_rows("osm_addresses", "housenumber")?;

        let node1 = kvstore::get_node(kv, 1)?;
        let node2 = kvstore::get_node(kv, 2)?;
        let way100 = kvstore::get_way(kv, 100)?;
        let way960 = kvstore::get_way(kv, 960)?;
        let mut node1_ways = kvstore::get_node_to_ways(kv, 1)?;
        let mut node2_ways = kvstore::get_node_to_ways(kv, 2)?;
        node1_ways.sort();
        node2_ways.sort();

        Ok(format!(
            "buildings={buildings:?}\naddresses={addresses:?}\n\
             node1={node1:?} node2={node2:?}\nway100={way100:?} way960={way960:?}\n\
             node1_ways={node1_ways:?} node2_ways={node2_ways:?}"
        ))
    }

    /// OSM-update-side analogue of `compare::drain_refresh_concurrency`
    /// (`src/compare/mod.rs`). That module's test drives a
    /// *government-refresh*-shaped writer against a concurrent drain; this
    /// drives an *OSM-apply*-shaped writer instead, because 2d's batching
    /// holds `apply_batch`'s write transaction open for several sequences'
    /// worth of work rather than one, directly widening the window
    /// `match_dirty_cells` (append from the OSM side, delete-after-recompute
    /// from the drain side) has to overlap the drain in.
    ///
    /// Calls `apply_batch` directly with in-memory `FetchedSequence` values
    /// -- no HTTP/network involved -- the same way
    /// `compare::drain_refresh_concurrency`'s writer thread calls `refresh()`
    /// directly rather than going through a full CLI/network stack.
    #[test]
    fn osm_apply_batch_and_match_refresh_drain_do_not_collide() {
        use crate::compare::drain::drain_batch;
        use crate::compare::reconcile::enqueue_all;

        let tmpdir = tempfile::tempdir().unwrap();
        let kv = Arc::new(kvstore::open(tmpdir.path(), 8, 4, 8).unwrap());
        let init_commands = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init_commands, Some(kv.clone())).unwrap();

        // `bdot10k_buildings` spread across many z14 cells (0.03 deg stride
        // -- same rationale as `compare::drain_refresh_concurrency`'s
        // `rows_sql`: cells are ~0.022 deg wide at this latitude, so 0.03
        // deg guarantees distinct cells): real, independent work for the
        // drain thread that has nothing to do with what the OSM-apply
        // thread writes (that writes OSM buildings around lon 20, these
        // government buildings sit at lon 30+).
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom)
             SELECT 'b' || i, ST_MakeEnvelope(30.0 + i * 0.03, 52.0, 30.0 + i * 0.03 + 0.002, 52.002)
             FROM range(200) t(i);
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 rodzaj_kod VARCHAR, kondygnacje_nadziemne INTEGER,
                 kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, geom GEOMETRY);",
        )
        .unwrap();
        enqueue_all(&conn).unwrap();

        let drain_conn = conn.try_clone().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_drain = stop.clone();
        let drained = Arc::new(AtomicU64::new(0));
        let drained_thread = drained.clone();

        let handle = std::thread::spawn(move || {
            let mut errors: Vec<String> = Vec::new();
            let mut productive_batches: u64 = 0;
            while !stop_drain.load(Ordering::SeqCst) {
                match drain_batch(&drain_conn, 16, &|| false) {
                    Ok(stats) => {
                        drained_thread.fetch_add(stats.cells, Ordering::SeqCst);
                        if stats.cells > 0 {
                            productive_batches += 1;
                        }
                        if stats.failed > 0 {
                            errors.push(format!("{} cells failed to recompute", stats.failed));
                        }
                    }
                    Err(e) => errors.push(format!("drain_batch errored: {e:#}")),
                }
            }
            (errors, productive_batches)
        });

        // Meanwhile, apply several OSM batches -- each batch a handful of
        // sequences creating a brand-new building at a distinct location, so
        // apply_batch does real INSERT + match_dirty_cells work inside a
        // transaction long enough to genuinely overlap the drain thread.
        let mut apply_errors: Vec<String> = Vec::new();
        for batch_idx in 0..10u64 {
            let seqs: Vec<FetchedSequence> = (0..3u64)
                .map(|i| {
                    let seq = batch_idx * 3 + i;
                    synthetic_building_sequence(seq, 20.0 + seq as f64 * 0.01, 40.0)
                })
                .collect();
            if let Err(e) = apply_batch(&conn, &kv, &seqs, "2024-01-01T00:00:00Z") {
                apply_errors.push(format!("apply_batch({batch_idx}) errored: {e:#}"));
            }
        }

        stop.store(true, Ordering::SeqCst);
        let (drain_errors, productive_batches) = handle.join().unwrap();

        assert!(
            apply_errors.is_empty(),
            "OSM apply_batch must not abort against a concurrent drain: {apply_errors:?}"
        );
        assert!(
            drain_errors.is_empty(),
            "drain must not abort against a concurrent OSM apply_batch: {drain_errors:?}"
        );
        assert!(
            productive_batches >= 2,
            "drain made {productive_batches} productive batches during the OSM apply run -- \
             expected steady progress, not serialization behind apply_batch's transactions"
        );
        assert!(
            drained.load(Ordering::SeqCst) > 0,
            "drain thread never drained a cell -- the test did not exercise the overlap"
        );

        // Whatever interleaving happened, the queue must still converge.
        loop {
            let s = drain_batch(&conn, 1000, &|| false).unwrap();
            assert_eq!(s.failed, 0, "post-run drain reported failed cells");
            if s.cells == 0 {
                break;
            }
        }
        let queued: i64 = conn
            .query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(queued, 0, "queue must drain to empty");
    }

    /// Build one synthetic replication sequence that creates a small,
    /// self-contained square building (4 fresh nodes + 1 way, all newly
    /// created ids derived from `seq` so different sequences never collide)
    /// at `(lon0, lat0)`. Used only by the concurrency test above, where the
    /// exact building shape doesn't matter -- only that `apply_batch` does
    /// real, distinct DuckDB + RocksDB writes per sequence.
    fn synthetic_building_sequence(seq: u64, lon0: f64, lat0: f64) -> FetchedSequence {
        let base: i64 = 1_000_000 + seq as i64 * 10;
        let d = 0.0005;
        let n = |i: i64| base + i;
        let nodes = vec![
            NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: n(1),
                lon: lon0,
                lat: lat0,
                tags: vec![],
            },
            NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: n(2),
                lon: lon0 + d,
                lat: lat0,
                tags: vec![],
            },
            NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: n(3),
                lon: lon0 + d,
                lat: lat0 + d,
                tags: vec![],
            },
            NodeChange {
                action: ChangeAction::Create,
                version: 1,
                id: n(4),
                lon: lon0,
                lat: lat0 + d,
                tags: vec![],
            },
        ];
        let way = WayChange {
            action: ChangeAction::Create,
            version: 1,
            id: n(5),
            node_refs: vec![n(1), n(2), n(3), n(4), n(1)],
            tags: vec![("building".into(), "yes".into())],
        };
        FetchedSequence {
            seq,
            changes: OsmChange {
                nodes,
                ways: vec![way],
                relations: vec![],
            },
        }
    }

    // --- 2c + 2d end-to-end, through the real `update()` loop ---

    /// A syntactically valid but empty OsmChange -- no create/modify/delete
    /// blocks at all. `parse_osc` returns `OsmChange::default()` for it, so
    /// `apply_changes` does real work (opens/uses the transaction, notes no
    /// dirty cells) without needing distinct interesting content per
    /// sequence; the point of the test below is loop mechanics, not content.
    const EMPTY_OSC_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?><osmChange version="0.6" generator="test"></osmChange>"#;

    fn gzip_bytes(data: &[u8]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write as _;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    /// Multi-connection, multi-request blocking mock server (unlike this
    /// file's other mock servers, which are all one-shot): answers
    /// `GET /state.txt` from `head`, read per request so one server can
    /// serve several successive `update()` ticks with a moving replication
    /// head, and every other GET with `osc_gz_body`, forever, on however
    /// many connections arrive. Needed because a single `update()` run makes
    /// many requests -- `state.txt` once, plus one per sequence.
    ///
    /// Returns the log of every sequence requested (`0` standing for
    /// `state.txt`) rather than a bare counter, because the interesting
    /// properties are per-sequence: *which* sequence was asked for twice
    /// (the duplicate download that used to orphan a file), and whether the
    /// log is strictly increasing (which would mean the prefetch thread
    /// never ran ahead of the apply loop at all).
    fn spawn_replication_mock_server(
        head: Arc<AtomicU64>,
        osc_gz_body: Vec<u8>,
    ) -> (std::net::SocketAddr, Arc<std::sync::Mutex<Vec<u64>>>) {
        use std::io::Write as _;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let requested: Arc<std::sync::Mutex<Vec<u64>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let log_for_thread = Arc::clone(&requested);

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let osc_gz_body = osc_gz_body.clone();
                let log = Arc::clone(&log_for_thread);
                let head = Arc::clone(&head);
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let path = request
                        .strip_prefix("GET ")
                        .and_then(|r| r.split(' ').next())
                        .unwrap_or("");
                    let is_state = path.ends_with("state.txt");
                    // `sequence_to_path` nests the zero-padded sequence
                    // across three directory levels, so the digits of the
                    // whole path *are* the sequence number.
                    let seq: u64 = if is_state {
                        0
                    } else {
                        path.chars()
                            .filter(|c| c.is_ascii_digit())
                            .collect::<String>()
                            .parse()
                            .unwrap_or(u64::MAX)
                    };
                    log.lock().unwrap().push(seq);

                    if is_state {
                        let state_body = format!(
                            "sequenceNumber={}\ntimestamp=2024-01-01T00\\:00\\:00Z\n",
                            head.load(Ordering::SeqCst)
                        );
                        let headers = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            state_body.len()
                        );
                        let _ = stream.write_all(headers.as_bytes());
                        let _ = stream.write_all(state_body.as_bytes());
                    } else {
                        let headers = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                            osc_gz_body.len()
                        );
                        let _ = stream.write_all(headers.as_bytes());
                        let _ = stream.write_all(&osc_gz_body);
                    }
                });
            }
        });

        (addr, requested)
    }

    /// Every file sitting in `dir`, sorted. Used by the cleanup regressions
    /// below, which assert on the *whole* directory rather than on the
    /// absence of one expected name: the leak they pin was a file nobody
    /// intended to create, so naming the file the assertion looks for would
    /// be assuming the shape of the next bug.
    fn files_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// Sequences requested more than once, with their counts.
    fn duplicate_requests(log: &[u64]) -> Vec<(u64, usize)> {
        let mut counts: std::collections::BTreeMap<u64, usize> = std::collections::BTreeMap::new();
        for seq in log.iter().filter(|s| **s != 0) {
            *counts.entry(*seq).or_default() += 1;
        }
        counts.into_iter().filter(|(_, c)| *c > 1).collect()
    }

    /// End-to-end coverage of the new `update()` loop: batching (several
    /// sequences committed together once `pending` exceeds
    /// `batch_commit_threshold`), prefetching (the bounded-window thread
    /// downloading ahead of the apply loop, sharing `download_dir` and
    /// `osc_local_file_name` so the apply loop's own download calls become
    /// no-ops for whatever the prefetcher already fetched), and cancellation
    /// checked between batches. `update()` had no test at all before this
    /// change; `update_stops_before_applying_a_sequence_when_already_cancelled`
    /// above is the one prior test, extended here to a multi-sequence,
    /// multi-batch, always-on server.
    ///
    /// This does NOT prove prefetching makes anything faster -- there is no
    /// outbound network in this environment, and even this local mock
    /// server answers so quickly that any timing difference would be noise,
    /// not signal. What it does prove: the prefetch thread runs concurrently
    /// with the apply loop without erroring, deadlocking, or leaking
    /// (`update()` joins it before returning here), the exists-check dedup
    /// keeps the total request count close to the number of distinct
    /// sequences needed rather than every sequence being fetched twice,
    /// batching groups sequences into one commit at the configured chunk
    /// size, and cancellation is honored between batches rather than
    /// mid-batch or only after the whole backlog.
    #[test]
    fn update_applies_in_batches_with_prefetch_and_stops_on_cancellation() -> Result<()> {
        let (conn, kv, _kv_dir) = setup_test_db_and_kv()?; // current_seq = 1000

        const PENDING: u64 = 13;
        const LATEST_SEQ: u64 = 1000 + PENDING;
        let osc_gz_body = gzip_bytes(EMPTY_OSC_XML.as_bytes());
        let (addr, requested) =
            spawn_replication_mock_server(Arc::new(AtomicU64::new(LATEST_SEQ)), osc_gz_body);
        let base_url = format!("http://{addr}");

        let download_dir = tempfile::tempdir().unwrap();
        let mut config = Config {
            download_dir: Some(download_dir.path().to_string_lossy().into_owned()),
            ..Config::default()
        };
        config.jobs.osm_update.batch_commit_threshold = 10;
        config.jobs.osm_update.batch_size = 10;
        config.jobs.osm_update.prefetch_ahead = 4;

        // Cancel from the SECOND poll of `is_cancelled` onward: the first
        // poll happens before batch 1 (must proceed), the second before
        // batch 2 (must stop) -- pinning "checked between batches", not
        // "checked between sequences" or "checked once up front".
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let is_cancelled = || calls.fetch_add(1, Ordering::SeqCst) >= 1;

        update(&conn, &kv, &config, &base_url, false, &is_cancelled)?;

        // 13 pending > batch_commit_threshold(10), so chunk_size =
        // batch_size = 10: batch 1 is sequences 1001..=1010 (10 sequences),
        // batch 2 would be 1011..=1013 (3 sequences). Cancellation must have
        // stopped the loop before batch 2 committed, so the stamp sits at
        // 1010 -- not 1000 (which would mean batching broke, or cancellation
        // fired too early) and not 1013 (which would mean cancellation was
        // ignored, or checked only after the whole backlog).
        let seq = get_current_sequence(&conn)?;
        assert_eq!(
            seq, 1010,
            "batch 1 (10 sequences) must have committed as one transaction before \
             cancellation, checked between batches, stopped the loop before batch 2"
        );

        // The prefetcher and the apply loop must not each download the same
        // sequence: at most 1 (state.txt) + PENDING, with no slack. There
        // used to be slack here "for a genuine prefetch/apply race landing
        // on the same sequence at the same time", and that race was the
        // file-orphaning bug rather than an acceptable cost -- the apply
        // loop claims each batch through `fetch_frontier` before fetching
        // it, and waits out a prefetch already in flight on a claimed
        // sequence (`PrefetchInFlight`), so the two never both fetch one.
        let log = requested.lock().unwrap().clone();
        let requests = log.len();
        assert!(
            requests <= 1 + PENDING as usize,
            "too many requests ({requests}) for {PENDING} pending sequences -- the \
             prefetcher and the apply loop look like they are both downloading the \
             same sequences; duplicates: {:?}",
            duplicate_requests(&log)
        );
        // At least the 10 sequences actually applied must have been fetched
        // by someone (prefetcher or apply loop), plus state.txt (11 total).
        assert!(
            requests > 10,
            "fewer requests ({requests}) than sequences actually applied -- some \
             applied sequence's content came from nowhere"
        );

        Ok(())
    }

    /// Regression for the orphaned-`.osc.gz` leak: a steady-state tick must
    /// leave `download_dir` empty, and must download the single sequence it
    /// needs exactly once.
    ///
    /// This is the shape production runs in -- a minutely feed with one
    /// pending sequence per tick -- and it is where the old apply-time
    /// prefetch floor (`last_applied`, advanced inside `apply_batch`) failed
    /// every single time. Both the prefetcher and the apply loop downloaded
    /// the one pending sequence, the apply loop consumed and unlinked it in
    /// `decompress_and_remove`, and the prefetcher's `do_download` then
    /// renamed its own copy onto that same path. Nothing ever read that
    /// sequence again, so the file stayed in `download_dir` -- the system
    /// temp directory by default -- and one more joined it every tick.
    ///
    /// Ten ticks rather than one, asserting after each, because the failure
    /// was cumulative: a single-tick assertion cannot tell "cleaned up" from
    /// "replaced by the next tick's orphan".
    ///
    /// The request-count half is not a bonus assertion, it is the root
    /// cause. One request per sequence means the two never targeted it
    /// concurrently, which is what makes the unlink/rename race impossible
    /// rather than merely unobserved on this run.
    #[test]
    fn steady_state_ticks_leave_no_downloaded_diff_behind() -> Result<()> {
        let (conn, kv, _kv_dir) = setup_test_db_and_kv()?; // current_seq = 1000
        const TICKS: u64 = 10;

        let head = Arc::new(AtomicU64::new(1000));
        let (addr, requested) =
            spawn_replication_mock_server(Arc::clone(&head), gzip_bytes(EMPTY_OSC_XML.as_bytes()));
        let base_url = format!("http://{addr}");

        let download_dir = tempfile::tempdir().unwrap();
        // Stock defaults throughout (prefetch_ahead = 8, batch_size = 20,
        // batch_commit_threshold = 20): the leak needs no unusual tuning,
        // and pinning it under the shipped configuration is the point.
        let config = Config {
            download_dir: Some(download_dir.path().to_string_lossy().into_owned()),
            ..Config::default()
        };

        for tick in 1..=TICKS {
            head.store(1000 + tick, Ordering::SeqCst);
            update(&conn, &kv, &config, &base_url, false, &|| false)?;

            assert_eq!(get_current_sequence(&conn)?, 1000 + tick);
            assert_eq!(
                files_in(download_dir.path()),
                Vec::<String>::new(),
                "tick {tick} left a downloaded replication diff behind in download_dir"
            );
        }

        let log = requested.lock().unwrap().clone();
        assert_eq!(
            duplicate_requests(&log),
            vec![],
            "a sequence was downloaded more than once -- the prefetcher and the apply \
             loop are both targeting it, which is what orphaned a file per tick"
        );
        // One state.txt plus one diff per tick, and nothing else.
        assert_eq!(log.len(), (2 * TICKS) as usize);

        Ok(())
    }

    /// Regression: sequences the prefetch thread downloaded but the apply
    /// loop never reached must be unlinked when `update()` stops early.
    ///
    /// A cancelled run (a supervisor timeout, a shutdown, or a failed batch)
    /// leaves the prefetcher up to `prefetch_ahead` sequences ahead of the
    /// stamp. Those files are harmless in *content* -- replication diffs are
    /// immutable, so a later run resuming onto them reads them happily, and
    /// that is why this leak was bounded and self-healing where the
    /// steady-state one above was neither. But a run that stops for good
    /// never resumes, and the documented contract (`example_config.toml`, on
    /// `cleanup_downloaded_files`) is that replication diffs are always
    /// cleaned up regardless of that setting.
    ///
    /// `batch_commit_threshold` is set above `pending` so `chunk_size` is 1
    /// and the apply loop advances one sequence at a time, which is what
    /// lets the prefetcher genuinely get ahead of it and leave a backlog to
    /// clean up.
    #[test]
    fn a_cancelled_update_cleans_up_prefetched_sequences_it_never_applied() -> Result<()> {
        let (conn, kv, _kv_dir) = setup_test_db_and_kv()?; // current_seq = 1000
        const APPLY_BEFORE_CANCEL: usize = 10;

        let (addr, _requested) = spawn_replication_mock_server(
            Arc::new(AtomicU64::new(1100)),
            gzip_bytes(EMPTY_OSC_XML.as_bytes()),
        );
        let base_url = format!("http://{addr}");

        let download_dir = tempfile::tempdir().unwrap();
        let mut config = Config {
            download_dir: Some(download_dir.path().to_string_lossy().into_owned()),
            ..Config::default()
        };
        config.jobs.osm_update.batch_commit_threshold = 1000; // => chunk_size 1
        config.jobs.osm_update.prefetch_ahead = 8;

        let calls = std::sync::atomic::AtomicUsize::new(0);
        let is_cancelled = || calls.fetch_add(1, Ordering::SeqCst) >= APPLY_BEFORE_CANCEL;

        update(&conn, &kv, &config, &base_url, false, &is_cancelled)?;

        // Cancellation must have cut the run short well before the head,
        // otherwise there was no prefetch backlog for the cleanup to have
        // anything to do and this test would pass vacuously.
        let stamp = get_current_sequence(&conn)?;
        assert_eq!(stamp, 1000 + APPLY_BEFORE_CANCEL as u64);

        assert_eq!(
            files_in(download_dir.path()),
            Vec::<String>::new(),
            "a cancelled update left prefetched diffs in download_dir; the prefetch \
             thread's cleanup pass must unlink whatever the apply loop never consumed"
        );

        Ok(())
    }

    /// The prefetch thread must stay out of the batch the apply loop has
    /// claimed -- and must still be prefetching.
    ///
    /// Both halves are load-bearing. Not downloading a sequence twice is the
    /// property that closes the leak, but `prefetch_ahead = 0` satisfies it
    /// trivially, so on its own this test would keep passing if a later
    /// change disabled prefetching outright. The inversion count is the
    /// other half: a request for sequence N arriving before a request for
    /// some sequence below N can only be the prefetch thread running ahead
    /// of the apply loop, so a strictly increasing log means nothing was
    /// prefetched. The `prefetch_ahead = 0` arm establishes that baseline
    /// within the same test rather than asserting a bare `> 0` against an
    /// assumption.
    ///
    /// **Why the duplicate count is exactly zero, not a bound.** The
    /// frontier removes the *systematic* overlap (16 of 100 before it
    /// existed), but it is read before a download, not held across one, so
    /// on its own the apply loop could still catch up to a sequence the
    /// prefetcher was mid-download of and fetch it too. That residue used
    /// to be tolerated as a bound calibrated at one CPU load, and it was not
    /// a bound at all: it grows with how long a starved prefetch thread
    /// runs in lockstep with the apply loop, and exceeded the bound under
    /// load. `PrefetchInFlight` makes the apply loop wait out that download
    /// instead, so any duplicate here is a regression.
    #[test]
    fn the_prefetcher_stays_out_of_the_batch_the_apply_loop_claimed() -> Result<()> {
        const PENDING: u64 = 100;

        /// Requests that arrived after a strictly higher sequence had
        /// already been requested.
        fn inversions(log: &[u64]) -> usize {
            let mut count = 0;
            let mut highest = 0u64;
            for &seq in log.iter().filter(|s| **s != 0) {
                if seq < highest {
                    count += 1;
                }
                highest = highest.max(seq);
            }
            count
        }

        let mut inversions_by_window = Vec::new();

        for prefetch_ahead in [0usize, 8] {
            let (conn, kv, _kv_dir) = setup_test_db_and_kv()?; // current_seq = 1000
            let (addr, requested) = spawn_replication_mock_server(
                Arc::new(AtomicU64::new(1000 + PENDING)),
                gzip_bytes(EMPTY_OSC_XML.as_bytes()),
            );
            let download_dir = tempfile::tempdir().unwrap();
            let mut config = Config {
                download_dir: Some(download_dir.path().to_string_lossy().into_owned()),
                ..Config::default()
            };
            config.jobs.osm_update.prefetch_ahead = prefetch_ahead;
            config.jobs.osm_update.batch_commit_threshold = 10; // => batching engages
            config.jobs.osm_update.batch_size = 20;

            update(
                &conn,
                &kv,
                &config,
                &format!("http://{addr}"),
                false,
                &|| false,
            )?;

            assert_eq!(get_current_sequence(&conn)?, 1000 + PENDING);
            assert_eq!(
                files_in(download_dir.path()),
                Vec::<String>::new(),
                "prefetch_ahead={prefetch_ahead}: a completed catch-up left files behind"
            );

            // With the old apply-time floor this configuration downloaded
            // 16 of the 100 sequences twice -- the first `prefetch_ahead` of
            // every batch the apply loop claimed.
            let log = requested.lock().unwrap().clone();
            assert_eq!(
                duplicate_requests(&log),
                Vec::new(),
                "prefetch_ahead={prefetch_ahead}: sequences downloaded twice -- the \
                 prefetcher and the apply loop both fetched them"
            );
            inversions_by_window.push(inversions(&log));
        }

        assert_eq!(
            inversions_by_window[0], 0,
            "with prefetching off the apply loop walks sequences in order, so the \
             request log must be strictly increasing -- if it is not, `inversions` is \
             measuring something other than prefetch activity"
        );
        assert!(
            inversions_by_window[1] > 0,
            "with prefetch_ahead=8 nothing ever ran ahead of the apply loop: the \
             duplicate-free result above is vacuous because prefetching is disabled"
        );

        Ok(())
    }

    /// The prefetch thread's cleanup pass must not run while the apply loop
    /// is still fetching.
    ///
    /// Reaching the end of the download loop does not mean the run is over:
    /// it also ends on its own once `next` passes `latest_seq`, which
    /// happens as soon as the frontier gets within `prefetch_ahead` of the
    /// head -- with the apply loop still working through the backlog behind
    /// it. Everything the thread downloaded is then still wanted, so a
    /// cleanup there deletes live files: the apply loop re-downloads them
    /// at best, and at worst the unlink lands between its exists-check and
    /// `decompress_gz`'s open and fails the whole run.
    ///
    /// `prefetch_ahead` above `pending` with `chunk_size` at 1 is the
    /// sharpest form of that: the prefetcher grabs the entire backlog in one
    /// go, exits its loop while the apply loop is still near the start, and
    /// an ungated cleanup then wipes nearly all of it. That configuration
    /// measured 63 of 100 sequences re-downloaded, reproducibly, which is
    /// what this test's request count pins -- the run still *succeeded*
    /// every time, so nothing but the request count catches it.
    #[test]
    fn the_prefetch_cleanup_does_not_delete_files_the_apply_loop_still_needs() -> Result<()> {
        const PENDING: u64 = 100;

        let (conn, kv, _kv_dir) = setup_test_db_and_kv()?; // current_seq = 1000
        let (addr, requested) = spawn_replication_mock_server(
            Arc::new(AtomicU64::new(1000 + PENDING)),
            gzip_bytes(EMPTY_OSC_XML.as_bytes()),
        );
        let download_dir = tempfile::tempdir().unwrap();
        let mut config = Config {
            download_dir: Some(download_dir.path().to_string_lossy().into_owned()),
            ..Config::default()
        };
        config.jobs.osm_update.batch_commit_threshold = 1000; // => chunk_size 1
        config.jobs.osm_update.prefetch_ahead = 200; // deliberately > PENDING

        update(
            &conn,
            &kv,
            &config,
            &format!("http://{addr}"),
            false,
            &|| false,
        )?;

        assert_eq!(get_current_sequence(&conn)?, 1000 + PENDING);

        assert_eq!(files_in(download_dir.path()), Vec::<String>::new());

        // An ungated cleanup re-downloaded 63 of the 100 sequences here.
        // Exactly zero, not a bound: see
        // `the_prefetcher_stays_out_of_the_batch_the_apply_loop_claimed` for
        // why the in-flight race no longer produces any duplicates at all.
        let log = requested.lock().unwrap().clone();
        assert_eq!(
            duplicate_requests(&log),
            Vec::new(),
            "sequences downloaded twice -- the prefetch thread's cleanup pass looks \
             like it is deleting files the apply loop had not consumed yet"
        );

        Ok(())
    }
}
