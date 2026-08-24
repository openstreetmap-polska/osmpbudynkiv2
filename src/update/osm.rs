use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use duckdb::{Connection, OptionalExt};
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::{debug, info};

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

    // `last_applied` starts at `current_seq` (nothing past it is applied
    // yet) and is advanced by `apply_batch` as it works through each batch,
    // one sequence at a time -- see that function's doc comment for why it
    // moves mid-batch rather than only on commit. The prefetch thread reads
    // it to stay within `prefetch_ahead` of real progress; `stop` is how the
    // main thread tells the prefetcher to give up promptly on any exit path
    // below, so `update()` never blocks its `join()` on a full backoff wait.
    let last_applied = Arc::new(AtomicU64::new(current_seq));
    let stop = Arc::new(AtomicBool::new(false));
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
            Arc::clone(&last_applied),
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

            let batch_end = (seq + chunk_size as u64 - 1).min(latest_seq);

            // Fetch and parse the whole batch BEFORE opening the DuckDB
            // transaction in apply_batch -- see apply_batch's doc comment for
            // why that bound matters.
            let mut batch = Vec::with_capacity((batch_end - seq + 1) as usize);
            for s in seq..=batch_end {
                batch.push(fetch_and_parse_sequence(
                    s,
                    replication_base_url,
                    &download_dir,
                )?);
            }

            apply_batch(conn, kv, &batch, &latest_timestamp, &last_applied)?;

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

/// How often the prefetch thread rechecks `stop` while waiting for its
/// window to reopen (see [`spawn_prefetcher`]). Deliberately short and fixed
/// rather than growing like `download_with_retry`'s backoff: there is no
/// "increasing cost" to justify growth here, since the window reopens the
/// moment `last_applied` advances -- a short fixed poll just bounds how long
/// `update()`'s `join()` can be kept waiting once it sets `stop`.
const PREFETCH_WINDOW_POLL_INTERVAL: Duration = Duration::from_millis(50);

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
/// Bounded by `last_applied` so it never runs more than `prefetch_ahead`
/// sequences ahead of real progress (memory/disk for `prefetch_ahead`
/// buffered `.osc.gz` files, not the whole backlog), and by `latest_seq` so
/// it never prefetches a sequence that doesn't exist yet. Also bounded
/// *behind*: a `next` still sitting at or below `last_applied` is skipped
/// straight to `last_applied + 1` rather than downloaded, since the apply
/// loop has already consumed (and deleted) that sequence's file -- see the
/// comment at the skip site for why this matters more than it looks like it
/// should.
///
/// A download failure here is non-fatal and is logged at `debug!` rather
/// than retried: `download_file_as_quiet` (via `download_with_retry`)
/// already gave the sequence three attempts with exponential backoff, and
/// retrying again on top of that would let the prefetcher fall behind its
/// window chasing one stubborn sequence for no benefit -- the apply loop's
/// own download call is the authority regardless, and just downloads the
/// sequence itself if the prefetch never landed.
fn spawn_prefetcher(
    replication_base_url: String,
    download_dir: PathBuf,
    current_seq: u64,
    latest_seq: u64,
    prefetch_ahead: usize,
    last_applied: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut next = current_seq + 1;

        while next <= latest_seq {
            if stop.load(Ordering::SeqCst) {
                return;
            }

            let applied = last_applied.load(Ordering::SeqCst);
            if next <= applied {
                // Already consumed by the apply loop -- which fetches a
                // whole batch synchronously before calling `apply_batch`,
                // then deletes each `.osc.gz` right after decompressing it
                // (`decompress_and_remove`). If that batch's commit lands
                // between two of this thread's window checks (very possible:
                // `apply_batch` can finish well inside
                // `PREFETCH_WINDOW_POLL_INTERVAL`), `last_applied` can jump
                // past several `next` values this thread was sitting behind
                // in one stride. Downloading them now would just be
                // re-fetching bytes the apply loop already used and threw
                // away -- nobody reads a prefetched file whose sequence is
                // behind `last_applied`. Skip straight to the first
                // not-yet-applied sequence instead of downloading one we
                // know is stale.
                next = applied + 1;
                continue;
            }

            let window_ceiling = applied + prefetch_ahead as u64;
            if next > window_ceiling {
                // Window full: wait for the apply loop to advance, checking
                // `stop` between short sleeps rather than one long one, so a
                // cancelled or failed update() doesn't block its join() for
                // the whole wait.
                std::thread::sleep(PREFETCH_WINDOW_POLL_INTERVAL);
                continue;
            }

            let path = sequence_to_path(next);
            let url = format!("{replication_base_url}/{path}");
            if let Err(e) = download_file_as_quiet(&url, &download_dir, &osc_local_file_name(next))
            {
                debug!(
                    seq = next,
                    error = %e,
                    "prefetch download failed; the apply loop will download it synchronously instead"
                );
            }

            next += 1;
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
/// caller loop guarantees both. Steady state and any catch-up small enough
/// to stay under `batch_commit_threshold` always call this with a
/// single-element batch (`catch_up_chunk_size` returns `1`), which is
/// exactly the pre-batching `apply_sequence` behaviour: one BEGIN, one
/// `apply_changes`, one metadata stamp, one COMMIT.
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
/// **Crash-safety argument, and the one thing it rests on.** Every RocksDB
/// primitive `apply_changes` calls is either an unconditional upsert/delete
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
/// `apply_changes` (this *update* path) exclusively uses the get-modify-put
/// functions and never a merge; `replaying_a_batch_over_a_partially_written_kv_store_converges_to_the_golden_state`
/// (this file's test module) pins the resulting convergence directly against
/// `apply_changes`, not a description of it.
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
    last_applied: &AtomicU64,
) -> Result<()> {
    let last_seq = batch
        .last()
        .map(|f| f.seq)
        .expect("apply_batch must not be called with an empty batch");

    conn.execute_batch("BEGIN TRANSACTION")?;

    let result = (|| -> Result<()> {
        for fetched in batch {
            apply_changes(conn, kv, &fetched.changes)?;
            // Advance as each sequence is applied, not only once the whole
            // batch commits -- "currently-being-applied", per this field's
            // doc comment at its declaration in `update()`. This lets the
            // prefetcher's window slide forward smoothly during a large
            // batch instead of stalling until the batch's transaction
            // commits; if the transaction later rolls back, `update()`
            // propagates the error and joins the prefetcher immediately, so
            // an optimistic bump here never has a chance to matter.
            last_applied.store(fetched.seq, Ordering::SeqCst);
        }

        conn.execute_batch(&format!(
            "DELETE FROM metadata WHERE key IN ('osm_replication_sequence', 'osm_replication_timestamp');
             INSERT INTO metadata VALUES ('osm_replication_sequence', '{last_seq}');
             INSERT INTO metadata VALUES ('osm_replication_timestamp', '{timestamp}');"
        ))?;

        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
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

fn apply_changes(conn: &Connection, kv: &RocksDB, changes: &OsmChange) -> Result<()> {
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
    for &way_id in &affected_way_ids {
        rebuild_way_geometry(conn, kv, way_id, &changes.ways, &mut dirty)?;
    }

    // Cascade way changes to relations
    for &way_id in &affected_way_ids {
        let rel_ids = kvstore::get_way_to_relations(kv, way_id)?;
        affected_relation_ids.extend(&rel_ids);
    }

    // --- Rebuild affected relation geometries ---
    for &relation_id in &affected_relation_ids {
        rebuild_relation_geometry(conn, kv, relation_id, &changes.relations, &mut dirty)?;
    }

    dirty.flush(conn)?;

    Ok(())
}

fn tag_value(tags: &[(String, String)], key: &str) -> Option<String> {
    tags.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}

fn rebuild_way_geometry(
    conn: &Connection,
    kv: &RocksDB,
    way_id: i64,
    way_changes: &[WayChange],
    dirty: &mut DirtyCells,
) -> Result<()> {
    if kvstore::get_way(kv, way_id)?.is_none() {
        return Ok(());
    }

    // Determine tags: from the change if directly affected, else from DuckDB existence.
    // For indirectly affected ways, check DuckDB BEFORE deleting old entries.
    let way_change = way_changes.iter().find(|w| w.id == way_id);
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
        None => {
            let has_building: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way')",
                    [way_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            let has_address: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way')",
                    [way_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            // Unlike has_building/has_address, keep the stored key/value rather
            // than throwing them away: the tag determination further down
            // still needs them to re-insert with the SAME lifecycle key, not a
            // hardcoded default the way the building arm does with 'yes'.
            let former: Option<(String, String)> = conn
                .query_row(
                    "SELECT lifecycle_key, lifecycle_value FROM osm_former_buildings
                     WHERE osm_id = ? AND osm_type = 'way'",
                    [way_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            // Load-bearing: without `&& former.is_none()`, a former-building
            // way whose node moved would return here before the delete/
            // re-insert below, so its row would keep stale pre-move geometry.
            if !has_building && !has_address && former.is_none() {
                return Ok(());
            }
            (
                if has_building {
                    Some("yes".to_string())
                } else {
                    None
                },
                if has_address {
                    Some(String::new())
                } else {
                    None
                },
                None,
                None,
                None,
                former,
            )
        }
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
    relation_changes: &[RelationChange],
    dirty: &mut DirtyCells,
) -> Result<()> {
    let members = match kvstore::get_relation(kv, relation_id)? {
        Some(m) => m,
        None => return Ok(()),
    };

    // Determine tags: from the change if directly affected, else from DuckDB existence.
    // Check DuckDB BEFORE deleting old entries.
    let rel_change = relation_changes.iter().find(|r| r.id == relation_id);
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
        None => {
            let has_building: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation')",
                    [relation_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            let has_address: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation')",
                    [relation_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            // Keep the stored key/value, mirroring rebuild_way_geometry's
            // inferred arm -- do not collapse to a hardcoded default.
            let former: Option<(String, String)> = conn
                .query_row(
                    "SELECT lifecycle_key, lifecycle_value FROM osm_former_buildings
                     WHERE osm_id = ? AND osm_type = 'relation'",
                    [relation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            // Load-bearing, same as rebuild_way_geometry: without
            // `&& former.is_none()`, a former-building relation whose member
            // way moved would return here before the delete/re-insert below.
            if !has_building && !has_address && former.is_none() {
                return Ok(());
            }
            (
                if has_building {
                    Some("yes".to_string())
                } else {
                    None
                },
                if has_address {
                    Some(String::new())
                } else {
                    None
                },
                None,
                None,
                None,
                former,
            )
        }
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
        let kv = Arc::new(kvstore::open(tmpdir.path(), 8, 4)?);
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

    #[test]
    fn test_apply_way_delete() -> Result<()> {
        let (conn, kv, _dir) = setup_test_db_and_kv()?;

        let changes = OsmChange {
            ways: vec![WayChange {
                action: ChangeAction::Delete,
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
                id: 960,
                node_refs: vec![1, 2, 3, 4, 1],
                tags: vec![("building".into(), "yes".into())],
            }],
            ..Default::default()
        };
        let seq2 = OsmChange {
            nodes: vec![NodeChange {
                action: ChangeAction::Modify,
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
        let kv = Arc::new(kvstore::open(tmpdir.path(), 8, 4).unwrap());
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
        let last_applied = AtomicU64::new(0);
        let mut apply_errors: Vec<String> = Vec::new();
        for batch_idx in 0..10u64 {
            let seqs: Vec<FetchedSequence> = (0..3u64)
                .map(|i| {
                    let seq = batch_idx * 3 + i;
                    synthetic_building_sequence(seq, 20.0 + seq as f64 * 0.01, 40.0)
                })
                .collect();
            if let Err(e) = apply_batch(&conn, &kv, &seqs, "2024-01-01T00:00:00Z", &last_applied) {
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
                id: n(1),
                lon: lon0,
                lat: lat0,
                tags: vec![],
            },
            NodeChange {
                action: ChangeAction::Create,
                id: n(2),
                lon: lon0 + d,
                lat: lat0,
                tags: vec![],
            },
            NodeChange {
                action: ChangeAction::Create,
                id: n(3),
                lon: lon0 + d,
                lat: lat0 + d,
                tags: vec![],
            },
            NodeChange {
                action: ChangeAction::Create,
                id: n(4),
                lon: lon0,
                lat: lat0 + d,
                tags: vec![],
            },
        ];
        let way = WayChange {
            action: ChangeAction::Create,
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
    /// `GET /state.txt` with `state_body` and every other GET with
    /// `osc_gz_body`, forever, on however many connections arrive. Needed
    /// here because a single `update()` run now makes many requests --
    /// `state.txt` once, plus one per distinct sequence from whichever of
    /// the prefetch thread or the apply loop reaches it first, potentially
    /// both for one sequence if they race.
    fn spawn_replication_mock_server(
        state_body: String,
        osc_gz_body: Vec<u8>,
    ) -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
        use std::io::Write as _;
        use std::sync::atomic::AtomicUsize;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let count_for_thread = request_count.clone();

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                count_for_thread.fetch_add(1, Ordering::SeqCst);
                let state_body = state_body.clone();
                let osc_gz_body = osc_gz_body.clone();
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let is_state = request.starts_with("GET /state.txt");

                    if is_state {
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

        (addr, request_count)
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
        let state_body =
            format!("sequenceNumber={LATEST_SEQ}\ntimestamp=2024-01-01T00\\:00\\:00Z\n");
        let (addr, request_count) = spawn_replication_mock_server(state_body, osc_gz_body);
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

        // The exists-check dedup must keep the prefetcher and the apply loop
        // from each downloading every sequence independently: at most 1
        // (state.txt) + PENDING (every sequence at most once) + a small
        // slack for a genuine prefetch/apply race landing on the same
        // sequence at the same time.
        let requests = request_count.load(Ordering::SeqCst);
        assert!(
            requests <= 1 + PENDING as usize + 5,
            "too many requests ({requests}) for {PENDING} pending sequences -- the \
             exists-check dedup between the prefetcher and the apply loop looks broken"
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
}
