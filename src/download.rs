use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::runtime::Runtime;
use tracing::{debug, info};

const MAX_RETRIES: u32 = 3;

/// Worker-thread count for the shared download runtime (see
/// [`download_runtime`]). Small and fixed: this runtime only ever drives
/// HTTP downloads, never CPU-bound work, so a handful of threads is enough
/// to let a few downloads make real concurrent progress without paying for
/// a large pool that would sit mostly idle. More than one thread is what
/// makes it worth being multi-thread at all -- see the doc comment below.
const DOWNLOAD_RUNTIME_WORKER_THREADS: usize = 4;

/// The single background-download Tokio runtime, shared by every call in
/// this module instead of each building its own.
///
/// Before this, `download_file_impl`, `download_file_as_impl` and
/// `fetch_etag` each constructed a fresh `Runtime::new()` per call. During
/// an `osm_update` run that replays roughly 1440 minutely replication
/// sequences serially (`update::osm::apply_sequence`, one download per
/// sequence), that was ~1440 runtime spin-ups. Turning this into a
/// `OnceLock` singleton is behaviour-neutral: every existing call site
/// already did exactly one `block_on` per invocation on its own private
/// runtime, so nothing about *when* work runs or *which thread* awaits it
/// changes -- only that the runtime (and its worker-thread pool) is now
/// built once and reused.
///
/// **Must be multi-thread, not `new_current_thread()`.** `do_download` does
/// blocking `std::fs::File::create`/`write_all` calls inside an `async fn`
/// (see its body below, roughly lines 165-178). On a current-thread runtime
/// those block the one and only thread driving the runtime, so two
/// downloads sharing that runtime would serialise on file I/O; multi-thread
/// lets them actually overlap. This matters because several jobs can
/// download concurrently: the `refresh_lock` in
/// `server::jobs::dataset_update` does NOT cover `osm_update`, so a
/// government-dataset refresh and the OSM replication catch-up can both be
/// downloading through this runtime at once. `rt-multi-thread` is already
/// enabled for the `tokio` dependency in `Cargo.toml` (verified), so no
/// `Cargo.toml` change is needed to use `Builder::new_multi_thread()` here.
///
/// **Panics if called from a thread that is currently *driving* async tasks**
/// -- `Runtime::block_on` bails with "Cannot start a runtime from within a
/// runtime" rather than deadlocking the executor. Every current caller is
/// safe, but for two different reasons, and the distinction matters:
///
/// - The CLI (`src/main.rs`) is synchronous except for `Command::Run`, which
///   builds its own runtime for the HTTP server and never calls into this
///   module from inside it. No runtime context at all.
/// - The server-side background jobs run via `tokio::task::spawn_blocking`
///   (`src/server/jobs/mod.rs:311`). A blocking-pool thread **does** carry an
///   ambient runtime context -- `Handle::current()` succeeds there, so "no
///   ambient runtime" is the wrong mental model -- but it is not entered as a
///   runtime *driver*, which is the condition `block_on` actually checks. So
///   `block_on` on this separate runtime is permitted there.
///   `download_from_inside_spawn_blocking_is_permitted` pins both halves of
///   that empirically, because getting it wrong in either direction (assuming
///   it panics, or assuming there is no context) would send a future reader
///   down the wrong path.
///
/// A future caller invoked directly from an async handler on the server's own
/// runtime *would* panic here -- it would need `spawn_blocking` too.
///
/// Uses `OnceLock::get_or_init`, whose init closure is infallible, so a
/// failure to build the runtime becomes a panic rather than a propagated
/// `Result`. Accepted trade-off: on the job path, `supervise()` already
/// converts a panicking job into `JobOutcome::Error` instead of taking down
/// the server (`src/server/jobs/mod.rs:316`: `Ok(Err(join_err)) =>
/// JobOutcome::Error(format!("task panicked: {join_err}"))`, verified). On
/// the CLI path a failure here means the process cannot do networking at
/// all, which was already an unrecoverable startup condition.
fn download_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(DOWNLOAD_RUNTIME_WORKER_THREADS)
            .enable_all()
            .build()
            .expect("failed to build shared download runtime")
    })
}

/// The single `reqwest::Client` shared by every download and `fetch_etag`
/// call in this module, instead of each building its own.
///
/// `Client` is `Arc`-backed internally and `Send + Sync`, so sharing one
/// across threads is safe and is the entire point of this change: reuse is
/// what buys connection pooling and TLS session reuse across the ~1440
/// sequential downloads an `osm_update` run makes, instead of every single
/// one paying a fresh TCP+TLS handshake and rustls root-store build. See
/// [`download_runtime`]'s doc comment for why several downloads can be
/// concurrent (`refresh_lock` does not cover `osm_update`), which is also
/// why a plain `Client`, not a `Mutex<Client>`, is the right shape here.
fn download_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Download a file from `url` to `dest_dir`, returning the path to the downloaded file.
/// Uses exponential backoff on failure.
pub fn download_file(url: &str, dest_dir: &Path) -> Result<PathBuf> {
    download_file_impl(url, dest_dir, true)
}

/// Same as [`download_file`], but without a progress bar. For callers that
/// download many small files in a loop (e.g. OSM replication diffs), where a
/// bar per file is just noise rather than useful progress.
pub fn download_file_quiet(url: &str, dest_dir: &Path) -> Result<PathBuf> {
    download_file_impl(url, dest_dir, false)
}

fn download_file_impl(url: &str, dest_dir: &Path, show_progress: bool) -> Result<PathBuf> {
    let file_name = url
        .rsplit('/')
        .next()
        .context("Could not extract filename from URL")?;
    let dest_path = dest_dir.join(file_name);

    if dest_path.exists() {
        if show_progress {
            info!(path = %dest_path.display(), "File already exists, skipping download");
        } else {
            debug!(path = %dest_path.display(), "File already exists, skipping download");
        }
        return Ok(dest_path);
    }

    std::fs::create_dir_all(dest_dir)
        .with_context(|| format!("Failed to create directory {dest_dir:?}"))?;

    let rt = download_runtime();
    rt.block_on(download_with_retry(
        download_client(),
        url,
        &dest_path,
        show_progress,
    ))?;

    Ok(dest_path)
}

/// Download a file from `url` to `dest_dir` with an explicit `file_name`.
/// Useful when the URL doesn't contain a clean filename (e.g. query-string URLs).
pub fn download_file_as(url: &str, dest_dir: &Path, file_name: &str) -> Result<PathBuf> {
    download_file_as_impl(url, dest_dir, file_name, true)
}

/// Same as [`download_file_as`], but without a progress bar. For callers
/// that only ever run as a background job (e.g. the street-name/building-type
/// mapping refreshes), where a redrawn bar just spams the log. See
/// [`download_file_quiet`].
pub fn download_file_as_quiet(url: &str, dest_dir: &Path, file_name: &str) -> Result<PathBuf> {
    download_file_as_impl(url, dest_dir, file_name, false)
}

fn download_file_as_impl(
    url: &str,
    dest_dir: &Path,
    file_name: &str,
    show_progress: bool,
) -> Result<PathBuf> {
    let dest_path = dest_dir.join(file_name);

    if dest_path.exists() {
        if show_progress {
            info!(path = %dest_path.display(), "File already exists, skipping download");
        } else {
            debug!(path = %dest_path.display(), "File already exists, skipping download");
        }
        return Ok(dest_path);
    }

    std::fs::create_dir_all(dest_dir)
        .with_context(|| format!("Failed to create directory {dest_dir:?}"))?;

    let rt = download_runtime();
    rt.block_on(download_with_retry(
        download_client(),
        url,
        &dest_path,
        show_progress,
    ))?;

    Ok(dest_path)
}

async fn download_with_retry(
    client: &reqwest::Client,
    url: &str,
    dest_path: &Path,
    show_progress: bool,
) -> Result<()> {
    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        if show_progress {
            info!(url, attempt, "Downloading");
        } else {
            debug!(url, attempt, "Downloading");
        }

        match do_download(client, url, dest_path, show_progress).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::warn!(attempt, error = %e, "Download failed");
                last_error = Some(e);
                // No cleanup needed here, unlike before: `do_download` now
                // streams each attempt to its own uniquely-named temp file
                // (see `temp_dest_path`) and removes *that* on failure, never
                // touching `dest_path` until a successful rename. This must
                // NOT delete `dest_path` on the way out any more -- with a
                // prefetcher (`update::osm`'s bounded-window prefetch
                // thread), the apply loop and the prefetcher can race to
                // download the same sequence, so by the time one attempt
                // fails here `dest_path` may already be a *different*
                // writer's completed download.
                if attempt < MAX_RETRIES {
                    // Checked BEFORE the backoff sleep, not after: a
                    // government snapshot download is multi-GB, so a failed
                    // attempt can itself have taken a long time, and by then
                    // a shutdown may already be pending. Without this check
                    // here, Ctrl+C during that window would still sit
                    // through up to 2^attempt seconds of backoff before even
                    // starting the next (doomed) attempt. Bails with an Err,
                    // matching `import::osm::import`'s `check_shutdown`
                    // convention -- a download abandoned mid-retry is a
                    // failed download, not a silently-accepted partial one.
                    crate::shutdown::check_requested()?;
                    let delay = std::time::Duration::from_secs(2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap()).context(format!(
        "Failed to download {url} after {MAX_RETRIES} attempts"
    ))
}

/// Process-wide counter for building a unique temp-file name per download
/// attempt (see [`temp_dest_path`]).
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temp path in the same directory as `dest_path`, so the rename in
/// `do_download` that follows a successful download is a same-filesystem
/// (and therefore atomic, per `std::fs::rename`'s guarantee) move rather than
/// a cross-filesystem copy.
///
/// Includes both the process id and a process-wide atomic counter: the pid
/// alone would not be enough, because two downloads *within the same
/// process* can now legitimately target the same `dest_path` at once (see
/// the doc comment on the call site in `do_download`).
fn temp_dest_path(dest_path: &Path) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = dest_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download");
    dest_path.with_file_name(format!("{file_name}.{}.{counter}.part", std::process::id()))
}

async fn do_download(
    client: &reqwest::Client,
    url: &str,
    dest_path: &Path,
    show_progress: bool,
) -> Result<()> {
    let response = client.get(url).send().await?.error_for_status()?;

    let total_size = response.content_length();

    let pb = if show_progress {
        Some(match total_size {
            Some(size) => {
                let pb = ProgressBar::new(size);
                pb.set_style(
                    ProgressStyle::with_template(
                        "{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
                    )
                    .unwrap()
                    .progress_chars("=>-"),
                );
                pb
            }
            None => {
                let pb = ProgressBar::new_spinner();
                pb.set_style(
                    ProgressStyle::with_template(
                        "{msg}\n{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec})",
                    )
                    .unwrap(),
                );
                pb
            }
        })
    } else {
        None
    };
    if let Some(pb) = &pb {
        pb.set_message(format!(
            "Downloading {}",
            dest_path.file_name().unwrap_or_default().to_string_lossy()
        ));
    }

    // Stream into a unique temp file rather than straight onto `dest_path`,
    // then rename onto `dest_path` only after a successful flush. Before a
    // prefetcher existed, nothing ever targeted the same `dest_path`
    // concurrently, so writing directly was safe. With one, the apply loop
    // advancing onto a sequence the prefetcher is still downloading is a
    // normal occurrence (see `update::osm`'s prefetch thread), and both
    // would otherwise stream into the same file and could interleave into
    // garbage. `std::fs::rename` is atomic within one filesystem (guaranteed
    // same filesystem here, since `temp_dest_path` places the temp file next
    // to `dest_path`), so the worst case becomes two complete downloads of
    // identical bytes, one of which simply loses the rename race.
    //
    // This also closes a pre-existing hole: writing straight onto
    // `dest_path` meant a crash mid-download left a partial file sitting
    // exactly where the exists-check (`download_file_as_impl`) would find it
    // and hand back as a "complete" download on the next run. Streaming to a
    // temp name means `dest_path` only ever holds a complete, flushed file.
    let temp_path = temp_dest_path(dest_path);
    match write_response_to_file(&temp_path, response, pb.as_ref()).await {
        Ok(()) => {
            std::fs::rename(&temp_path, dest_path)
                .with_context(|| format!("Failed to move {temp_path:?} to {dest_path:?}"))?;
        }
        Err(e) => {
            // Each attempt owns its own temp file (a fresh name every call),
            // so cleaning up here is enough -- see the comment on
            // `download_with_retry`'s error arm for why `dest_path` itself
            // must NOT be touched.
            let _ = std::fs::remove_file(&temp_path);
            return Err(e);
        }
    }

    if let Some(pb) = &pb {
        pb.finish_with_message(format!(
            "Downloaded {}",
            dest_path.file_name().unwrap_or_default().to_string_lossy()
        ));
    }
    if show_progress {
        info!(path = %dest_path.display(), "Download complete");
    } else {
        debug!(path = %dest_path.display(), "Download complete");
    }
    Ok(())
}

/// Create `temp_path`, stream `response`'s body into it, and flush. Split out
/// of `do_download` so the temp-file lifecycle around it (success: rename
/// onto the real destination; failure: remove the temp file, in
/// `do_download`) has one clear boundary: this function either leaves a
/// complete, flushed file at `temp_path`, or leaves nothing there at all.
async fn write_response_to_file(
    temp_path: &Path,
    response: reqwest::Response,
    pb: Option<&ProgressBar>,
) -> Result<()> {
    let file = std::fs::File::create(temp_path)
        .with_context(|| format!("Failed to create {temp_path:?}"))?;
    let mut writer = BufWriter::new(file);
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        // A government snapshot is multi-GB; without this, Ctrl+C during the
        // transfer would do nothing until the whole thing finished. Bails
        // with an Err rather than returning as if the download had
        // succeeded -- the caller (`do_download`) treats that exactly like
        // any other failed attempt: the temp file this function has been
        // writing into is removed on the way out, `dest_path` is left
        // untouched (see the comment on `download_with_retry`'s error arm
        // for why that matters with a concurrent prefetcher), and the
        // caller sees a genuine error instead of silently keeping a
        // truncated "successful" download.
        crate::shutdown::check_requested()?;
        let chunk = chunk.context("Error reading download stream")?;
        writer
            .write_all(&chunk)
            .with_context(|| format!("Failed to write to {temp_path:?}"))?;
        if let Some(pb) = pb {
            pb.inc(chunk.len() as u64);
        }
    }

    writer
        .flush()
        .with_context(|| format!("Failed to flush {temp_path:?}"))?;
    Ok(())
}

/// Fetch a cheap change validator for `url` via HEAD: the `ETag` if the
/// server sends one, else `Last-Modified`, else `None`.
///
/// `None` means "cannot tell" and callers MUST treat it as changed — never
/// as unchanged — or a refresh could be skipped forever.
pub fn fetch_etag(url: &str) -> Result<Option<String>> {
    let rt = download_runtime();
    rt.block_on(do_fetch_etag(download_client(), url))
}

async fn do_fetch_etag(client: &reqwest::Client, url: &str) -> Result<Option<String>> {
    let response = client.head(url).send().await?.error_for_status()?;
    let headers = response.headers();
    let value = headers
        .get(reqwest::header::ETAG)
        .or_else(|| headers.get(reqwest::header::LAST_MODIFIED))
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn serve_and_download(body: &'static [u8], with_content_length: bool) -> Vec<u8> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        let body_len = body.len();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.readable().await;
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;

            let headers = if with_content_length {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {body_len}\r\nContent-Type: application/octet-stream\r\n\r\n"
                )
            } else {
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n"
                    .to_string()
            };
            tokio::io::AsyncWriteExt::write_all(&mut stream, headers.as_bytes())
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut stream, body)
                .await
                .unwrap();
        });

        let tmp = tempfile::tempdir().unwrap();
        let url = format!("http://{addr}/testfile.bin");
        // download_file uses block_on internally; call do_download directly to avoid nested runtime
        let client = reqwest::Client::new();
        let dest = tmp.path().join("testfile.bin");
        do_download(&client, &url, &dest, true).await.unwrap();
        std::fs::read(&dest).unwrap()
    }

    #[tokio::test]
    async fn download_with_known_size() {
        let data = b"hello world progress bar test data";
        let result = serve_and_download(data, true).await;
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn download_without_content_length() {
        let data = b"response without content-length header";
        let result = serve_and_download(data, false).await;
        assert_eq!(result, data);
    }

    async fn serve_head(header_line: &'static str) -> Option<String> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: std::net::SocketAddr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.readable().await;
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
            let resp = format!("HTTP/1.1 200 OK\r\n{header_line}Content-Length: 0\r\n\r\n");
            tokio::io::AsyncWriteExt::write_all(&mut stream, resp.as_bytes())
                .await
                .unwrap();
        });

        let client = reqwest::Client::new();
        do_fetch_etag(&client, &format!("http://{addr}/f.bin"))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn etag_header_is_preferred() {
        let v = serve_head("ETag: \"abc123\"\r\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n")
            .await;
        assert_eq!(v.as_deref(), Some("\"abc123\""));
    }

    #[tokio::test]
    async fn falls_back_to_last_modified() {
        let v = serve_head("Last-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n").await;
        assert_eq!(v.as_deref(), Some("Wed, 01 Jan 2025 00:00:00 GMT"));
    }

    #[tokio::test]
    async fn returns_none_when_server_offers_no_validator() {
        let v = serve_head("").await;
        assert_eq!(v, None);
    }

    // --- Shared runtime / shared client tests ---
    //
    // These exercise the public sync wrappers (`download_file_as_quiet`),
    // which internally call `download_runtime().block_on(...)`. That means
    // they must run as plain `#[test]` functions, NOT `#[tokio::test]`: a
    // `#[tokio::test]` body already runs inside a tokio runtime context on
    // its thread, and calling `block_on` on our separate shared runtime
    // from there would hit the exact "Cannot start a runtime from within a
    // runtime" panic documented on `download_runtime`. A bare `#[test]`
    // thread has no ambient runtime, matching the CLI/`spawn_blocking`
    // callers this code actually runs under. The mock servers below are
    // therefore built on blocking `std::net`, not `tokio::net`, so spinning
    // one up doesn't itself require a runtime either.

    /// One-shot blocking HTTP mock server: accepts exactly one connection,
    /// replies with `body`, and closes. Runs on its own OS thread.
    fn spawn_one_shot_test_server(body: &'static [u8]) -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = std::io::Read::read(&mut stream, &mut buf);
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = std::io::Write::write_all(&mut stream, headers.as_bytes());
                let _ = std::io::Write::write_all(&mut stream, body);
            }
        });
        addr
    }

    /// Like [`spawn_one_shot_test_server`], but replies to every connection
    /// it accepts (rather than just the first) and increments `count` once
    /// per accepted connection, so a test can assert on how many requests
    /// actually reached the network.
    fn spawn_counting_test_server(body: &'static [u8], count: Arc<AtomicUsize>) -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                count.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let _ = std::io::Read::read(&mut stream, &mut buf);
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = std::io::Write::write_all(&mut stream, headers.as_bytes());
                let _ = std::io::Write::write_all(&mut stream, body);
            }
        });
        addr
    }

    #[test]
    fn shared_runtime_block_on_works_from_a_bare_thread() {
        // Regression for the singleton conversion: the shared runtime must
        // still be usable via `block_on` from a plain `std::thread` with no
        // ambient tokio context, exactly like the CLI path (`src/main.rs`
        // is sync outside `Command::Run`) and the `spawn_blocking` closure
        // background jobs run in (`src/server/jobs/mod.rs:311`).
        let body: &'static [u8] = b"downloaded from a bare std::thread";
        let addr = spawn_one_shot_test_server(body);
        let tmp = tempfile::tempdir().unwrap();
        let dest_dir = tmp.path().to_path_buf();
        let url = format!("http://{addr}/f.bin");

        let handle =
            std::thread::spawn(move || download_file_as_quiet(&url, &dest_dir, "f.bin").unwrap());
        let path = handle.join().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), body);
    }

    /// The actual production path for every server-side download, and until
    /// now untested: background jobs call this module from inside
    /// `tokio::task::spawn_blocking` (`src/server/jobs/mod.rs:311`), i.e.
    /// from a thread owned by *another* runtime. Pins both halves of the
    /// claim in `download_runtime`'s doc comment, which are easy to get
    /// backwards:
    ///
    /// 1. a blocking-pool thread *does* carry an ambient runtime context
    ///    (`Handle::current()` succeeds) -- so "spawn_blocking gives you a
    ///    plain thread with no runtime context" is the wrong reason to
    ///    believe this works; and
    /// 2. `Runtime::block_on` on our separate runtime is nonetheless
    ///    permitted there, because the check is "is this thread driving
    ///    async tasks", not "does this thread have a runtime context".
    ///
    /// Reason 2 is what the pre-existing code already relied on (it built a
    /// fresh `Runtime` per call in exactly the same place), so this test
    /// documents a property the singleton conversion inherited rather than
    /// introduced.
    #[test]
    fn download_from_inside_spawn_blocking_is_permitted() {
        let body: &'static [u8] = b"downloaded from inside spawn_blocking";
        let addr = spawn_one_shot_test_server(body);
        let tmp = tempfile::tempdir().unwrap();
        let dest_dir = tmp.path().to_path_buf();
        let url = format!("http://{addr}/f.bin");

        let outer = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let (had_context, path) = outer.block_on(async move {
            tokio::task::spawn_blocking(move || {
                let had_context = tokio::runtime::Handle::try_current().is_ok();
                let path = download_file_as_quiet(&url, &dest_dir, "f.bin").unwrap();
                (had_context, path)
            })
            .await
            .unwrap()
        });

        assert!(
            had_context,
            "a spawn_blocking thread carries an ambient runtime context; if this ever \
             stops being true, download_runtime's doc comment needs rewriting"
        );
        assert_eq!(std::fs::read(&path).unwrap(), body);
    }

    #[test]
    fn concurrent_downloads_through_shared_client_both_succeed() {
        // Two threads download through the shared `download_runtime`/
        // `download_client` singletons at the same time. Distinct bodies
        // and distinct dest paths so a mix-up (e.g. a race writing through
        // a wrongly-shared file handle) would show up as wrong bytes on one
        // side, not just a hang or an error.
        let body_a: &'static [u8] = b"payload for thread A";
        let body_b: &'static [u8] = b"a different, longer payload for thread B";
        let addr_a = spawn_one_shot_test_server(body_a);
        let addr_b = spawn_one_shot_test_server(body_b);
        let tmp = tempfile::tempdir().unwrap();
        let dir_a = tmp.path().to_path_buf();
        let dir_b = tmp.path().to_path_buf();
        let url_a = format!("http://{addr_a}/a.bin");
        let url_b = format!("http://{addr_b}/b.bin");

        let t_a =
            std::thread::spawn(move || download_file_as_quiet(&url_a, &dir_a, "a.bin").unwrap());
        let t_b =
            std::thread::spawn(move || download_file_as_quiet(&url_b, &dir_b, "b.bin").unwrap());

        let path_a = t_a.join().unwrap();
        let path_b = t_b.join().unwrap();
        assert_eq!(std::fs::read(&path_a).unwrap(), body_a);
        assert_eq!(std::fs::read(&path_b).unwrap(), body_b);
    }

    #[test]
    fn existing_file_skips_download_entirely() {
        // Direct test of `download_file_as_impl`'s "already exists, skip"
        // branch, which was previously untested. A later prefetch step
        // leans on this branch entirely (a prefetched file must not be
        // re-downloaded when the sequential loop reaches it), so it needs
        // its own coverage regardless of the runtime/client sharing work
        // in this change.
        let request_count = Arc::new(AtomicUsize::new(0));
        let body: &'static [u8] = b"stable body, fetched at most once";
        let addr = spawn_counting_test_server(body, request_count.clone());
        let tmp = tempfile::tempdir().unwrap();
        let url = format!("http://{addr}/whatever");

        let first = download_file_as_quiet(&url, tmp.path(), "shared_name.bin").unwrap();
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        assert_eq!(std::fs::read(&first).unwrap(), body);

        let second = download_file_as_quiet(&url, tmp.path(), "shared_name.bin").unwrap();
        assert_eq!(second, first);
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            1,
            "second call must not hit the network at all since the file already exists on disk"
        );
    }

    /// Every entry in `dir` whose name ends in `.part` -- i.e. any leftover
    /// temp file from `temp_dest_path`.
    fn part_files_in(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".part"))
            .collect()
    }

    #[tokio::test]
    async fn successful_download_leaves_no_part_file_behind() {
        // Regression for the temp-file+rename change: `do_download` must
        // rename its temp file onto `dest_path` on success, not leave both
        // sitting in the directory.
        let data = b"a complete, successful download";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.readable().await;
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                data.len()
            );
            tokio::io::AsyncWriteExt::write_all(&mut stream, headers.as_bytes())
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut stream, data)
                .await
                .unwrap();
        });

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("ok.bin");
        let client = reqwest::Client::new();
        let url = format!("http://{addr}/ok.bin");
        do_download(&client, &url, &dest, false).await.unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), data);
        assert_eq!(
            part_files_in(tmp.path()),
            Vec::<String>::new(),
            "a successful download must not leave its temp file behind"
        );
    }

    #[tokio::test]
    async fn failed_download_does_not_touch_a_preexisting_dest_path() {
        // The server advertises a Content-Length larger than what it
        // actually sends, then drops the connection -- reqwest's body
        // stream surfaces the truncation as a read error partway through,
        // so this exercises `write_response_to_file`'s failure branch (and
        // therefore `do_download`'s temp-file cleanup on the way out), not
        // just an early `error_for_status` bail before any file is touched.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.readable().await;
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
            let headers = "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nContent-Type: application/octet-stream\r\n\r\n";
            tokio::io::AsyncWriteExt::write_all(&mut stream, headers.as_bytes())
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut stream, b"only a few bytes, not 1000")
                .await
                .unwrap();
            // Drop `stream` here (closing the connection) instead of sending
            // the rest of the advertised 1000 bytes.
        });

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("existing.bin");
        let preexisting: &'static [u8] = b"already-downloaded content that must survive";
        std::fs::write(&dest, preexisting).unwrap();

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/truncated.bin");
        let result = do_download(&client, &url, &dest, false).await;

        assert!(
            result.is_err(),
            "a truncated body must surface as an error, or this test isn't exercising the failure path"
        );
        assert_eq!(
            std::fs::read(&dest).unwrap(),
            preexisting,
            "a failed download must never touch a pre-existing dest_path -- with a \
             prefetcher, that path can be another writer's already-completed download"
        );
        assert_eq!(
            part_files_in(tmp.path()),
            Vec::<String>::new(),
            "a failed download must clean up its own temp file"
        );
    }
}
