//! Background job scheduler used by the HTTP server.
//!
//! See plan docs/ for design rationale. Headline rules:
//! - The supervisor awaits the previous run's `JoinHandle` before starting
//!   the next tick, so runs of a given job never overlap.
//! - On timeout we record `TimedOut` but KEEP awaiting the blocking handle
//!   to preserve no-overlap (`spawn_blocking` cannot be aborted).

pub mod export_log_prune;
pub mod osm_update;
pub mod status_handler;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use duckdb::Connection;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use crate::config::{Config as AppConfig, JobConfig};
use crate::osm::kvstore::RocksDB;

/// Per-job-run context handed to the blocking closure.
///
/// `cancel` is set by the supervisor on timeout or shutdown. Jobs SHOULD poll
/// it (via [`JobContext::is_cancelled`]) periodically and return early when
/// it is true. The bundled OSM update job relies on the global shutdown flag
/// instead, so `cancel` and `is_cancelled` are part of the public contract for
/// future jobs that want cooperative cancellation.
#[derive(Clone)]
pub struct JobContext {
    pub write: Arc<StdMutex<Connection>>,
    pub kv: Arc<RocksDB>,
    pub config: Arc<AppConfig>,
    #[allow(dead_code)]
    pub cancel: Arc<AtomicBool>,
}

impl JobContext {
    #[allow(dead_code)]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

pub trait Job: Send + Sync + 'static {
    /// Stable identifier; used as registry key and TOML config key
    /// (e.g. "osm_update" → `[jobs.osm_update]`).
    fn name(&self) -> &'static str;

    /// The actual work. Runs inside `spawn_blocking`. Must be sync. Should
    /// honor `ctx.is_cancelled()` periodically when reasonable.
    fn run(&self, ctx: &JobContext) -> Result<()>;
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "message")]
pub enum JobOutcome {
    Success,
    Error(String),
    TimedOut,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Idle,
    Running,
    Disabled,
}

#[derive(Clone, Debug, Serialize)]
pub struct JobStatus {
    pub name: &'static str,
    pub enabled: bool,
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    pub state: JobState,
    pub last_started_at: Option<String>,
    pub last_finished_at: Option<String>,
    pub last_duration_ms: Option<u64>,
    pub last_outcome: Option<JobOutcome>,
    pub next_run_at: Option<String>,
    pub run_count: u64,
}

/// In-memory registry shared between supervisors and the `/status` handler.
pub struct JobRegistry {
    order: Vec<&'static str>,
    entries: HashMap<&'static str, StdMutex<JobStatus>>,
}

impl JobRegistry {
    fn new() -> Self {
        Self {
            order: Vec::new(),
            entries: HashMap::new(),
        }
    }

    fn insert(&mut self, status: JobStatus) {
        self.order.push(status.name);
        self.entries.insert(status.name, StdMutex::new(status));
    }

    /// Snapshot the full registry in registration order. Stable JSON.
    pub fn snapshot(&self) -> Vec<JobStatus> {
        self.order
            .iter()
            .map(|name| self.entries[name].lock().unwrap().clone())
            .collect()
    }

    pub(crate) fn update<F: FnOnce(&mut JobStatus)>(&self, name: &'static str, f: F) {
        let mut s = self.entries[name].lock().unwrap();
        f(&mut s);
    }

    #[cfg(test)]
    pub fn new_for_tests(initial: Vec<JobStatus>) -> Self {
        let mut r = Self::new();
        for s in initial {
            r.insert(s);
        }
        r
    }
}

/// Per-job resolved config (defaults already applied).
#[derive(Clone, Debug)]
pub struct JobConfigResolved {
    pub enabled: bool,
    pub interval: Duration,
    pub timeout: Duration,
}

impl From<&JobConfig> for JobConfigResolved {
    fn from(c: &JobConfig) -> Self {
        Self {
            enabled: c.enabled,
            interval: Duration::from_secs(c.interval_seconds),
            timeout: Duration::from_secs(c.timeout_seconds),
        }
    }
}

/// Owns the running supervisor tasks. Returned from `Scheduler::start`,
/// joined by the server during graceful shutdown.
pub struct Scheduler {
    pub registry: Arc<JobRegistry>,
    join_set: JoinSet<()>,
    stop_flags: Vec<Arc<AtomicBool>>,
    cancel_flags: Vec<Arc<AtomicBool>>,
    shutdown_notify: Arc<Notify>,
}

impl Scheduler {
    pub fn start(
        jobs: Vec<(Arc<dyn Job>, JobConfigResolved)>,
        write: Arc<StdMutex<Connection>>,
        kv: Arc<RocksDB>,
        config: Arc<AppConfig>,
    ) -> Self {
        let mut registry = JobRegistry::new();
        for (job, cfg) in &jobs {
            let initial_state = if cfg.enabled {
                JobState::Idle
            } else {
                JobState::Disabled
            };
            registry.insert(JobStatus {
                name: job.name(),
                enabled: cfg.enabled,
                interval_seconds: cfg.interval.as_secs(),
                timeout_seconds: cfg.timeout.as_secs(),
                state: initial_state,
                last_started_at: None,
                last_finished_at: None,
                last_duration_ms: None,
                last_outcome: None,
                next_run_at: None,
                run_count: 0,
            });
        }
        let registry = Arc::new(registry);
        let shutdown_notify = Arc::new(Notify::new());
        let mut join_set = JoinSet::new();
        let mut stop_flags = Vec::new();
        let mut cancel_flags = Vec::new();

        for (job, cfg) in jobs {
            if !cfg.enabled {
                continue;
            }
            let stop = Arc::new(AtomicBool::new(false));
            let cancel = Arc::new(AtomicBool::new(false));
            stop_flags.push(stop.clone());
            cancel_flags.push(cancel.clone());

            let fut = supervise(
                job,
                cfg,
                registry.clone(),
                shutdown_notify.clone(),
                stop,
                cancel,
                write.clone(),
                kv.clone(),
                config.clone(),
            );
            join_set.spawn(fut);
        }

        Self {
            registry,
            join_set,
            stop_flags,
            cancel_flags,
            shutdown_notify,
        }
    }

    /// Handle the server's graceful-shutdown path uses to wake supervisors.
    pub fn shutdown_notify(&self) -> Arc<Notify> {
        self.shutdown_notify.clone()
    }

    /// Signal all supervisors to exit and wait up to `grace` for in-flight
    /// jobs to drain. After `grace`, the `JoinSet` is dropped and any
    /// remaining blocking work keeps running on the runtime until the
    /// runtime itself is dropped on process exit.
    pub async fn shutdown(mut self, grace: Duration) {
        for f in &self.stop_flags {
            f.store(true, Ordering::SeqCst);
        }
        for f in &self.cancel_flags {
            f.store(true, Ordering::SeqCst);
        }
        self.shutdown_notify.notify_waiters();

        let drain = async { while self.join_set.join_next().await.is_some() {} };
        if tokio::time::timeout(grace, drain).await.is_err() {
            warn!(
                grace_s = grace.as_secs(),
                "scheduler shutdown grace exceeded; dropping JoinSet"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn supervise(
    job: Arc<dyn Job>,
    cfg: JobConfigResolved,
    registry: Arc<JobRegistry>,
    shutdown_notify: Arc<Notify>,
    stop: Arc<AtomicBool>,
    cancel: Arc<AtomicBool>,
    write: Arc<StdMutex<Connection>>,
    kv: Arc<RocksDB>,
    config: Arc<AppConfig>,
) {
    let name = job.name();
    let mut ticker = tokio::time::interval(cfg.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        // Wait for next tick or shutdown. On shutdown notify we loop back
        // and re-check `stop` at the top.
        tokio::select! {
            biased;
            _ = shutdown_notify.notified() => continue,
            _ = ticker.tick() => {}
        }

        if stop.load(Ordering::SeqCst) {
            break;
        }

        // Reset per-run cancel (might be set from a previous timeout drain).
        cancel.store(false, Ordering::SeqCst);

        let started_sys = SystemTime::now();
        let started_inst = Instant::now();
        registry.update(name, |s| {
            s.state = JobState::Running;
            s.last_started_at = Some(format_rfc3339(started_sys));
            s.next_run_at = None;
        });

        let ctx = JobContext {
            write: write.clone(),
            kv: kv.clone(),
            config: config.clone(),
            cancel: cancel.clone(),
        };
        let job_clone = job.clone();
        let mut handle = tokio::task::spawn_blocking(move || job_clone.run(&ctx));

        let outcome = match tokio::time::timeout(cfg.timeout, &mut handle).await {
            Ok(Ok(Ok(()))) => JobOutcome::Success,
            Ok(Ok(Err(e))) => JobOutcome::Error(format!("{e:#}")),
            Ok(Err(join_err)) => JobOutcome::Error(format!("task panicked: {join_err}")),
            Err(_elapsed) => {
                cancel.store(true, Ordering::SeqCst);
                warn!(
                    job = name,
                    timeout_s = cfg.timeout.as_secs(),
                    "job timed out; awaiting handle drain"
                );
                // MUST wait — abandoning the handle would let the next tick
                // start a second writer and violate no-overlap.
                match (&mut handle).await {
                    Ok(Ok(())) => {
                        tracing::info!(job = name, "job completed AFTER timeout was recorded");
                    }
                    Ok(Err(e)) => {
                        tracing::info!(job = name, error = %e, "job errored after timeout");
                    }
                    Err(e) => {
                        tracing::error!(job = name, error = %e, "job panicked after timeout");
                    }
                }
                JobOutcome::TimedOut
            }
        };

        let finished_sys = SystemTime::now();
        let elapsed_ms = started_inst.elapsed().as_millis() as u64;
        let next = finished_sys + cfg.interval;
        registry.update(name, |s| {
            s.state = JobState::Idle;
            s.last_finished_at = Some(format_rfc3339(finished_sys));
            s.last_duration_ms = Some(elapsed_ms);
            s.last_outcome = Some(outcome);
            s.next_run_at = Some(format_rfc3339(next));
            s.run_count += 1;
        });
    }
}

/// Format a `SystemTime` as RFC3339 UTC, e.g. "2026-05-28T12:34:56Z".
/// Pre-epoch times clamp to UNIX_EPOCH.
pub(crate) fn format_rfc3339(t: SystemTime) -> String {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let secs = dur.as_secs() as i64;
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let hour = sod / 3600;
    let minute = (sod / 60) % 60;
    let second = sod % 60;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Howard Hinnant's civil-from-days. Returns (year, month [1-12], day [1-31]).
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;

    fn make_parts() -> (
        Arc<StdMutex<Connection>>,
        Arc<RocksDB>,
        Arc<AppConfig>,
        tempfile::TempDir,
    ) {
        let conn = Connection::open_in_memory().expect("in-memory duckdb");
        let dir = tempfile::tempdir().expect("tempdir");
        let kv = Arc::new(crate::osm::kvstore::open(dir.path(), 8, 4).expect("kvstore open"));
        let cfg = Arc::new(AppConfig::default());
        (Arc::new(StdMutex::new(conn)), kv, cfg, dir)
    }

    // ---- pure-function tests ----

    #[test]
    fn format_rfc3339_epoch() {
        assert_eq!(format_rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn format_rfc3339_known_dates() {
        // 2021-01-01T00:00:00Z = 1609459200
        let t = UNIX_EPOCH + Duration::from_secs(1_609_459_200);
        assert_eq!(format_rfc3339(t), "2021-01-01T00:00:00Z");

        // 2026-05-28T12:34:56Z = 1779971696
        let t = UNIX_EPOCH + Duration::from_secs(1_779_971_696);
        assert_eq!(format_rfc3339(t), "2026-05-28T12:34:56Z");

        // 2000-02-29T23:59:59Z (leap day) = 951868799
        let t = UNIX_EPOCH + Duration::from_secs(951_868_799);
        assert_eq!(format_rfc3339(t), "2000-02-29T23:59:59Z");
    }

    #[test]
    fn outcome_serialization() {
        let s = serde_json::to_value(JobOutcome::Success).unwrap();
        assert_eq!(s, serde_json::json!({"kind": "Success"}));

        let e = serde_json::to_value(JobOutcome::Error("boom".to_string())).unwrap();
        assert_eq!(e, serde_json::json!({"kind": "Error", "message": "boom"}));

        let t = serde_json::to_value(JobOutcome::TimedOut).unwrap();
        assert_eq!(t, serde_json::json!({"kind": "TimedOut"}));
    }

    #[test]
    fn registry_preserves_registration_order() {
        let mut r = JobRegistry::new();
        for name in ["zulu", "alpha", "mike"] {
            r.insert(JobStatus {
                name,
                enabled: true,
                interval_seconds: 1,
                timeout_seconds: 1,
                state: JobState::Idle,
                last_started_at: None,
                last_finished_at: None,
                last_duration_ms: None,
                last_outcome: None,
                next_run_at: None,
                run_count: 0,
            });
        }
        let snap = r.snapshot();
        assert_eq!(
            snap.iter().map(|s| s.name).collect::<Vec<_>>(),
            vec!["zulu", "alpha", "mike"]
        );
    }

    // ---- supervisor behavior tests ----

    /// A scripted job. Each call: increments call_count, then sleeps `sleep_each`
    /// while honoring cancel, then returns the (i % outcomes.len()) outcome.
    struct ScriptedJob {
        name: &'static str,
        sleep_each: Duration,
        outcomes: Vec<std::result::Result<(), String>>,
        call_count: Arc<AtomicUsize>,
        current: Arc<AtomicUsize>,
        max_concurrent: Arc<AtomicUsize>,
    }

    impl Job for ScriptedJob {
        fn name(&self) -> &'static str {
            self.name
        }
        fn run(&self, ctx: &JobContext) -> Result<()> {
            let i = self.call_count.fetch_add(1, Ordering::SeqCst);
            let cur = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            // Track max concurrency (compare-exchange loop).
            let mut prev = self.max_concurrent.load(Ordering::SeqCst);
            while cur > prev {
                match self.max_concurrent.compare_exchange(
                    prev,
                    cur,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(p) => prev = p,
                }
            }

            // Sleep in small slices, honoring cancel.
            let slice = Duration::from_millis(20);
            let mut left = self.sleep_each;
            while left > Duration::ZERO {
                if ctx.is_cancelled() {
                    break;
                }
                let s = left.min(slice);
                std::thread::sleep(s);
                left = left.saturating_sub(s);
            }

            self.current.fetch_sub(1, Ordering::SeqCst);

            match self
                .outcomes
                .get(i % self.outcomes.len())
                .cloned()
                .unwrap_or(Ok(()))
            {
                Ok(()) => Ok(()),
                Err(e) => Err(anyhow::anyhow!("{e}")),
            }
        }
    }

    fn make_registry_for(name: &'static str, cfg: &JobConfigResolved) -> Arc<JobRegistry> {
        let mut r = JobRegistry::new();
        r.insert(JobStatus {
            name,
            enabled: cfg.enabled,
            interval_seconds: cfg.interval.as_secs(),
            timeout_seconds: cfg.timeout.as_secs(),
            state: if cfg.enabled {
                JobState::Idle
            } else {
                JobState::Disabled
            },
            last_started_at: None,
            last_finished_at: None,
            last_duration_ms: None,
            last_outcome: None,
            next_run_at: None,
            run_count: 0,
        });
        Arc::new(r)
    }

    async fn wait_until<F: Fn() -> bool>(cond: F, timeout: Duration) -> bool {
        let start = Instant::now();
        while !cond() {
            if start.elapsed() > timeout {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supervisor_records_success() {
        let (w, kv, cfg, _dir) = make_parts();
        let job_cfg = JobConfigResolved {
            enabled: true,
            interval: Duration::from_millis(50),
            timeout: Duration::from_secs(5),
        };
        let call_count = Arc::new(AtomicUsize::new(0));
        let job = Arc::new(ScriptedJob {
            name: "test_success",
            sleep_each: Duration::from_millis(5),
            outcomes: vec![Ok(())],
            call_count: call_count.clone(),
            current: Arc::new(AtomicUsize::new(0)),
            max_concurrent: Arc::new(AtomicUsize::new(0)),
        });
        let registry = make_registry_for("test_success", &job_cfg);
        let notify = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let cancel = Arc::new(AtomicBool::new(false));

        let handle = tokio::spawn(supervise(
            job,
            job_cfg,
            registry.clone(),
            notify.clone(),
            stop.clone(),
            cancel,
            w,
            kv,
            cfg,
        ));

        let cc = call_count.clone();
        assert!(
            wait_until(|| cc.load(Ordering::SeqCst) >= 2, Duration::from_secs(2)).await,
            "expected at least 2 runs"
        );

        stop.store(true, Ordering::SeqCst);
        notify.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        let snap = &registry.snapshot()[0];
        assert!(snap.run_count >= 1);
        assert_eq!(snap.last_outcome, Some(JobOutcome::Success));
        assert_eq!(snap.state, JobState::Idle);
        assert!(snap.last_started_at.is_some());
        assert!(snap.last_finished_at.is_some());
        assert!(snap.next_run_at.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supervisor_records_error_and_keeps_running() {
        let (w, kv, cfg, _dir) = make_parts();
        let job_cfg = JobConfigResolved {
            enabled: true,
            interval: Duration::from_millis(50),
            timeout: Duration::from_secs(5),
        };
        let call_count = Arc::new(AtomicUsize::new(0));
        let job = Arc::new(ScriptedJob {
            name: "test_error",
            sleep_each: Duration::from_millis(5),
            outcomes: vec![Err("boom".into())],
            call_count: call_count.clone(),
            current: Arc::new(AtomicUsize::new(0)),
            max_concurrent: Arc::new(AtomicUsize::new(0)),
        });
        let registry = make_registry_for("test_error", &job_cfg);
        let notify = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let cancel = Arc::new(AtomicBool::new(false));

        let handle = tokio::spawn(supervise(
            job,
            job_cfg,
            registry.clone(),
            notify.clone(),
            stop.clone(),
            cancel,
            w,
            kv,
            cfg,
        ));

        let cc = call_count.clone();
        assert!(wait_until(|| cc.load(Ordering::SeqCst) >= 2, Duration::from_secs(2)).await);

        stop.store(true, Ordering::SeqCst);
        notify.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        let snap = &registry.snapshot()[0];
        assert!(
            snap.run_count >= 2,
            "supervisor must keep ticking after errors"
        );
        match &snap.last_outcome {
            Some(JobOutcome::Error(m)) => assert!(m.contains("boom"), "got: {m}"),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supervisor_records_timeout_without_overlap() {
        let (w, kv, cfg, _dir) = make_parts();
        let job_cfg = JobConfigResolved {
            enabled: true,
            interval: Duration::from_millis(50),
            timeout: Duration::from_millis(100),
        };
        let call_count = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let job = Arc::new(ScriptedJob {
            name: "test_timeout",
            // Sleep 400ms — well beyond the 100ms timeout. Honors cancel so
            // the drain after timeout completes quickly.
            sleep_each: Duration::from_millis(400),
            outcomes: vec![Ok(())],
            call_count: call_count.clone(),
            current: current.clone(),
            max_concurrent: max_concurrent.clone(),
        });
        let registry = make_registry_for("test_timeout", &job_cfg);
        let notify = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let cancel = Arc::new(AtomicBool::new(false));

        let handle = tokio::spawn(supervise(
            job,
            job_cfg,
            registry.clone(),
            notify.clone(),
            stop.clone(),
            cancel,
            w,
            kv,
            cfg,
        ));

        let cc = call_count.clone();
        assert!(
            wait_until(|| cc.load(Ordering::SeqCst) >= 2, Duration::from_secs(3)).await,
            "expected at least 2 runs"
        );

        stop.store(true, Ordering::SeqCst);
        notify.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        let snap = &registry.snapshot()[0];
        assert_eq!(snap.last_outcome, Some(JobOutcome::TimedOut));
        assert_eq!(
            max_concurrent.load(Ordering::SeqCst),
            1,
            "two runs of the same job must never be in flight at once"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supervisor_honors_shutdown_during_idle() {
        let (w, kv, cfg, _dir) = make_parts();
        // Long interval — supervisor will spend most of its time in select.
        let job_cfg = JobConfigResolved {
            enabled: true,
            interval: Duration::from_secs(60),
            timeout: Duration::from_secs(5),
        };
        let job = Arc::new(ScriptedJob {
            name: "test_shutdown",
            sleep_each: Duration::from_millis(5),
            outcomes: vec![Ok(())],
            call_count: Arc::new(AtomicUsize::new(0)),
            current: Arc::new(AtomicUsize::new(0)),
            max_concurrent: Arc::new(AtomicUsize::new(0)),
        });
        let registry = make_registry_for("test_shutdown", &job_cfg);
        let notify = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let cancel = Arc::new(AtomicBool::new(false));

        let handle = tokio::spawn(supervise(
            job,
            job_cfg,
            registry,
            notify.clone(),
            stop.clone(),
            cancel,
            w,
            kv,
            cfg,
        ));

        // Give the first immediate tick time to complete.
        tokio::time::sleep(Duration::from_millis(200)).await;

        stop.store(true, Ordering::SeqCst);
        notify.notify_waiters();

        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(res.is_ok(), "supervisor did not exit within 2s of shutdown");
    }

    // Silence "field is never read" if any test struct goes unused.
    #[allow(dead_code)]
    fn _check_path_used() -> &'static Path {
        Path::new(".")
    }
}
