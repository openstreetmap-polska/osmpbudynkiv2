use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};

use crate::cli::UpdateSource;
use crate::dataset::DatasetSpec;
use crate::server::jobs::{Job, JobContext};

/// Serializes the three dataset refreshes against each other.
///
/// The scheduler's supervisor only guarantees no overlap *per job*, so
/// without this all three could stage ~16M rows simultaneously against the
/// configured `memory_limit`. They are not latency-sensitive, so running
/// them one at a time costs nothing that matters.
fn refresh_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Background job that refreshes one government dataset from a fresh snapshot.
///
/// Delegates to [`crate::update::run`], passing `&|| ctx.is_cancelled()`
/// through to whichever update function the matched source needs it
/// (`dataset::refresh` for `Bdot10k`/`Egib`, `import::prg::update_prg` for
/// `Prg`). Both poll it -- alongside the process-global shutdown flag -- at
/// well-defined points before their apply transaction begins (see
/// `update::dataset::refresh`'s doc comment), so a supervisor timeout on a
/// long-running refresh actually stops it instead of only being recorded
/// after the fact once the whole staging + diff + apply sequence has already
/// run to completion on its own.
pub struct DatasetUpdateJob {
    spec: &'static DatasetSpec,
    name: &'static str,
}

impl DatasetUpdateJob {
    pub fn new(spec: &'static DatasetSpec, name: &'static str) -> Self {
        Self { spec, name }
    }

    fn source(&self) -> UpdateSource {
        match self.spec.name {
            "bdot10k" => UpdateSource::Bdot10k { file: None },
            "egib" => UpdateSource::Egib { file: None },
            "prg" => UpdateSource::Prg {
                file: None,
                terc_file: None,
            },
            other => unreachable!("unknown dataset spec {other}"),
        }
    }
}

impl Job for DatasetUpdateJob {
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        // Poisoning only means a previous refresh panicked; the lock guards
        // memory headroom, not shared state, so recovering is correct.
        let _guard = refresh_lock().lock().unwrap_or_else(|e| e.into_inner());

        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        crate::update::run(
            &conn,
            &ctx.kv,
            self.source(),
            &ctx.config,
            &ctx.config.download_urls,
            false,
            &|| ctx.is_cancelled(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{BDOT10K, EGIB, PRG};

    #[test]
    fn name_is_the_registry_key_not_the_spec_name() {
        let job = DatasetUpdateJob::new(&BDOT10K, "bdot10k_update");
        assert_eq!(job.name(), "bdot10k_update");
    }

    #[test]
    fn source_maps_each_spec_to_its_update_source_variant() {
        let job = DatasetUpdateJob::new(&BDOT10K, "bdot10k_update");
        assert!(matches!(job.source(), UpdateSource::Bdot10k { file: None }));

        let job = DatasetUpdateJob::new(&EGIB, "egib_update");
        assert!(matches!(job.source(), UpdateSource::Egib { file: None }));

        let job = DatasetUpdateJob::new(&PRG, "prg_update");
        assert!(matches!(
            job.source(),
            UpdateSource::Prg {
                file: None,
                terc_file: None
            }
        ));
    }

    /// The lock is a single process-wide `'static` instance shared across
    /// distinct `DatasetUpdateJob`s (rather than e.g. one per instance),
    /// which is what makes it able to serialize *different* sources against
    /// each other in the first place.
    #[test]
    fn refresh_lock_is_shared_across_all_dataset_jobs() {
        let a = refresh_lock();
        let b = refresh_lock();
        assert!(std::ptr::eq(a, b));
    }

    /// Proves the serialization actually happens: while one thread holds the
    /// lock (standing in for an in-flight refresh of, say, bdot10k), a
    /// concurrent attempt to acquire it (standing in for egib or prg's
    /// scheduled tick) must block rather than proceed. No network or DuckDB
    /// I/O involved — this isolates the mutex behavior `DatasetUpdateJob::run`
    /// relies on from everything else `run` does.
    #[test]
    fn refresh_lock_blocks_a_concurrent_acquisition() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (holder_ready_tx, holder_ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();

        let holder = std::thread::spawn(move || {
            let _guard = refresh_lock().lock().unwrap_or_else(|e| e.into_inner());
            holder_ready_tx.send(()).unwrap();
            // Hold the lock until the main thread has observed contention.
            let _ = release_rx.recv();
        });

        holder_ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("holder thread never acquired the lock");

        // The lock is held by `holder`, so a non-blocking attempt must fail.
        assert!(
            refresh_lock().try_lock().is_err(),
            "expected the lock to be contended while another refresh holds it"
        );

        release_tx.send(()).unwrap();
        holder.join().unwrap();

        // Now that it's released, acquisition must succeed again.
        assert!(refresh_lock().try_lock().is_ok());
    }
}
