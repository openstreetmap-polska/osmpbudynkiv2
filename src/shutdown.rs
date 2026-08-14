use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use duckdb::InterruptHandle;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Every DuckDB connection whose long-running statements should be aborted on
/// the first shutdown signal. `import`/`update`/`compare` spend nearly all
/// their wall time inside a single `execute_batch` call, and the flag above is
/// only polled *between* statements -- it cannot touch one already running.
/// Registering a connection's interrupt handle here closes that gap: the
/// ctrlc handler below calls `.interrupt()` on everything registered, which
/// makes DuckDB fail the in-flight statement instead of waiting for it.
///
/// `Mutex::new`/`Vec::new` are both `const`, so this needs no `OnceLock` or
/// lazy-init dance.
static INTERRUPT_HANDLES: Mutex<Vec<Arc<InterruptHandle>>> = Mutex::new(Vec::new());

/// Register a connection's interrupt handle so the first shutdown signal
/// aborts whatever statement it's currently running (see `INTERRUPT_HANDLES`
/// above). Safe to call multiple times; every registered handle is
/// interrupted, and `InterruptHandle::interrupt` is documented as a no-op
/// once its connection has been dropped, so a handle for a connection that
/// later closes normally is harmless to leave registered.
pub fn register_interrupt_handle(handle: Arc<InterruptHandle>) {
    INTERRUPT_HANDLES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(handle);
}

/// Register a handler for SIGINT and SIGTERM.
/// First signal sets the flag; second signal force-exits.
pub fn install_handler() {
    ctrlc::set_handler(move || {
        if SHUTDOWN_REQUESTED.swap(true, Ordering::SeqCst) {
            eprintln!("\nForce shutdown");
            std::process::exit(1);
        }
        eprintln!("\nShutdown requested, finishing current operation...");

        // Locking a Mutex and touching an Arc's refcount here is safe, even
        // though neither is async-signal-safe and this closure was handed to
        // something called `set_handler`. `ctrlc` (verified in ctrlc-3.5.2's
        // `set_handler_inner`, src/lib.rs) never invokes this closure from
        // inside an actual POSIX signal handler:
        // it installs a minimal OS-level handler that just unblocks a
        // dedicated background thread, and that thread calls `user_handler()`
        // (this closure) as an ordinary function call. So none of the
        // async-signal-safety restrictions that would forbid locking a mutex
        // or allocating apply here. Don't "fix" this back to something
        // signal-safe -- there is nothing to fix.
        for handle in INTERRUPT_HANDLES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            handle.interrupt();
        }
    })
    .expect("Failed to install signal handler");
}

pub fn is_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

/// The one home for "stop here because the user asked us to stop": bails with
/// [`SHUTDOWN_BAIL_MESSAGE`] if a signal has arrived, otherwise `Ok(())`.
///
/// Every cooperative cancellation seam in the long-running CLI paths
/// (`import`'s per-source and per-batch loops, `download`'s chunk and retry
/// loops, `compare`'s grid and between-sub-compare checks, `reconcile`'s
/// per-source sweep) calls this rather than spelling out its own
/// `if is_requested() { bail!("...") }`. That matters for one specific
/// reason: the message is load-bearing. `compare::buildings`'s
/// `cancelled_compare_leaves_the_previous_contents_intact` asserts on it to
/// distinguish a cancellation from a genuine query failure, and an operator
/// reading `job_run_log` needs "the run was interrupted" to look the same
/// whichever seam happened to notice first. Nine hand-written copies of the
/// same literal drifted apart the moment one of them got reworded.
///
/// Two seams deliberately do NOT call this, and both are correct:
/// - `update::osm::update` returns `Ok(())` instead of bailing -- it commits
///   one replication batch at a time and resumes from a `metadata` stamp, so
///   stopping early is durable partial progress, not a failure.
/// - `compare::buildings::compare_buildings_with_cancel` and
///   `update::dataset::check_cancelled` take an *injected* cancel closure,
///   because they must also honour a job supervisor's per-run cancel flag,
///   not just this process-global signal.
pub fn check_requested() -> anyhow::Result<()> {
    if is_requested() {
        anyhow::bail!(SHUTDOWN_BAIL_MESSAGE);
    }
    Ok(())
}

/// The error text [`check_requested`] bails with. Named so tests can assert
/// on it by constant instead of retyping the literal -- see CLAUDE.md's
/// fixture gotcha about substring assertions silently pinning the wrong
/// guard.
pub const SHUTDOWN_BAIL_MESSAGE: &str = "Shutdown requested";

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::mpsc;

    /// The codebase's error paths run cleanup SQL (`ROLLBACK`,
    /// `DROP TABLE IF EXISTS`) on the *same connection* right after a
    /// statement fails -- see e.g. `compare::in_transaction`'s rollback arm.
    /// If DuckDB left the connection itself poisoned after an interrupt,
    /// that cleanup would also fail, and the caller's real error would be
    /// masked by a second, confusing one.
    ///
    /// This test interrupts a genuinely long-running statement on a
    /// background thread and then proves the *same* connection still accepts
    /// ordinary statements afterwards. Empirically (observed running this
    /// test): the interrupted statement fails with a `DuckDBFailure` whose
    /// message contains "INTERRUPT", and the connection is immediately
    /// reusable -- `BEGIN TRANSACTION; ROLLBACK;` and `SELECT 1` both
    /// succeed right after. DuckDB's interrupt is scoped to the one running
    /// statement, not the connection.
    #[test]
    fn interrupting_a_running_statement_does_not_poison_the_connection() {
        let conn = duckdb::Connection::open(Path::new(":memory:")).unwrap();
        let handle = conn.interrupt_handle();

        // `InterruptHandle` is scoped to the exact `duckdb_connection` it was
        // obtained from, not to "the database" -- confirmed empirically here
        // by first writing this test against `conn.try_clone()` (as
        // `server::ClonedConnectionManager` and every drain/refresh thread in
        // this codebase do) and observing the query run to completion,
        // uninterrupted, because `try_clone` opens a brand-new
        // `duckdb_connection` with its own independent interrupt handle (see
        // `InnerConnection::try_clone`, duckdb-1.10505.0). So this test must
        // run the long statement on `conn` itself, moved into the worker
        // thread, and hand it back afterwards rather than cloning it -- which
        // is also exactly why Gap 1's `main.rs` registration only covers the
        // CLI's single connection, not the server's cloned pool.
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let query_thread = std::thread::spawn(move || {
            let _ = started_tx.send(());
            // A cross join big enough to run for seconds with no natural
            // short-circuit, giving the interrupting thread a wide window to
            // land mid-statement rather than racing a query that might
            // finish first.
            let result =
                conn.execute_batch("SELECT count(*) FROM range(100000000) a, range(1000) b;");
            // Send the connection back regardless of outcome, so the main
            // thread can run further statements on the very same connection
            // object that was interrupted.
            let _ = done_tx.send((result, conn));
        });

        // Wait for the query thread to at least start before interrupting,
        // then give DuckDB a moment to actually begin executing (as opposed
        // to still being parsed/planned) so the interrupt has something
        // running to land on.
        started_rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        handle.interrupt();

        let (query_result, conn) = done_rx.recv().unwrap();
        query_thread.join().unwrap();

        let err = query_result.expect_err("an interrupted statement must return Err");
        let msg = format!("{err}").to_uppercase();
        assert!(
            msg.contains("INTERRUPT"),
            "expected the interrupt error to mention INTERRUPT, got: {err}"
        );

        // The valuable assertion: the same connection, right after the
        // failure above, still accepts ordinary statements -- exactly the
        // shape of `compare::in_transaction`'s ROLLBACK-after-error and
        // `dataset`'s `DROP TABLE IF EXISTS` cleanup.
        conn.execute_batch("BEGIN TRANSACTION; ROLLBACK;")
            .expect("connection must accept a statement immediately after an interrupt");
        let one: i64 = conn.query_row("SELECT 1", [], |r| r.get(0)).unwrap();
        assert_eq!(one, 1, "connection must still answer ordinary queries");
    }
}
