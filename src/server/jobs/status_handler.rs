use std::collections::BTreeMap;

use anyhow::{Context, Result};
use axum::Json;
use axum::extract::State;
use duckdb::Connection;
use serde::Serialize;

use crate::job_log::{self, JobRunLogEntry};
use crate::server::AppState;
use crate::server::jobs::JobStatus;

#[derive(Serialize, Default)]
pub struct MatchStaleness {
    pub pending_total: i64,
    /// A map, not a `Vec<(String, i64)>` -- the latter serializes as a JSON
    /// array of 2-element arrays (e.g. `[["bdot10k",1],["prg",1]]`) rather
    /// than an object, which is an awkward shape for any consumer of this
    /// brand-new public field. `BTreeMap` also gives stable (sorted) key
    /// order for free.
    pub pending_by_source: BTreeMap<String, i64>,
    /// Oldest queued cell, i.e. how far behind the drain is.
    ///
    /// **Biased pessimistically just after a long government refresh.** DuckDB's
    /// `now()` is transaction-*start*-scoped, and the refresh enqueues its dirty
    /// cells inside the apply transaction, so a 5-minute BDOT10k refresh stamps
    /// every cell it touched with its BEGIN time. This field then reads ~5 min
    /// staler than reality until those cells drain. Cosmetic only: the drain's
    /// own cutoff logic is snapshot-based (`drain_batch` reads one `batch_start`
    /// and uses it for both the select and the paired delete), so correctness is
    /// unaffected.
    pub oldest_enqueued_at: Option<String>,
}

// The design (line 298) also lists a `last_drained_at`. It is deliberately not
// a field here: `/status` already reports `jobs[]`, and the `match_refresh`
// entry's `last_finished_at` is exactly that timestamp. A second copy would be
// a second thing to keep in sync for no new information.

/// Pending `match_dirty_cells` rows, deduped on `(source, cell_x, cell_y)` —
/// a cell enqueued multiple times (e.g. by overlapping OSM edits) counts once.
pub fn compute_match_staleness(conn: &Connection) -> Result<MatchStaleness> {
    let pending_total: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM (SELECT DISTINCT source, cell_x, cell_y FROM match_dirty_cells)",
            [],
            |r| r.get(0),
        )
        .context("count pending cells")?;
    let mut pending_by_source = BTreeMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT source, COUNT(*) FROM (SELECT DISTINCT source, cell_x, cell_y FROM match_dirty_cells)
             GROUP BY source ORDER BY source",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        for row in rows {
            let (source, count) = row?;
            pending_by_source.insert(source, count);
        }
    }
    let oldest_enqueued_at: Option<String> = conn
        .query_row(
            "SELECT MIN(enqueued_at)::VARCHAR FROM match_dirty_cells",
            [],
            |r| r.get(0),
        )
        .context("oldest enqueued")?;
    Ok(MatchStaleness {
        pending_total,
        pending_by_source,
        oldest_enqueued_at,
    })
}

/// Acquires a pool connection and computes staleness, falling back to a
/// zeroed `MatchStaleness` (rather than propagating an error) if the pool
/// is exhausted, the connection is broken, or `match_dirty_cells` can't be
/// queried (e.g. missing in a bare test connection) -- the queue is a
/// secondary diagnostic, not a reason for `/status` to fail.
fn match_staleness_or_default(state: &AppState) -> MatchStaleness {
    let outcome = (|| -> Result<MatchStaleness> {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        compute_match_staleness(&conn)
    })();
    match outcome {
        Ok(staleness) => staleness,
        Err(e) => {
            tracing::warn!(error = %e, "failed to compute match staleness for /status; falling back to empty");
            MatchStaleness::default()
        }
    }
}

/// Acquires a pool connection and reads `job_run_log`, falling back to an
/// empty map (rather than propagating an error) for the same reason
/// `match_staleness_or_default` does: this is a secondary diagnostic, not a
/// reason for `/status` to fail.
fn job_run_log_or_default(state: &AppState) -> BTreeMap<String, JobRunLogEntry> {
    let outcome = (|| -> Result<BTreeMap<String, JobRunLogEntry>> {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        job_log::read_all(&conn)
    })();
    match outcome {
        Ok(log) => log,
        Err(e) => {
            tracing::warn!(error = %e, "failed to read job_run_log for /status; falling back to empty");
            BTreeMap::new()
        }
    }
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub jobs: Vec<JobStatus>,
    pub match_staleness: MatchStaleness,
    pub job_run_log: BTreeMap<String, JobRunLogEntry>,
}

pub async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let jobs = state.registry.snapshot();
    let (match_staleness, job_run_log) = tokio::task::spawn_blocking(move || {
        (
            match_staleness_or_default(&state),
            job_run_log_or_default(&state),
        )
    })
    .await
    .unwrap_or_default();
    Json(StatusResponse {
        jobs,
        match_staleness,
        job_run_log,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    #[test]
    fn staleness_counts_distinct_cells_per_source() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,1,1,now()), ('bdot10k',14,1,1,now()), -- dup, one distinct cell
                 ('prg',14,2,2,now());",
        )
        .unwrap();
        let s = compute_match_staleness(&conn).unwrap();
        assert_eq!(s.pending_total, 2, "distinct (source,cell) pairs");
        assert_eq!(
            s.pending_by_source,
            BTreeMap::from([("bdot10k".to_string(), 1), ("prg".to_string(), 1)])
        );
        assert!(s.oldest_enqueued_at.is_some());
    }

    #[test]
    fn job_run_log_or_default_reads_recorded_entries() {
        use crate::db::init_db;
        use crate::server::jobs::JobRegistry;
        use std::sync::Arc;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        crate::job_log::record(
            &conn,
            "import:bdot10k",
            "Success",
            Some("no invalid geometry"),
        )
        .unwrap();

        let pool = crate::server::build_pool(conn, 2).unwrap();
        let state = AppState {
            pool,
            registry: Arc::new(JobRegistry::new_for_tests(vec![])),
            config: Arc::new(crate::config::Config::default()),
        };

        let log = job_run_log_or_default(&state);
        let entry = log.get("import:bdot10k").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("no invalid geometry"));
    }
}
