use anyhow::{Context, Result};
use axum::Json;
use axum::extract::State;
use duckdb::Connection;
use serde::Serialize;

use crate::server::AppState;
use crate::server::jobs::JobStatus;

#[derive(Serialize, Default)]
pub struct MatchStaleness {
    pub pending_total: i64,
    pub pending_by_source: Vec<(String, i64)>,
    pub oldest_enqueued_at: Option<String>,
}

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
    let mut pending_by_source = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT source, COUNT(*) FROM (SELECT DISTINCT source, cell_x, cell_y FROM match_dirty_cells)
             GROUP BY source ORDER BY source",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        for row in rows {
            pending_by_source.push(row?);
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

#[derive(Serialize)]
pub struct StatusResponse {
    pub jobs: Vec<JobStatus>,
    pub match_staleness: MatchStaleness,
}

pub async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let jobs = state.registry.snapshot();
    let match_staleness = tokio::task::spawn_blocking(move || match_staleness_or_default(&state))
        .await
        .unwrap_or_default();
    Json(StatusResponse {
        jobs,
        match_staleness,
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
        assert!(
            s.pending_by_source
                .iter()
                .any(|(k, v)| k == "bdot10k" && *v == 1)
        );
        assert!(s.oldest_enqueued_at.is_some());
    }
}
