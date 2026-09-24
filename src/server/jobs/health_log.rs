use anyhow::Result;
use tracing::{info, warn};

use crate::server::jobs::status_handler::{OsmReplicationState, read_osm_replication};
use crate::server::jobs::{Job, JobContext};

/// Background job that writes the process's health into the journal: DuckDB
/// memory by tag, the process's own memory, and the OSM replication lag. See
/// `config::HealthLogConfig` for why.
///
/// Read-only. Each figure degrades to a note on its own, so a starved pool
/// still leaves the process line, which is when it is wanted most.
pub struct HealthLogJob;

impl Job for HealthLogJob {
    fn name(&self) -> &'static str {
        "health_log"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let process_memory = crate::process_memory::read()
            .map(|m| m.summary())
            .unwrap_or_else(|| "unavailable".to_string());
        let (duckdb_memory, replication) = match ctx.pool.get() {
            Ok(conn) => (
                crate::db_memory::summary_or_note(&conn),
                read_osm_replication(&conn)
                    .inspect_err(
                        |e| warn!(error = %e, "health_log: failed to read OSM replication state"),
                    )
                    .ok(),
            ),
            Err(e) => (format!("unavailable: {e}"), None),
        };
        let replication = replication.unwrap_or_default();

        info!(
            %duckdb_memory,
            %process_memory,
            osm_sequence = replication.sequence_number,
            osm_lag_seconds = replication.lag_seconds,
            "health"
        );

        let cfg = &ctx.config.jobs;
        if osm_lag_exceeds(
            &replication,
            cfg.health_log.osm_lag_warn_seconds,
            cfg.osm_update.enabled,
        ) {
            warn!(
                osm_lag_minutes = replication.lag_seconds.unwrap_or(0) / 60,
                osm_sequence = replication.sequence_number,
                osm_timestamp = replication.timestamp.as_deref().unwrap_or("?"),
                "OSM replication is behind"
            );
        }
        Ok(())
    }
}

/// Whether the lag deserves a WARN. Never while `osm_update` is off (the lag
/// grows by design then) or with the threshold at 0, and never on a missing
/// reading: an unstamped database is a fresh one, not a stalled one.
fn osm_lag_exceeds(
    replication: &OsmReplicationState,
    threshold_seconds: u64,
    osm_update_enabled: bool,
) -> bool {
    if !osm_update_enabled || threshold_seconds == 0 {
        return false;
    }
    replication
        .lag_seconds
        .is_some_and(|lag| lag > threshold_seconds as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lagging(lag_seconds: Option<i64>) -> OsmReplicationState {
        OsmReplicationState {
            sequence_number: Some(7298234),
            timestamp: Some("2026-09-22T22:04:02Z".to_string()),
            lag_seconds,
        }
    }

    #[test]
    fn warns_only_past_the_threshold_while_osm_update_runs() {
        assert!(osm_lag_exceeds(&lagging(Some(1801)), 1800, true));
        assert!(!osm_lag_exceeds(&lagging(Some(1800)), 1800, true));
        assert!(!osm_lag_exceeds(&lagging(Some(90)), 1800, true));

        assert!(
            !osm_lag_exceeds(&lagging(Some(99_999)), 1800, false),
            "a disabled osm_update falls behind by design"
        );
        assert!(
            !osm_lag_exceeds(&lagging(Some(99_999)), 0, true),
            "0 disables the warning"
        );
        assert!(
            !osm_lag_exceeds(&lagging(None), 1800, true),
            "no stamp is a fresh database, not a stalled one"
        );
    }
}
