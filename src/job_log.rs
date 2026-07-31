//! Last-run status/log per job or CLI command, persisted in `job_run_log` so
//! it survives across separate process invocations (e.g. a standalone
//! `import` run, which never shares memory with a later `run` server) and is
//! readable by `/status`.
//!
//! Deliberately a snapshot, not a history: `record` deletes the previous row
//! for `job_name` before inserting the new one, matching the same
//! delete-then-insert convention `dataset::stamp_row_hash_version` already
//! uses instead of `ON CONFLICT`.
//!
//! This module must not depend on `crate::server` -- `import`/`update` run
//! independently of whether the HTTP server is up.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use duckdb::Connection;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobRunLogEntry {
    pub ran_at: String,
    /// "Success" | "Error" -- matches `server::jobs::JobOutcome`'s casing.
    pub outcome: String,
    pub message: Option<String>,
}

/// Record the outcome of one job/command run, replacing any previous row for
/// the same `job_name`. Callers are expected to log-and-ignore any error this
/// returns -- a failure to write the log must never fail the job itself.
pub fn record(
    conn: &Connection,
    job_name: &str,
    outcome: &str,
    message: Option<&str>,
) -> Result<()> {
    conn.execute(
        "DELETE FROM job_run_log WHERE job_name = ?",
        duckdb::params![job_name],
    )
    .context("Failed to clear previous job_run_log row")?;
    conn.execute(
        "INSERT INTO job_run_log (job_name, ran_at, outcome, message) VALUES (?, now(), ?, ?)",
        duckdb::params![job_name, outcome, message],
    )
    .context("Failed to write job_run_log row")?;
    Ok(())
}

/// All current entries, keyed by job_name. A `BTreeMap` rather than
/// `Vec<(String, _)>` for the same reason
/// `server::jobs::status_handler::MatchStaleness::pending_by_source` is one:
/// a stable, sorted JSON object shape instead of an array of pairs.
pub fn read_all(conn: &Connection) -> Result<BTreeMap<String, JobRunLogEntry>> {
    let mut stmt = conn
        .prepare(
            "SELECT job_name, ran_at::VARCHAR, outcome, message FROM job_run_log
             ORDER BY job_name",
        )
        .context("Failed to prepare job_run_log read")?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                JobRunLogEntry {
                    ran_at: r.get(1)?,
                    outcome: r.get(2)?,
                    message: r.get(3)?,
                },
            ))
        })
        .context("Failed to read job_run_log")?;
    let mut out = BTreeMap::new();
    for row in rows {
        let (name, entry) = row.context("Failed to decode job_run_log row")?;
        out.insert(name, entry);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    #[test]
    fn record_then_read_round_trips() {
        let conn = conn();
        record(&conn, "import:bdot10k", "Success", Some("skipped 3 rows")).unwrap();

        let log = read_all(&conn).unwrap();
        let entry = log.get("import:bdot10k").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("skipped 3 rows"));
        assert!(!entry.ran_at.is_empty());
    }

    #[test]
    fn record_keeps_only_the_latest_run_per_job_name() {
        let conn = conn();
        record(
            &conn,
            "update:bdot10k",
            "Error",
            Some("first attempt failed"),
        )
        .unwrap();
        record(
            &conn,
            "update:bdot10k",
            "Success",
            Some("second attempt: no issues"),
        )
        .unwrap();

        let log = read_all(&conn).unwrap();
        assert_eq!(log.len(), 1, "only one row per job_name must survive");
        let entry = &log["update:bdot10k"];
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("second attempt: no issues"));
    }

    #[test]
    fn read_all_keys_entries_by_job_name_in_sorted_order() {
        let conn = conn();
        record(&conn, "update:egib", "Success", None).unwrap();
        record(&conn, "import:bdot10k", "Success", None).unwrap();

        let log = read_all(&conn).unwrap();
        let names: Vec<String> = log.keys().cloned().collect();
        assert_eq!(names, vec!["import:bdot10k", "update:egib"]);
    }

    #[test]
    fn record_allows_a_null_message() {
        let conn = conn();
        record(&conn, "update:egib", "Success", None).unwrap();

        let log = read_all(&conn).unwrap();
        assert_eq!(log["update:egib"].message, None);
    }
}
