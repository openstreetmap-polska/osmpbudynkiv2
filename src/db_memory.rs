//! DuckDB's own memory accounting, read from `duckdb_memory()`.
//!
//! **Nothing else can see this memory.** DuckDB's buffer pool is served by its
//! bundled `duckdb_je_*` allocator (CLAUDE.md's jemalloc gotcha), so the heap
//! profiles in `docs/heap_profiling.md` never show it. And `run` holds the
//! database file exclusively, so no outside process can query it either. This
//! gets asked from inside the process, through `/status` and the dataset jobs'
//! log lines.
//!
//! Added 2026-09-18. Two days after a restart, all three daily refreshes began
//! failing with `Out of Memory Error ... (3.7 GiB/3.7 GiB used)` at the
//! staging step. Each ran alone under `refresh_lock`, and the identical step
//! had succeeded on the first day. So something inside DuckDB kept a growing
//! share of `memory_limit`. The per-tag split is what says which subsystem.

use anyhow::{Context, Result};
use duckdb::Connection;
use serde::Serialize;

/// One `duckdb_memory()` row. The tag names are DuckDB's own (`BASE_TABLE`,
/// `ART_INDEX`, `HASH_TABLE`, `EXTENSION`, ...).
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct TagUsage {
    pub tag: String,
    pub memory_usage_bytes: i64,
    pub temporary_storage_bytes: i64,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct DuckDbMemory {
    /// `memory_limit` as DuckDB formats it (e.g. `3.7 GiB`). This is the same
    /// figure as the denominator in an `Out of Memory Error`, which is not the
    /// configured `'4GB'`: DuckDB reads `GB` as 10^9 bytes and reports in GiB.
    pub memory_limit: String,
    /// Sum over all tags: the numerator of that same error message.
    pub memory_usage_bytes: i64,
    /// Bytes spilled to `temp_directory` across all tags.
    pub temporary_storage_bytes: i64,
    /// Tags that hold anything, largest first. All-zero tags are dropped.
    pub by_tag: Vec<TagUsage>,
}

pub fn read(conn: &Connection) -> Result<DuckDbMemory> {
    let memory_limit: String = conn
        .query_row("SELECT current_setting('memory_limit')", [], |r| r.get(0))
        .context("read memory_limit")?;
    let mut stmt = conn
        .prepare(
            "SELECT tag, memory_usage_bytes, temporary_storage_bytes
             FROM duckdb_memory()
             WHERE memory_usage_bytes > 0 OR temporary_storage_bytes > 0
             ORDER BY memory_usage_bytes DESC, tag",
        )
        .context("prepare duckdb_memory()")?;
    let by_tag = stmt
        .query_map([], |r| {
            Ok(TagUsage {
                tag: r.get(0)?,
                memory_usage_bytes: r.get(1)?,
                temporary_storage_bytes: r.get(2)?,
            })
        })?
        .collect::<duckdb::Result<Vec<_>>>()
        .context("read duckdb_memory()")?;
    Ok(DuckDbMemory {
        memory_limit,
        memory_usage_bytes: by_tag.iter().map(|t| t.memory_usage_bytes).sum(),
        temporary_storage_bytes: by_tag.iter().map(|t| t.temporary_storage_bytes).sum(),
        by_tag,
    })
}

impl DuckDbMemory {
    /// One log line for grepping the journal as a time series:
    /// `used=3.52GiB limit=3.7 GiB spilled=0MiB | BASE_TABLE=1.20GiB ...`.
    pub fn summary(&self) -> String {
        let tags: Vec<String> = self
            .by_tag
            .iter()
            .map(|t| format!("{}={}", t.tag, fmt_bytes(t.memory_usage_bytes)))
            .collect();
        format!(
            "used={} limit={} spilled={} | {}",
            fmt_bytes(self.memory_usage_bytes),
            self.memory_limit,
            fmt_bytes(self.temporary_storage_bytes),
            tags.join(" ")
        )
    }
}

fn fmt_bytes(b: i64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    let mib = b as f64 / MIB;
    if mib >= 1024.0 {
        format!("{:.2}GiB", mib / 1024.0)
    } else {
        format!("{mib:.0}MiB")
    }
}

/// Reads and summarizes, degrading to a note rather than failing: callers log
/// this around work that matters more than the diagnostic.
pub fn summary_or_note(conn: &Connection) -> String {
    match read(conn) {
        Ok(m) => m.summary(),
        Err(e) => format!("unavailable: {e:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_the_limit_and_accounts_a_real_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "SET memory_limit = '1GB';
             CREATE TABLE t AS SELECT range AS i, repeat('x', 100) AS s FROM range(200000);",
        )
        .unwrap();
        let m = read(&conn).unwrap();
        assert!(
            m.memory_limit.contains("MiB") || m.memory_limit.contains("GiB"),
            "{m:?}"
        );
        assert!(
            m.memory_usage_bytes > 0,
            "a 200k-row table must be accounted: {m:?}"
        );
        assert_eq!(
            m.memory_usage_bytes,
            m.by_tag.iter().map(|t| t.memory_usage_bytes).sum::<i64>()
        );
        assert!(
            m.by_tag
                .iter()
                .all(|t| t.memory_usage_bytes > 0 || t.temporary_storage_bytes > 0)
        );
        assert!(
            m.by_tag
                .windows(2)
                .all(|w| w[0].memory_usage_bytes >= w[1].memory_usage_bytes),
            "largest first: {m:?}"
        );
    }

    #[test]
    fn summary_names_the_total_the_limit_and_each_tag() {
        let m = DuckDbMemory {
            memory_limit: "3.7 GiB".into(),
            memory_usage_bytes: 3 * 1024 * 1024 * 1024 + 512 * 1024 * 1024,
            temporary_storage_bytes: 0,
            by_tag: vec![
                TagUsage {
                    tag: "ART_INDEX".into(),
                    memory_usage_bytes: 3 * 1024 * 1024 * 1024,
                    temporary_storage_bytes: 0,
                },
                TagUsage {
                    tag: "BASE_TABLE".into(),
                    memory_usage_bytes: 512 * 1024 * 1024,
                    temporary_storage_bytes: 0,
                },
            ],
        };
        assert_eq!(
            m.summary(),
            "used=3.50GiB limit=3.7 GiB spilled=0MiB | ART_INDEX=3.00GiB BASE_TABLE=512MiB"
        );
    }
}
