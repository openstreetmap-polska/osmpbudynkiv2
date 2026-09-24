//! This process's memory as the kernel accounts it, read from
//! `/proc/self/status`.
//!
//! [`crate::db_memory`] sees only DuckDB's buffer pool. RocksDB's caches and
//! memtables, GEOS and the Rust heap sit outside it, and so does whatever the
//! kernel has pushed to swap. Until this was added (2026-09-24) the only view
//! of those in production was an ad-hoc sampler script in `/tmp`, which does
//! not survive a reboot. On 2026-09-24 it showed 3.7 GiB of the process in
//! swap, which no in-process figure reported.
//!
//! Linux only: elsewhere [`read`] returns `None` and callers report nothing.

use serde::Serialize;

#[derive(Serialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessMemory {
    /// Resident set (`VmRSS`).
    pub rss_bytes: u64,
    /// Swapped out (`VmSwap`). RSS plus this is what the process holds.
    pub swap_bytes: u64,
    /// Peak resident set since start (`VmHWM`).
    pub peak_rss_bytes: u64,
}

pub fn read() -> Option<ProcessMemory> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|text| parse_proc_status(&text))
}

/// `None` unless all three lines are present: a partial reading would be
/// reported as a confident zero.
fn parse_proc_status(text: &str) -> Option<ProcessMemory> {
    let field = |name: &str| -> Option<u64> {
        let line = text.lines().find(|l| l.starts_with(name))?;
        let kib: u64 = line[name.len()..]
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse()
            .ok()?;
        Some(kib * 1024)
    };
    Some(ProcessMemory {
        rss_bytes: field("VmRSS:")?,
        swap_bytes: field("VmSwap:")?,
        peak_rss_bytes: field("VmHWM:")?,
    })
}

impl ProcessMemory {
    /// `rss=3.43GiB swap=3.58GiB peak_rss=6.62GiB`, in the same units as
    /// [`crate::db_memory::DuckDbMemory::summary`].
    pub fn summary(&self) -> String {
        use crate::db_memory::fmt_bytes;
        format!(
            "rss={} swap={} peak_rss={}",
            fmt_bytes(self.rss_bytes as i64),
            fmt_bytes(self.swap_bytes as i64),
            fmt_bytes(self.peak_rss_bytes as i64)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_three_fields_from_a_real_status_file() {
        // Trimmed from the production process on 2026-09-24.
        let text = "Name:\tosmpbudynkiv2\n\
                    VmPeak:\t12345678 kB\n\
                    VmHWM:\t 6945024 kB\n\
                    VmRSS:\t 3594260 kB\n\
                    RssAnon:\t 3400000 kB\n\
                    VmSwap:\t 3749360 kB\n\
                    Threads:\t64\n";
        assert_eq!(
            parse_proc_status(text),
            Some(ProcessMemory {
                rss_bytes: 3594260 * 1024,
                swap_bytes: 3749360 * 1024,
                peak_rss_bytes: 6945024 * 1024,
            })
        );
    }

    #[test]
    fn a_missing_field_is_no_reading_rather_than_a_zero() {
        assert_eq!(
            parse_proc_status("VmRSS:\t 100 kB\nVmHWM:\t 200 kB\n"),
            None
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reads_this_test_process() {
        let m = read().expect("/proc/self/status on Linux");
        assert!(m.rss_bytes > 0 && m.peak_rss_bytes >= m.rss_bytes, "{m:?}");
    }

    #[test]
    fn summary_uses_the_duckdb_line_s_units() {
        let m = ProcessMemory {
            rss_bytes: 3 * 1024 * 1024 * 1024,
            swap_bytes: 0,
            peak_rss_bytes: 512 * 1024 * 1024,
        };
        assert_eq!(m.summary(), "rss=3.00GiB swap=0MiB peak_rss=512MiB");
    }
}
