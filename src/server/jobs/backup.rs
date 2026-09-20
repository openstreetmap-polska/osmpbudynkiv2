//! Background job that dumps `object_reports` to a file outside the database.
//!
//! `object_reports` is the only table in this schema that cannot be rebuilt
//! from an external source -- every other one is an `import` away -- so it is
//! the only thing here that needs backing up at all. A full copy of the
//! database would be a large, slow way to protect data that is already
//! re-downloadable.
//!
//! **Why this has to run in-process.** DuckDB lets exactly one process hold
//! the database file, and `run` holds it read-write for its whole lifetime, so
//! a `reports export` invoked from cron against the live file does not produce
//! a stale dump -- it fails to open the database at all. So does a *read-only*
//! open, which is the part that surprises (`IO Error: Could not set lock on
//! file ... Conflicting lock is held`). Anything periodic therefore lives
//! here, inside the process that already holds the lock.
//!
//! **Writing the file is not yet a backup.** The dump lands next to the
//! database it came from, which survives a bad deploy, a wrong `DELETE` or a
//! botched migration, but not the loss of the host. Serving the directory (see
//! `example_config.toml`) is what lets something off-host pull it; a directory
//! nobody pulls from is a dump, not a backup.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::server::jobs::{Job, JobContext, format_rfc3339};

/// `job_run_log` key this job reports under (see `Job::log_keys`).
const JOB_LOG_KEY: &str = "backup";

/// Prefix and extension every dump shares. `LATEST_NAME` matches both, so
/// every scan of the directory has to exclude it by name -- see
/// `dated_dumps`.
const FILE_PREFIX: &str = "reports-";
const FILE_SUFFIX: &str = ".jsonl";

/// Stable name, refreshed on every run, so a puller can fetch one URL without
/// listing the directory or parsing timestamps first.
const LATEST_NAME: &str = "reports-latest.jsonl";

/// Suffix for the write-then-rename temporary. Deliberately does NOT end in
/// `FILE_SUFFIX`: a partially written file must not be able to match the scan
/// that picks the newest dump or the one that prunes.
const TMP_SUFFIX: &str = ".tmp";

pub struct BackupJob;

impl Job for BackupJob {
    fn name(&self) -> &'static str {
        "backup"
    }

    fn log_keys(&self) -> &'static [&'static str] {
        &[JOB_LOG_KEY]
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let cfg = &ctx.config.jobs.backup;
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;

        let outcome = write_backup(&conn, Path::new(&cfg.dir), cfg.keep_days, SystemTime::now());

        match &outcome {
            Ok(stats) => {
                let _ =
                    crate::job_log::record(&conn, JOB_LOG_KEY, "Success", Some(&stats.summary()));
            }
            Err(e) => {
                let _ =
                    crate::job_log::record(&conn, JOB_LOG_KEY, "Error", Some(&format!("{e:#}")));
            }
        }

        outcome.map(|_| ()).context("Failed to back up reports")
    }
}

/// What one run did, for the `job_run_log` message.
#[derive(Debug, PartialEq, Eq)]
pub struct BackupOutcome {
    pub rows: usize,
    pub bytes: usize,
    /// The dated dump this run wrote, or `None` when the content was
    /// byte-identical to the newest existing one.
    pub wrote: Option<String>,
    pub pruned: usize,
}

impl BackupOutcome {
    fn summary(&self) -> String {
        let what = match &self.wrote {
            Some(name) => format!("wrote {name}"),
            None => "unchanged since the last dump".to_string(),
        };
        format!(
            "{what}; {} reports, {} bytes, pruned {}",
            self.rows, self.bytes, self.pruned
        )
    }
}

/// Dump every report to `dir`, skipping the dated copy when nothing changed.
///
/// `now` is injected rather than read here so the tests can age a directory
/// past `keep_days` without touching file mtimes.
pub fn write_backup(
    conn: &Connection,
    dir: &Path,
    keep_days: u64,
    now: SystemTime,
) -> Result<BackupOutcome> {
    fs::create_dir_all(dir)
        .with_context(|| format!("Failed to create backup directory {}", dir.display()))?;

    let text = crate::reports::export_to_string(conn)?;
    let rows = text.lines().count();

    // Only a *changed* table earns a new dated file. Reports arrive a handful
    // a day at most, so without this an hourly job would fill the directory
    // with identical copies and make its listing useless for seeing when
    // anything actually happened.
    let existing = dated_dumps(dir)?;
    let unchanged = match existing.last() {
        // A read error here (a truncated file from a crashed run, a mangled
        // permission) means "cannot prove it is the same", so write a fresh
        // dump rather than skipping on a failure to compare.
        Some(newest) => fs::read_to_string(newest)
            .map(|prev| prev == text)
            .unwrap_or(false),
        None => false,
    };

    let wrote = if unchanged {
        None
    } else {
        let name = format!("{FILE_PREFIX}{}{FILE_SUFFIX}", stamp_for(now));
        write_atomically(&dir.join(&name), &text)?;
        Some(name)
    };

    // Refreshed every run, changed or not: a puller that only ever fetches
    // this one name must not be able to tell whether a dated file was
    // written, and its mtime doubles as "the job is alive".
    write_atomically(&dir.join(LATEST_NAME), &text)?;

    let pruned = prune_dated(dir, keep_days, now)?;

    Ok(BackupOutcome {
        rows,
        bytes: text.len(),
        wrote,
        pruned,
    })
}

/// Every dated dump in `dir`, oldest first.
///
/// The names are fixed-width UTC stamps, so lexicographic order *is*
/// chronological order and nothing has to parse a timestamp. `LATEST_NAME`
/// shares the prefix and the extension, so it is excluded explicitly --
/// letting it through would make it the "newest" dump on every scan (it sorts
/// after any digit) and both the unchanged-comparison and the prune would
/// then be looking at the wrong file.
fn dated_dumps(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(e).with_context(|| format!("Failed to list {}", dir.display()));
        }
    };

    let mut out = Vec::new();
    for entry in entries {
        let entry =
            entry.with_context(|| format!("Failed to read an entry in {}", dir.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name == LATEST_NAME {
            continue;
        }
        if name.starts_with(FILE_PREFIX) && name.ends_with(FILE_SUFFIX) {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

/// Write `text` to a temporary beside `path`, then rename it into place.
///
/// The rename is what makes a half-written file unreachable: a client fetching
/// the dump over HTTP while this runs sees either the previous complete
/// version or the new one, never a prefix of either. Same-directory rename, so
/// it cannot cross a filesystem boundary and stop being atomic.
fn write_atomically(path: &Path, text: &str) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(TMP_SUFFIX);
    let tmp = PathBuf::from(tmp);

    fs::write(&tmp, text).with_context(|| format!("Failed to write {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("Failed to rename {} to {}", tmp.display(), path.display()))?;
    Ok(())
}

/// Delete dated dumps older than `keep_days`, returning how many went.
///
/// Two rules, both load-bearing:
///
/// - `keep_days == 0` disables pruning rather than deleting everything, so a
///   mis-set config loses no data.
/// - The newest dump is never pruned, whatever its age. Dumps are only written
///   when the content changed, so a quiet table's single dump would otherwise
///   age past the window and delete the only copy there is -- the one failure
///   mode this whole job exists to prevent.
fn prune_dated(dir: &Path, keep_days: u64, now: SystemTime) -> Result<usize> {
    if keep_days == 0 {
        return Ok(0);
    }
    let max_age = Duration::from_secs(keep_days * 86_400);

    let dumps = dated_dumps(dir)?;
    // `split_last` rather than an index: an empty directory has no newest to
    // exempt and nothing to prune.
    let Some((_newest, older)) = dumps.split_last() else {
        return Ok(0);
    };

    let mut pruned = 0;
    for path in older {
        let modified = fs::metadata(path)
            .and_then(|m| m.modified())
            .with_context(|| format!("Failed to stat {}", path.display()))?;
        // `duration_since` errors when the file is *newer* than `now`, which
        // is not old, so treat that as "keep".
        let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
        if age > max_age {
            fs::remove_file(path)
                .with_context(|| format!("Failed to remove {}", path.display()))?;
            pruned += 1;
        }
    }
    Ok(pruned)
}

/// Filename stamp: `format_rfc3339` with the colons removed, since a colon in
/// a filename is trouble on other filesystems and in URLs. Reuses that
/// function rather than re-deriving civil-from-days here.
fn stamp_for(now: SystemTime) -> String {
    format_rfc3339(now).replace(':', "")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::config::Config as AppConfig;
    use crate::db::init_db;
    use crate::reports::ReportRow;

    /// `object_reports` and `job_run_log` are both created by `create_schema`
    /// (via `init_db`), so unlike the reports/reconcile fixtures this one
    /// needs no source tables: nothing here reads a government row.
    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET GLOBAL geometry_always_xy = true".to_string(),
        ];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    /// Reports are inserted directly rather than through `reports::insert`,
    /// which would drag in the three source tables to compute a signature.
    fn add_report(conn: &Connection, id: i64, key: &str) {
        conn.execute(
            &format!(
                "INSERT INTO object_reports
                     (report_id, source, record_key, signature, reported_at,
                      cell_x, cell_y, status, resolved_at)
                 VALUES ({id}, 'bdot10k', ['04', '{key}'], 'sig-{key}',
                         TIMESTAMPTZ '2026-09-20 10:00:00+00', 1, 2, 'active', NULL)"
            ),
            [],
        )
        .unwrap();
    }

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// 2026-09-20T12:00:00Z, and a day later.
    const T0: u64 = 1_789_041_600;
    const T1: u64 = T0 + 86_400;

    fn dated_names(dir: &Path) -> Vec<String> {
        dated_dumps(dir)
            .unwrap()
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect()
    }

    /// The property the whole job exists for: what it writes is what
    /// `reports import` reads. Asserting the file parses as JSON would not
    /// show that -- this restores into an emptied table and compares.
    #[test]
    fn a_dump_round_trips_back_through_reports_import() {
        let c = conn();
        add_report(&c, 1, "bud-1");
        add_report(&c, 2, "bud-2");
        let dir = tempfile::tempdir().unwrap();

        let out = write_backup(&c, dir.path(), 30, at(T0)).unwrap();
        assert_eq!(out.rows, 2);

        let text = fs::read_to_string(dir.path().join(LATEST_NAME)).unwrap();
        let rows: Vec<ReportRow> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();

        c.execute("DELETE FROM object_reports", []).unwrap();
        let imported = crate::reports::import_rows(&c, &rows).unwrap();
        assert_eq!(imported, 2);

        // Sorted before comparing: `list` orders by `report_id DESC` and
        // `import_rows` reallocates ids in input order, so a round trip
        // reverses the listing. That is a documented property of the import,
        // not something this test is about -- it is about the content.
        let mut keys: Vec<String> = crate::reports::list(&c, None, None, None, None)
            .unwrap()
            .iter()
            .map(|r| r.record_key.join("/"))
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["04/bud-1".to_string(), "04/bud-2".to_string()]);
    }

    /// An hourly job against a table that gets a handful of rows a day must
    /// not fill the directory with identical copies.
    #[test]
    fn an_unchanged_table_adds_no_second_dated_dump_but_still_refreshes_latest() {
        let c = conn();
        add_report(&c, 1, "bud-1");
        let dir = tempfile::tempdir().unwrap();

        let first = write_backup(&c, dir.path(), 30, at(T0)).unwrap();
        assert!(first.wrote.is_some());

        let second = write_backup(&c, dir.path(), 30, at(T1)).unwrap();
        assert_eq!(second.wrote, None, "unchanged content must not add a file");
        assert_eq!(dated_names(dir.path()).len(), 1);

        // latest is still complete and current regardless.
        let latest = fs::read_to_string(dir.path().join(LATEST_NAME)).unwrap();
        assert_eq!(latest.lines().count(), 1);
    }

    #[test]
    fn a_new_report_adds_a_dated_dump() {
        let c = conn();
        add_report(&c, 1, "bud-1");
        let dir = tempfile::tempdir().unwrap();

        write_backup(&c, dir.path(), 30, at(T0)).unwrap();
        add_report(&c, 2, "bud-2");
        let out = write_backup(&c, dir.path(), 30, at(T1)).unwrap();

        assert!(out.wrote.is_some());
        assert_eq!(dated_names(dir.path()).len(), 2);
        assert_eq!(out.rows, 2);
    }

    /// The trap in combining "only write when changed" with "prune by age":
    /// a table nobody has reported against in `keep_days` has one old dump,
    /// and pruning it by age deletes the only copy in existence.
    #[test]
    fn the_newest_dump_is_never_pruned_however_old_it_is() {
        let c = conn();
        add_report(&c, 1, "bud-1");
        let dir = tempfile::tempdir().unwrap();

        write_backup(&c, dir.path(), 1, at(T0)).unwrap();
        // A year later, still nothing reported.
        let out = write_backup(&c, dir.path(), 1, at(T0 + 365 * 86_400)).unwrap();

        assert_eq!(out.wrote, None);
        assert_eq!(out.pruned, 0);
        assert_eq!(dated_names(dir.path()).len(), 1, "the only dump survived");
    }

    #[test]
    fn dumps_past_the_window_are_pruned_and_keep_days_zero_disables_it() {
        let dir = tempfile::tempdir().unwrap();
        let c = conn();
        for (i, t) in [T0, T0 + 86_400, T0 + 2 * 86_400].iter().enumerate() {
            add_report(&c, i as i64 + 1, &format!("bud-{i}"));
            write_backup(&c, dir.path(), 0, at(*t)).unwrap();
        }
        assert_eq!(dated_names(dir.path()).len(), 3);

        // keep_days = 0 leaves everything alone even far in the future.
        let out = write_backup(&c, dir.path(), 0, at(T0 + 900 * 86_400)).unwrap();
        assert_eq!(out.pruned, 0);
        assert_eq!(dated_names(dir.path()).len(), 3);

        // With a window, everything but the newest goes. The files were all
        // written moments ago, so `now` does the ageing, not their mtimes.
        let out = write_backup(&c, dir.path(), 1, at(T0 + 900 * 86_400)).unwrap();
        assert_eq!(out.pruned, 2);
        assert_eq!(dated_names(dir.path()).len(), 1);
    }

    /// A temporary left in the directory would be served by the file server
    /// in front of it, and a `.jsonl`-suffixed one would also be picked up as
    /// a real dump by the scan.
    #[test]
    fn no_temporary_files_are_left_behind() {
        let c = conn();
        add_report(&c, 1, "bud-1");
        let dir = tempfile::tempdir().unwrap();
        write_backup(&c, dir.path(), 30, at(T0)).unwrap();

        let leftovers: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(TMP_SUFFIX))
            .collect();
        assert!(leftovers.is_empty(), "left behind: {leftovers:?}");
    }

    /// An empty table is a legitimate state, not an error: the dump is empty
    /// and a later restore is a no-op.
    #[test]
    fn an_empty_table_dumps_an_empty_file() {
        let c = conn();
        let dir = tempfile::tempdir().unwrap();
        let out = write_backup(&c, dir.path(), 30, at(T0)).unwrap();
        assert_eq!(out.rows, 0);
        assert_eq!(
            fs::read_to_string(dir.path().join(LATEST_NAME)).unwrap(),
            ""
        );
    }

    #[test]
    fn the_job_creates_a_missing_directory_and_records_what_it_did() {
        let c = conn();
        add_report(&c, 1, "bud-1");
        let pool = crate::server::build_pool(c, 2).unwrap();
        let kvdir = tempfile::tempdir().unwrap();
        let kv = Arc::new(crate::osm::kvstore::open(kvdir.path(), 8, 4, 8).unwrap());

        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("not/created/yet");
        let mut config = AppConfig::default();
        config.jobs.backup.dir = nested.to_string_lossy().into_owned();

        let ctx = JobContext {
            pool,
            kv,
            config: Arc::new(config),
            cancel: Arc::new(AtomicBool::new(false)),
        };

        BackupJob.run(&ctx).unwrap();

        assert!(nested.join(LATEST_NAME).exists());
        let conn = ctx.pool.get().unwrap();
        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get(JOB_LOG_KEY).expect("backup row in job_run_log");
        assert_eq!(entry.outcome, "Success");
        assert!(
            entry.message.as_deref().unwrap().contains("1 reports"),
            "message was {:?}",
            entry.message
        );
    }
}
