//! `db copy`: rewrite the DuckDB database into a fresh file.
//!
//! Three things only a fresh file gives, none of which any other command can:
//!
//! 1. **The newest storage version.** `storage_compatibility_version` (set in
//!    `db::init_db`) applies only when a file is *created*; an existing file is
//!    read and written at the version in its header forever
//!    (`SingleFileBlockManager::Initialize`). So a database created under an
//!    older DuckDB keeps that release's encodings through every later upgrade
//!    and every `import full` — `import` rebuilds tables, never the file.
//! 2. **Compaction.** DuckDB reuses free blocks but almost never shrinks the
//!    file; the local Poland database measured 24% free blocks. The copy
//!    writes live data only.
//! 3. **Recovery.** The copy reads what is visible, so delete marks a dead
//!    transaction left behind (`duckb-to-investigate.txt`) or a file that will
//!    not checkpoint (`docs/duckdb_checkpoint_failure.md`) do not come along.
//!
//! **Not `COPY FROM DATABASE`, deliberately**, for two reasons that both fail
//! silently. It creates every index *before* the data and then inserts every
//! table in one statement, so each RTREE is grown row by row inside one
//! transaction holding the whole database, instead of bulk-built by
//! `CREATE INDEX` over a full table. And it does nothing about row order:
//! under production's `preserve_insertion_order = false` a parallel
//! `INSERT ... SELECT` scrambles it (measured: all 3,000,000 rows of an
//! ordered table moved), which silently kills zonemap pruning on anything
//! that relies on append order — `dataset_change_areas.detected_at`, read by
//! every z5–z11 tile, is the one CLAUDE.md names. So this copies one table per
//! transaction with order preserved, then replays the indexes over the full
//! tables.
//!
//! The copy is written to `<dest>.partial` and renamed into place only after
//! every table's row count and the index set have been checked, so a file at
//! `<dest>` is always complete. Not `<dest>.tmp`: that is DuckDB's default
//! `temp_directory` for a database at `<dest>`, and a spill directory left
//! there by an earlier run would collide with it.
//!
//! RocksDB is not copied. The OSM replication stamp lives in this database's
//! `metadata` while the node store it describes lives in RocksDB, so this is a
//! rewrite of the DuckDB half in place, not a backup of the pair.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use tracing::{info, warn};

use crate::compare::in_transaction;
use crate::shutdown;

/// Catalog name the destination is attached under for the copy's duration.
const DEST_ALIAS: &str = "db_copy_dest";

/// Copy the database `conn` has open into a new file at `dest`.
///
/// `source_path` is only read for the before/after report (its storage
/// version and file size); the data comes through `conn`. Requires exclusive
/// access, like every offline command: a `run` server holds the file lock.
pub fn copy_database(conn: &Connection, source_path: &Path, dest: &Path) -> Result<()> {
    let partial = with_suffix(dest, ".partial");
    refuse_existing(dest, &partial)?;

    let source: String = conn.query_row("SELECT current_database()", [], |r| r.get(0))?;
    refuse_uncopyable_objects(conn, &source)?;

    let tables = query_pairs(
        conn,
        "SELECT table_name, sql FROM duckdb_tables()
         WHERE database_name = ? AND schema_name = 'main' AND NOT temporary
         ORDER BY table_name",
        &source,
    )?;
    let indexes = query_pairs(
        conn,
        "SELECT index_name, sql FROM duckdb_indexes()
         WHERE database_name = ? AND schema_name = 'main'
         ORDER BY index_name",
        &source,
    )?;

    // Session scope only: this process exits when the copy is done.
    conn.execute_batch("SET preserve_insertion_order = true")?;
    conn.execute_batch(&format!(
        "ATTACH {} AS {DEST_ALIAS} (STORAGE_VERSION 'latest')",
        sql_string(&partial.to_string_lossy())
    ))
    .with_context(|| format!("Failed to create {partial:?}"))?;
    let mut guard = PartialCopy {
        conn,
        source: &source,
        path: partial.clone(),
        attached: true,
    };
    // The table and index DDL is unqualified, so it lands in whatever the
    // default catalog is; the source side of every INSERT is qualified.
    conn.execute_batch(&format!("USE {DEST_ALIAS}"))?;

    let started = Instant::now();
    for (table, ddl) in &tables {
        shutdown::check_requested()?;
        let t = Instant::now();
        let rows = in_transaction(conn, &format!("db copy {table}"), || {
            conn.execute_batch(ddl)
                .with_context(|| format!("Failed to create {table}: {ddl}"))?;
            let rows = conn
                .execute(
                    &format!(
                        "INSERT INTO {t} SELECT * FROM {s}.main.{t}",
                        t = quote_ident(table),
                        s = quote_ident(&source)
                    ),
                    [],
                )
                .with_context(|| format!("Failed to copy rows of {table}"))?;
            Ok(rows)
        })?;
        info!(table = %table, rows, elapsed_s = t.elapsed().as_secs_f64(), "Copied table");
    }

    for (index, ddl) in &indexes {
        shutdown::check_requested()?;
        let t = Instant::now();
        conn.execute_batch(ddl)
            .with_context(|| format!("Failed to rebuild index {index}: {ddl}"))?;
        info!(index = %index, elapsed_s = t.elapsed().as_secs_f64(), "Rebuilt index");
    }

    verify(conn, &source, &tables, &indexes)?;
    conn.execute_batch(&format!("CHECKPOINT {DEST_ALIAS}"))?;
    guard.detach()?;

    // A WAL next to the copy would be replayed into it on open. A completed
    // checkpoint and DETACH leave none, so one here means something is wrong.
    let wal = with_suffix(&partial, ".wal");
    if wal.exists() {
        bail!("{wal:?} survived the checkpoint; refusing to move an unfinished copy into place");
    }
    std::fs::rename(&partial, dest)
        .with_context(|| format!("Failed to rename {partial:?} to {dest:?}"))?;
    guard.path = PathBuf::new();

    report(
        conn,
        source_path,
        dest,
        tables.len(),
        indexes.len(),
        started,
    );
    Ok(())
}

/// Removes the half-written copy unless it was moved into place, so an
/// interrupted or failed run leaves nothing at `<dest>.partial` to trip the
/// next one.
struct PartialCopy<'a> {
    conn: &'a Connection,
    source: &'a str,
    /// Emptied once the file has been renamed into place.
    path: PathBuf,
    attached: bool,
}

impl PartialCopy<'_> {
    fn detach(&mut self) -> Result<()> {
        // The default catalog cannot be detached, so switch back first.
        self.conn
            .execute_batch(&format!("USE {}", quote_ident(self.source)))?;
        self.conn
            .execute_batch(&format!("DETACH {DEST_ALIAS}"))
            .context("Failed to detach the copy")?;
        self.attached = false;
        Ok(())
    }
}

impl Drop for PartialCopy<'_> {
    fn drop(&mut self) {
        if self.attached
            && let Err(e) = self.detach()
        {
            warn!(error = %e, "could not detach the unfinished copy");
        }
        if self.path.as_os_str().is_empty() {
            return;
        }
        for path in [self.path.clone(), with_suffix(&self.path, ".wal")] {
            if let Err(e) = std::fs::remove_file(&path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                warn!(path = %path.display(), error = %e, "could not remove the unfinished copy");
            }
        }
    }
}

/// Never overwrite anything. `<dest>.wal` counts too: DuckDB would replay a
/// stray WAL into the new file the first time it is opened.
fn refuse_existing(dest: &Path, partial: &Path) -> Result<()> {
    for path in [
        dest.to_path_buf(),
        with_suffix(dest, ".wal"),
        partial.to_path_buf(),
        with_suffix(partial, ".wal"),
    ] {
        if path.exists() {
            bail!("{path:?} already exists; remove it or choose another destination");
        }
    }
    Ok(())
}

/// The copy moves tables and indexes only. The schema has nothing else today;
/// if something is added, fail rather than drop it from the copy silently.
fn refuse_uncopyable_objects(conn: &Connection, source: &str) -> Result<()> {
    let found: Vec<String> = conn
        .prepare(
            "SELECT 'schema ' || schema_name FROM duckdb_schemas()
               WHERE database_name = $1 AND schema_name <> 'main'
             UNION ALL SELECT 'sequence ' || sequence_name FROM duckdb_sequences()
               WHERE database_name = $1
             UNION ALL SELECT 'view ' || view_name FROM duckdb_views()
               WHERE database_name = $1 AND NOT internal
             UNION ALL SELECT 'macro ' || function_name FROM duckdb_functions()
               WHERE database_name = $1 AND function_type LIKE '%macro' AND NOT internal
             UNION ALL SELECT 'type ' || type_name FROM duckdb_types()
               WHERE database_name = $1 AND NOT internal
             ORDER BY 1",
        )?
        .query_map([source], |r| r.get(0))?
        .collect::<duckdb::Result<_>>()?;
    if !found.is_empty() {
        bail!(
            "db copy copies tables and indexes only, and the database also has: {}",
            found.join(", ")
        );
    }
    Ok(())
}

/// Every table holds exactly the source's row count, and the index set is
/// the source's. Counts rather than content: the rows went through one
/// `INSERT ... SELECT *` each, so a count mismatch is the failure this can
/// actually have (a statement that did less than it reported).
fn verify(
    conn: &Connection,
    source: &str,
    tables: &[(String, String)],
    indexes: &[(String, String)],
) -> Result<()> {
    for (table, _) in tables {
        let (src, dst): (i64, i64) = conn.query_row(
            &format!(
                "SELECT (SELECT count(*) FROM {s}.main.{t}), (SELECT count(*) FROM {DEST_ALIAS}.main.{t})",
                s = quote_ident(source),
                t = quote_ident(table)
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if src != dst {
            bail!("{table}: source has {src} rows, the copy {dst}");
        }
    }
    let copied: Vec<String> = conn
        .prepare(
            "SELECT index_name FROM duckdb_indexes()
             WHERE database_name = ? AND schema_name = 'main' ORDER BY index_name",
        )?
        .query_map([DEST_ALIAS], |r| r.get(0))?
        .collect::<duckdb::Result<_>>()?;
    let expected: Vec<&String> = indexes.iter().map(|(name, _)| name).collect();
    if copied.iter().collect::<Vec<_>>() != expected {
        bail!("index set differs: source {expected:?}, copy {copied:?}");
    }
    Ok(())
}

fn report(
    conn: &Connection,
    source_path: &Path,
    dest: &Path,
    tables: usize,
    indexes: usize,
    started: Instant,
) {
    let source_blocks: Option<(i64, i64, i64)> = conn
        .query_row(
            "SELECT block_size, used_blocks, free_blocks FROM pragma_database_size()
             WHERE database_name = current_database()",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .ok();
    let source_bytes = std::fs::metadata(source_path).map(|m| m.len()).ok();
    let dest_bytes = std::fs::metadata(dest).map(|m| m.len()).ok();
    info!(
        tables,
        indexes,
        elapsed_s = started.elapsed().as_secs_f64(),
        source_bytes,
        source_free_bytes = source_blocks.map(|(size, _, free)| size * free),
        dest_bytes,
        source_storage_version = storage_version(source_path).ok(),
        dest_storage_version = storage_version(dest).ok(),
        dest = %dest.display(),
        "Database copied; stop the service and move it over the original to use it"
    );
}

/// The storage version stamped in a database file's active header — the
/// number `storage_compatibility_version` resolves to: 7 for `v1.5.0`
/// (DuckDB 1.5.x's `latest`), 69 for DuckDB 2.0's `latest`, whose files also
/// carry main-header format 999 where 1.5 wrote 68, and which DuckDB 1.5
/// refuses to open. DuckDB exposes no SQL for it, which is why this reads the
/// file.
///
/// Layout (`single_file_block_manager.cpp`): a main header at offset 0 with
/// the `DUCK` magic at byte 8 and the format number at byte 12, then two
/// alternating database headers at 4096 and 8192, each a checksum followed by
/// `iteration`, five more `u64`s and the storage version. The active one is
/// the higher `iteration`. Format 64 predates the field and means version 1.
fn storage_version(path: &Path) -> Result<u64> {
    const HEADER: usize = 4096;
    let mut buf = vec![0u8; HEADER * 3];
    File::open(path)
        .and_then(|mut f| f.read_exact(&mut buf))
        .with_context(|| format!("Failed to read the header of {path:?}"))?;
    let u64_at = |off: usize| u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
    if &buf[8..12] != b"DUCK" {
        bail!("{path:?} is not a DuckDB database file");
    }
    if u64_at(12) == 64 {
        return Ok(1);
    }
    let active = [HEADER, 2 * HEADER]
        .into_iter()
        .max_by_key(|&h| u64_at(h + 8))
        .unwrap();
    Ok(u64_at(active + 8 + 6 * 8))
}

fn query_pairs(conn: &Connection, sql: &str, param: &str) -> Result<Vec<(String, String)>> {
    let rows = conn
        .prepare(sql)?
        .query_map([param], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<duckdb::Result<_>>()?;
    Ok(rows)
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;

    fn spatial() -> Vec<String> {
        vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()]
    }

    /// A source database in a file, so it has a header to compare against.
    fn file_db(dir: &Path) -> Result<Connection> {
        init_db(&dir.join("source.duckdb"), &spatial(), None)
    }

    /// The command's whole contract in one pass: every table arrives with its
    /// rows, the index set arrives too (and is usable, not just listed), and
    /// nothing is left at the partial path. The destination is attached
    /// rather than opened through `init_db`, whose `create_schema` would paper
    /// over a table the copy had dropped.
    #[test]
    fn copies_every_table_and_index_and_the_indexes_are_used() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let conn = file_db(dir.path())?;
        conn.execute_batch(
            "INSERT INTO metadata VALUES ('osm_replication_sequence', '42');
             INSERT INTO prg_unmatched (lokalny_id, geom)
               SELECT 'id' || i, ST_Point(14 + i / 1000.0, 52) FROM range(500) r(i);
             CREATE TABLE extra_from_an_import AS SELECT range AS i FROM range(10);",
        )?;
        let dest = dir.path().join("copy.duckdb");

        copy_database(&conn, &dir.path().join("source.duckdb"), &dest)?;

        assert!(dest.exists());
        assert!(!with_suffix(&dest, ".partial").exists());
        conn.execute_batch(&format!(
            "ATTACH {} AS c (READ_ONLY)",
            sql_string(&dest.to_string_lossy())
        ))?;
        let catalog_diff: i64 = conn.query_row(
            "SELECT count(*) FROM (
               (SELECT table_name, sql FROM duckdb_tables() WHERE database_name = 'source'
                EXCEPT SELECT table_name, sql FROM duckdb_tables() WHERE database_name = 'c')
               UNION ALL
               (SELECT index_name, sql FROM duckdb_indexes() WHERE database_name = 'source'
                EXCEPT SELECT index_name, sql FROM duckdb_indexes() WHERE database_name = 'c'))",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(catalog_diff, 0);
        let rows: (i64, i64, String) = conn.query_row(
            "SELECT (SELECT count(*) FROM c.prg_unmatched),
                    (SELECT count(*) FROM c.extra_from_an_import),
                    (SELECT value FROM c.metadata)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(rows, (500, 10, "42".to_string()));

        let plan: String = conn
            .prepare(
                "EXPLAIN SELECT * FROM c.prg_unmatched
                 WHERE ST_Intersects(geom, ST_MakeEnvelope(14.0, 51.9, 14.1, 52.1))",
            )?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<duckdb::Result<Vec<_>>>()?
            .join("\n");
        assert!(
            plan.contains(crate::db::EXPLAIN_RTREE_SCAN),
            "copied index not used:\n{plan}"
        );
        Ok(())
    }

    /// The reason the copy sets `preserve_insertion_order` — and the negative
    /// control that makes the assertion mean something: under production's
    /// parallel-insert settings a plain `INSERT ... SELECT` of the same table
    /// does scramble it, so a copy that forgot the setting would fail here.
    #[test]
    fn row_order_survives_the_copy_under_parallel_insert_settings() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let conn = file_db(dir.path())?;
        conn.execute_batch(
            "SET GLOBAL threads = 4;
             SET GLOBAL preserve_insertion_order = false;
             CREATE TABLE ordered AS SELECT range AS i FROM range(1000000) ORDER BY i;",
        )?;
        let misordered = |table: &str| -> Result<i64> {
            Ok(conn.query_row(
                &format!("SELECT count(*) FILTER (WHERE i <> rowid) FROM {table}"),
                [],
                |r| r.get(0),
            )?)
        };
        assert_eq!(misordered("source.ordered")?, 0);

        let control = dir.path().join("control.duckdb");
        conn.execute_batch(&format!(
            "ATTACH {} AS control;
             CREATE TABLE control.ordered (i BIGINT);
             INSERT INTO control.ordered SELECT * FROM source.ordered;",
            sql_string(&control.to_string_lossy())
        ))?;
        assert!(
            misordered("control.ordered")? > 0,
            "negative control: a plain parallel INSERT kept the order, so this test proves nothing"
        );
        conn.execute_batch("DETACH control; SET GLOBAL preserve_insertion_order = false")?;

        let dest = dir.path().join("copy.duckdb");
        copy_database(&conn, &dir.path().join("source.duckdb"), &dest)?;
        conn.execute_batch(&format!(
            "ATTACH {} AS c (READ_ONLY)",
            sql_string(&dest.to_string_lossy())
        ))?;
        assert_eq!(misordered("c.ordered")?, 0);
        Ok(())
    }

    /// The headline use: a file created at an old storage version comes out at
    /// the latest. Opening it with `init_db` (which sets `latest`) first is the
    /// point — that alone does not upgrade it.
    #[test]
    fn the_copy_is_written_at_the_latest_storage_version() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let source = dir.path().join("source.duckdb");
        drop(Connection::open_with_flags(
            &source,
            duckdb::Config::default()
                .with("storage_compatibility_version", "v1.0.0")
                .unwrap(),
        )?);
        let conn = init_db(&source, &spatial(), None)?;
        conn.execute_batch("CHECKPOINT")?;
        let old = storage_version(&source)?;

        let dest = dir.path().join("copy.duckdb");
        copy_database(&conn, &source, &dest)?;

        assert_eq!(
            storage_version(&source)?,
            old,
            "opening must not upgrade the source"
        );
        assert!(storage_version(&dest)? > old);
        // Sanity check on the header reader itself: a file created by this
        // binary carries the same version the copy does.
        drop(Connection::open_with_flags(
            dir.path().join("fresh.duckdb"),
            duckdb::Config::default()
                .with("storage_compatibility_version", "latest")
                .unwrap(),
        )?);
        assert_eq!(
            storage_version(&dest)?,
            storage_version(&dir.path().join("fresh.duckdb"))?
        );
        Ok(())
    }

    #[test]
    fn refuses_to_overwrite_the_destination_or_adopt_a_stray_wal() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let conn = file_db(dir.path())?;
        let source = dir.path().join("source.duckdb");
        let dest = dir.path().join("copy.duckdb");

        std::fs::write(&dest, b"keep me")?;
        let err = copy_database(&conn, &source, &dest).unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err:#}");
        assert_eq!(std::fs::read(&dest)?, b"keep me");
        std::fs::remove_file(&dest)?;

        std::fs::write(with_suffix(&dest, ".wal"), b"")?;
        assert!(copy_database(&conn, &source, &dest).is_err());
        assert!(!dest.exists());
        Ok(())
    }

    /// A view would not survive the copy, so its presence must stop it before
    /// any file is written.
    #[test]
    fn refuses_a_database_holding_objects_it_would_not_copy() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let conn = file_db(dir.path())?;
        conn.execute_batch("CREATE VIEW v AS SELECT 1")?;
        let dest = dir.path().join("copy.duckdb");

        let err = copy_database(&conn, &dir.path().join("source.duckdb"), &dest).unwrap_err();

        assert!(err.to_string().contains("view v"), "{err:#}");
        assert!(!dest.exists());
        assert!(!with_suffix(&dest, ".partial").exists());
        Ok(())
    }

    /// A failure mid-copy leaves nothing behind, and the source connection is
    /// still usable afterwards: back on its own catalog, the copy detached.
    ///
    /// The trigger is a generated column, which `SELECT *` returns but an
    /// INSERT refuses — the schema has none, so failing loudly on one is
    /// acceptable, and it is a real failure rather than an injected one. Named
    /// to sort last, so every other table is already committed into the
    /// partial file when it fails.
    #[test]
    fn a_failed_copy_removes_its_partial_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let conn = file_db(dir.path())?;
        conn.execute_batch(
            "CREATE TABLE zz_generated (a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1));
             INSERT INTO zz_generated (a) VALUES (1);",
        )?;
        let dest = dir.path().join("copy.duckdb");

        let err = copy_database(&conn, &dir.path().join("source.duckdb"), &dest).unwrap_err();

        assert!(format!("{err:#}").contains("zz_generated"), "{err:#}");
        assert!(!dest.exists());
        assert!(!with_suffix(&dest, ".partial").exists());
        assert!(!with_suffix(&dest, ".partial.wal").exists());
        let db: String = conn.query_row("SELECT current_database()", [], |r| r.get(0))?;
        assert_eq!(db, "source");
        let attached: i64 = conn.query_row(
            "SELECT count(*) FROM duckdb_databases() WHERE database_name = ?",
            [DEST_ALIAS],
            |r| r.get(0),
        )?;
        assert_eq!(attached, 0);
        Ok(())
    }
}
