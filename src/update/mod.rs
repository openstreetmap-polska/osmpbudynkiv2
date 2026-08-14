pub mod changeset;
pub mod dataset;
pub mod diff;
pub mod dirty_cells;
pub mod osm;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::cli::UpdateSource;
use crate::config::{Config, DownloadUrls};
use crate::dataset as spec;
use crate::download::{
    download_file, download_file_as, download_file_as_quiet, download_file_quiet,
};
use crate::osm::kvstore::RocksDB;

/// `show_progress` distinguishes the interactive CLI path (`main.rs`'s
/// `Command::Update`, `true`) from the scheduled background path
/// (`server::jobs::dataset_update::DatasetUpdateJob`, `false`), same
/// rationale as `osm::update`'s parameter of the same name -- a redrawn
/// progress bar is fine on a terminal but just spams a log file. Note OSM's
/// *background* job bypasses `run` entirely and calls `osm::update` directly
/// (see `server::jobs::osm_update`), so this only ever reaches the `Osm` arm
/// from the CLI; the other three arms are reachable from both paths.
///
/// `is_cancelled` is forwarded unchanged into whichever update function the
/// matched arm calls (`osm::update`, `dataset::refresh` for `Bdot10k`/`Egib`,
/// `import::prg::update_prg` for `Prg`) -- `run` has no opinion of its own on
/// cancellation, it only routes the caller's signal through. What concrete
/// closure a given call passes depends on the caller: `main.rs`'s
/// `Command::Update` arm passes `&|| false` (the CLI has no job supervisor to
/// cancel it), `server::jobs::dataset_update::DatasetUpdateJob` passes
/// `&|| ctx.is_cancelled()`.
pub fn run(
    conn: &Connection,
    kv: &RocksDB,
    source: UpdateSource,
    config: &Config,
    urls: &DownloadUrls,
    show_progress: bool,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<()> {
    match source {
        UpdateSource::Osm => osm::update(
            conn,
            kv,
            config,
            &urls.osm_replication,
            show_progress,
            is_cancelled,
        ),
        UpdateSource::Bdot10k { file } => {
            let mut etag = None;
            if file.is_none() {
                let (unchanged, remote) =
                    source_unchanged(conn, spec::BDOT10K.name, &urls.bdot10k)?;
                etag = remote;
                if unchanged {
                    tracing::info!(
                        source = spec::BDOT10K.name,
                        "source unchanged; skipping refresh"
                    );
                    record_noop_refresh(conn, spec::BDOT10K.name, etag.as_deref())?;
                    return Ok(());
                }
            }
            let (path, was_downloaded) =
                resolve(file.as_deref(), config, &urls.bdot10k, None, show_progress)?;
            let p = path_str(&path)?;
            let result = dataset::refresh(
                conn,
                &spec::BDOT10K,
                |c, target| crate::import::bdot10k::load_into(c, target, &p),
                etag.as_deref(),
                is_cancelled,
            );
            cleanup_if_downloaded(&path, was_downloaded && config.cleanup_downloaded_files);
            result.map(|_| ())
        }
        UpdateSource::Egib { file } => {
            let mut etag = None;
            if file.is_none() {
                let (unchanged, remote) = source_unchanged(conn, spec::EGIB.name, &urls.egib)?;
                etag = remote;
                if unchanged {
                    tracing::info!(
                        source = spec::EGIB.name,
                        "source unchanged; skipping refresh"
                    );
                    record_noop_refresh(conn, spec::EGIB.name, etag.as_deref())?;
                    return Ok(());
                }
            }
            let (path, was_downloaded) =
                resolve(file.as_deref(), config, &urls.egib, None, show_progress)?;
            let p = path_str(&path)?;
            let result = dataset::refresh(
                conn,
                &spec::EGIB,
                |c, target| crate::import::egib::load_into(c, target, &p),
                etag.as_deref(),
                is_cancelled,
            );
            cleanup_if_downloaded(&path, was_downloaded && config.cleanup_downloaded_files);
            result.map(|_| ())
        }
        UpdateSource::Prg { file, terc_file } => {
            let mut etag = None;
            if file.is_none() {
                let (unchanged, remote) = source_unchanged(conn, spec::PRG.name, &urls.prg)?;
                etag = remote;
                if unchanged {
                    tracing::info!(
                        source = spec::PRG.name,
                        "source unchanged; skipping refresh"
                    );
                    record_noop_refresh(conn, spec::PRG.name, etag.as_deref())?;
                    return Ok(());
                }
            }
            crate::import::prg::update_prg(
                conn,
                config,
                file.as_deref(),
                terc_file.as_deref(),
                &urls.prg,
                etag.as_deref(),
                show_progress,
                is_cancelled,
            )
        }
    }
}

/// True when the remote snapshot still carries the validator recorded by the
/// last successful refresh of `source`.
///
/// A missing validator on either side means "unknown", which is treated as
/// changed — skipping must only ever happen on a positive match.
fn source_unchanged(conn: &Connection, source: &str, url: &str) -> Result<(bool, Option<String>)> {
    let remote = match crate::download::fetch_etag(url) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(source, error = %e, "HEAD check failed; downloading anyway");
            return Ok((false, None));
        }
    };
    let Some(remote) = remote else {
        return Ok((false, None));
    };

    let stored: Option<String> = conn
        .query_row(
            "SELECT source_etag FROM dataset_refreshes
             WHERE source = ? AND source_etag IS NOT NULL
             ORDER BY snapshot_id DESC LIMIT 1",
            duckdb::params![source],
            |row| row.get(0),
        )
        .ok();

    Ok((stored.as_deref() == Some(remote.as_str()), Some(remote)))
}

/// Record a refresh that ran but had nothing to do, so "ran and found no
/// changes" stays distinguishable from "never ran" in /status.
fn record_noop_refresh(conn: &Connection, source: &str, etag: Option<&str>) -> Result<()> {
    // Same rule as the apply path: allocate snapshot_id in the same
    // transaction that consumes it, so the MAX+1 read cannot be split from
    // the INSERT by a concurrent refresh.
    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin no-op refresh transaction")?;

    let recorded = (|| -> Result<()> {
        let snapshot_id: i64 = conn.query_row(
            "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dataset_refreshes",
            [],
            |row| row.get(0),
        )?;
        conn.execute(
            "INSERT INTO dataset_refreshes VALUES (?, ?, now(), now(), ?, 0, 0, 0)",
            duckdb::params![snapshot_id, source, etag],
        )
        .context("Failed to record no-op refresh")?;
        Ok(())
    })();

    match recorded {
        Ok(()) => {
            if let Err(e) = conn.execute_batch("COMMIT") {
                if let Err(rb) = conn.execute_batch("ROLLBACK") {
                    tracing::warn!(error = %rb, "failed to roll back after failed commit");
                }
                return Err(e).context("Failed to commit no-op refresh");
            }
        }
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                tracing::warn!(error = %rb, "failed to roll back no-op refresh");
            }
            return Err(e);
        }
    }
    Ok(())
}

/// Resolve a local path or download the snapshot, then verify it is a
/// non-empty regular file BEFORE any staging work begins. Returns whether
/// the file was downloaded (vs. a user-supplied `--file`), so callers know
/// whether it's theirs to delete once consumed.
fn resolve(
    file: Option<&Path>,
    config: &Config,
    url: &str,
    download_as: Option<&str>,
    show_progress: bool,
) -> Result<(PathBuf, bool)> {
    let (path, was_downloaded) = match file {
        Some(p) => (p.to_path_buf(), false),
        None => {
            let path = match (download_as, show_progress) {
                (Some(name), true) => download_file_as(url, &config.download_dir(), name)?,
                (Some(name), false) => download_file_as_quiet(url, &config.download_dir(), name)?,
                (None, true) => download_file(url, &config.download_dir())?,
                (None, false) => download_file_quiet(url, &config.download_dir())?,
            };
            (path, true)
        }
    };
    let meta = std::fs::metadata(&path)
        .with_context(|| format!("Source file {} is not readable", path.display()))?;
    if !meta.is_file() {
        anyhow::bail!("Source path {} is not a regular file", path.display());
    }
    if meta.len() == 0 {
        anyhow::bail!(
            "Source file {} is empty — refusing to proceed",
            path.display()
        );
    }
    Ok((path, was_downloaded))
}

/// Remove a downloaded snapshot once it's been fully consumed. `should_delete`
/// must already fold in both "we downloaded it ourselves" (a user-supplied
/// `--file` is never deleted) and `config.cleanup_downloaded_files`.
fn cleanup_if_downloaded(path: &Path, should_delete: bool) {
    if should_delete {
        tracing::info!(path = %path.display(), "Cleaning up downloaded file");
        let _ = std::fs::remove_file(path);
    }
}

fn path_str(p: &Path) -> Result<String> {
    p.to_str()
        .map(str::to_string)
        .with_context(|| format!("Path {} is not valid UTF-8", p.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    /// One-shot blocking HTTP server (std, not tokio — this runs from a
    /// plain `#[test]`, and `fetch_etag` builds its own tokio runtime
    /// internally) that answers a single HEAD request with `header_line`
    /// (already CRLF-terminated per header) and then exits.
    fn serve_head_once(header_line: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let resp = format!("HTTP/1.1 200 OK\r\n{header_line}Content-Length: 0\r\n\r\n");
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        format!("http://{addr}/f.bin")
    }

    /// One-shot blocking HTTP server that answers a single GET with `body`.
    fn serve_body_once(body: &'static [u8], name: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.write_all(body);
            }
        });
        format!("http://{addr}/{name}")
    }

    fn config_with_download_dir(dir: &Path) -> Config {
        Config {
            download_dir: Some(dir.to_string_lossy().into_owned()),
            ..Config::default()
        }
    }

    #[test]
    fn resolve_downloads_and_reports_was_downloaded() {
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_download_dir(dir.path());
        let url = serve_body_once(b"snapshot data", "resolve_download.bin");

        let (path, was_downloaded) = resolve(None, &config, &url, None, true).unwrap();

        assert!(was_downloaded);
        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), b"snapshot data");
    }

    #[test]
    fn resolve_uses_local_file_without_downloading() {
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_download_dir(dir.path());
        let mut local = tempfile::NamedTempFile::new().unwrap();
        local.write_all(b"local snapshot").unwrap();

        let (path, was_downloaded) =
            resolve(Some(local.path()), &config, "unused", None, true).unwrap();

        assert!(!was_downloaded);
        assert_eq!(path, local.path());
    }

    #[test]
    fn cleanup_if_downloaded_removes_file_when_should_delete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("downloaded.bin");
        std::fs::write(&path, b"data").unwrap();

        cleanup_if_downloaded(&path, true);

        assert!(!path.exists());
    }

    #[test]
    fn cleanup_if_downloaded_keeps_file_when_should_not_delete() {
        let tmp = tempfile::NamedTempFile::new().unwrap();

        cleanup_if_downloaded(tmp.path(), false);

        assert!(tmp.path().exists());
    }

    fn insert_refresh_with_etag(conn: &Connection, source: &str, etag: Option<&str>) {
        conn.execute(
            "INSERT INTO dataset_refreshes VALUES (
                 (SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dataset_refreshes),
                 ?, now(), now(), ?, 0, 0, 0)",
            duckdb::params![source, etag],
        )
        .unwrap();
    }

    #[test]
    fn source_unchanged_true_when_remote_etag_matches_stored() {
        let conn = conn();
        insert_refresh_with_etag(&conn, "bdot10k", Some("\"same\""));
        let url = serve_head_once("ETag: \"same\"\r\n");

        let (unchanged, remote) = source_unchanged(&conn, "bdot10k", &url).unwrap();
        assert!(unchanged);
        assert_eq!(remote.as_deref(), Some("\"same\""));
    }

    #[test]
    fn source_unchanged_false_when_remote_etag_differs() {
        let conn = conn();
        insert_refresh_with_etag(&conn, "bdot10k", Some("\"old\""));
        let url = serve_head_once("ETag: \"new\"\r\n");

        let (unchanged, remote) = source_unchanged(&conn, "bdot10k", &url).unwrap();
        assert!(!unchanged);
        assert_eq!(remote.as_deref(), Some("\"new\""));
    }

    #[test]
    fn source_unchanged_false_when_no_stored_etag() {
        let conn = conn();
        let url = serve_head_once("ETag: \"new\"\r\n");

        let (unchanged, remote) = source_unchanged(&conn, "bdot10k", &url).unwrap();
        assert!(!unchanged);
        assert_eq!(remote.as_deref(), Some("\"new\""));
    }

    #[test]
    fn source_unchanged_false_when_server_offers_no_validator() {
        let conn = conn();
        insert_refresh_with_etag(&conn, "bdot10k", Some("\"old\""));
        let url = serve_head_once("");

        let (unchanged, remote) = source_unchanged(&conn, "bdot10k", &url).unwrap();
        assert!(!unchanged);
        assert_eq!(remote, None);
    }

    #[test]
    fn source_unchanged_is_scoped_to_its_own_source() {
        let conn = conn();
        insert_refresh_with_etag(&conn, "egib", Some("\"same\""));
        let url = serve_head_once("ETag: \"same\"\r\n");

        // bdot10k has no stored etag of its own, so a match on egib's row
        // must not leak across sources.
        let (unchanged, remote) = source_unchanged(&conn, "bdot10k", &url).unwrap();
        assert!(!unchanged);
        assert_eq!(remote.as_deref(), Some("\"same\""));
    }

    #[test]
    fn record_noop_refresh_writes_zeroed_row() {
        let conn = conn();
        record_noop_refresh(&conn, "bdot10k", Some("\"abc\"")).unwrap();

        let (source, etag, added, modified, removed): (String, String, i32, i32, i32) = conn
            .query_row(
                "SELECT source, source_etag, added, modified, removed FROM dataset_refreshes",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!(source, "bdot10k");
        assert_eq!(etag, "\"abc\"");
        assert_eq!((added, modified, removed), (0, 0, 0));
    }

    #[test]
    fn record_noop_refresh_snapshot_ids_increment() {
        let conn = conn();
        record_noop_refresh(&conn, "bdot10k", None).unwrap();
        record_noop_refresh(&conn, "bdot10k", None).unwrap();

        let ids: Vec<i64> = {
            let mut stmt = conn
                .prepare("SELECT snapshot_id FROM dataset_refreshes ORDER BY snapshot_id")
                .unwrap();
            let r = stmt.query_map([], |r| r.get(0)).unwrap();
            r.map(|x| x.unwrap()).collect()
        };
        assert_eq!(ids, vec![1, 2]);
    }
}
