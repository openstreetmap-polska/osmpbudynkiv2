pub mod changeset;
pub mod dataset;
pub mod diff;
pub mod osm;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::cli::UpdateSource;
use crate::config::{Config, DownloadUrls};
use crate::dataset as spec;
use crate::download::{download_file, download_file_as};
use crate::osm::kvstore::RocksDB;

pub fn run(
    conn: &Connection,
    kv: &RocksDB,
    source: UpdateSource,
    config: &Config,
    urls: &DownloadUrls,
) -> Result<()> {
    match source {
        UpdateSource::Osm => osm::update(conn, kv, config, &urls.osm_replication),
        UpdateSource::Bdot10k { file } => {
            let path = resolve(file.as_deref(), config, &urls.bdot10k, None)?;
            let p = path_str(&path)?;
            dataset::refresh(
                conn,
                &spec::BDOT10K,
                |c, target| crate::import::bdot10k::load_into(c, target, &p),
                None,
            )
            .map(|_| ())
        }
        UpdateSource::Egib { file } => {
            let path = resolve(file.as_deref(), config, &urls.egib, None)?;
            let p = path_str(&path)?;
            dataset::refresh(
                conn,
                &spec::EGIB,
                |c, target| crate::import::egib::load_into(c, target, &p),
                None,
            )
            .map(|_| ())
        }
        UpdateSource::Prg { file, terc_file } => crate::import::prg::update_prg(
            conn,
            config,
            file.as_deref(),
            terc_file.as_deref(),
            &urls.prg,
        ),
    }
}

/// Resolve a local path or download the snapshot, then verify it is a
/// non-empty regular file BEFORE any staging work begins.
fn resolve(
    file: Option<&Path>,
    config: &Config,
    url: &str,
    download_as: Option<&str>,
) -> Result<PathBuf> {
    let path = match file {
        Some(p) => p.to_path_buf(),
        None => match download_as {
            Some(name) => download_file_as(url, &config.download_dir(), name)?,
            None => download_file(url, &config.download_dir())?,
        },
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
    Ok(path)
}

fn path_str(p: &Path) -> Result<String> {
    p.to_str()
        .map(str::to_string)
        .with_context(|| format!("Path {} is not valid UTF-8", p.display()))
}
