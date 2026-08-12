//! Periodic refresh of the curated BDOT10k/EGIB building-type mapping files.
//!
//! Mirrors `street_mappings_update`: applied at serving time, so a new file
//! takes effect on the next `/package` request with no recompute, and the
//! ETags live in `metadata` rather than `dataset_refreshes`. Unlike the
//! street-name job this covers two independent files, each with its own ETag
//! and its own `job_run_log` entry -- a failure or no-op on one file must not
//! gate the other, so the two are refreshed and reported one at a time rather
//! than through a single combined outcome.

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::{info, warn};

use crate::mappings::building_types::{BDOT10K, BuildingTypeSource, BuildingTypeStats, EGIB};
use crate::server::jobs::{Job, JobContext};

pub struct BuildingTypesUpdateJob;

impl BuildingTypesUpdateJob {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BuildingTypesUpdateJob {
    fn default() -> Self {
        Self::new()
    }
}

impl Job for BuildingTypesUpdateJob {
    fn name(&self) -> &'static str {
        "building_types_update"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;

        let bdot10k = refresh_one(
            &conn,
            ctx,
            &BDOT10K,
            &ctx.config.download_urls.bdot10k_building_types,
            "bdot10k_building_types_etag",
            "bdot10k_building_types.csv",
            "update:bdot10k-building-types",
        );
        let egib = refresh_one(
            &conn,
            ctx,
            &EGIB,
            &ctx.config.download_urls.egib_building_types,
            "egib_building_types_etag",
            "egib_building_types.csv",
            "update:egib-building-types",
        );

        // Run both regardless of which failed (already done above); surface
        // an error if either did, so the scheduler's own run log sees it, but
        // never let one side's failure skip the other's attempt.
        bdot10k.and(egib)
    }
}

#[allow(clippy::too_many_arguments)]
fn refresh_one(
    conn: &Connection,
    ctx: &JobContext,
    source: &BuildingTypeSource,
    url: &str,
    etag_key: &str,
    download_filename: &str,
    job_log_name: &str,
) -> Result<()> {
    let etag = crate::download::fetch_etag(url).unwrap_or(None);
    if let Some(current) = &etag {
        let previous: Option<String> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = ?",
                duckdb::params![etag_key],
                |r| r.get(0),
            )
            .ok();
        if previous.as_ref() == Some(current) {
            info!(
                url,
                source = source.name,
                "Building-type mapping unchanged (ETag match), skipping"
            );
            return Ok(());
        }
    }

    let outcome = (|| -> Result<BuildingTypeStats> {
        let path = crate::download::download_file_as_quiet(
            url,
            &ctx.config.download_dir(),
            download_filename,
        )?;
        let stats = crate::mappings::building_types::load_from_path(conn, source, &path)?;

        if ctx.config.cleanup_downloaded_files {
            info!(path = %path.display(), "Cleaning up downloaded file");
            let _ = std::fs::remove_file(&path);
        } else {
            warn!(
                path = %path.display(),
                "cleanup_downloaded_files is false; leaving downloaded file in place \
                 (it will be reused on the next run since download_file_as skips \
                 re-downloading an existing destination)"
            );
        }

        Ok(stats)
    })();

    match &outcome {
        Ok(stats) => {
            if let Some(current) = &etag {
                conn.execute(
                    "DELETE FROM metadata WHERE key = ?",
                    duckdb::params![etag_key],
                )?;
                conn.execute(
                    "INSERT INTO metadata (key, value) VALUES (?, ?)",
                    duckdb::params![etag_key, current],
                )?;
            }
            let msg = format!(
                "loaded {} rows ({} keys absent from source, {} source keys / {} source rows \
                 uncovered)",
                stats.rows_loaded,
                stats.keys_absent_from_source,
                stats.source_keys_uncovered,
                stats.source_rows_uncovered,
            );
            let _ = crate::job_log::record(conn, job_log_name, "Success", Some(&msg));
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, job_log_name, "Error", Some(&format!("{e:#}")));
        }
    }

    outcome.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::jobs::Job;

    #[test]
    fn job_is_named_for_its_config_key() {
        assert_eq!(
            BuildingTypesUpdateJob::new().name(),
            "building_types_update"
        );
    }
}
