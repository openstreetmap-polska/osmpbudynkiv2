//! Periodic refresh of the curated street-name mapping file.
//!
//! Unlike the dataset refreshes this touches no geometry, but it does now
//! enqueue dirty cells: the PRG<->OSM address match rule has a branch that
//! compares PRG's street name resolved through `street_name_mappings`
//! against OSM's `addr:street`, so a mapping edit can change which
//! addresses are unmatched, not just how they render. `mappings::load_from_path`
//! diffs the old and new mapping contents and enqueues the affected z14
//! cells for the drain -- see `mappings::street_names::enqueue_mapping_delta_cells`.
//! The last seen ETag lives in `metadata` rather than `dataset_refreshes`,
//! whose columns exist for snapshot diffing that does not apply here.

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::mappings::LoadStats;
use crate::server::jobs::{Job, JobContext};

const ETAG_KEY: &str = "street_mappings_etag";
const JOB_LOG_KEY: &str = "update:street-mappings";

pub struct StreetMappingsUpdateJob;

impl StreetMappingsUpdateJob {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StreetMappingsUpdateJob {
    fn default() -> Self {
        Self::new()
    }
}

impl Job for StreetMappingsUpdateJob {
    fn name(&self) -> &'static str {
        "street_mappings_update"
    }

    fn log_keys(&self) -> &'static [&'static str] {
        &[JOB_LOG_KEY]
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx
            .pool
            .get()
            .context("failed to acquire pool connection")?;
        let url = &ctx.config.download_urls.street_mappings;

        let etag = crate::download::fetch_etag(url).unwrap_or(None);
        if let Some(current) = &etag {
            let previous: Option<String> = conn
                .query_row(
                    "SELECT value FROM metadata WHERE key = ?",
                    duckdb::params![ETAG_KEY],
                    |r| r.get(0),
                )
                .ok();
            if previous.as_ref() == Some(current) {
                info!(url, "Street mappings unchanged (ETag match), skipping");
                return Ok(());
            }
        }

        // Download and load are wrapped in one outcome-capturing closure, same
        // as `import::bdot10k::import`, so a download failure gets reported to
        // `job_run_log` just like a load failure -- not just successes.
        let outcome = (|| -> Result<LoadStats> {
            let path = crate::download::download_file_as_quiet(
                url,
                &ctx.config.download_dir(),
                "street_names_mappings.csv",
            )?;
            let stats = crate::mappings::load_from_path(&conn, &path)?;

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
                        duckdb::params![ETAG_KEY],
                    )?;
                    conn.execute(
                        "INSERT INTO metadata (key, value) VALUES (?, ?)",
                        duckdb::params![ETAG_KEY, current],
                    )?;
                }
                let msg = format!(
                    "loaded {} mapping rows ({} not present in current PRG data, \
                     {} dirty cell(s) enqueued)",
                    stats.rows_loaded, stats.rows_absent_from_prg, stats.cells_enqueued
                );
                let _ = crate::job_log::record(&conn, JOB_LOG_KEY, "Success", Some(&msg));
            }
            Err(e) => {
                let _ =
                    crate::job_log::record(&conn, JOB_LOG_KEY, "Error", Some(&format!("{e:#}")));
            }
        }

        outcome.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::jobs::Job;

    #[test]
    fn job_is_named_for_its_config_key() {
        assert_eq!(
            StreetMappingsUpdateJob::new().name(),
            "street_mappings_update"
        );
    }
}
