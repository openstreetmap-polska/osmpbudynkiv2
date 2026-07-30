use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct TerytConfig {
    /// When true, download TERYT dictionary from the government SOAP API.
    /// When false, a local file must be provided via `file_path` or `--terc-file`.
    pub download: bool,
    /// Username for TERYT API. Falls back to TERYT_API_USERNAME env var.
    pub api_username: Option<String>,
    /// Password for TERYT API. Falls back to TERYT_API_PASSWORD env var.
    pub api_password: Option<String>,
    /// Path to a local TERC dictionary file (.zip or .xml).
    /// Overridden by the --terc-file CLI flag.
    pub file_path: Option<String>,
}

impl Default for TerytConfig {
    fn default() -> Self {
        Self {
            download: true,
            api_username: None,
            api_password: None,
            file_path: None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub db_path: String,
    pub rocksdb_path: String,
    pub rocksdb_block_cache_mb: u64,
    pub rocksdb_write_buffer_mb: u64,
    pub log_level: String,
    pub http_listen_addr: String,
    pub download_dir: Option<String>,
    pub teryt: TerytConfig,
    pub duckdb_init_commands: Vec<String>,
    /// Number of connections in the shared DuckDB pool used by the `run`
    /// (HTTP server) command. All connections are `try_clone()`s of one base
    /// connection (see `server::ClonedConnectionManager`), so they share live
    /// MVCC state -- raising this increases how many queries (reads and
    /// writes) can genuinely run concurrently instead of queueing behind a
    /// single connection.
    pub db_pool_size: u32,
    pub download_urls: DownloadUrls,
    pub jobs: JobsConfig,
    pub package: PackageConfig,
    pub updates: UpdatesConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct JobConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 60,
            timeout_seconds: 600,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ExportLogPruneConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    /// How long package_exports rows are kept before being pruned.
    pub retention_days: u64,
}

impl Default for ExportLogPruneConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 86400,
            timeout_seconds: 60,
            retention_days: 365,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct MatchRefreshConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    /// Max number of distinct dirty (source, cell) recomputed per tick.
    pub batch_size: usize,
}

impl Default for MatchRefreshConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 30,
            timeout_seconds: 300,
            batch_size: 512,
        }
    }
}

/// Periodic `enqueue_all` sweep — the design's safety net against a dropped
/// enqueue (design ~line 282).
///
/// **Disabled by default, deliberately.** Two measured reasons, both of which
/// go away once the per-cell recompute stops full-scanning the government table
/// (see `docs/per_cell_recompute_full_scan.md`):
///
/// 1. A sweep enqueues ~339,000 cells. At the drain's measured ~0.9 s/cell that
///    is ~85 h of work, so any interval under ~4 days would pile sweeps on top
///    of each other.
/// 2. Worse, the drain is deliberately oldest-enqueued-first (so no source
///    starves). A bulk sweep therefore sits *in front of* every subsequent OSM
///    edit: a building edited a minute after the sweep started would not reach
///    the serving tables until the whole sweep drained. The safety net would
///    cost hours of freshness on the thing it is protecting.
///
/// Turn it on once a cell recompute is cheap enough that a sweep completes in
/// well under the interval.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct MatchReconcileConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
}

impl Default for MatchReconcileConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: 86400,
            timeout_seconds: 1800,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct UpdatesConfig {
    pub default_minutes: u64,
    pub max_minutes: u64,
}

impl Default for UpdatesConfig {
    fn default() -> Self {
        Self {
            default_minutes: 60,
            max_minutes: 1440,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct JobsConfig {
    pub osm_update: JobConfig,
    pub export_log_prune: ExportLogPruneConfig,
    pub bdot10k_update: JobConfig,
    pub egib_update: JobConfig,
    pub prg_update: JobConfig,
    pub match_refresh: MatchRefreshConfig,
    pub match_reconcile: MatchReconcileConfig,
}

impl Default for JobsConfig {
    fn default() -> Self {
        // Government snapshots are republished irregularly, so a daily poll
        // is plenty; the ETag HEAD check makes a no-op poll nearly free.
        let daily = |timeout_seconds| JobConfig {
            enabled: true,
            interval_seconds: 86400,
            timeout_seconds,
        };
        Self {
            osm_update: JobConfig::default(),
            export_log_prune: ExportLogPruneConfig::default(),
            bdot10k_update: daily(3600),
            egib_update: daily(3600),
            // PRG streams ~16 GML files out of a ~1.7GB zip, so it needs longer.
            prg_update: daily(7200),
            match_refresh: MatchRefreshConfig::default(),
            match_reconcile: MatchReconcileConfig::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct PackageConfig {
    /// Maximum allowed area of a /package request bounding box, in square degrees.
    pub max_area_sq_deg: f64,
}

impl Default for PackageConfig {
    fn default() -> Self {
        Self {
            max_area_sq_deg: 0.04,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DownloadUrls {
    pub osm_pbf: String,
    pub bdot10k: String,
    pub egib: String,
    pub prg: String,
    pub osm_replication: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_path: "./osmpbudynkiv2.duckdb".to_string(),
            rocksdb_path: "./osmpbudynkiv2.rocksdb".to_string(),
            rocksdb_block_cache_mb: 512,
            rocksdb_write_buffer_mb: 64,
            log_level: "info".to_string(),
            http_listen_addr: "127.0.0.1:3000".to_string(),
            download_dir: None,
            teryt: TerytConfig::default(),
            // All SET commands use GLOBAL scope. The `run` server pools multiple
            // connections that are try_clone()s of one base connection (see
            // server::ClonedConnectionManager); most DuckDB settings default to
            // GLOBAL scope and are automatically visible to every clone, but a
            // few (e.g. geometry_always_xy, which the spatial extension
            // registers as SESSION-scoped) are NOT and silently only apply to
            // whichever single connection ran the bare `SET` -- verified
            // empirically, see docs/duckdb_connection_visibility_investigation.md.
            // Writing `SET GLOBAL` for all of them makes every setting behave
            // the same way (apply once, visible to every pooled connection)
            // instead of depending on which options happen to default to
            // SESSION scope today.
            duckdb_init_commands: vec![
                "INSTALL spatial".to_string(),
                "LOAD spatial".to_string(),
                "INSTALL icu".to_string(),
                "LOAD icu".to_string(),
                "SET GLOBAL preserve_insertion_order = false".to_string(),
                "SET GLOBAL geometry_always_xy = true".to_string(),
                "SET GLOBAL temp_directory = './osmpbudynkiv2.duckdb.tmp'".to_string(),
                "SET GLOBAL memory_limit = '4GB'".to_string(),
                "SET GLOBAL threads = 8".to_string(),
            ],
            db_pool_size: 8,
            download_urls: DownloadUrls::default(),
            jobs: JobsConfig::default(),
            package: PackageConfig::default(),
            updates: UpdatesConfig::default(),
        }
    }
}

impl Default for DownloadUrls {
    fn default() -> Self {
        Self {
            osm_pbf: "https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf"
                .to_string(),
            bdot10k: "https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GeoParquet/OT_BUBD_A.parquet"
                .to_string(),
            egib: "https://opendata.geoportal.gov.pl/InneDane/latest_exports/eziudp_wfs/PARQUET/0_budynki.parquet"
                .to_string(),
            prg: "https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml"
                .to_string(),
            osm_replication:
                "https://download.openstreetmap.fr/replication/europe/poland/minute".to_string(),
        }
    }
}

impl Config {
    /// Returns the effective download directory: the configured path, or the system temp dir.
    pub fn download_dir(&self) -> PathBuf {
        match &self.download_dir {
            Some(dir) => PathBuf::from(dir),
            None => std::env::temp_dir(),
        }
    }
}

pub fn load_config(path: Option<&Path>) -> Result<Config> {
    match path {
        Some(p) => {
            let content = fs::read_to_string(p)
                .with_context(|| format!("Failed to read config file: {p:?}"))?;
            let config: Config = toml::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {p:?}"))?;
            Ok(config)
        }
        None => Ok(Config::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_load_config_none_returns_defaults() {
        let config = load_config(None).unwrap();
        assert_eq!(config.db_path, "./osmpbudynkiv2.duckdb");
        assert_eq!(config.rocksdb_path, "./osmpbudynkiv2.rocksdb");
        assert_eq!(config.rocksdb_block_cache_mb, 512);
        assert_eq!(config.rocksdb_write_buffer_mb, 64);
        assert_eq!(config.log_level, "info");
        assert!(config.download_dir.is_none());
        assert_eq!(config.download_dir(), std::env::temp_dir());
        assert_eq!(config.duckdb_init_commands.len(), 9);
        assert_eq!(
            config.download_urls.osm_pbf,
            "https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf"
        );
        assert_eq!(
            config.download_urls.osm_replication,
            "https://download.openstreetmap.fr/replication/europe/poland/minute"
        );
    }

    #[test]
    fn test_load_config_partial_file() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "db_path = \"/custom/path.duckdb\"").unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.db_path, "/custom/path.duckdb");
        // Other fields should be defaults
        assert_eq!(config.log_level, "info");
        assert_eq!(config.duckdb_init_commands.len(), 9);
    }

    #[test]
    fn test_load_config_full_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
db_path = "/other/db.duckdb"
log_level = "debug"
duckdb_init_commands = ["INSTALL spatial", "LOAD spatial"]

[download_urls]
osm_pbf = "https://example.com/osm.pbf"
bdot10k = "https://example.com/bdot10k.zip"
egib = "https://example.com/egib.parquet"
prg = "https://example.com/prg.zip"
osm_replication = "https://example.com/replication"
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.db_path, "/other/db.duckdb");
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.duckdb_init_commands.len(), 2);
        assert_eq!(config.download_urls.osm_pbf, "https://example.com/osm.pbf");
        assert_eq!(
            config.download_urls.osm_replication,
            "https://example.com/replication"
        );
    }

    #[test]
    fn test_load_config_nonexistent_file() {
        let result = load_config(Some(Path::new("/nonexistent/config.toml")));
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_invalid_toml() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, "this is not valid toml [[[").unwrap();

        let result = load_config(Some(tmp.path()));
        assert!(result.is_err());
    }
    #[test]
    fn test_download_dir_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "download_dir = \"/my/downloads\"").unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.download_dir.as_deref(), Some("/my/downloads"));
        assert_eq!(
            config.download_dir(),
            std::path::PathBuf::from("/my/downloads")
        );
    }

    #[test]
    fn test_rocksdb_config_defaults() {
        let config = load_config(None).unwrap();
        assert_eq!(config.rocksdb_path, "./osmpbudynkiv2.rocksdb");
        assert_eq!(config.rocksdb_block_cache_mb, 512);
        assert_eq!(config.rocksdb_write_buffer_mb, 64);
    }

    #[test]
    fn test_rocksdb_config_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
rocksdb_path = "/custom/rocksdb"
rocksdb_block_cache_mb = 256
rocksdb_write_buffer_mb = 32
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.rocksdb_path, "/custom/rocksdb");
        assert_eq!(config.rocksdb_block_cache_mb, 256);
        assert_eq!(config.rocksdb_write_buffer_mb, 32);
    }

    #[test]
    fn test_teryt_config_defaults() {
        let config = load_config(None).unwrap();
        assert!(config.teryt.download);
        assert!(config.teryt.api_username.is_none());
        assert!(config.teryt.api_password.is_none());
        assert!(config.teryt.file_path.is_none());
    }

    #[test]
    fn test_teryt_config_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[teryt]
download = false
api_username = "testuser"
api_password = "testpass"
file_path = "/data/TERC.zip"
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert!(!config.teryt.download);
        assert_eq!(config.teryt.api_username.as_deref(), Some("testuser"));
        assert_eq!(config.teryt.api_password.as_deref(), Some("testpass"));
        assert_eq!(config.teryt.file_path.as_deref(), Some("/data/TERC.zip"));
    }

    #[test]
    fn test_jobs_config_defaults() {
        let config = load_config(None).unwrap();
        assert!(config.jobs.osm_update.enabled);
        assert_eq!(config.jobs.osm_update.interval_seconds, 60);
        assert_eq!(config.jobs.osm_update.timeout_seconds, 600);
        assert!(config.jobs.match_refresh.enabled);
        assert_eq!(config.jobs.match_refresh.interval_seconds, 30);
        assert_eq!(config.jobs.match_refresh.timeout_seconds, 300);
        assert_eq!(config.jobs.match_refresh.batch_size, 512);
        // Off by default on purpose: a sweep enqueues ~339k cells, which the
        // drain cannot absorb at its current per-cell cost, and would starve
        // fresh OSM edits behind it. See MatchReconcileConfig.
        assert!(!config.jobs.match_reconcile.enabled);
        assert_eq!(config.jobs.match_reconcile.interval_seconds, 86400);
    }

    /// The reconcile job must still be configurable on, or the safety net is
    /// unreachable without a code change.
    #[test]
    fn match_reconcile_can_be_enabled_from_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.toml");
        std::fs::write(
            &path,
            "[jobs.match_reconcile]\nenabled = true\ninterval_seconds = 604800\n",
        )
        .unwrap();
        let config = load_config(Some(path.as_path())).unwrap();
        assert!(config.jobs.match_reconcile.enabled);
        assert_eq!(config.jobs.match_reconcile.interval_seconds, 604800);
        // Unset field still falls back to the default.
        assert_eq!(config.jobs.match_reconcile.timeout_seconds, 1800);
    }

    #[test]
    fn test_jobs_config_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[jobs.osm_update]
enabled = false
interval_seconds = 30
timeout_seconds = 120
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert!(!config.jobs.osm_update.enabled);
        assert_eq!(config.jobs.osm_update.interval_seconds, 30);
        assert_eq!(config.jobs.osm_update.timeout_seconds, 120);
    }

    #[test]
    fn test_jobs_config_partial_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[jobs.osm_update]
interval_seconds = 120
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        // overridden
        assert_eq!(config.jobs.osm_update.interval_seconds, 120);
        // defaults preserved
        assert!(config.jobs.osm_update.enabled);
        assert_eq!(config.jobs.osm_update.timeout_seconds, 600);
    }

    #[test]
    fn test_package_config_defaults() {
        let config = load_config(None).unwrap();
        assert_eq!(config.package.max_area_sq_deg, 0.04);
    }

    #[test]
    fn test_package_config_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[package]
max_area_sq_deg = 0.1
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.package.max_area_sq_deg, 0.1);
    }

    #[test]
    fn test_export_log_prune_config_defaults() {
        let config = load_config(None).unwrap();
        assert!(config.jobs.export_log_prune.enabled);
        assert_eq!(config.jobs.export_log_prune.interval_seconds, 86400);
        assert_eq!(config.jobs.export_log_prune.timeout_seconds, 60);
        assert_eq!(config.jobs.export_log_prune.retention_days, 365);
    }

    #[test]
    fn test_export_log_prune_config_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[jobs.export_log_prune]
enabled = false
interval_seconds = 3600
timeout_seconds = 30
retention_days = 30
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert!(!config.jobs.export_log_prune.enabled);
        assert_eq!(config.jobs.export_log_prune.interval_seconds, 3600);
        assert_eq!(config.jobs.export_log_prune.timeout_seconds, 30);
        assert_eq!(config.jobs.export_log_prune.retention_days, 30);
    }

    #[test]
    fn test_updates_config_defaults() {
        let config = load_config(None).unwrap();
        assert_eq!(config.updates.default_minutes, 60);
        assert_eq!(config.updates.max_minutes, 1440);
    }

    #[test]
    fn test_updates_config_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[updates]
default_minutes = 30
max_minutes = 720
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.updates.default_minutes, 30);
        assert_eq!(config.updates.max_minutes, 720);
    }

    #[test]
    fn test_dataset_update_job_defaults() {
        let config = load_config(None).unwrap();
        assert!(config.jobs.bdot10k_update.enabled);
        assert_eq!(config.jobs.bdot10k_update.interval_seconds, 86400);
        assert_eq!(config.jobs.bdot10k_update.timeout_seconds, 3600);
        assert!(config.jobs.egib_update.enabled);
        assert_eq!(config.jobs.egib_update.interval_seconds, 86400);
        assert_eq!(config.jobs.egib_update.timeout_seconds, 3600);
        assert!(config.jobs.prg_update.enabled);
        assert_eq!(config.jobs.prg_update.interval_seconds, 86400);
        assert_eq!(config.jobs.prg_update.timeout_seconds, 7200);
    }

    #[test]
    fn test_dataset_update_job_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[jobs.bdot10k_update]
enabled = false
interval_seconds = 3600
timeout_seconds = 300
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert!(!config.jobs.bdot10k_update.enabled);
        assert_eq!(config.jobs.bdot10k_update.interval_seconds, 3600);
        assert_eq!(config.jobs.bdot10k_update.timeout_seconds, 300);
        // Unrelated jobs keep their defaults.
        assert!(config.jobs.egib_update.enabled);
        assert_eq!(config.jobs.egib_update.interval_seconds, 86400);
    }

    #[test]
    fn test_teryt_partial_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[teryt]
download = false
file_path = "/data/TERC.zip"
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert!(!config.teryt.download);
        assert!(config.teryt.api_username.is_none());
        assert_eq!(config.teryt.file_path.as_deref(), Some("/data/TERC.zip"));
    }
}
