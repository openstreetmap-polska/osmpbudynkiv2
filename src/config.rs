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
    pub download_urls: DownloadUrls,
    pub jobs: JobsConfig,
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

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
pub struct JobsConfig {
    pub osm_update: JobConfig,
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
            duckdb_init_commands: vec![
                "INSTALL spatial".to_string(),
                "LOAD spatial".to_string(),
                "SET preserve_insertion_order = false".to_string(),
                "SET geometry_always_xy = true".to_string(),
                "SET temp_directory = './osmpbudynkiv2.duckdb.tmp'".to_string(),
                "SET memory_limit = '4GB'".to_string(),
                "SET threads = 8".to_string(),
            ],
            download_urls: DownloadUrls::default(),
            jobs: JobsConfig::default(),
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
        assert_eq!(config.duckdb_init_commands.len(), 7);
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
        assert_eq!(config.duckdb_init_commands.len(), 7);
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
