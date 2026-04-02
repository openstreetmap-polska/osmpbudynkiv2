use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub db_path: String,
    pub rocksdb_path: String,
    pub rocksdb_block_cache_mb: u64,
    pub rocksdb_write_buffer_mb: u64,
    pub log_level: String,
    pub duckdb_init_commands: Vec<String>,
    pub download_urls: DownloadUrls,
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
        }
    }
}

impl Default for DownloadUrls {
    fn default() -> Self {
        Self {
            osm_pbf: "https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf"
                .to_string(),
            bdot10k: "https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GeoParquet/Polska_BDOT10k_GeoParquet.zip"
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
        write!(tmp, "db_path = \"/custom/path.duckdb\"\n").unwrap();

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
}
