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
    /// Directory the `run` HTTP server serves static frontend assets from
    /// (mounted as a fallback route, so it never shadows `/health`,
    /// `/status`, `/tiles`, `/package`, `/updates`). Deployed and versioned
    /// separately from the binary — see the frontend architecture decision:
    /// the release ships binary + config + this directory as sibling files
    /// rather than embedding assets in the binary, since a deployment
    /// already has multiple files (config, systemd unit) to manage.
    /// A missing directory is not an error at startup; requests that would
    /// have served a file just 404.
    pub web_dir: String,
    pub download_dir: Option<String>,
    /// When true (default), files that `import`/`update` downloaded
    /// themselves are deleted once consumed. Set to false to keep them in
    /// `download_dir` — e.g. to avoid re-downloading a large snapshot across
    /// repeated local runs.
    ///
    /// Never applies to a user-supplied `--file` input, which is never
    /// deleted regardless of this setting.
    ///
    /// Gotcha: `download_file`/`download_file_as` skip downloading when a
    /// file already exists at the destination (see `src/download.rs`), and
    /// datasets that always download to the same filename (PRG's zip,
    /// BDOT10k/EGIB's parquet) will silently reuse a stale leftover file
    /// instead of fetching the current snapshot if it was never cleaned up.
    /// Turning this off is safe for one-off/local use but risky for the
    /// unattended `update`/background-job paths — a stale snapshot would be
    /// re-applied as if it were fresh.
    ///
    /// Does NOT cover the OSM incremental replication downloads
    /// (`state.txt`, per-sequence `.osc.gz`) in `update::osm` — those are
    /// always cleaned up unconditionally, because `state.txt` downloads to a
    /// fixed filename and a leftover copy would make every subsequent
    /// `update osm` read a stale sequence number and silently stop applying
    /// new changes.
    pub cleanup_downloaded_files: bool,
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
    pub cache: CacheConfig,
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

/// Config for the `osm_update` background job (OSM minutely replication
/// catch-up). Same three fields and defaults as the generic [`JobConfig`] it
/// replaces (`enabled = true`, `interval_seconds = 60`, `timeout_seconds =
/// 600`), plus three fields consumed by a follow-up task (prefetching diff
/// downloads ahead of the sequence being applied, and batching commits during
/// catch-up) -- this change only adds the config plumbing, not the behaviour,
/// so the three new fields are read by nothing yet.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct OsmUpdateConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    /// How many replication diffs to download ahead of the sequence
    /// currently being applied, during catch-up.
    pub prefetch_ahead: usize,
    /// Batching only engages once the number of pending sequences exceeds
    /// this threshold, so steady state (one pending sequence per tick) stays
    /// on today's one-sequence-per-transaction path byte-for-byte. Applies
    /// only during catch-up.
    pub batch_commit_threshold: u64,
    /// Sequences per DuckDB transaction while catching up. A failed batch
    /// re-downloads every sequence in it (downloaded `.osc.gz` files are
    /// deleted right after decompression, so there is nothing on disk to
    /// resume from), which is a reason to keep this modest. Applies only
    /// during catch-up.
    pub batch_size: usize,
}

impl Default for OsmUpdateConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 60,
            timeout_seconds: 600,
            prefetch_ahead: 8,
            batch_commit_threshold: 20,
            batch_size: 20,
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
/// **Disabled by default, deliberately.** Two measured reasons. The first has
/// largely gone away; the second has not, which is why the default has not
/// moved.
///
/// 1. *Mostly resolved.* A sweep enqueues ~339,000 cells. At the drain's
///    then-measured ~0.9 s/cell that was ~85 h of work, so any interval under
///    ~4 days would pile sweeps on top of each other. Restoring the centroid
///    RTREE on the per-cell recompute cut that to ~0.02–0.05 s/cell (see
///    `docs/per_cell_recompute_cell_guard_scan.md`, sequel to
///    `per_cell_recompute_full_scan.md`), i.e. roughly **2–5 h** — which does
///    fit inside the 24 h default interval.
/// 2. *Still true, and now the sole blocker.* The drain is deliberately
///    oldest-enqueued-first (so no source starves). A bulk sweep therefore
///    sits *in front of* every subsequent OSM edit: a building edited a minute
///    after the sweep started would not reach the serving tables until the
///    whole sweep drained. Even at 2–5 h that is hours of lost freshness on
///    the thing the safety net exists to protect.
///
/// So the remaining work is not "make the cell cheaper" — it is to stop a
/// sweep from head-of-line-blocking live edits (e.g. drain reconcile-sourced
/// cells at a lower priority than OSM-sourced ones, which would need a
/// provenance column on `match_dirty_cells`). Until then, leave this off and
/// rely on `compare reconcile` being run by hand.
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

/// `Cache-Control` policy for every response the `run` HTTP server sends --
/// see `server::http_cache` for the one place these values turn into actual
/// header bytes and the full per-endpoint policy table.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct CacheConfig {
    /// z14 `/tiles` response max-age. z14 is the finest zoom -- the one
    /// `match_refresh` keeps freshest -- so it gets the shortest TTL of the
    /// two tile tiers.
    pub tile_max_age_seconds: u64,
    /// z5..=z13 (aggregated bins and unbinned points) `/tiles` response
    /// max-age. Coarser zooms move less per individual edit, so they tolerate
    /// a longer TTL than z14.
    pub agg_tile_max_age_seconds: u64,
    /// `/updates` response max-age. Matches the endpoint's pre-Phase-2
    /// hardcoded value (60s) -- see `server::updates`.
    pub updates_max_age_seconds: u64,
    /// max-age for static frontend assets under `web_dir/vendor/` (e.g. the
    /// MapLibre GL JS bundle) -- versioned by path but not content-hashed, so
    /// it gets a cautious week rather than the `immutable` treatment fonts get.
    pub static_max_age_seconds: u64,
    /// max-age for `web_dir/fonts/**`. Glyph PBFs are named by a fixed byte
    /// range per font and never change in place, so they're safe to mark
    /// `immutable` and cache for a year.
    pub font_max_age_seconds: u64,
    /// Maximum size in bytes of the in-process z14 `/tiles` response cache
    /// (`server::tile_cache::TileCache`) -- see that module's doc for the
    /// two-generation eviction design. Setting this to `0` disables the
    /// cache entirely: `TileCache::new(0)` is a genuine no-op (every lookup
    /// misses, every insert drops), not a placeholder that still allocates.
    pub tile_cache_max_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            tile_max_age_seconds: 60,
            agg_tile_max_age_seconds: 300,
            updates_max_age_seconds: 60,
            static_max_age_seconds: 604_800,
            font_max_age_seconds: 31_536_000,
            tile_cache_max_bytes: 268_435_456,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct JobsConfig {
    pub osm_update: OsmUpdateConfig,
    pub export_log_prune: ExportLogPruneConfig,
    pub bdot10k_update: JobConfig,
    pub egib_update: JobConfig,
    pub prg_update: JobConfig,
    pub match_refresh: MatchRefreshConfig,
    pub match_reconcile: MatchReconcileConfig,
    pub street_mappings_update: JobConfig,
    pub building_types_update: JobConfig,
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
            osm_update: OsmUpdateConfig::default(),
            export_log_prune: ExportLogPruneConfig::default(),
            bdot10k_update: daily(3600),
            egib_update: daily(3600),
            // PRG streams ~16 GML files out of a ~1.7GB zip, so it needs longer.
            prg_update: daily(7200),
            match_refresh: MatchRefreshConfig::default(),
            match_reconcile: MatchReconcileConfig::default(),
            street_mappings_update: JobConfig {
                enabled: false,
                interval_seconds: 86400,
                timeout_seconds: 300,
            },
            // Same rationale as street_mappings_update: applied at serve
            // time, so a stale mapping is harmless and there is no urgency
            // to auto-refresh -- an operator opts in explicitly.
            building_types_update: JobConfig {
                enabled: false,
                interval_seconds: 86400,
                timeout_seconds: 300,
            },
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
    pub street_mappings: String,
    pub bdot10k_building_types: String,
    pub egib_building_types: String,
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
            web_dir: "./web".to_string(),
            download_dir: None,
            cleanup_downloaded_files: true,
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
            cache: CacheConfig::default(),
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
            street_mappings:
                "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/street_names_mappings.csv"
                    .to_string(),
            bdot10k_building_types:
                "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/bdot10k_building_types.csv"
                    .to_string(),
            egib_building_types:
                "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/egib_building_types.csv"
                    .to_string(),
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
        assert_eq!(config.web_dir, "./web");
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
    fn test_web_dir_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "web_dir = \"/srv/osmpbudynkiv2/web\"").unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.web_dir, "/srv/osmpbudynkiv2/web");
    }

    #[test]
    fn test_cleanup_downloaded_files_default() {
        let config = load_config(None).unwrap();
        assert!(config.cleanup_downloaded_files);
    }

    #[test]
    fn test_cleanup_downloaded_files_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "cleanup_downloaded_files = false").unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert!(!config.cleanup_downloaded_files);
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
        assert_eq!(config.jobs.osm_update.prefetch_ahead, 8);
        assert_eq!(config.jobs.osm_update.batch_commit_threshold, 20);
        assert_eq!(config.jobs.osm_update.batch_size, 20);
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
        // the three new prefetch/batch fields also fall back to defaults
        // when the TOML doesn't mention them
        assert_eq!(config.jobs.osm_update.prefetch_ahead, 8);
        assert_eq!(config.jobs.osm_update.batch_commit_threshold, 20);
        assert_eq!(config.jobs.osm_update.batch_size, 20);
    }

    /// The three fields added for the prefetch/batched-commit follow-up
    /// (unread by any code yet -- this pins only that they parse and
    /// default correctly) round-trip through TOML, and omitting one of them
    /// still falls back to its own default rather than to zero.
    #[test]
    fn osm_update_config_prefetch_and_batch_fields_parse_from_toml() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[jobs.osm_update]
prefetch_ahead = 16
batch_commit_threshold = 50
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.jobs.osm_update.prefetch_ahead, 16);
        assert_eq!(config.jobs.osm_update.batch_commit_threshold, 50);
        // omitted -- falls back to its own default, not zero
        assert_eq!(config.jobs.osm_update.batch_size, 20);
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
    fn street_mappings_job_is_disabled_by_default() {
        let cfg = Config::default();
        assert!(!cfg.jobs.street_mappings_update.enabled);
        assert_eq!(cfg.jobs.street_mappings_update.interval_seconds, 86400);
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
    fn building_types_job_is_disabled_by_default() {
        let cfg = Config::default();
        assert!(!cfg.jobs.building_types_update.enabled);
        assert_eq!(cfg.jobs.building_types_update.interval_seconds, 86400);
    }

    #[test]
    fn building_types_urls_default_to_this_repo() {
        let cfg = Config::default();
        assert_eq!(
            cfg.download_urls.bdot10k_building_types,
            "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/bdot10k_building_types.csv"
        );
        assert_eq!(
            cfg.download_urls.egib_building_types,
            "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/egib_building_types.csv"
        );
    }

    #[test]
    fn building_types_urls_can_be_overridden() {
        let toml = r#"
[download_urls]
bdot10k_building_types = "https://example.test/b.csv"
egib_building_types = "https://example.test/e.csv"
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert_eq!(
            cfg.download_urls.bdot10k_building_types,
            "https://example.test/b.csv"
        );
        assert_eq!(
            cfg.download_urls.egib_building_types,
            "https://example.test/e.csv"
        );
    }

    #[test]
    fn street_mappings_url_defaults_to_this_repo() {
        let cfg = Config::default();
        assert_eq!(
            cfg.download_urls.street_mappings,
            "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/street_names_mappings.csv"
        );
    }

    #[test]
    fn street_mappings_url_can_be_overridden() {
        let toml = r#"
[download_urls]
street_mappings = "https://example.test/m.csv"
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert_eq!(
            cfg.download_urls.street_mappings,
            "https://example.test/m.csv"
        );
    }

    #[test]
    fn test_cache_config_defaults() {
        let config = load_config(None).unwrap();
        assert_eq!(config.cache.tile_max_age_seconds, 60);
        assert_eq!(config.cache.agg_tile_max_age_seconds, 300);
        assert_eq!(config.cache.updates_max_age_seconds, 60);
        assert_eq!(config.cache.static_max_age_seconds, 604_800);
        assert_eq!(config.cache.font_max_age_seconds, 31_536_000);
        assert_eq!(config.cache.tile_cache_max_bytes, 268_435_456);
    }

    #[test]
    fn test_cache_config_partial_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[cache]
tile_max_age_seconds = 30
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.cache.tile_max_age_seconds, 30);
        // Unset fields still fall back to their defaults.
        assert_eq!(config.cache.agg_tile_max_age_seconds, 300);
        assert_eq!(config.cache.updates_max_age_seconds, 60);
        assert_eq!(config.cache.static_max_age_seconds, 604_800);
        assert_eq!(config.cache.font_max_age_seconds, 31_536_000);
        assert_eq!(config.cache.tile_cache_max_bytes, 268_435_456);
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
