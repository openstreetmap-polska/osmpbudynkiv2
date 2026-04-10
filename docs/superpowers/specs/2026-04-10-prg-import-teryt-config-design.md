# PRG Import: TERYT Auto-Download & Config Expansion

## Summary

Expand PRG address import to support automatic TERYT dictionary download from the government SOAP API, automatic PRG zip download (matching bdot10k/egib pattern), and consolidate TERYT-related config into a dedicated `[teryt]` section.

## Current State

- PRG import requires `--file <ZIP>` (no auto-download) and `--terc-file <PATH>` or `terc_path` in config (local file only).
- prg_convert's `get_teryt_mapping()` already supports downloading TERYT from the API when `download_teryt=true` with username/password credentials.
- Other imports (bdot10k, egib) auto-download via `download_file()` to `config.download_dir()` when no `--file` is given.

## Design

### 1. Config Changes

Replace the top-level `terc_path` with a `[teryt]` section:

```toml
[teryt]
# When true, download TERYT dictionary from the government API automatically.
# When false, a local file must be provided via `file_path` or `--terc-file`.
download = true

# Credentials for the TERYT SOAP API. Falls back to TERYT_API_USERNAME /
# TERYT_API_PASSWORD environment variables when not set.
# api_username = "myuser"
# api_password = "mypass"

# Path to a local TERC dictionary file (.zip or .xml). Used when download = false.
# Overridden by the --terc-file CLI flag.
# file_path = "./data/TERC_Urzedowy.zip"
```

In the Rust `Config` struct:

```rust
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct TerytConfig {
    pub download: bool,          // default: true
    pub api_username: Option<String>,
    pub api_password: Option<String>,
    pub file_path: Option<String>,
}
```

Default: `download = true`, others `None`. When `download` is true and no explicit credentials in config, fall back to `TERYT_API_USERNAME` / `TERYT_API_PASSWORD` env vars (matching prg_convert's own CLI behavior).

### 2. PRG Auto-Download

When `--file` is not passed, download the PRG zip using the existing `download_file()` utility to `config.download_dir()`, using `config.download_urls.prg` as the URL. This matches the bdot10k/egib pattern exactly.

### 3. TERYT Resolution Logic

Priority order for TERYT data:
1. `--terc-file` CLI flag (highest priority, local file)
2. `config.teryt.file_path` (local file from config)
3. Auto-download from API when `config.teryt.download` is true (default)

When downloading, credentials are resolved as:
1. `config.teryt.api_username` / `config.teryt.api_password`
2. `TERYT_API_USERNAME` / `TERYT_API_PASSWORD` env vars

Error if downloading is selected but no credentials are available from either source.

### 4. CLI Changes

- `--terc-file` remains as an optional override on `import prg` (no change to flag itself)
- Remove the hard error when no TERC source is provided — the default is now auto-download

### 5. What Stays the Same

- `OutputFormat::CSV` / `SCHEMA_CSV` — the GeoParquet schema's geometry column doesn't help (DuckDB arrow vtab receives geoarrow struct, not DuckDB GEOMETRY)
- Batch size stays at 2048 (arrow vtab constraint)
- CRS stays at EPSG:4326
- PRG download is handled by this project, not prg_convert

### 6. Files to Modify

- `src/config.rs` — add `TerytConfig`, replace `terc_path`, update defaults/tests
- `src/import/prg.rs` — add PRG download, update TERYT resolution logic
- `src/import/mod.rs` — no change needed (already passes config through)
- `src/cli.rs` — no structural changes needed
- `example_config.toml` — add `[teryt]` section, remove old `terc_path`
- `tests/cli_import_prg.rs` — update tests for new config shape
