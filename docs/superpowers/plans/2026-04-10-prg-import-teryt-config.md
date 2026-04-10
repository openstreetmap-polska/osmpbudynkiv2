# PRG Import: TERYT Auto-Download & Config Expansion

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable PRG import to auto-download both PRG data and TERYT dictionary, with TERYT config consolidated into a `[teryt]` section.

**Architecture:** Add `TerytConfig` struct nested under `Config`. Update PRG import to download PRG zip via existing `download_file` infrastructure and resolve TERYT data from API/file/CLI with priority chain. Keep existing parser interface (`OutputFormat::CSV`, batch size 2048, CRS EPSG:4326).

**Tech Stack:** Rust, prg_convert (library), DuckDB, reqwest (via prg_convert for TERYT API)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/config.rs` | Modify | Add `TerytConfig` struct, replace `terc_path` field, update `Default`, update tests |
| `src/download.rs` | Modify | Add `download_file_as()` for URLs where filename can't be extracted |
| `src/import/prg.rs` | Modify | Add PRG auto-download, refactor TERYT resolution with priority chain |
| `example_config.toml` | Modify | Replace `terc_path` with `[teryt]` section |
| `tests/cli_import_prg.rs` | Modify | Update tests for new config shape, add test for teryt config file_path |

---

### Task 1: Add `TerytConfig` to config

**Files:**
- Modify: `src/config.rs`

- [ ] **Step 1: Write tests for the new TerytConfig**

Add these tests to the existing `mod tests` in `src/config.rs`:

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib config::tests`
Expected: compilation errors — `TerytConfig` and `config.teryt` don't exist yet.

- [ ] **Step 3: Implement TerytConfig**

In `src/config.rs`, add the struct and update `Config`:

```rust
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
```

In the `Config` struct, replace `terc_path: Option<String>` with `teryt: TerytConfig`:

```rust
pub struct Config {
    pub db_path: String,
    pub rocksdb_path: String,
    pub rocksdb_block_cache_mb: u64,
    pub rocksdb_write_buffer_mb: u64,
    pub log_level: String,
    pub download_dir: Option<String>,
    pub duckdb_init_commands: Vec<String>,
    pub download_urls: DownloadUrls,
    pub teryt: TerytConfig,
}
```

Update `Config::default()` — remove `terc_path: None`, add `teryt: TerytConfig::default()`.

- [ ] **Step 4: Fix existing tests that reference `terc_path`**

In `test_load_config_none_returns_defaults`, remove the line:
```rust
assert!(config.download_dir.is_none());
```
Wait — that line is about `download_dir`, not `terc_path`. Search for any test referencing `terc_path` and update. The existing defaults test doesn't assert on `terc_path`, so no changes needed to existing tests.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib config::tests`
Expected: all tests pass, including the 3 new ones.

- [ ] **Step 6: Commit**

```bash
git add src/config.rs
git commit -m "feat(config): add TerytConfig section, replace terc_path"
```

---

### Task 2: Add `download_file_as` to download module

**Files:**
- Modify: `src/download.rs`

The PRG download URL (`https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml`) doesn't have a clean filename extractable via `rsplit('/')`. Add a variant that accepts an explicit filename.

- [ ] **Step 1: Add `download_file_as` function**

In `src/download.rs`, add after the existing `download_file` function:

```rust
/// Download a file from `url` to `dest_dir` with an explicit `file_name`.
/// Useful when the URL doesn't contain a clean filename (e.g. query-string URLs).
pub fn download_file_as(url: &str, dest_dir: &Path, file_name: &str) -> Result<PathBuf> {
    let dest_path = dest_dir.join(file_name);

    if dest_path.exists() {
        info!(path = %dest_path.display(), "File already exists, skipping download");
        return Ok(dest_path);
    }

    std::fs::create_dir_all(dest_dir)
        .with_context(|| format!("Failed to create directory {dest_dir:?}"))?;

    let rt = Runtime::new().context("Failed to create tokio runtime")?;
    rt.block_on(download_with_retry(url, &dest_path))?;

    Ok(dest_path)
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build`
Expected: compiles without errors.

- [ ] **Step 3: Commit**

```bash
git add src/download.rs
git commit -m "feat(download): add download_file_as for URLs without clean filenames"
```

---

### Task 3: Update PRG import with auto-download and TERYT resolution

**Files:**
- Modify: `src/import/prg.rs`

- [ ] **Step 1: Update the import function signature and add PRG download**

Replace the current `import` function's file-handling and TERYT-resolution sections. The full updated function:

```rust
use crate::download::download_file_as;

// ... existing imports stay ...

const PRG_DOWNLOAD_FILENAME: &str = "PRG-punkty_adresowe.zip";

pub fn import(
    conn: &Connection,
    config: &Config,
    file: Option<&Path>,
    terc_file: Option<&Path>,
    url: &str,
) -> Result<()> {
    // --- Resolve PRG zip path ---
    let zip_path = match file {
        Some(p) => PathBuf::from(p),
        None => {
            info!(url, "Downloading PRG data");
            download_file_as(url, &config.download_dir(), PRG_DOWNLOAD_FILENAME)
                .context("Failed to download PRG data")?
        }
    };

    let zip_str = zip_path
        .to_str()
        .context("PRG zip path is not valid UTF-8")?;

    // --- Resolve TERYT mapping ---
    // Priority: --terc-file CLI flag > config.teryt.file_path > auto-download from API
    let terc_file_path = terc_file
        .map(PathBuf::from)
        .or_else(|| config.teryt.file_path.as_ref().map(PathBuf::from));

    info!(
        path = zip_str,
        teryt_source = if terc_file_path.is_some() { "file" } else { "api" },
        "Importing PRG addresses (2021 schema)"
    );

    let total = std::time::Instant::now();

    let t = std::time::Instant::now();
    let terc = if let Some(ref path) = terc_file_path {
        let terc_str = path.to_str().context("TERC path is not valid UTF-8")?;
        info!(path = terc_str, "Loading TERYT mapping from file");
        get_teryt_mapping(false, &None, &None, &Some(path.clone()))
            .with_context(|| format!("Failed to load TERC mapping from {terc_str}"))?
    } else {
        // Auto-download from API
        let username = config
            .teryt
            .api_username
            .clone()
            .or_else(|| std::env::var("TERYT_API_USERNAME").ok())
            .context(
                "TERYT API username required: set teryt.api_username in config \
                 or TERYT_API_USERNAME env var",
            )?;
        let password = config
            .teryt
            .api_password
            .clone()
            .or_else(|| std::env::var("TERYT_API_PASSWORD").ok())
            .context(
                "TERYT API password required: set teryt.api_password in config \
                 or TERYT_API_PASSWORD env var",
            )?;
        info!("Downloading TERYT mapping from API");
        get_teryt_mapping(true, &Some(username), &Some(password), &None)
            .context("Failed to download TERC mapping from TERYT API")?
    };
    info!(
        entries = terc.len(),
        elapsed = %format_duration(t.elapsed()),
        "Step done: load TERC mapping"
    );

    // ... rest of the function stays the same from `let mut archive = ...` onwards ...
```

The code from `let mut archive = ZipArchive::new(...)` through the end of the function stays unchanged.

- [ ] **Step 2: Verify it compiles**

Run: `cargo build`
Expected: compiles without errors. The `_url` parameter is now used (rename to `url`).

- [ ] **Step 3: Run existing test to verify backward compatibility**

Run: `cargo test --test cli_import_prg test_import_prg_from_fixture`
Expected: PASS — the `--file` + `--terc-file` path still works identically.

- [ ] **Step 4: Commit**

```bash
git add src/import/prg.rs
git commit -m "feat(import): add PRG auto-download and TERYT API resolution"
```

---

### Task 4: Update example_config.toml

**Files:**
- Modify: `example_config.toml`

- [ ] **Step 1: Replace terc_path with [teryt] section**

Remove the commented-out `terc_path` line and add the `[teryt]` section. The final file should have this replacing the old `terc_path` block:

```toml
# TERYT dictionary configuration for PRG address imports.
# The TERYT (TERC) dictionary maps administrative unit codes to names.
[teryt]
# When true, download TERYT dictionary from the government SOAP API automatically.
# When false, a local file must be provided via `file_path` below or `--terc-file` CLI flag.
download = true

# Credentials for the TERYT SOAP API (https://api.stat.gov.pl/Home/TerytApi).
# Falls back to TERYT_API_USERNAME / TERYT_API_PASSWORD environment variables when not set.
# api_username = "myuser"
# api_password = "mypass"

# Path to a local TERC dictionary file (.zip or .xml). Used when download = false.
# Can be overridden by the --terc-file CLI flag.
# file_path = "./data/TERC_Urzedowy.zip"
```

- [ ] **Step 2: Commit**

```bash
git add example_config.toml
git commit -m "docs: update example_config.toml with [teryt] section"
```

---

### Task 5: Update integration tests

**Files:**
- Modify: `tests/cli_import_prg.rs`

- [ ] **Step 1: Update `test_import_prg_missing_terc_fails` for new behavior**

With the new config defaults (`teryt.download = true`), running without `--terc-file` will attempt an API download (which will fail without credentials in CI). Update the test to set `teryt.download = false` in config and verify it still requires a file:

```rust
#[test]
fn test_import_prg_missing_terc_fails() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    // Append teryt config that disables download but provides no file
    use std::io::Write;
    let mut cfg_file = std::fs::OpenOptions::new()
        .append(true)
        .open(cfg.path())
        .unwrap();
    writeln!(cfg_file, "\n[teryt]\ndownload = false").unwrap();

    // No --terc-file and teryt.download = false with no file_path → should fail
    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
        ])
        .assert()
        .failure();
}
```

- [ ] **Step 2: Add test for teryt.file_path config option**

```rust
#[test]
fn test_import_prg_teryt_from_config_file_path() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    // Set teryt.file_path in config instead of using --terc-file CLI flag
    use std::io::Write;
    let mut cfg_file = std::fs::OpenOptions::new()
        .append(true)
        .open(cfg.path())
        .unwrap();
    writeln!(
        cfg_file,
        "\n[teryt]\ndownload = false\nfile_path = \"fixtures/teryt.zip\""
    )
    .unwrap();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("PRG import complete"));

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM prg_addresses", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3);
}
```

- [ ] **Step 3: Add test for missing credentials when download is enabled**

```rust
#[test]
fn test_import_prg_download_teryt_missing_credentials_fails() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    // teryt.download defaults to true, but no credentials in config or env
    cmd()
        .env_remove("TERYT_API_USERNAME")
        .env_remove("TERYT_API_PASSWORD")
        .args([
            "--config",
            &cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
        ])
        .assert()
        .failure();
}
```

- [ ] **Step 4: Run all PRG import tests**

Run: `cargo test --test cli_import_prg`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/cli_import_prg.rs
git commit -m "test: update PRG import tests for teryt config and auto-download"
```

---

### Task 6: Final verification

- [ ] **Step 1: Run full test suite**

Run: `cargo test`
Expected: all tests pass.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy`
Expected: no warnings.

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt -- --check`
Expected: no formatting issues.
