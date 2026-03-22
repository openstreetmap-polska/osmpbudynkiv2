use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::runtime::Runtime;
use tracing::info;

const MAX_RETRIES: u32 = 3;

/// Download a file from `url` to `dest_dir`, returning the path to the downloaded file.
/// Uses exponential backoff on failure.
pub fn download_file(url: &str, dest_dir: &Path) -> Result<PathBuf> {
    let file_name = url
        .rsplit('/')
        .next()
        .context("Could not extract filename from URL")?;
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

async fn download_with_retry(url: &str, dest_path: &Path) -> Result<()> {
    let client = reqwest::Client::new();
    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        info!(url, attempt, "Downloading");

        match do_download(&client, url, dest_path).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::warn!(attempt, error = %e, "Download failed");
                last_error = Some(e);
                if attempt < MAX_RETRIES {
                    let delay = std::time::Duration::from_secs(2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap()).context(format!(
        "Failed to download {url} after {MAX_RETRIES} attempts"
    ))
}

async fn do_download(client: &reqwest::Client, url: &str, dest_path: &Path) -> Result<()> {
    let response = client.get(url).send().await?.error_for_status()?;

    let total_size = response.content_length();
    if let Some(size) = total_size {
        info!(size_mb = size / 1_048_576, "Download size");
    }

    let bytes = response.bytes().await?;
    std::fs::write(dest_path, &bytes)
        .with_context(|| format!("Failed to write to {dest_path:?}"))?;

    info!(path = %dest_path.display(), "Download complete");
    Ok(())
}
