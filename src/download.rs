use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
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
                // Remove partial file so the next attempt starts clean
                let _ = std::fs::remove_file(dest_path);
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

    let pb = match total_size {
        Some(size) => {
            let pb = ProgressBar::new(size);
            pb.set_style(
                ProgressStyle::with_template(
                    "{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
                )
                .unwrap()
                .progress_chars("=>-"),
            );
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template(
                    "{msg}\n{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec})",
                )
                .unwrap(),
            );
            pb
        }
    };
    pb.set_message(format!(
        "Downloading {}",
        dest_path.file_name().unwrap_or_default().to_string_lossy()
    ));

    let file = std::fs::File::create(dest_path)
        .with_context(|| format!("Failed to create {dest_path:?}"))?;
    let mut writer = BufWriter::new(file);
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error reading download stream")?;
        writer
            .write_all(&chunk)
            .with_context(|| format!("Failed to write to {dest_path:?}"))?;
        pb.inc(chunk.len() as u64);
    }

    writer
        .flush()
        .with_context(|| format!("Failed to flush {dest_path:?}"))?;

    pb.finish_with_message(format!(
        "Downloaded {}",
        dest_path.file_name().unwrap_or_default().to_string_lossy()
    ));
    info!(path = %dest_path.display(), "Download complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    async fn serve_and_download(body: &'static [u8], with_content_length: bool) -> Vec<u8> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        let body_len = body.len();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.readable().await;
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;

            let headers = if with_content_length {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {body_len}\r\nContent-Type: application/octet-stream\r\n\r\n"
                )
            } else {
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n"
                    .to_string()
            };
            tokio::io::AsyncWriteExt::write_all(&mut stream, headers.as_bytes())
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut stream, body)
                .await
                .unwrap();
        });

        let tmp = tempfile::tempdir().unwrap();
        let url = format!("http://{addr}/testfile.bin");
        // download_file uses block_on internally; call do_download directly to avoid nested runtime
        let client = reqwest::Client::new();
        let dest = tmp.path().join("testfile.bin");
        do_download(&client, &url, &dest).await.unwrap();
        std::fs::read(&dest).unwrap()
    }

    #[tokio::test]
    async fn download_with_known_size() {
        let data = b"hello world progress bar test data";
        let result = serve_and_download(data, true).await;
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn download_without_content_length() {
        let data = b"response without content-length header";
        let result = serve_and_download(data, false).await;
        assert_eq!(result, data);
    }
}
