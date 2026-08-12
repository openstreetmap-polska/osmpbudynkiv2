use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::runtime::Runtime;
use tracing::{debug, info};

const MAX_RETRIES: u32 = 3;

/// Download a file from `url` to `dest_dir`, returning the path to the downloaded file.
/// Uses exponential backoff on failure.
pub fn download_file(url: &str, dest_dir: &Path) -> Result<PathBuf> {
    download_file_impl(url, dest_dir, true)
}

/// Same as [`download_file`], but without a progress bar. For callers that
/// download many small files in a loop (e.g. OSM replication diffs), where a
/// bar per file is just noise rather than useful progress.
pub fn download_file_quiet(url: &str, dest_dir: &Path) -> Result<PathBuf> {
    download_file_impl(url, dest_dir, false)
}

fn download_file_impl(url: &str, dest_dir: &Path, show_progress: bool) -> Result<PathBuf> {
    let file_name = url
        .rsplit('/')
        .next()
        .context("Could not extract filename from URL")?;
    let dest_path = dest_dir.join(file_name);

    if dest_path.exists() {
        if show_progress {
            info!(path = %dest_path.display(), "File already exists, skipping download");
        } else {
            debug!(path = %dest_path.display(), "File already exists, skipping download");
        }
        return Ok(dest_path);
    }

    std::fs::create_dir_all(dest_dir)
        .with_context(|| format!("Failed to create directory {dest_dir:?}"))?;

    let rt = Runtime::new().context("Failed to create tokio runtime")?;
    rt.block_on(download_with_retry(url, &dest_path, show_progress))?;

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
    rt.block_on(download_with_retry(url, &dest_path, true))?;

    Ok(dest_path)
}

async fn download_with_retry(url: &str, dest_path: &Path, show_progress: bool) -> Result<()> {
    let client = reqwest::Client::new();
    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        if show_progress {
            info!(url, attempt, "Downloading");
        } else {
            debug!(url, attempt, "Downloading");
        }

        match do_download(&client, url, dest_path, show_progress).await {
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

async fn do_download(
    client: &reqwest::Client,
    url: &str,
    dest_path: &Path,
    show_progress: bool,
) -> Result<()> {
    let response = client.get(url).send().await?.error_for_status()?;

    let total_size = response.content_length();

    let pb = if show_progress {
        Some(match total_size {
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
        })
    } else {
        None
    };
    if let Some(pb) = &pb {
        pb.set_message(format!(
            "Downloading {}",
            dest_path.file_name().unwrap_or_default().to_string_lossy()
        ));
    }

    let file = std::fs::File::create(dest_path)
        .with_context(|| format!("Failed to create {dest_path:?}"))?;
    let mut writer = BufWriter::new(file);
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error reading download stream")?;
        writer
            .write_all(&chunk)
            .with_context(|| format!("Failed to write to {dest_path:?}"))?;
        if let Some(pb) = &pb {
            pb.inc(chunk.len() as u64);
        }
    }

    writer
        .flush()
        .with_context(|| format!("Failed to flush {dest_path:?}"))?;

    if let Some(pb) = &pb {
        pb.finish_with_message(format!(
            "Downloaded {}",
            dest_path.file_name().unwrap_or_default().to_string_lossy()
        ));
    }
    if show_progress {
        info!(path = %dest_path.display(), "Download complete");
    } else {
        debug!(path = %dest_path.display(), "Download complete");
    }
    Ok(())
}

/// Fetch a cheap change validator for `url` via HEAD: the `ETag` if the
/// server sends one, else `Last-Modified`, else `None`.
///
/// `None` means "cannot tell" and callers MUST treat it as changed — never
/// as unchanged — or a refresh could be skipped forever.
pub fn fetch_etag(url: &str) -> Result<Option<String>> {
    let rt = Runtime::new().context("Failed to create tokio runtime")?;
    let client = reqwest::Client::new();
    rt.block_on(do_fetch_etag(&client, url))
}

async fn do_fetch_etag(client: &reqwest::Client, url: &str) -> Result<Option<String>> {
    let response = client.head(url).send().await?.error_for_status()?;
    let headers = response.headers();
    let value = headers
        .get(reqwest::header::ETAG)
        .or_else(|| headers.get(reqwest::header::LAST_MODIFIED))
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    Ok(value)
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
        do_download(&client, &url, &dest, true).await.unwrap();
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

    async fn serve_head(header_line: &'static str) -> Option<String> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: std::net::SocketAddr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.readable().await;
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
            let resp = format!("HTTP/1.1 200 OK\r\n{header_line}Content-Length: 0\r\n\r\n");
            tokio::io::AsyncWriteExt::write_all(&mut stream, resp.as_bytes())
                .await
                .unwrap();
        });

        let client = reqwest::Client::new();
        do_fetch_etag(&client, &format!("http://{addr}/f.bin"))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn etag_header_is_preferred() {
        let v = serve_head("ETag: \"abc123\"\r\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n")
            .await;
        assert_eq!(v.as_deref(), Some("\"abc123\""));
    }

    #[tokio::test]
    async fn falls_back_to_last_modified() {
        let v = serve_head("Last-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n").await;
        assert_eq!(v.as_deref(), Some("Wed, 01 Jan 2025 00:00:00 GMT"));
    }

    #[tokio::test]
    async fn returns_none_when_server_offers_no_validator() {
        let v = serve_head("").await;
        assert_eq!(v, None);
    }
}
