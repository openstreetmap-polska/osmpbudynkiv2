use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};
use flate2::read::ZlibDecoder;

fn read_varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *data.get(*pos)?;
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

fn skip_field(data: &[u8], pos: &mut usize, wire_type: u64) -> Option<()> {
    match wire_type {
        0 => {
            read_varint(data, pos)?;
        }
        1 => {
            *pos = pos.checked_add(8)?;
        }
        2 => {
            let len = read_varint(data, pos)? as usize;
            *pos = pos.checked_add(len)?;
        }
        5 => {
            *pos = pos.checked_add(4)?;
        }
        _ => return None,
    }
    Some(())
}

fn parse_blob_header(data: &[u8]) -> Option<(String, usize)> {
    let mut pos = 0;
    let mut type_str = None;
    let mut datasize = None;
    while pos < data.len() {
        let tag = read_varint(data, &mut pos)?;
        match (tag >> 3, tag & 0x7) {
            (1, 2) => {
                let len = read_varint(data, &mut pos)? as usize;
                type_str = Some(String::from_utf8_lossy(&data[pos..pos + len]).into_owned());
                pos += len;
            }
            (3, 0) => {
                datasize = Some(read_varint(data, &mut pos)? as usize);
            }
            (_, wire) => skip_field(data, &mut pos, wire)?,
        }
    }
    Some((type_str?, datasize?))
}

fn decode_blob(data: &[u8]) -> Option<Vec<u8>> {
    let mut pos = 0;
    while pos < data.len() {
        let tag = read_varint(data, &mut pos)?;
        match (tag >> 3, tag & 0x7) {
            (1, 2) => {
                let len = read_varint(data, &mut pos)? as usize;
                return Some(data[pos..pos + len].to_vec());
            }
            (3, 2) => {
                let len = read_varint(data, &mut pos)? as usize;
                let mut decoder = ZlibDecoder::new(&data[pos..pos + len]);
                let mut out = Vec::new();
                decoder.read_to_end(&mut out).ok()?;
                return Some(out);
            }
            (_, wire) => skip_field(data, &mut pos, wire)?,
        }
    }
    None
}

/// Returns (sequence_number, timestamp_secs) from the HeaderBlock.
/// Fields: 32 = osmosis_replication_timestamp (int64, epoch seconds)
///         33 = osmosis_replication_sequence_number (int64)
fn parse_header_block_replication(data: &[u8]) -> (Option<i64>, Option<i64>) {
    let mut pos = 0;
    let mut seq = None;
    let mut ts_secs = None;
    while pos < data.len() {
        let Some(tag) = read_varint(data, &mut pos) else {
            break;
        };
        match (tag >> 3, tag & 0x7) {
            (32, 0) => {
                ts_secs = Some(read_varint(data, &mut pos).unwrap_or(0) as i64);
            }
            (33, 0) => {
                seq = Some(read_varint(data, &mut pos).unwrap_or(0) as i64);
            }
            (_, wire) => {
                let _ = skip_field(data, &mut pos, wire);
            }
        }
    }
    (seq, ts_secs)
}

/// Convert Unix timestamp (seconds since epoch) to ISO 8601 UTC string.
/// Uses Howard Hinnant's civil date algorithm.
fn epoch_secs_to_iso8601(secs: i64) -> String {
    let time_of_day = secs.rem_euclid(86400);
    let days = (secs - time_of_day) / 86400;

    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    let h = time_of_day / 3600;
    let min = (time_of_day % 3600) / 60;
    let s = time_of_day % 60;

    format!("{y:04}-{m:02}-{d:02}T{h:02}:{min:02}:{s:02}Z")
}

/// Read OSM replication metadata from a PBF file's header block.
/// Returns `None` if the fields are absent — not all extracts set them.
pub fn read_replication_info(path: &Path) -> Result<Option<(i64, String)>> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open PBF: {}", path.display()))?;

    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf)
        .context("Failed to read BlobHeader length")?;
    let header_len = u32::from_be_bytes(len_buf) as usize;

    let mut header_data = vec![0u8; header_len];
    file.read_exact(&mut header_data)
        .context("Failed to read BlobHeader")?;

    let (block_type, data_size) =
        parse_blob_header(&header_data).context("Failed to parse BlobHeader")?;
    if block_type != "OSMHeader" {
        bail!("Expected OSMHeader block, got: {block_type}");
    }

    let mut blob_data = vec![0u8; data_size];
    file.read_exact(&mut blob_data)
        .context("Failed to read Blob")?;

    let header_block = decode_blob(&blob_data).context("Failed to decode Blob")?;
    let (seq, ts_secs) = parse_header_block_replication(&header_block);

    match (seq, ts_secs) {
        (Some(seq), Some(ts)) => Ok(Some((seq, epoch_secs_to_iso8601(ts)))),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_replication_info_from_fixture() {
        let info = read_replication_info(std::path::Path::new("fixtures/osm.pbf"))
            .expect("Should not error on valid PBF");
        let (seq, timestamp) = info.expect("Fixture PBF should have replication metadata");
        assert_eq!(seq, 7027262);
        assert_eq!(timestamp, "2026-03-15T01:41:51Z");
    }

    #[test]
    fn test_epoch_secs_to_iso8601() {
        assert_eq!(epoch_secs_to_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(epoch_secs_to_iso8601(1_773_538_911), "2026-03-15T01:41:51Z");
    }
}
