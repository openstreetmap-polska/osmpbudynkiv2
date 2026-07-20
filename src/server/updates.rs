//! GET /updates: recent /package export activity as a GeoJSON FeatureCollection,
//! browser-cacheable for 60 seconds.
//!
//! See docs/superpowers/specs/2026-07-20-export-log-updates-endpoint-design.md
//! and docs/duckdb_connection_visibility_investigation.md. Reads run via
//! state.write (not state.read_pool) -- read_pool never observes writes made
//! through write while the server is running.

// Not yet wired into a handler — the GET /updates route and its use of this
// function land in the next task.
#[allow(dead_code)]
pub fn parse_minutes(
    s: Option<&str>,
    default_minutes: u64,
    max_minutes: u64,
) -> Result<u64, String> {
    let s = match s {
        None => return Ok(default_minutes),
        Some(s) if s.trim().is_empty() => return Ok(default_minutes),
        Some(s) => s.trim(),
    };
    let minutes: u64 = s
        .parse()
        .map_err(|_| format!("minutes value '{s}' is not a positive integer"))?;
    if minutes == 0 {
        return Err("minutes must be at least 1".to_string());
    }
    if minutes > max_minutes {
        return Err(format!("minutes {minutes} exceeds maximum {max_minutes}"));
    }
    Ok(minutes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minutes_default_when_absent() {
        assert_eq!(parse_minutes(None, 60, 1440).unwrap(), 60);
        assert_eq!(parse_minutes(Some("  "), 60, 1440).unwrap(), 60);
    }

    #[test]
    fn parse_minutes_accepts_valid_override() {
        assert_eq!(parse_minutes(Some("15"), 60, 1440).unwrap(), 15);
        assert_eq!(parse_minutes(Some(" 90 "), 60, 1440).unwrap(), 90);
    }

    #[test]
    fn parse_minutes_rejects_over_cap() {
        let err = parse_minutes(Some("1441"), 60, 1440).unwrap_err();
        assert!(err.contains("1440"));
    }

    #[test]
    fn parse_minutes_rejects_zero_and_non_numeric() {
        assert!(parse_minutes(Some("0"), 60, 1440).is_err());
        assert!(parse_minutes(Some("-5"), 60, 1440).is_err());
        assert!(parse_minutes(Some("abc"), 60, 1440).is_err());
    }
}
