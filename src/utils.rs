use std::time::Duration;

/// Formats a duration as a human-readable string.
///
/// Examples: `"5.3s"`, `"7m 35s"`, `"1h 2m 5s"`
pub fn format_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    let subsec = d.subsec_millis();

    if total_secs < 60 {
        format!("{}.{}s", total_secs, subsec / 100)
    } else if total_secs < 3600 {
        let m = total_secs / 60;
        let s = total_secs % 60;
        format!("{m}m {s}s")
    } else {
        let h = total_secs / 3600;
        let m = (total_secs % 3600) / 60;
        let s = total_secs % 60;
        format!("{h}h {m}m {s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_millis(5300)), "5.3s");
        assert_eq!(format_duration(Duration::from_secs(8)), "8.0s");
        assert_eq!(format_duration(Duration::from_secs(455)), "7m 35s");
        assert_eq!(format_duration(Duration::from_secs(798)), "13m 18s");
        assert_eq!(format_duration(Duration::from_secs(2325)), "38m 45s");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m 1s");
    }
}
