//! GeoJSON data package endpoint: returns government-registry records missing
//! from OSM within a requested area, tagged for direct JOSM import.
//!
//! See docs/superpowers/specs/2026-07-19-geojson-package-endpoint-design.md.
//! Matching semantics mirror src/compare/ but run as pure SELECTs scoped to
//! the request area, so they work on the read-only connection pool.

/// Datasets that can be included in a package. Output order is fixed:
/// Prg, Bdot10k, Egib.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dataset {
    Prg,
    Bdot10k,
    Egib,
}

pub const ALL_DATASETS: [Dataset; 3] = [Dataset::Prg, Dataset::Bdot10k, Dataset::Egib];

/// A validated request area. `polygon_geojson` always holds the exact request
/// geometry as a GeoJSON string — for bbox (GET) requests it is the envelope
/// itself as a Polygon — so the query layer has a single code path.
#[derive(Clone, Debug)]
pub struct RequestArea {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
    pub polygon_geojson: String,
}

impl RequestArea {
    fn from_envelope(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Self {
        let polygon_geojson = serde_json::json!({
            "type": "Polygon",
            "coordinates": [[
                [min_lon, min_lat],
                [max_lon, min_lat],
                [max_lon, max_lat],
                [min_lon, max_lat],
                [min_lon, min_lat],
            ]],
        })
        .to_string();
        Self {
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            polygon_geojson,
        }
    }

    fn bbox_area_sq_deg(&self) -> f64 {
        (self.max_lon - self.min_lon) * (self.max_lat - self.min_lat)
    }
}

pub fn parse_bbox(s: &str) -> Result<RequestArea, String> {
    let parts: Vec<&str> = s.split(',').map(str::trim).collect();
    if parts.len() != 4 {
        return Err(format!(
            "bbox must be 4 comma-separated numbers (minLon,minLat,maxLon,maxLat), got {} values",
            parts.len()
        ));
    }
    let mut nums = [0f64; 4];
    for (i, part) in parts.iter().enumerate() {
        let n: f64 = part
            .parse()
            .map_err(|_| format!("bbox value '{part}' is not a number"))?;
        if !n.is_finite() {
            return Err(format!("bbox value '{part}' is not finite"));
        }
        nums[i] = n;
    }
    let [min_lon, min_lat, max_lon, max_lat] = nums;
    if !(-180.0..=180.0).contains(&min_lon) || !(-180.0..=180.0).contains(&max_lon) {
        return Err("bbox longitudes must be within -180..180".to_string());
    }
    if !(-90.0..=90.0).contains(&min_lat) || !(-90.0..=90.0).contains(&max_lat) {
        return Err("bbox latitudes must be within -90..90".to_string());
    }
    if min_lon >= max_lon || min_lat >= max_lat {
        return Err("bbox must satisfy minLon < maxLon and minLat < maxLat".to_string());
    }
    Ok(RequestArea::from_envelope(
        min_lon, min_lat, max_lon, max_lat,
    ))
}

pub fn parse_datasets(s: Option<&str>) -> Result<Vec<Dataset>, String> {
    let s = match s {
        None => return Ok(ALL_DATASETS.to_vec()),
        Some(s) if s.trim().is_empty() => return Ok(ALL_DATASETS.to_vec()),
        Some(s) => s,
    };
    let (mut prg, mut bdot10k, mut egib) = (false, false, false);
    for name in s.split(',') {
        match name.trim().to_ascii_lowercase().as_str() {
            "prg" => prg = true,
            "bdot10k" => bdot10k = true,
            "egib" => egib = true,
            "all" => {
                prg = true;
                bdot10k = true;
                egib = true;
            }
            other => {
                return Err(format!(
                    "unknown dataset '{other}' (expected prg, bdot10k, egib, or all)"
                ));
            }
        }
    }
    let mut out = Vec::new();
    if prg {
        out.push(Dataset::Prg);
    }
    if bdot10k {
        out.push(Dataset::Bdot10k);
    }
    if egib {
        out.push(Dataset::Egib);
    }
    Ok(out)
}

pub fn check_area(area: &RequestArea, max_sq_deg: f64) -> Result<(), String> {
    let a = area.bbox_area_sq_deg();
    if a > max_sq_deg {
        return Err(format!(
            "requested area {a:.4} square degrees exceeds maximum {max_sq_deg} square degrees"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bbox_valid() {
        let area = parse_bbox("20.9, 52.1,21.0,52.2").unwrap();
        assert_eq!(area.min_lon, 20.9);
        assert_eq!(area.min_lat, 52.1);
        assert_eq!(area.max_lon, 21.0);
        assert_eq!(area.max_lat, 52.2);
        // The envelope is materialized as a GeoJSON Polygon for the query path.
        assert!(area.polygon_geojson.contains("\"Polygon\""));
        assert!(area.polygon_geojson.contains("20.9"));
    }

    #[test]
    fn parse_bbox_rejects_wrong_count() {
        assert!(parse_bbox("1,2,3").is_err());
        assert!(parse_bbox("").is_err());
        assert!(parse_bbox("1,2,3,4,5").is_err());
    }

    #[test]
    fn parse_bbox_rejects_non_numeric_and_non_finite() {
        assert!(parse_bbox("a,2,3,4").is_err());
        assert!(parse_bbox("NaN,2,3,4").is_err());
        assert!(parse_bbox("inf,2,3,4").is_err());
    }

    #[test]
    fn parse_bbox_rejects_min_not_below_max() {
        assert!(parse_bbox("21.0,52.1,20.9,52.2").is_err()); // min_lon >= max_lon
        assert!(parse_bbox("20.9,52.2,21.0,52.1").is_err()); // min_lat >= max_lat
        assert!(parse_bbox("20.9,52.1,20.9,52.2").is_err()); // equal lon
    }

    #[test]
    fn parse_bbox_rejects_out_of_range() {
        assert!(parse_bbox("-181,52.1,21.0,52.2").is_err());
        assert!(parse_bbox("20.9,-91,21.0,52.2").is_err());
        assert!(parse_bbox("20.9,52.1,181,52.2").is_err());
    }

    #[test]
    fn parse_datasets_default_is_all() {
        assert_eq!(parse_datasets(None).unwrap(), ALL_DATASETS.to_vec());
        assert_eq!(parse_datasets(Some("  ")).unwrap(), ALL_DATASETS.to_vec());
    }

    #[test]
    fn parse_datasets_subset_in_fixed_order() {
        assert_eq!(parse_datasets(Some("prg")).unwrap(), vec![Dataset::Prg]);
        // Input order does not matter; output order is always Prg, Bdot10k, Egib.
        assert_eq!(
            parse_datasets(Some("egib,prg")).unwrap(),
            vec![Dataset::Prg, Dataset::Egib]
        );
    }

    #[test]
    fn parse_datasets_all_alias_and_case_insensitive() {
        assert_eq!(parse_datasets(Some("ALL")).unwrap(), ALL_DATASETS.to_vec());
        assert_eq!(
            parse_datasets(Some("all,prg")).unwrap(),
            ALL_DATASETS.to_vec()
        );
        assert_eq!(
            parse_datasets(Some("Bdot10K")).unwrap(),
            vec![Dataset::Bdot10k]
        );
    }

    #[test]
    fn parse_datasets_rejects_unknown() {
        let err = parse_datasets(Some("prg,foo")).unwrap_err();
        assert!(err.contains("foo"));
    }

    #[test]
    fn check_area_enforces_cap() {
        // 0.1 x 0.2 = 0.02 square degrees; do not test the exact boundary
        // (floating point) — only clearly-under and clearly-over.
        let small = parse_bbox("20.9,52.0,21.0,52.2").unwrap();
        assert!(check_area(&small, 0.04).is_ok());

        let big = parse_bbox("14.0,49.0,25.0,55.0").unwrap();
        let err = check_area(&big, 0.04).unwrap_err();
        assert!(err.contains("exceeds"));
    }
}
