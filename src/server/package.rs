//! GeoJSON data package endpoint: returns government-registry records missing
//! from OSM within a requested area, tagged for direct JOSM import.
//!
//! See docs/superpowers/specs/2026-07-19-geojson-package-endpoint-design.md.
//! Matching semantics mirror src/compare/ but run as pure SELECTs scoped to
//! the request area, so they work on the read-only connection pool.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::value::RawValue;

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

/// Parse a POST body as a GeoJSON Polygon/MultiPolygon geometry, optionally
/// wrapped in a Feature. The returned area's `polygon_geojson` is the
/// re-serialized geometry object and the envelope is its bounding box.
pub fn parse_polygon_body(body: &str) -> Result<RequestArea, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid JSON body: {e}"))?;
    let geometry = match value.get("type").and_then(|t| t.as_str()) {
        Some("Feature") => value
            .get("geometry")
            .filter(|g| !g.is_null())
            .ok_or_else(|| "Feature has no geometry".to_string())?
            .clone(),
        _ => value,
    };
    match geometry.get("type").and_then(|t| t.as_str()) {
        Some("Polygon") | Some("MultiPolygon") => {}
        Some(other) => {
            return Err(format!(
                "geometry type must be Polygon or MultiPolygon, got '{other}'"
            ));
        }
        None => return Err("body has no GeoJSON type".to_string()),
    }
    let coordinates = geometry
        .get("coordinates")
        .ok_or_else(|| "geometry has no coordinates".to_string())?;
    let mut positions = Vec::new();
    collect_positions(coordinates, &mut positions)?;
    if positions.is_empty() {
        return Err("geometry has no coordinate positions".to_string());
    }
    let (mut min_lon, mut min_lat) = (f64::INFINITY, f64::INFINITY);
    let (mut max_lon, mut max_lat) = (f64::NEG_INFINITY, f64::NEG_INFINITY);
    for (lon, lat) in &positions {
        min_lon = min_lon.min(*lon);
        min_lat = min_lat.min(*lat);
        max_lon = max_lon.max(*lon);
        max_lat = max_lat.max(*lat);
    }
    if min_lon >= max_lon || min_lat >= max_lat {
        return Err("polygon envelope is degenerate (zero width or height)".to_string());
    }
    Ok(RequestArea {
        min_lon,
        min_lat,
        max_lon,
        max_lat,
        polygon_geojson: geometry.to_string(),
    })
}

/// Recursively walk nested GeoJSON coordinate arrays, collecting
/// (lon, lat) positions and validating each.
fn collect_positions(v: &serde_json::Value, out: &mut Vec<(f64, f64)>) -> Result<(), String> {
    let arr = v
        .as_array()
        .ok_or_else(|| "coordinates must be arrays".to_string())?;
    if arr.is_empty() {
        return Ok(());
    }
    if arr[0].is_number() {
        if arr.len() < 2 {
            return Err("coordinate position must have at least 2 numbers".to_string());
        }
        let lon = arr[0]
            .as_f64()
            .ok_or_else(|| "invalid longitude".to_string())?;
        let lat = arr[1]
            .as_f64()
            .ok_or_else(|| "invalid latitude".to_string())?;
        if !lon.is_finite() || !lat.is_finite() {
            return Err("coordinates must be finite numbers".to_string());
        }
        if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
            return Err(format!("coordinate ({lon}, {lat}) out of lon/lat range"));
        }
        out.push((lon, lat));
        return Ok(());
    }
    for item in arr {
        collect_positions(item, out)?;
    }
    Ok(())
}

/// One unmatched PRG address row, as returned by the package address query.
#[derive(Debug)]
pub struct AddressRow {
    pub geometry_geojson: String,
    pub housenumber: Option<String>,
    pub street: Option<String>,
    pub city: Option<String>,
    pub postcode: Option<String>,
    pub simc: Option<String>,
}

const SOURCE_ADDR: &str = "gugik.gov.pl";
const SOURCE_BUILDING: &str = "geoportal.gov.pl";

fn non_empty(v: &Option<String>) -> Option<&str> {
    v.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

/// Map a PRG address row to OSM tags following Polish community conventions:
/// with a street the settlement goes to addr:city, without one to addr:place.
pub fn address_tags(row: &AddressRow) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    if let Some(hn) = non_empty(&row.housenumber) {
        tags.insert("addr:housenumber".to_string(), hn.to_string());
    }
    if let Some(street) = non_empty(&row.street) {
        tags.insert("addr:street".to_string(), street.to_string());
        if let Some(city) = non_empty(&row.city) {
            tags.insert("addr:city".to_string(), city.to_string());
        }
    } else if let Some(city) = non_empty(&row.city) {
        tags.insert("addr:place".to_string(), city.to_string());
    }
    if let Some(postcode) = non_empty(&row.postcode) {
        tags.insert("addr:postcode".to_string(), postcode.to_string());
    }
    if let Some(simc) = non_empty(&row.simc) {
        tags.insert("addr:city:simc".to_string(), simc.to_string());
    }
    tags.insert("source:addr".to_string(), SOURCE_ADDR.to_string());
    tags
}

/// Building type mapping from BDOT10k function codes is a separate roadmap
/// item; packages currently emit a plain building=yes.
pub fn building_tags() -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    tags.insert("building".to_string(), "yes".to_string());
    tags.insert("source:building".to_string(), SOURCE_BUILDING.to_string());
    tags
}

#[derive(Serialize)]
pub struct Feature {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub geometry: Box<RawValue>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub struct FeatureCollection {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub features: Vec<Feature>,
}

/// Build a Feature embedding the ST_AsGeoJSON output without re-parsing it
/// into a serde_json::Value (RawValue still validates it is well-formed JSON).
pub fn feature(
    geometry_geojson: String,
    properties: BTreeMap<String, String>,
) -> anyhow::Result<Feature> {
    Ok(Feature {
        kind: "Feature",
        geometry: RawValue::from_string(geometry_geojson)?,
        properties,
    })
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
    fn parse_polygon_body_polygon() {
        let body = r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#;
        let area = parse_polygon_body(body).unwrap();
        assert_eq!(area.min_lon, 21.0);
        assert_eq!(area.min_lat, 52.2);
        assert_eq!(area.max_lon, 21.01);
        assert_eq!(area.max_lat, 52.21);
        assert!(area.polygon_geojson.contains("\"Polygon\""));
    }

    #[test]
    fn parse_polygon_body_multipolygon() {
        let body = r#"{"type":"MultiPolygon","coordinates":[[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]],[[[22.0,53.0],[22.01,53.0],[22.0,53.01],[22.0,53.0]]]]}"#;
        let area = parse_polygon_body(body).unwrap();
        // Envelope spans both polygons.
        assert_eq!(area.min_lon, 21.0);
        assert_eq!(area.max_lon, 22.01);
        assert_eq!(area.max_lat, 53.01);
    }

    #[test]
    fn parse_polygon_body_unwraps_feature() {
        let body = r#"{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}}"#;
        let area = parse_polygon_body(body).unwrap();
        assert_eq!(area.max_lon, 21.01);
        // The stored geometry is the unwrapped Polygon, not the Feature.
        assert!(!area.polygon_geojson.contains("Feature"));
    }

    #[test]
    fn parse_polygon_body_rejects_bad_input() {
        assert!(parse_polygon_body("not json").is_err());
        assert!(parse_polygon_body(r#"{"type":"Point","coordinates":[21.0,52.2]}"#).is_err());
        assert!(parse_polygon_body(r#"{"type":"Polygon"}"#).is_err()); // no coordinates
        assert!(parse_polygon_body(r#"{"type":"Polygon","coordinates":[]}"#).is_err()); // empty
        assert!(parse_polygon_body(r#"{"type":"Feature","properties":{}}"#).is_err()); // no geometry
        // Out-of-range coordinate.
        assert!(
            parse_polygon_body(
                r#"{"type":"Polygon","coordinates":[[[210.0,52.2],[21.01,52.2],[21.0,52.21],[210.0,52.2]]]}"#
            )
            .is_err()
        );
        // Degenerate: all points identical → zero-area envelope.
        assert!(
            parse_polygon_body(
                r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.0,52.2],[21.0,52.2]]]}"#
            )
            .is_err()
        );
    }

    fn addr_row() -> AddressRow {
        AddressRow {
            geometry_geojson: r#"{"type":"Point","coordinates":[21.001,52.201]}"#.to_string(),
            housenumber: Some(" 12A ".to_string()),
            street: Some("Marszałkowska".to_string()),
            city: Some("Warszawa".to_string()),
            postcode: Some("00-590".to_string()),
            simc: Some("0918123".to_string()),
        }
    }

    #[test]
    fn address_tags_with_street_uses_city() {
        let tags = address_tags(&addr_row());
        assert_eq!(tags.get("addr:housenumber").unwrap(), "12A"); // trimmed
        assert_eq!(tags.get("addr:street").unwrap(), "Marszałkowska");
        assert_eq!(tags.get("addr:city").unwrap(), "Warszawa");
        assert_eq!(tags.get("addr:postcode").unwrap(), "00-590");
        assert_eq!(tags.get("addr:city:simc").unwrap(), "0918123");
        assert_eq!(tags.get("source:addr").unwrap(), "gugik.gov.pl");
        assert!(!tags.contains_key("addr:place"));
    }

    #[test]
    fn address_tags_without_street_uses_place() {
        let mut row = addr_row();
        row.street = None;
        let tags = address_tags(&row);
        assert_eq!(tags.get("addr:place").unwrap(), "Warszawa");
        assert!(!tags.contains_key("addr:street"));
        assert!(!tags.contains_key("addr:city"));
    }

    #[test]
    fn address_tags_whitespace_street_treated_as_absent() {
        let mut row = addr_row();
        row.street = Some("   ".to_string());
        let tags = address_tags(&row);
        assert!(tags.contains_key("addr:place"));
        assert!(!tags.contains_key("addr:street"));
    }

    #[test]
    fn address_tags_omits_empty_optionals() {
        let mut row = addr_row();
        row.housenumber = None;
        row.postcode = None;
        row.simc = Some(String::new());
        let tags = address_tags(&row);
        assert!(!tags.contains_key("addr:housenumber"));
        assert!(!tags.contains_key("addr:postcode"));
        assert!(!tags.contains_key("addr:city:simc"));
    }

    #[test]
    fn building_tags_are_fixed() {
        let tags = building_tags();
        assert_eq!(tags.get("building").unwrap(), "yes");
        assert_eq!(tags.get("source:building").unwrap(), "geoportal.gov.pl");
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn feature_collection_serializes_valid_geojson() {
        let f = feature(
            r#"{"type":"Point","coordinates":[21.0,52.2]}"#.to_string(),
            building_tags(),
        )
        .unwrap();
        let fc = FeatureCollection {
            kind: "FeatureCollection",
            features: vec![f],
        };
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&fc).unwrap()).unwrap();
        assert_eq!(json["type"], "FeatureCollection");
        assert_eq!(json["features"][0]["type"], "Feature");
        assert_eq!(json["features"][0]["geometry"]["type"], "Point");
        assert_eq!(json["features"][0]["geometry"]["coordinates"][0], 21.0);
        assert_eq!(json["features"][0]["properties"]["building"], "yes");
    }

    #[test]
    fn feature_rejects_invalid_geometry_json() {
        assert!(feature("not json".to_string(), building_tags()).is_err());
    }

    #[test]
    fn empty_feature_collection_serializes() {
        let fc = FeatureCollection {
            kind: "FeatureCollection",
            features: vec![],
        };
        assert_eq!(
            serde_json::to_string(&fc).unwrap(),
            r#"{"type":"FeatureCollection","features":[]}"#
        );
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
