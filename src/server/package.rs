//! GeoJSON data package endpoint: returns government-registry records missing
//! from OSM within a requested area, tagged for direct JOSM import.
//!
//! See docs/superpowers/specs/2026-07-19-geojson-package-endpoint-design.md.
//! Matching itself is precomputed upstream (see src/compare/) into the
//! `*_unmatched` serving tables; this module runs pure SELECTs against those
//! tables scoped to the request area, so it works on the read-only
//! connection pool.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::AppState;

/// Datasets that can be included in a package. Output order is fixed:
/// Prg, Bdot10k, Egib.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dataset {
    Prg,
    Bdot10k,
    Egib,
}

pub const ALL_DATASETS: [Dataset; 3] = [Dataset::Prg, Dataset::Bdot10k, Dataset::Egib];

impl Dataset {
    fn sql_name(self) -> &'static str {
        match self {
            Dataset::Prg => "prg",
            Dataset::Bdot10k => "bdot10k",
            Dataset::Egib => "egib",
        }
    }
}

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

/// PRG addresses in the request area that are unmatched against OSM.
/// Matching is precomputed upstream (see src/compare/) into `prg_unmatched`;
/// this is a plain spatial read of that serving table clipped to the polygon.
/// `addr:street` is resolved through `street_name_mappings` here — the only
/// place PRG street names reach the outside world. The COALESCE chain *is* the
/// priority rule: settlement row, then global row, then the raw PRG name, so
/// an empty mapping table degrades to serving names verbatim rather than
/// erroring. Matching never reads street names (see compare::addresses), so
/// this cannot change which addresses are unmatched.
pub fn unmatched_addresses(conn: &Connection, area: &RequestArea) -> Result<Vec<AddressRow>> {
    let (x1, y1, x2, y2) = (area.min_lon, area.min_lat, area.max_lon, area.max_lat);
    // Envelope bounds are validated finite f64s (parsed and range-checked by
    // parse_bbox/parse_polygon_body, never raw request text) formatted
    // straight into the SQL text -- a constant predicate enables an R-tree
    // index scan, the same pattern used in compare::buildings/compare::rule.
    // The polygon itself stays a bound parameter (`?`), since it is
    // arbitrary-length user-supplied geometry, not a handful of numbers.
    let sql = format!(
        "SELECT ST_AsGeoJSON(a.geom), a.numer_porzadkowy,
                COALESCE(loc.osm_street_name, gl.osm_street_name, a.ulica),
                a.miejscowosc, a.kod_pocztowy, a.teryt_miejscowosc
         FROM prg_unmatched a
         LEFT JOIN street_name_mappings loc
                ON lower(trim(loc.prg_street_name)) = lower(trim(a.ulica))
               AND loc.teryt_simc_code = a.teryt_miejscowosc
         LEFT JOIN street_name_mappings gl
                ON lower(trim(gl.prg_street_name)) = lower(trim(a.ulica))
               AND gl.teryt_simc_code IS NULL
         WHERE ST_Intersects(a.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           AND ST_Intersects(a.geom, ST_GeomFromGeoJSON(?))"
    );
    let mut stmt = conn
        .prepare(&sql)
        .context("Failed to prepare package address query")?;
    let rows = stmt
        .query_map([area.polygon_geojson.as_str()], |row| {
            Ok(AddressRow {
                geometry_geojson: row.get(0)?,
                housenumber: row.get(1)?,
                street: row.get(2)?,
                city: row.get(3)?,
                postcode: row.get(4)?,
                simc: row.get(5)?,
            })
        })
        .context("Failed to run package address query")?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.context("Failed to read package address row")?);
    }
    Ok(out)
}

/// Government buildings in the request area (by centroid) that are unmatched
/// against OSM. Matching is precomputed upstream into `bdot10k_unmatched`/
/// `egib_unmatched`; this is a plain spatial read of that serving table.
/// `dest_table` is "bdot10k_unmatched" or "egib_unmatched" (a code-level
/// constant, never user input). Returns ST_AsGeoJSON geometry strings.
pub fn unmatched_buildings(
    conn: &Connection,
    dest_table: &str,
    area: &RequestArea,
) -> Result<Vec<String>> {
    let (x1, y1, x2, y2) = (area.min_lon, area.min_lat, area.max_lon, area.max_lat);
    // Same bbox-interpolation rationale as unmatched_addresses above.
    let sql = format!(
        "SELECT ST_AsGeoJSON(b.geom)
         FROM {dest_table} b
         WHERE ST_Intersects(b.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           AND ST_Intersects(ST_Centroid(b.geom), ST_GeomFromGeoJSON(?))"
    );
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("Failed to prepare package building query for {dest_table}"))?;
    let rows = stmt
        .query_map([area.polygon_geojson.as_str()], |row| row.get(0))
        .with_context(|| format!("Failed to run package building query for {dest_table}"))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(
            row.with_context(|| format!("Failed to read package building row from {dest_table}"))?,
        );
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
pub struct PackageParams {
    pub bbox: Option<String>,
    pub datasets: Option<String>,
}

pub async fn get_package(
    State(state): State<AppState>,
    Query(params): Query<PackageParams>,
) -> Response {
    let bbox = match params.bbox.as_deref() {
        Some(b) => b,
        None => {
            return error_response(StatusCode::BAD_REQUEST, "missing required parameter 'bbox'");
        }
    };
    let area = match parse_bbox(bbox) {
        Ok(a) => a,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    let datasets = match parse_datasets(params.datasets.as_deref()) {
        Ok(d) => d,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    serve_package(state, area, datasets).await
}

pub async fn post_package(
    State(state): State<AppState>,
    Query(params): Query<PackageParams>,
    body: String,
) -> Response {
    let area = match parse_polygon_body(&body) {
        Ok(a) => a,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    let datasets = match parse_datasets(params.datasets.as_deref()) {
        Ok(d) => d,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    serve_package(state, area, datasets).await
}

async fn serve_package(state: AppState, area: RequestArea, datasets: Vec<Dataset>) -> Response {
    if let Err(e) = check_area(&area, state.config.package.max_area_sq_deg) {
        return error_response(StatusCode::BAD_REQUEST, &e);
    }
    let result = tokio::task::spawn_blocking(move || build_package(&state, &area, &datasets)).await;
    match result {
        Ok(Ok(body)) => {
            let mut resp = body.into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/geo+json"),
            );
            resp.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_static("attachment; filename=\"package.geojson\""),
            );
            resp
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "package query failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
        Err(e) => {
            tracing::error!(error = %e, "package task panicked");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

fn build_package(state: &AppState, area: &RequestArea, datasets: &[Dataset]) -> Result<String> {
    let conn = state
        .pool
        .get()
        .context("Failed to acquire pool connection")?;
    let mut features = Vec::new();
    let mut address_count: i32 = 0;
    let mut building_count: i32 = 0;
    for dataset in datasets {
        match dataset {
            Dataset::Prg => {
                for row in unmatched_addresses(&conn, area)? {
                    let properties = address_tags(&row);
                    features.push(feature(row.geometry_geojson, properties)?);
                    address_count += 1;
                }
            }
            Dataset::Bdot10k => {
                for geometry in unmatched_buildings(&conn, "bdot10k_unmatched", area)? {
                    features.push(feature(geometry, building_tags())?);
                    building_count += 1;
                }
            }
            Dataset::Egib => {
                for geometry in unmatched_buildings(&conn, "egib_unmatched", area)? {
                    features.push(feature(geometry, building_tags())?);
                    building_count += 1;
                }
            }
        }
    }
    drop(conn);
    log_export(state, area, datasets, address_count, building_count);
    let collection = FeatureCollection {
        kind: "FeatureCollection",
        features,
    };
    Ok(serde_json::to_string(&collection)?)
}

/// Best-effort export logging: failures are logged via `tracing::warn!` and
/// never affect the package response. `datasets` is drawn from the closed
/// `Dataset` enum, so building the SQL list literal directly from it is safe
/// -- no request text reaches this string. Runs via `state.pool`, same as
/// every other query -- see docs/duckdb_connection_visibility_investigation.md
/// for why a single shared pool (rather than a separate read-only pool) makes
/// this write immediately visible to `/updates`.
fn log_export(
    state: &AppState,
    area: &RequestArea,
    datasets: &[Dataset],
    address_count: i32,
    building_count: i32,
) {
    let datasets_sql = format!(
        "[{}]",
        datasets
            .iter()
            .map(|d| format!("'{}'", d.sql_name()))
            .collect::<Vec<_>>()
            .join(",")
    );
    let conn = match state.pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            tracing::warn!(error = %e, "failed to acquire pool connection, skipping package export log");
            return;
        }
    };
    let sql = format!(
        "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
         VALUES (now(), ST_GeomFromGeoJSON(?), {datasets_sql}, ?, ?)"
    );
    if let Err(e) = conn.execute(
        &sql,
        duckdb::params![area.polygon_geojson, address_count, building_count],
    ) {
        tracing::warn!(error = %e, "failed to log package export");
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({ "error": message }).to_string();
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::db::init_db;

    /// In-memory DuckDB with spatial loaded via init_db, whose schema already
    /// creates prg_unmatched/bdot10k_unmatched/egib_unmatched (plus
    /// osm_addresses/osm_buildings, unused by the package queries below) —
    /// no extra CREATE TABLE needed here, just seed data via named-column
    /// INSERTs so column order tracks src/db.rs rather than test-local
    /// assumptions.
    fn setup_db() -> duckdb::Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    fn test_area() -> RequestArea {
        parse_bbox("21.0,52.2,21.01,52.21").unwrap()
    }

    #[test]
    fn unmatched_addresses_returns_row_with_all_fields() {
        let conn = setup_db();
        conn.execute_batch(
            // Matching is precomputed upstream; prg_unmatched only ever holds
            // rows already known to be unmatched.
            "INSERT INTO prg_unmatched
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  teryt_miejscowosc, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('a1', '12', 'Długa', 'Warszawa', '00-263', '0918123',
                  ST_Point(21.001, 52.201), 8000, 4900, now());",
        )
        .unwrap();

        let rows = unmatched_addresses(&conn, &test_area()).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.housenumber.as_deref(), Some("12"));
        assert_eq!(row.street.as_deref(), Some("Długa"));
        assert_eq!(row.city.as_deref(), Some("Warszawa"));
        assert_eq!(row.postcode.as_deref(), Some("00-263"));
        assert_eq!(row.simc.as_deref(), Some("0918123"));
        assert!(row.geometry_geojson.contains("\"Point\""));
    }

    #[test]
    fn unmatched_addresses_respects_polygon_not_just_envelope() {
        let conn = setup_db();
        conn.execute_batch(
            // Both addresses are inside the triangle's envelope, but only
            // 'a1' is inside the triangle itself.
            "INSERT INTO prg_unmatched
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  teryt_miejscowosc, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('a1', '1', NULL, 'Zalesie', NULL, NULL, ST_Point(21.001, 52.201), 8000, 4900, now());
             INSERT INTO prg_unmatched
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  teryt_miejscowosc, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('a2', '2', NULL, 'Zalesie', NULL, NULL, ST_Point(21.008, 52.208), 8000, 4900, now());",
        )
        .unwrap();

        let triangle = parse_polygon_body(
            r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#,
        )
        .unwrap();
        let rows = unmatched_addresses(&conn, &triangle).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].housenumber.as_deref(), Some("1"));
    }

    #[test]
    fn unmatched_buildings_centroid_containment() {
        let conn = setup_db();
        conn.execute_batch(
            // Matching is precomputed upstream; bdot10k_unmatched only ever
            // holds rows already known to be unmatched.
            "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('b1', ST_MakeEnvelope(21.0060, 52.2060, 21.0062, 52.2062), 8000, 4900, now());",
        )
        .unwrap();

        let geoms = unmatched_buildings(&conn, "bdot10k_unmatched", &test_area()).unwrap();
        assert_eq!(geoms.len(), 1);
        assert!(geoms[0].contains("\"Polygon\""));
    }

    /// Seed one unmatched address whose street is the abbreviated PRG form.
    fn seed_abbreviated_address(conn: &duckdb::Connection) {
        conn.execute_batch(
            "INSERT INTO prg_unmatched
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  teryt_miejscowosc, geom, cell_x, cell_y, computed_at)
             VALUES ('a1', '12', 'gen. Kruka', 'Kock', '21-150', '0956069',
                     ST_Point(21.001, 52.201), 8000, 4900, now());",
        )
        .unwrap();
    }

    #[test]
    fn street_is_returned_raw_when_no_mapping_is_loaded() {
        let conn = setup_db();
        seed_abbreviated_address(&conn);
        let rows = unmatched_addresses(&conn, &test_area()).unwrap();
        assert_eq!(rows[0].street.as_deref(), Some("gen. Kruka"));
    }

    #[test]
    fn global_mapping_row_rewrites_the_street() {
        let conn = setup_db();
        seed_abbreviated_address(&conn);
        conn.execute_batch(
            "INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');",
        )
        .unwrap();
        let rows = unmatched_addresses(&conn, &test_area()).unwrap();
        assert_eq!(rows[0].street.as_deref(), Some("Generała Kruka"));
    }

    #[test]
    fn settlement_mapping_row_beats_the_global_row() {
        let conn = setup_db();
        seed_abbreviated_address(&conn);
        conn.execute_batch(
            "INSERT INTO street_name_mappings VALUES
                 (NULL, 'gen. Kruka', 'Generała Kruka'),
                 ('0956069', 'gen. Kruka', 'Generała Michała Heydenreicha \"Kruka\"');",
        )
        .unwrap();
        let rows = unmatched_addresses(&conn, &test_area()).unwrap();
        assert_eq!(
            rows[0].street.as_deref(),
            Some("Generała Michała Heydenreicha \"Kruka\"")
        );
    }

    /// PRG has re-capitalised its leading tokens once already; an exact match
    /// would silently stop rewriting instead of failing loudly.
    #[test]
    fn lookup_ignores_case_and_surrounding_whitespace() {
        let conn = setup_db();
        conn.execute_batch(
            "INSERT INTO prg_unmatched
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  teryt_miejscowosc, geom, cell_x, cell_y, computed_at)
             VALUES ('a1', '12', '  Gen. Kruka ', 'Kock', '21-150', '0956069',
                     ST_Point(21.001, 52.201), 8000, 4900, now());
             INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');",
        )
        .unwrap();
        let rows = unmatched_addresses(&conn, &test_area()).unwrap();
        assert_eq!(rows[0].street.as_deref(), Some("Generała Kruka"));
    }

    /// A settlement row must not leak into a different settlement.
    #[test]
    fn settlement_row_does_not_apply_to_another_settlement() {
        let conn = setup_db();
        seed_abbreviated_address(&conn);
        conn.execute_batch(
            "INSERT INTO street_name_mappings VALUES
                 ('9999999', 'gen. Kruka', 'Generała Someone Else');",
        )
        .unwrap();
        let rows = unmatched_addresses(&conn, &test_area()).unwrap();
        assert_eq!(rows[0].street.as_deref(), Some("gen. Kruka"));
    }

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::super::AppState;

    /// Shared seed: an unmatched address with street tags, an unmatched
    /// BDOT10k building, an unmatched EGIB building. Matching is precomputed
    /// upstream, so these serving tables only ever hold unmatched rows —
    /// there is no OSM-matched counterpart to seed or exclude here. Everything
    /// lives inside bbox 21.0,52.2,21.01,52.21.
    const SEED: &str = "
        CREATE TABLE prg_unmatched (
            lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
            miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
            geom GEOMETRY, cell_x INTEGER, cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE);
        CREATE TABLE bdot10k_unmatched (
            lokalnyid VARCHAR, geom GEOMETRY, cell_x INTEGER, cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE);
        CREATE TABLE egib_unmatched (
            id_budynku VARCHAR, geom GEOMETRY, cell_x INTEGER, cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE);

        CREATE TABLE street_name_mappings (
            teryt_simc_code VARCHAR,
            prg_street_name VARCHAR,
            osm_street_name VARCHAR);

        INSERT INTO prg_unmatched VALUES
            ('a1', '12', 'Marszałkowska', 'Warszawa', '00-590', '0918123',
             ST_Point(21.001, 52.201), 8000, 4900, now());
        INSERT INTO bdot10k_unmatched VALUES
            ('b1', ST_MakeEnvelope(21.0060, 52.2060, 21.0062, 52.2062), 8000, 4900, now());
        INSERT INTO egib_unmatched VALUES
            ('e1', ST_MakeEnvelope(21.0080, 52.2080, 21.0082, 52.2082), 8000, 4900, now());
    ";

    /// One connection seeded with both the government serving tables and
    /// `package_exports`, wrapped in a small pool — every handler and the
    /// export log now share the same pool (see server::ClonedConnectionManager).
    fn make_seeded_state() -> AppState {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial; SET GLOBAL geometry_always_xy = true;")
            .unwrap();
        conn.execute_batch(SEED).unwrap();
        conn.execute_batch(
            "CREATE TABLE package_exports (
                exported_at TIMESTAMP WITH TIME ZONE,
                area GEOMETRY('epsg:4326'),
                datasets VARCHAR[],
                address_count INTEGER,
                building_count INTEGER
            )",
        )
        .unwrap();
        let pool = crate::server::build_pool(conn, 2).unwrap();
        AppState {
            pool,
            registry: std::sync::Arc::new(crate::server::jobs::JobRegistry::new_for_tests(vec![])),
            config: std::sync::Arc::new(crate::config::Config::default()),
        }
    }

    fn package_app(state: AppState) -> Router {
        Router::new()
            .route(
                "/package",
                axum::routing::get(get_package).post(post_package),
            )
            .with_state(state)
    }

    async fn body_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn get_package_returns_missing_features_with_tags() {
        let state = make_seeded_state();
        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/geo+json");
        assert_eq!(
            response.headers()["content-disposition"],
            "attachment; filename=\"package.geojson\""
        );

        let json = body_json(response).await;
        assert_eq!(json["type"], "FeatureCollection");
        let features = json["features"].as_array().unwrap();
        // a1 (unmatched address) + b1 (unmatched bdot10k) + e1 (unmatched egib).
        // Order: Prg, Bdot10k, Egib.
        assert_eq!(features.len(), 3);
        let addr = &features[0];
        assert_eq!(addr["geometry"]["type"], "Point");
        assert_eq!(addr["properties"]["addr:housenumber"], "12");
        assert_eq!(addr["properties"]["addr:street"], "Marszałkowska");
        assert_eq!(addr["properties"]["addr:city"], "Warszawa");
        assert_eq!(addr["properties"]["addr:postcode"], "00-590");
        assert_eq!(addr["properties"]["addr:city:simc"], "0918123");
        assert_eq!(addr["properties"]["source:addr"], "gugik.gov.pl");
        assert_eq!(features[1]["properties"]["building"], "yes");
        assert_eq!(
            features[1]["properties"]["source:building"],
            "geoportal.gov.pl"
        );
        assert_eq!(features[2]["geometry"]["type"], "Polygon");
    }

    #[tokio::test]
    async fn get_package_datasets_filter() {
        let state = make_seeded_state();
        let app = package_app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21&datasets=prg")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json = body_json(response).await;
        assert_eq!(json["features"].as_array().unwrap().len(), 1);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21&datasets=bdot10k,egib")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json = body_json(response).await;
        assert_eq!(json["features"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn get_package_validation_errors() {
        let state = make_seeded_state();
        let app = package_app(state);

        // Area over the 0.04 default cap.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=14,49,25,55")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        let json = body_json(response).await;
        assert!(json["error"].as_str().unwrap().contains("exceeds"));

        // Malformed bbox.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=1,2,3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);

        // Missing bbox.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/package")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);

        // Unknown dataset.
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21&datasets=foo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_package_empty_area_returns_empty_collection() {
        let state = make_seeded_state();
        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=22.0,53.0,22.01,53.01")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["features"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn post_package_polygon_filters_exactly() {
        let state = make_seeded_state();
        // Triangle covering a1 but not b1/e1, although its envelope covers all.
        let triangle = r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#;
        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from(triangle))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let json = body_json(response).await;
        let features = json["features"].as_array().unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(features[0]["properties"]["addr:housenumber"], "12");
    }

    #[tokio::test]
    async fn post_package_accepts_feature_wrapper() {
        let state = make_seeded_state();
        let body = r#"{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}}"#;
        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["features"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn post_package_rejects_bad_bodies() {
        let state = make_seeded_state();
        let app = package_app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from(r#"{"type":"Point","coordinates":[21.0,52.2]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_package_logs_export_with_counts_datasets_and_geometry() {
        let state = make_seeded_state();
        let pool = state.pool.clone();

        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let conn = pool.get().unwrap();
        let (address_count, building_count): (i32, i32) = conn
            .query_row(
                "SELECT address_count, building_count FROM package_exports
                 ORDER BY exported_at DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(address_count, 1); // a1
        assert_eq!(building_count, 2); // b1 + e1

        let datasets_json: String = conn
            .query_row(
                "SELECT to_json(datasets) FROM package_exports ORDER BY exported_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(datasets_json, r#"["prg","bdot10k","egib"]"#);

        let area_geojson: String = conn
            .query_row(
                "SELECT ST_AsGeoJSON(area) FROM package_exports ORDER BY exported_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(area_geojson.contains("\"Polygon\""));
    }

    #[tokio::test]
    async fn get_package_logs_export_even_when_empty() {
        let state = make_seeded_state();
        let pool = state.pool.clone();

        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=22.0,53.0,22.01,53.01")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let conn = pool.get().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM package_exports", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let (address_count, building_count): (i32, i32) = conn
            .query_row(
                "SELECT address_count, building_count FROM package_exports",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(address_count, 0);
        assert_eq!(building_count, 0);
    }

    #[tokio::test]
    async fn post_package_logs_the_submitted_polygon_not_its_envelope() {
        let state = make_seeded_state();
        let pool = state.pool.clone();
        let triangle = r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#;

        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from(triangle))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let conn = pool.get().unwrap();
        let logged_geojson: String = conn
            .query_row(
                "SELECT ST_AsGeoJSON(area) FROM package_exports",
                [],
                |row| row.get(0),
            )
            .unwrap();
        // The logged area is the submitted triangle, a Polygon with 4 ring points
        // (closed), not a rectangle envelope.
        let parsed: serde_json::Value = serde_json::from_str(&logged_geojson).unwrap();
        assert_eq!(parsed["type"], "Polygon");
        assert_eq!(parsed["coordinates"][0].as_array().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn package_response_serves_the_expanded_street_name() {
        let state = make_seeded_state();
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch(
                "UPDATE prg_unmatched SET ulica = 'gen. Kruka' WHERE lokalny_id = 'a1';
                 INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');",
            )
            .unwrap();
        }
        let app = package_app(state);
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21&datasets=prg")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let feature = &json["features"][0];
        assert_eq!(feature["properties"]["addr:street"], "Generała Kruka");
    }

    #[test]
    fn log_export_does_not_panic_when_pool_is_exhausted() {
        // log_export()'s signature has no Result -- callers structurally
        // cannot observe a failure from it. What's worth verifying is that
        // pool exhaustion (e.g. every connection busy under concurrent load)
        // makes it return quietly instead of panicking on an unwrap.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial; SET GLOBAL geometry_always_xy = true;")
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE package_exports (
                exported_at TIMESTAMP WITH TIME ZONE,
                area GEOMETRY('epsg:4326'),
                datasets VARCHAR[],
                address_count INTEGER,
                building_count INTEGER
            )",
        )
        .unwrap();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(10))
            .build(crate::server::ClonedConnectionManager::new(conn))
            .unwrap();
        let state = AppState {
            pool: pool.clone(),
            registry: std::sync::Arc::new(crate::server::jobs::JobRegistry::new_for_tests(vec![])),
            config: std::sync::Arc::new(crate::config::Config::default()),
        };

        // Hold the pool's only connection so log_export's own pool.get() call
        // has nothing available and times out.
        let held = pool.get().unwrap();
        log_export(&state, &test_area(), &ALL_DATASETS, 1, 2);
        drop(held);

        let conn = pool.get().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM package_exports", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "no row should have been logged");
    }

    #[test]
    fn unmatched_buildings_respects_polygon_via_centroid() {
        let conn = setup_db();
        conn.execute_batch(
            // Centroid (21.0081, 52.2081) is inside the triangle's envelope
            // but outside the triangle → excluded.
            "INSERT INTO egib_unmatched (id_budynku, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('e1', ST_MakeEnvelope(21.0080, 52.2080, 21.0082, 52.2082), 8000, 4900, now());",
        )
        .unwrap();

        let triangle = parse_polygon_body(
            r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#,
        )
        .unwrap();
        assert_eq!(
            unmatched_buildings(&conn, "egib_unmatched", &triangle)
                .unwrap()
                .len(),
            0
        );
        // Sanity check: the plain bbox does include it.
        assert_eq!(
            unmatched_buildings(&conn, "egib_unmatched", &test_area())
                .unwrap()
                .len(),
            1
        );
    }

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
