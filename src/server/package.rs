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
    /// Whether `polygon_geojson` came from arbitrary client-drawn coordinates
    /// (`parse_polygon_body`, POST) rather than being built by `from_envelope`
    /// from a `bbox` query param (`parse_bbox`, GET). `check_request_geometry`
    /// (see `serve_package`) only needs to run against the former -- a bbox
    /// envelope is a rectangle this code constructed itself from four
    /// range-checked floats, so it cannot be topologically invalid, and
    /// running the GEOS round trip on it anyway would just be a wasted pool
    /// acquisition and query on every `GET /package`.
    pub is_user_supplied: bool,
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
            is_user_supplied: false,
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
        is_user_supplied: true,
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
/// `source:building` values -- the established OSM spellings for these two
/// registries, not our own dataset names (`Dataset::sql_name`'s "bdot10k" /
/// "egib" are internal identifiers, not tagging conventions).
const SOURCE_BUILDING_BDOT10K: &str = "BDOT";
const SOURCE_BUILDING_EGIB: &str = "EGiB";

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

/// `resolved` is the `;`-separated `k=v` tags string a mapping lookup
/// produced (see `unmatched_bdot10k_buildings`), or `None` when the source
/// has no mapping resolution yet (EGIB, until the design doc's step 5) or the
/// lookup found no matching row. Splits on `;`, then on the first `=`, and
/// always inserts `source:building` as `source` (`SOURCE_BUILDING_BDOT10K`
/// or `SOURCE_BUILDING_EGIB`). `None` or an empty string for `resolved`
/// yields `building=yes` -- this never warns, since the loader already
/// reports drift once per load (see `mappings::building_types`), which is
/// strictly more useful than a warning per packaged feature.
pub fn building_tags(resolved: Option<&str>, source: &str) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    let pairs = resolved
        .filter(|s| !s.trim().is_empty())
        .and_then(|s| crate::mappings::building_types::parse_tags(s).ok());
    match pairs {
        Some(pairs) => {
            for (k, v) in pairs {
                tags.insert(k, v);
            }
        }
        None => {
            tags.insert("building".to_string(), "yes".to_string());
        }
    }
    tags.insert("source:building".to_string(), source.to_string());
    tags
}

/// Insert `building:levels` from a source's above-ground storey count.
///
/// Both BDOT10k's `LICZBAKONDYGNACJI` and EGIB's `kondygnacje_nadziemne`
/// count above-ground storeys only -- confirmed against GUGiK's official
/// BDOT10k/BDOO object catalogue ("liczba nadziemnych kondygnacji budynku"),
/// which is exactly what OSM's `building:levels` counts, so the value is
/// emitted as-is with no unit conversion.
///
/// Deliberately not part of the CSV-resolved `tags` string `building_tags`
/// renders: the mapping table's `tags` column is a fixed set of `k=v` pairs
/// *per class* (every building matching a given key gets the same string),
/// while the storey count is a measured value that differs per building and
/// has to survive even when no mapping row matches (an unmatched building
/// with a known storey count but an unresolved classification still falls
/// through to `building=yes`, and should still report its storeys).
/// `0` ("budynek nie posiada kondygnacji" per the source's own definition)
/// and a missing value are both treated as nothing to report, the same as a
/// `None` resolved tags string falling back to `building=yes` rather than
/// asserting something the source didn't actually state.
fn with_building_levels(
    mut tags: BTreeMap<String, String>,
    levels: Option<i32>,
) -> BTreeMap<String, String> {
    if let Some(n) = levels
        && n >= 1
    {
        tags.insert("building:levels".to_string(), n.to_string());
    }
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
    // ST_MakeValid: the request polygon comes straight from the browser's
    // freehand/polygon drawing tool (parse_polygon_body checks JSON shape,
    // geometry type, coordinate ranges and a non-degenerate envelope, but not
    // topological validity), so a self-intersecting ("bowtie") ring is easy
    // to produce. DuckDB spatial is GEOS-backed and GEOS throws on an invalid
    // argument to ST_Intersects, which would surface as a 500 -- the same
    // class of failure as docs/invalid_geometry_tile_500s.md. Repairing here
    // rather than rejecting in parse_polygon_body protects the endpoint from
    // any client, not just our own frontend. Note ST_MakeValid on a bowtie
    // Polygon legitimately returns a MultiPolygon (it splits at the
    // self-intersection) -- that's the correct, intended repair, not
    // something to normalise away.
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
           AND ST_Intersects(a.geom, ST_MakeValid(ST_GeomFromGeoJSON(?)))"
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

/// One row from `unmatched_bdot10k_buildings` / `unmatched_egib_buildings`.
/// `levels` is the source's above-ground storey count (BDOT10k
/// `liczba_kondygnacji` / EGIB `kondygnacje_nadziemne`), read alongside the
/// mapping resolution so `build_package` doesn't need a second query --
/// rendered as `building:levels` by `with_building_levels`.
pub struct UnmatchedBuildingRow {
    pub geometry_geojson: String,
    pub tags: Option<String>,
    pub levels: Option<i32>,
}

/// The EGIB `rodzaj_kod` letter adjacency is restricted to -- the `m`
/// (residential) class, per the design doc's Adjacency section.
pub(crate) const EGIB_ADJACENCY_KEY: &str = "m";

/// EGIB buildings in the request area that are unmatched against OSM, paired
/// with the resolved tags string. Same three-CTE shape as
/// `unmatched_bdot10k_buildings`, but keyed off the precomputed
/// `rodzaj_kod` letter (see `mappings::egib`) instead of BDOT10k's raw
/// function columns, and with no tier-2 fallback -- EGIB has only one
/// cascade tier.
pub fn unmatched_egib_buildings(
    conn: &Connection,
    area: &RequestArea,
) -> Result<Vec<UnmatchedBuildingRow>> {
    let (x1, y1, x2, y2) = (area.min_lon, area.min_lat, area.max_lon, area.max_lat);
    let (nx1, ny1, nx2, ny2) = (
        x1 - ADJACENCY_READ_BUFFER_DEG,
        y1 - ADJACENCY_READ_BUFFER_DEG,
        x2 + ADJACENCY_READ_BUFFER_DEG,
        y2 + ADJACENCY_READ_BUFFER_DEG,
    );
    let sql = format!(
        "WITH pkg AS (
             SELECT b.rowid AS rid, b.geom,
                    ST_X(ST_Centroid(b.geom)) AS cx, ST_Y(ST_Centroid(b.geom)) AS cy,
                    b.rodzaj_kod, b.kondygnacje_nadziemne
             FROM egib_unmatched b
             -- Membership is intersection, not centroid containment: a building
             -- clipped by the edge of the request area is exported. The user
             -- chooses the area explicitly, so anything the selection touches
             -- is the intent; a building appearing in two separate exports the
             -- user deliberately made is their call to resolve, not something
             -- this query should pre-empt by dropping edge buildings.
             -- ST_MakeValid: see the comment in unmatched_addresses above --
             -- the request polygon may be a self-intersecting user-drawn ring.
             WHERE ST_Intersects(b.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
               AND ST_Intersects(b.geom, ST_MakeValid(ST_GeomFromGeoJSON(?)))
         ), nb AS (
             SELECT geom, ST_X(centroid) AS cx, ST_Y(centroid) AS cy
             FROM egib_buildings
             WHERE ST_Intersects(geom, ST_MakeEnvelope({nx1}, {ny1}, {nx2}, {ny2}))
               AND rodzaj_kod = '{EGIB_ADJACENCY_KEY}'
         ), cnt AS (
             SELECT p.rid, count(*) AS neighbours
             FROM pkg p JOIN nb
               ON (p.cx <> nb.cx OR p.cy <> nb.cy)
              AND ST_Intersects(p.geom, nb.geom)
             GROUP BY p.rid
         )
         SELECT ST_AsGeoJSON(pkg.geom), t.tags, pkg.kondygnacje_nadziemne
         FROM pkg
         LEFT JOIN cnt USING (rid)
         LEFT JOIN LATERAL (
             SELECT m.tags
             FROM egib_building_types m
             WHERE m.tier = 1 AND m.key = pkg.rodzaj_kod
               AND (m.min_levels     IS NULL OR pkg.kondygnacje_nadziemne >= m.min_levels)
               AND (m.max_levels     IS NULL OR pkg.kondygnacje_nadziemne <= m.max_levels)
               AND (m.max_neighbours IS NULL OR coalesce(cnt.neighbours, 0) <= m.max_neighbours)
             ORDER BY (m.min_levels     IS NOT NULL)::INT
                    + (m.max_levels     IS NOT NULL)::INT
                    + (m.max_neighbours IS NOT NULL)::INT DESC
             LIMIT 1
         ) t ON TRUE"
    );
    let mut stmt = conn
        .prepare(&sql)
        .context("Failed to prepare package egib building query")?;
    let rows = stmt
        .query_map([area.polygon_geojson.as_str()], |row| {
            Ok(UnmatchedBuildingRow {
                geometry_geojson: row.get(0)?,
                tags: row.get(1)?,
                levels: row.get(2)?,
            })
        })
        .context("Failed to run package egib building query")?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.context("Failed to read package egib building row")?);
    }
    Ok(out)
}

/// Buffer around the request bbox used to read `nb` adjacency candidates, so
/// a party wall shared with a neighbour just outside the request edge is not
/// missed. ~35 m -- comfortably wider than an ordinary building's footprint,
/// so it does not need to reach the neighbour's own centroid, only some part
/// of its polygon. See "Four things the query must get right" in
/// docs/superpowers/specs/2026-08-03-building-type-mappings-design.md.
pub(crate) const ADJACENCY_READ_BUFFER_DEG: f64 = 0.0005;

/// The BDOT10k tier-1 key adjacency is restricted to -- a code-level constant,
/// not a CSV column, so a CSV row cannot silently reference a neighbour count
/// the query never computes (enforced at load by
/// `mappings::building_types::load_from_path`).
pub(crate) const BDOT10K_ADJACENCY_KEY: &str = "budynek jednorodzinny";

/// BDOT10k buildings in the request area that are unmatched against OSM,
/// paired with the `;`-separated `k=v` tags string resolved by
/// `bdot10k_building_types` (or `None` if no row matches, which
/// `building_tags` renders as `building=yes`).
///
/// Three stages, each a CTE: `pkg` is the same read as the old
/// `unmatched_buildings` plus the raw classification columns; `nb`/`cnt`
/// count same-class neighbours (buffered read, `budynek jednorodzinny` only,
/// identity by centroid-coordinate inequality rather than `ST_Equals` --
/// cheaper and equivalent on this data); the outer `LEFT JOIN LATERAL`
/// resolves the mapping, most-specific-row-wins, falling through to `NULL`
/// (rendered as `building=yes`) when the mapping table is empty or nothing
/// matches. See the design doc's Serving/Adjacency sections for the
/// reasoning and the measured cost (~+0.1 s on a worst-case request).
pub fn unmatched_bdot10k_buildings(
    conn: &Connection,
    area: &RequestArea,
) -> Result<Vec<UnmatchedBuildingRow>> {
    let (x1, y1, x2, y2) = (area.min_lon, area.min_lat, area.max_lon, area.max_lat);
    let (nx1, ny1, nx2, ny2) = (
        x1 - ADJACENCY_READ_BUFFER_DEG,
        y1 - ADJACENCY_READ_BUFFER_DEG,
        x2 + ADJACENCY_READ_BUFFER_DEG,
        y2 + ADJACENCY_READ_BUFFER_DEG,
    );
    let sql = format!(
        "WITH pkg AS (
             SELECT b.rowid AS rid, b.geom,
                    ST_X(ST_Centroid(b.geom)) AS cx, ST_Y(ST_Centroid(b.geom)) AS cy,
                    b.funkcja_szczegolowa, b.funkcja_ogolna, b.liczba_kondygnacji
             FROM bdot10k_unmatched b
             -- Membership is intersection, not centroid containment: the user
             -- picks the area deliberately, so anything their selection touches
             -- is what they asked for. See the comment in
             -- unmatched_egib_buildings.
             -- ST_MakeValid: see the comment in unmatched_addresses above --
             -- the request polygon may be a self-intersecting user-drawn ring.
             WHERE ST_Intersects(b.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
               AND ST_Intersects(b.geom, ST_MakeValid(ST_GeomFromGeoJSON(?)))
         ), nb AS (
             SELECT geom, ST_X(centroid) AS cx, ST_Y(centroid) AS cy
             FROM bdot10k_buildings
             WHERE ST_Intersects(geom, ST_MakeEnvelope({nx1}, {ny1}, {nx2}, {ny2}))
               AND lower(trim(PRZEWAZAJACAFUNKCJABUDYNKU)) = '{BDOT10K_ADJACENCY_KEY}'
         ), cnt AS (
             SELECT p.rid, count(*) AS neighbours
             FROM pkg p JOIN nb
               ON (p.cx <> nb.cx OR p.cy <> nb.cy)
              AND ST_Intersects(p.geom, nb.geom)
             GROUP BY p.rid
         )
         SELECT ST_AsGeoJSON(pkg.geom), t.tags, pkg.liczba_kondygnacji
         FROM pkg
         LEFT JOIN cnt USING (rid)
         LEFT JOIN LATERAL (
             SELECT m.tags
             FROM bdot10k_building_types m
             WHERE ((m.tier = 1 AND m.key = lower(trim(pkg.funkcja_szczegolowa)))
                 OR (m.tier = 2 AND m.key = lower(trim(pkg.funkcja_ogolna))))
               AND (m.min_levels IS NULL OR pkg.liczba_kondygnacji >= m.min_levels)
               AND (m.max_levels IS NULL OR pkg.liczba_kondygnacji <= m.max_levels)
               AND (m.max_neighbours IS NULL OR coalesce(cnt.neighbours, 0) <= m.max_neighbours)
             ORDER BY m.tier ASC,
                      (m.min_levels IS NOT NULL)::INT
                    + (m.max_levels IS NOT NULL)::INT
                    + (m.max_neighbours IS NOT NULL)::INT DESC
             LIMIT 1
         ) t ON TRUE"
    );
    let mut stmt = conn
        .prepare(&sql)
        .context("Failed to prepare package bdot10k building query")?;
    let rows = stmt
        .query_map([area.polygon_geojson.as_str()], |row| {
            Ok(UnmatchedBuildingRow {
                geometry_geojson: row.get(0)?,
                tags: row.get(1)?,
                levels: row.get(2)?,
            })
        })
        .context("Failed to run package bdot10k building query")?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.context("Failed to read package bdot10k building row")?);
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
    // Topological validity needs GEOS, so it can't live in parse_polygon_body
    // (pure serde, no DB connection) -- run it here, off the async thread,
    // same spawn_blocking pattern as the build_package call below. Silently
    // repairing whatever arrives (the ST_MakeValid wrappers in the queries)
    // is the right move for a self-intersecting bowtie, but the wrong end
    // state for a repair that no longer resembles a shape with an interior --
    // see check_request_geometry's doc comment. Only user-supplied (POST)
    // geometry needs this: a bbox (GET) area is a rectangle this code built
    // itself from range-checked floats (see `RequestArea::is_user_supplied`),
    // so paying for a pool connection and a GEOS round trip on every GET
    // would just be validating something that cannot be invalid.
    if area.is_user_supplied {
        let validity_state = state.clone();
        let validity_area = area.clone();
        let validity = tokio::task::spawn_blocking(move || {
            let conn = validity_state
                .pool
                .get()
                .context("Failed to acquire pool connection")?;
            Ok::<Option<String>, anyhow::Error>(check_request_geometry(&conn, &validity_area))
        })
        .await;
        match validity {
            Ok(Ok(Some(message))) => return error_response(StatusCode::BAD_REQUEST, &message),
            Ok(Ok(None)) => {}
            Ok(Err(e)) => {
                tracing::error!(error = %e, "package geometry validity check failed");
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
            Err(e) => {
                tracing::error!(error = %e, "package geometry validity check task panicked");
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        }
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

/// Rejects a request polygon whose GEOS repair can't be trusted to still
/// mean what the user drew, instead of letting it flow through to the
/// per-query `ST_MakeValid` wrappers unconditionally.
///
/// `ST_MakeValid` at every query site (see `unmatched_addresses`) repairs
/// invalid geometry rather than rejecting it -- that's deliberate, it
/// protects the endpoint from any client. But repairing *everything*
/// silently is the wrong end state: a freehand scribble whose points are
/// effectively collinear repairs to a zero-area LineString, and "everything
/// intersecting a line" is not the package the user asked for. So this runs
/// the one query that decides, before any `*_unmatched` query does real
/// work: geometry already valid -> proceed unchanged; invalid but repairs to
/// a non-degenerate Polygon/MultiPolygon (a self-intersecting bowtie ring
/// correctly repairs to a MultiPolygon -- see the comment above, that's the
/// intended outcome, not something to second-guess here) -> proceed, the
/// per-query wrappers do the same repair again; anything else (empty,
/// zero-area, or a repair that isn't polygonal at all) -> reject.
///
/// `ST_GeomFromGeoJSON` itself can throw -- not just return invalid geometry
/// -- on coordinates that are syntactically valid JSON but not a legal
/// geometry (e.g. a ring with fewer than 4 points, or one that isn't
/// closed). `parse_polygon_body` never catches that, since it only inspects
/// the JSON and never touches GEOS, so any such throw is caught here too and
/// folded into the same rejection rather than bubbling up as a 500.
///
/// Returns `None` to proceed, `Some(message)` to reject with that message as
/// the 400 body -- the frontend shows it verbatim in a feedback modal.
/// Rejection message for a geometry `ST_GeomFromGeoJSON` itself throws on --
/// syntactically valid JSON that isn't a legal geometry. Named so the test
/// pinning it can assert equality without a second copy of the literal.
const INVALID_GEOMETRY_MESSAGE: &str = "polygon geometry is invalid and could not be processed by the spatial engine \
     (check that every ring is closed and has at least 4 points)";

/// Rejection message for a geometry that parses but is invalid and doesn't
/// repair into a non-degenerate Polygon/MultiPolygon. Named for the same
/// reason as `INVALID_GEOMETRY_MESSAGE`.
const DEGENERATE_GEOMETRY_MESSAGE: &str = "polygon geometry is self-intersecting or degenerate and does not repair into a shape \
     with area -- check for collinear points or a crossed outline";

fn check_request_geometry(conn: &Connection, area: &RequestArea) -> Option<String> {
    let probe: duckdb::Result<(bool, String, f64)> = conn.query_row(
        "SELECT ST_IsValid(g), ST_GeometryType(ST_MakeValid(g)), ST_Area(ST_MakeValid(g))
         FROM (SELECT ST_GeomFromGeoJSON(?) AS g)",
        [area.polygon_geojson.as_str()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    );
    let (is_valid, repaired_type, repaired_area) = match probe {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "request polygon rejected by the spatial engine");
            return Some(INVALID_GEOMETRY_MESSAGE.to_string());
        }
    };
    if is_valid {
        return None;
    }
    let repairs_to_polygon = repaired_type == "POLYGON" || repaired_type == "MULTIPOLYGON";
    if repairs_to_polygon && repaired_area > 0.0 {
        return None;
    }
    Some(DEGENERATE_GEOMETRY_MESSAGE.to_string())
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
                for row in unmatched_bdot10k_buildings(&conn, area)? {
                    let properties = with_building_levels(
                        building_tags(row.tags.as_deref(), SOURCE_BUILDING_BDOT10K),
                        row.levels,
                    );
                    features.push(feature(row.geometry_geojson, properties)?);
                    building_count += 1;
                }
            }
            Dataset::Egib => {
                for row in unmatched_egib_buildings(&conn, area)? {
                    let properties = with_building_levels(
                        building_tags(row.tags.as_deref(), SOURCE_BUILDING_EGIB),
                        row.levels,
                    );
                    features.push(feature(row.geometry_geojson, properties)?);
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
    // ST_MakeValid: same as the query sites in unmatched_addresses above --
    // the logged area must be the geometry actually queried (post-repair), so
    // the two can never disagree.
    let sql = format!(
        "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
         VALUES (now(), ST_MakeValid(ST_GeomFromGeoJSON(?)), {datasets_sql}, ?, ?)"
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
    fn unmatched_bdot10k_buildings_returns_row_in_area() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (
                 LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR);
             -- Matching is precomputed upstream; bdot10k_unmatched only ever
             -- holds rows already known to be unmatched.
             INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('b1', ST_MakeEnvelope(21.0060, 52.2060, 21.0062, 52.2062), 8000, 4900, now());",
        )
        .unwrap();

        let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].geometry_geojson.contains("\"Polygon\""));
    }

    /// Membership is intersection, not centroid containment: a building the
    /// request polygon merely clips is exported, because the user chose that
    /// area deliberately and everything it touches is what they asked for.
    /// Mirrored by `unmatched_egib_buildings_includes_building_clipped_by_edge`
    /// -- the two queries must not drift apart on this.
    #[test]
    fn unmatched_bdot10k_buildings_includes_building_clipped_by_edge() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (
                 LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR);
             INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at)
             VALUES
                 -- Straddles the triangle's hypotenuse: centroid (21.0055,
                 -- 52.2055) is outside it, the low corner is inside.
                 ('clipped', ST_MakeEnvelope(21.0040, 52.2040, 21.0070, 52.2070),
                  8000, 4900, now()),
                 -- Wholly beyond the hypotenuse, inside the bbox only.
                 ('outside', ST_MakeEnvelope(21.0080, 52.2080, 21.0082, 52.2082),
                  8000, 4900, now());",
        )
        .unwrap();

        let triangle = parse_polygon_body(
            r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#,
        )
        .unwrap();
        assert_eq!(
            unmatched_bdot10k_buildings(&conn, &triangle).unwrap().len(),
            1
        );
        // The polygon still filters -- this is not just the bbox passing
        // everything through.
        assert_eq!(
            unmatched_bdot10k_buildings(&conn, &test_area())
                .unwrap()
                .len(),
            2
        );
    }

    /// `unmatched_bdot10k_buildings` tests. `bdot10k_buildings` is the live
    /// table adjacency reads (never `bdot10k_unmatched` -- see "Where the
    /// mapping is applied"); `bdot10k_building_types` is the mapping table
    /// (empty by default, a valid state that falls through to `building=yes`).
    mod bdot10k_adjacency_and_mapping {
        use super::*;

        fn seed_live_table(conn: &Connection, rows: &str) {
            conn.execute_batch(
                "CREATE TABLE bdot10k_buildings (
                     LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                     PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR);",
            )
            .unwrap();
            if !rows.is_empty() {
                conn.execute_batch(rows).unwrap();
                conn.execute_batch("UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);")
                    .unwrap();
            }
        }

        fn insert_mapping_row(
            conn: &Connection,
            tier: i32,
            key: &str,
            min_levels: Option<i32>,
            max_levels: Option<i32>,
            max_neighbours: Option<i32>,
            tags: &str,
        ) {
            conn.execute(
                "INSERT INTO bdot10k_building_types
                     (tier, key, min_levels, max_levels, max_neighbours, tags)
                 VALUES (?, ?, ?, ?, ?, ?)",
                duckdb::params![tier, key, min_levels, max_levels, max_neighbours, tags],
            )
            .unwrap();
        }

        /// One isolated `budynek jednorodzinny`, tier-1 mapping present with
        /// a `max_neighbours=0` row and an unconstrained fallback row --
        /// pins the tier-1 hit path, the adjacency branch firing at exactly
        /// 0 (not merely "some small number"), and that an isolated building
        /// counts 0 real neighbours rather than a NULL that would slip past
        /// `coalesce`.
        #[test]
        fn tier1_hit_isolated_building_resolves_to_the_zero_neighbour_row() {
            let conn = setup_db();
            seed_live_table(
                &conn,
                "INSERT INTO bdot10k_buildings (LOKALNYID, geom, PRZEWAZAJACAFUNKCJABUDYNKU) VALUES
                     ('h1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 'budynek jednorodzinny');",
            );
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at, funkcja_szczegolowa)
                 VALUES
                     ('h1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'budynek jednorodzinny');",
            )
            .unwrap();
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                Some(0),
                "building=detached",
            );
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                None,
                "building=house",
            );

            let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags.as_deref(), Some("building=detached"));
        }

        /// Two `budynek jednorodzinny` buildings sharing a wall: each has 1
        /// neighbour, so the `max_neighbours=0` row must NOT fire -- pins
        /// "fires at 0, not at 1".
        #[test]
        fn two_touching_houses_each_get_one_neighbour_and_fall_to_the_unconstrained_row() {
            let conn = setup_db();
            seed_live_table(
                &conn,
                "INSERT INTO bdot10k_buildings (LOKALNYID, geom, PRZEWAZAJACAFUNKCJABUDYNKU) VALUES
                     ('h1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 'budynek jednorodzinny'),
                     ('h2', ST_MakeEnvelope(21.0062,52.2060,21.0064,52.2062), 'budynek jednorodzinny');",
            );
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at, funkcja_szczegolowa)
                 SELECT LOKALNYID, geom, 8000, 4900, now(), PRZEWAZAJACAFUNKCJABUDYNKU
                 FROM bdot10k_buildings;",
            )
            .unwrap();
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                Some(0),
                "building=detached",
            );
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                None,
                "building=house",
            );

            let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 2);
            for row in &rows {
                assert_eq!(row.tags.as_deref(), Some("building=house"));
            }
        }

        /// A touching building of a different class (not
        /// `budynek jednorodzinny`) must never be counted -- pins "a
        /// neighbour of a different class does not count".
        #[test]
        fn a_touching_neighbour_of_a_different_class_does_not_count() {
            let conn = setup_db();
            seed_live_table(
                &conn,
                "INSERT INTO bdot10k_buildings (LOKALNYID, geom, PRZEWAZAJACAFUNKCJABUDYNKU) VALUES
                     ('h1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 'budynek jednorodzinny'),
                     ('garaz', ST_MakeEnvelope(21.0058,52.2060,21.0060,52.2062), 'garaż');",
            );
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at, funkcja_szczegolowa)
                 VALUES
                     ('h1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'budynek jednorodzinny');",
            )
            .unwrap();
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                Some(0),
                "building=detached",
            );
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                None,
                "building=house",
            );

            let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].tags.as_deref(),
                Some("building=detached"),
                "the adjoining garage must not count as a residential neighbour"
            );
        }

        /// A neighbour whose polygon sits just past the request bbox edge --
        /// inside the `ADJACENCY_READ_BUFFER_DEG` buffer, outside the raw
        /// bbox -- must still be counted. Regression guard for the buffer.
        #[test]
        fn a_neighbour_just_outside_the_request_bbox_still_counts() {
            let conn = setup_db();
            let area = test_area(); // 21.0,52.2,21.01,52.21
            seed_live_table(
                &conn,
                "INSERT INTO bdot10k_buildings (LOKALNYID, geom, PRZEWAZAJACAFUNKCJABUDYNKU) VALUES
                     -- inside the bbox (max_lon 21.01), touching the edge building below
                     ('inside', ST_MakeEnvelope(21.0097,52.2050,21.0100,52.2052), 'budynek jednorodzinny'),
                     -- outside the raw bbox (min_lon starts past 21.01) but within the
                     -- 0.0005 deg adjacency read buffer, touching 'inside' at x=21.0100
                     ('outside', ST_MakeEnvelope(21.0100,52.2050,21.0103,52.2052), 'budynek jednorodzinny');",
            );
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at, funkcja_szczegolowa)
                 VALUES
                     ('inside', ST_MakeEnvelope(21.0097,52.2050,21.0100,52.2052), 8000, 4900, now(),
                      'budynek jednorodzinny');",
            )
            .unwrap();
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                Some(0),
                "building=detached",
            );
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                None,
                "building=house",
            );

            let rows = unmatched_bdot10k_buildings(&conn, &area).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].tags.as_deref(),
                Some("building=house"),
                "the neighbour just outside the bbox must still be read and counted"
            );
        }

        /// Tier-1 miss, tier-2 hit -- pins the cascade.
        #[test]
        fn tier1_miss_falls_through_to_tier2_hit() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at,
                      funkcja_szczegolowa, funkcja_ogolna)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'jakas nieznana funkcja', 'budynki biurowe');",
            )
            .unwrap();
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                Some(0),
                "building=detached",
            );
            insert_mapping_row(
                &conn,
                2,
                "budynki biurowe",
                None,
                None,
                None,
                "building=office",
            );

            let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags.as_deref(), Some("building=office"));
        }

        /// Neither tier matches -> resolved tags is None, which
        /// `building_tags` renders as plain `building=yes`.
        #[test]
        fn neither_tier_matching_yields_none_and_renders_as_building_yes() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at,
                      funkcja_szczegolowa, funkcja_ogolna)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'totally unknown', 'also unknown');",
            )
            .unwrap();
            insert_mapping_row(
                &conn,
                1,
                "budynek jednorodzinny",
                None,
                None,
                Some(0),
                "building=detached",
            );

            let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags, None);
            assert_eq!(
                building_tags(rows[0].tags.as_deref(), SOURCE_BUILDING_BDOT10K)
                    .get("building")
                    .unwrap(),
                "yes"
            );
        }

        /// `liczba_kondygnacji` is read through unchanged (no unit
        /// conversion -- confirmed above-ground-only against GUGiK's
        /// BDOT10k/BDOO object catalogue), and independent of whether a
        /// mapping row matched.
        #[test]
        fn levels_is_read_through_regardless_of_mapping_resolution() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at,
                      funkcja_szczegolowa, liczba_kondygnacji)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'totally unknown', 4);",
            )
            .unwrap();

            let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags, None, "no mapping row matches this key");
            assert_eq!(
                rows[0].levels,
                Some(4),
                "the storey count must come through regardless"
            );
            let tags = with_building_levels(
                building_tags(rows[0].tags.as_deref(), SOURCE_BUILDING_BDOT10K),
                rows[0].levels,
            );
            assert_eq!(tags.get("building").unwrap(), "yes");
            assert_eq!(tags.get("building:levels").unwrap(), "4");
        }

        /// An empty mapping table (the out-of-the-box state) must not error,
        /// and every row falls through to `building=yes`.
        #[test]
        fn empty_mapping_table_falls_through_for_every_row() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at, funkcja_szczegolowa)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'budynek gospodarczy');",
            )
            .unwrap();

            let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags, None);
        }

        /// A `man_made` pair round-trips through the full query into
        /// `building_tags`' output, not just through the unit-level parser.
        #[test]
        fn a_man_made_pair_round_trips_through_the_full_query() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at, funkcja_szczegolowa)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'silos');",
            )
            .unwrap();
            insert_mapping_row(
                &conn,
                1,
                "silos",
                None,
                None,
                None,
                "building=silo;man_made=silo",
            );

            let rows = unmatched_bdot10k_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            let tags = building_tags(rows[0].tags.as_deref(), SOURCE_BUILDING_BDOT10K);
            assert_eq!(tags.get("building").unwrap(), "silo");
            assert_eq!(tags.get("man_made").unwrap(), "silo");
        }
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
            computed_at TIMESTAMP WITH TIME ZONE,
            funkcja_szczegolowa VARCHAR, funkcja_ogolna VARCHAR, liczba_kondygnacji SMALLINT,
            KATEGORIAISTNIENIA VARCHAR, NAZWA VARCHAR, FSBUD VARCHAR,
            INFORMACJADODATKOWA VARCHAR, KODKST TINYINT, ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
        CREATE TABLE egib_unmatched (
            id_budynku VARCHAR, geom GEOMETRY, cell_x INTEGER, cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE,
            rodzaj_kod VARCHAR, kondygnacje_nadziemne INTEGER,
            kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
        -- unmatched_bdot10k_buildings' nb CTE reads this directly (adjacency
        -- is a self-comparison against the live table, not the serving
        -- table -- see the design doc's \"Where the mapping is applied\").
        CREATE TABLE bdot10k_buildings (
            LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
            PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR);
        CREATE TABLE bdot10k_building_types (
            tier INTEGER, key VARCHAR, min_levels INTEGER, max_levels INTEGER,
            max_neighbours INTEGER, tags VARCHAR);
        -- unmatched_egib_buildings' nb CTE reads this directly, same rationale
        -- as bdot10k_buildings above.
        CREATE TABLE egib_buildings (
            id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR);
        CREATE TABLE egib_building_types (
            tier INTEGER, key VARCHAR, min_levels INTEGER, max_levels INTEGER,
            max_neighbours INTEGER, tags VARCHAR);

        CREATE TABLE street_name_mappings (
            teryt_simc_code VARCHAR,
            prg_street_name VARCHAR,
            osm_street_name VARCHAR);

        INSERT INTO prg_unmatched VALUES
            ('a1', '12', 'Marszałkowska', 'Warszawa', '00-590', '0918123',
             ST_Point(21.001, 52.201), 8000, 4900, now());
        INSERT INTO bdot10k_unmatched (lokalnyid, geom, cell_x, cell_y, computed_at) VALUES
            ('b1', ST_MakeEnvelope(21.0060, 52.2060, 21.0062, 52.2062), 8000, 4900, now());
        INSERT INTO egib_unmatched (id_budynku, geom, cell_x, cell_y, computed_at) VALUES
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
        assert_eq!(features[1]["properties"]["source:building"], "BDOT");
        assert_eq!(features[2]["geometry"]["type"], "Polygon");
        assert_eq!(features[2]["properties"]["source:building"], "EGiB");
    }

    #[tokio::test]
    async fn get_package_emits_building_levels_end_to_end() {
        let state = make_seeded_state();
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch(
                "UPDATE bdot10k_unmatched SET liczba_kondygnacji = 3 WHERE lokalnyid = 'b1';
                 UPDATE egib_unmatched SET kondygnacje_nadziemne = 1 WHERE id_budynku = 'e1';",
            )
            .unwrap();
        }
        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21&datasets=bdot10k,egib")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let json = body_json(response).await;
        let features = json["features"].as_array().unwrap();
        assert_eq!(features.len(), 2); // b1, e1 -- order Bdot10k, Egib
        assert_eq!(features[0]["properties"]["building:levels"], "3");
        assert_eq!(features[1]["properties"]["building:levels"], "1");
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

    /// A self-intersecting ("bowtie") ring -- the shape a browser
    /// freehand/polygon drawing tool can easily produce, and which
    /// `parse_polygon_body` deliberately does not reject (see its doc
    /// comment) -- must not 500. `ST_MakeValid` around the request geometry
    /// in the query SQL is what prevents GEOS from throwing on it, and this
    /// also pins that `check_request_geometry`'s pre-flight check does NOT
    /// reject it: a bowtie repairs to a non-degenerate MultiPolygon, which is
    /// exactly the "invalid but repairs to a real shape" case that's allowed
    /// through.
    #[tokio::test]
    async fn post_package_bowtie_polygon_does_not_500() {
        let state = make_seeded_state();
        let bowtie = r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.21],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#;
        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from(bowtie))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["type"], "FeatureCollection");
        assert!(json["features"].as_array().is_some());
    }

    /// A ring whose points are collinear -- e.g. a degenerate freehand
    /// scribble -- repairs under `ST_MakeValid` to a zero-area
    /// `MULTILINESTRING`, not a polygon. Downloading "everything
    /// intersecting a line" is not what the user drew, so
    /// `check_request_geometry` must reject this with 400 rather than let it
    /// flow through to the queries.
    ///
    /// The ring must clear two upstream guards before it can exercise this
    /// one, or the test passes for the wrong reason:
    /// - `parse_polygon_body`'s degenerate-envelope check (min==max on an
    ///   axis) -- so lon and lat both have to vary, not just one.
    /// - `serve_package`'s `check_area` cap (0.04 sq deg default) -- so the
    ///   envelope has to stay under that.
    /// The three points here (0, 0.0625, 0.125 offsets -- eighths of a
    /// degree, exact binary fractions) are collinear along a diagonal, are
    /// exactly representable in `f64`, and were confirmed empirically
    /// (temporary probe, since removed) to make `ST_IsValid` report false and
    /// `ST_MakeValid` repair to a zero-area `MULTILINESTRING`. A first
    /// attempt with ordinary decimals (`21.005`, `21.01`) looked collinear on
    /// paper but rounds to a non-collinear point in `f64`, so GEOS reported
    /// it as a *valid*, non-degenerate sliver polygon and the request
    /// returned 200 -- worth calling out since it's the same trap the message
    /// assertion below is designed to catch (exact equality, not a substring
    /// that a different rejection path could also satisfy).
    #[tokio::test]
    async fn post_package_collinear_ring_is_rejected_not_silently_repaired() {
        let state = make_seeded_state();
        let collinear = r#"{"type":"Polygon","coordinates":[[[21.0,52.0],[21.0625,52.0625],[21.125,52.125],[21.0,52.0]]]}"#;
        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from(collinear))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        let json = body_json(response).await;
        assert_eq!(json["error"].as_str().unwrap(), DEGENERATE_GEOMETRY_MESSAGE);
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
    fn unmatched_egib_buildings_includes_building_clipped_by_edge() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE egib_buildings (
                 id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR);
             INSERT INTO egib_unmatched (id_budynku, geom, cell_x, cell_y, computed_at)
             VALUES
                 -- Straddles the triangle's hypotenuse: centroid (21.0055,
                 -- 52.2055) is outside it, the low corner is inside. Exported
                 -- anyway -- membership is intersection, not containment.
                 ('clipped', ST_MakeEnvelope(21.0040, 52.2040, 21.0070, 52.2070),
                  8000, 4900, now()),
                 -- Wholly beyond the hypotenuse, inside the bbox only.
                 ('outside', ST_MakeEnvelope(21.0080, 52.2080, 21.0082, 52.2082),
                  8000, 4900, now());",
        )
        .unwrap();

        let triangle = parse_polygon_body(
            r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#,
        )
        .unwrap();
        assert_eq!(unmatched_egib_buildings(&conn, &triangle).unwrap().len(), 1);
        // The polygon still filters -- this is not just the bbox passing
        // everything through.
        assert_eq!(
            unmatched_egib_buildings(&conn, &test_area()).unwrap().len(),
            2
        );
    }

    /// `unmatched_egib_buildings` tests -- mirrors
    /// `bdot10k_adjacency_and_mapping` above, but keyed off the precomputed
    /// `rodzaj_kod` letter with no tier-2 fallback and with the storey
    /// (`kondygnacje_nadziemne`) constraints exercised, since that's the axis
    /// BDOT10k's mapping rows don't currently use.
    mod egib_adjacency_and_mapping {
        use super::*;

        fn seed_live_table(conn: &Connection, rows: &str) {
            conn.execute_batch(
                "CREATE TABLE egib_buildings (
                     id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR);",
            )
            .unwrap();
            if !rows.is_empty() {
                conn.execute_batch(rows).unwrap();
                conn.execute_batch("UPDATE egib_buildings SET centroid = ST_Centroid(geom);")
                    .unwrap();
            }
        }

        fn insert_mapping_row(
            conn: &Connection,
            key: &str,
            min_levels: Option<i32>,
            max_levels: Option<i32>,
            max_neighbours: Option<i32>,
            tags: &str,
        ) {
            conn.execute(
                "INSERT INTO egib_building_types
                     (tier, key, min_levels, max_levels, max_neighbours, tags)
                 VALUES (1, ?, ?, ?, ?, ?)",
                duckdb::params![key, min_levels, max_levels, max_neighbours, tags],
            )
            .unwrap();
        }

        fn seed_mapping(conn: &Connection) {
            insert_mapping_row(conn, "m", Some(4), None, None, "building=apartments");
            insert_mapping_row(conn, "m", Some(1), Some(2), Some(0), "building=detached");
            insert_mapping_row(conn, "m", Some(1), Some(2), None, "building=house");
            insert_mapping_row(conn, "m", None, None, None, "building=residential");
        }

        /// 4+ storeys always resolves to apartments regardless of adjacency.
        #[test]
        fn four_plus_storeys_resolves_to_apartments() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod, kondygnacje_nadziemne)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'm', 5);",
            )
            .unwrap();
            seed_mapping(&conn);

            let rows = unmatched_egib_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags.as_deref(), Some("building=apartments"));
        }

        /// 1-2 storeys, isolated (no residential neighbour) -> detached.
        #[test]
        fn isolated_one_to_two_storey_resolves_to_detached() {
            let conn = setup_db();
            seed_live_table(
                &conn,
                "INSERT INTO egib_buildings (id_budynku, geom, rodzaj_kod) VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 'm');",
            );
            conn.execute_batch(
                "INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod, kondygnacje_nadziemne)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'm', 2);",
            )
            .unwrap();
            seed_mapping(&conn);

            let rows = unmatched_egib_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags.as_deref(), Some("building=detached"));
        }

        /// 1-2 storeys, touching another `m` building -> house, not detached.
        #[test]
        fn touching_one_to_two_storey_resolves_to_house() {
            let conn = setup_db();
            seed_live_table(
                &conn,
                "INSERT INTO egib_buildings (id_budynku, geom, rodzaj_kod) VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 'm'),
                     ('u2', ST_MakeEnvelope(21.0062,52.2060,21.0064,52.2062), 'm');",
            );
            conn.execute_batch(
                "INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod, kondygnacje_nadziemne)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'm', 1);",
            )
            .unwrap();
            seed_mapping(&conn);

            let rows = unmatched_egib_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags.as_deref(), Some("building=house"));
        }

        /// 3 storeys, or an unknown storey count, stays generic residential --
        /// the deliberately ambiguous band from
        /// docs/building_type_mappings.md.
        #[test]
        fn three_storeys_or_unknown_stays_residential() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod, kondygnacje_nadziemne)
                 VALUES
                     ('three', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'm', 3),
                     ('unknown', ST_MakeEnvelope(21.0070,52.2070,21.0072,52.2072), 8000, 4900, now(),
                      'm', NULL);",
            )
            .unwrap();
            seed_mapping(&conn);

            let rows = unmatched_egib_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 2);
            for row in &rows {
                assert_eq!(row.tags.as_deref(), Some("building=residential"));
            }
        }

        /// A `rodzaj_kod` the mapping table has no row for falls through to
        /// `building=yes` via `None`.
        #[test]
        fn unmapped_letter_falls_through_to_none() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod, kondygnacje_nadziemne)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'g', NULL);",
            )
            .unwrap();
            seed_mapping(&conn); // only has 'm' rows

            let rows = unmatched_egib_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags, None);
        }

        /// An empty mapping table -- the out-of-the-box state -- must not
        /// error, and falls through to `building=yes`.
        #[test]
        fn empty_mapping_table_falls_through() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(), 'm');",
            )
            .unwrap();

            let rows = unmatched_egib_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags, None);
        }

        /// A `rodzaj_kod` of NULL (rodzaj unresolved or absent) must not
        /// crash the LATERAL join's equality test and must fall through.
        #[test]
        fn null_rodzaj_kod_falls_through_without_erroring() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(), NULL);",
            )
            .unwrap();
            seed_mapping(&conn);

            let rows = unmatched_egib_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags, None);
        }

        /// `kondygnacje_nadziemne` is read through unchanged (already
        /// named above-ground-only in the source), independent of whether a
        /// mapping row matched.
        #[test]
        fn levels_is_read_through_regardless_of_mapping_resolution() {
            let conn = setup_db();
            seed_live_table(&conn, "");
            conn.execute_batch(
                "INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod, kondygnacje_nadziemne)
                 VALUES
                     ('u1', ST_MakeEnvelope(21.0060,52.2060,21.0062,52.2062), 8000, 4900, now(),
                      'x', 2);",
            )
            .unwrap();
            seed_mapping(&conn); // only has 'm' rows -- 'x' matches nothing

            let rows = unmatched_egib_buildings(&conn, &test_area()).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].tags, None, "no mapping row matches this letter");
            assert_eq!(
                rows[0].levels,
                Some(2),
                "the storey count must come through regardless"
            );
            let tags = with_building_levels(
                building_tags(rows[0].tags.as_deref(), SOURCE_BUILDING_EGIB),
                rows[0].levels,
            );
            assert_eq!(tags.get("building").unwrap(), "yes");
            assert_eq!(tags.get("building:levels").unwrap(), "2");
        }
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
        // A bbox rectangle is built by this code from range-checked floats,
        // never client-drawn -- serve_package skips check_request_geometry
        // for it on exactly this flag.
        assert!(!area.is_user_supplied);
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
        // Client-drawn geometry -- serve_package must run check_request_geometry
        // against it, unlike a bbox rectangle.
        assert!(area.is_user_supplied);
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
    fn building_tags_none_yields_plain_building_yes() {
        let tags = building_tags(None, SOURCE_BUILDING_BDOT10K);
        assert_eq!(tags.get("building").unwrap(), "yes");
        assert_eq!(tags.get("source:building").unwrap(), "BDOT");
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn building_tags_empty_string_yields_plain_building_yes() {
        let tags = building_tags(Some("  "), SOURCE_BUILDING_BDOT10K);
        assert_eq!(tags.get("building").unwrap(), "yes");
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn building_tags_resolved_string_overrides_the_default() {
        let tags = building_tags(Some("building=detached"), SOURCE_BUILDING_BDOT10K);
        assert_eq!(tags.get("building").unwrap(), "detached");
        assert_eq!(tags.get("source:building").unwrap(), "BDOT");
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn building_tags_resolved_multi_pair_round_trips() {
        let tags = building_tags(Some("building=silo;man_made=silo"), SOURCE_BUILDING_BDOT10K);
        assert_eq!(tags.get("building").unwrap(), "silo");
        assert_eq!(tags.get("man_made").unwrap(), "silo");
        assert_eq!(tags.get("source:building").unwrap(), "BDOT");
        assert_eq!(tags.len(), 3);
    }

    #[test]
    fn building_tags_uses_the_egib_source_label() {
        let tags = building_tags(None, SOURCE_BUILDING_EGIB);
        assert_eq!(tags.get("source:building").unwrap(), "EGiB");
    }

    #[test]
    fn with_building_levels_inserts_a_positive_count() {
        let tags = with_building_levels(building_tags(None, SOURCE_BUILDING_BDOT10K), Some(3));
        assert_eq!(tags.get("building:levels").unwrap(), "3");
        assert_eq!(tags.get("building").unwrap(), "yes");
    }

    #[test]
    fn with_building_levels_omits_zero() {
        // "budynek nie posiada kondygnacji" -- nothing to report, the same
        // way a missing value is nothing to report.
        let tags = with_building_levels(building_tags(None, SOURCE_BUILDING_BDOT10K), Some(0));
        assert!(!tags.contains_key("building:levels"));
    }

    #[test]
    fn with_building_levels_omits_none() {
        let tags = with_building_levels(building_tags(None, SOURCE_BUILDING_BDOT10K), None);
        assert!(!tags.contains_key("building:levels"));
    }

    #[test]
    fn with_building_levels_survives_an_unresolved_classification() {
        // No mapping row matched (tags is None -> building=yes), but the
        // storey count is still real data and must still be reported.
        let tags = with_building_levels(building_tags(None, SOURCE_BUILDING_BDOT10K), Some(2));
        assert_eq!(tags.get("building").unwrap(), "yes");
        assert_eq!(tags.get("building:levels").unwrap(), "2");
    }

    #[test]
    fn feature_collection_serializes_valid_geojson() {
        let f = feature(
            r#"{"type":"Point","coordinates":[21.0,52.2]}"#.to_string(),
            building_tags(None, SOURCE_BUILDING_BDOT10K),
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
        assert!(
            feature(
                "not json".to_string(),
                building_tags(None, SOURCE_BUILDING_BDOT10K)
            )
            .is_err()
        );
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
