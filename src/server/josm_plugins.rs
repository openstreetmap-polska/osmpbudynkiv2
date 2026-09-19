//! Compatibility endpoint for the JOSM `plbuildings` plugin's server
//! (praszuk/josm-plbuildings-server), which calls
//! `GET /josm_plugins/v2/nearest_building?lon=..&lat=..&search_distance=..`
//! exactly as the original gugik2osm did
//! (`app/resources/josm_plugins.py`, `NearestBuildingGeojson`).
//!
//! The response shape is the old one, byte-for-byte in structure, because the
//! consumer depends on it: a GeoJSON `FeatureCollection` whose features carry
//! their OSM tags *nested* under `properties.tags` (the client moves them up
//! itself -- `feature['properties'].update(feature['properties'].pop('tags'))`,
//! so a flat `properties` would raise `KeyError`), and `features` is always
//! an array, never `null` (the client iterates it unguarded; the old
//! Postgres `json_agg` returned `null` on no match, which only worked because
//! the old client checked differently).
//!
//! Two deliberate departures from `/package`:
//!
//! 1. **It reads the raw `<source>_buildings` table, not `<source>_unmatched`**,
//!    like the old query read `bdot_buildings_all`. The plugin's main use is
//!    replacing the geometry of a building *already in OSM* with the
//!    registry's, which the serving tables have by definition excluded. For
//!    the same reason no report veto and no `KATEGORIAISTNIENIA` filter is
//!    applied: the user pointed at a specific building and asked for it.
//! 2. **Distance is metric**, measured to the footprint's outline in a local
//!    projection centred on the requested point (see `meters_per_degree`) --
//!    a point inside a footprint is at distance 0. The consumer asks for 1 m,
//!    so a distance in raw degrees would be meaningless at that scale.
//!
//! Tags come from the same building-type resolution `/package` uses
//! (`package::bdot10k_building_tags_sql` / `egib_building_tags_sql`), so a
//! building is tagged identically whichever way it is exported.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::AppState;
use super::package::{
    ADJACENCY_READ_BUFFER_DEG, Dataset, SOURCE_BUILDING_BDOT10K, SOURCE_BUILDING_EGIB,
    bdot10k_building_tags_sql, building_tags, egib_building_tags_sql, error_response, log_export,
    with_building_levels,
};

/// The old endpoint's default when `search_distance` is absent or empty.
const DEFAULT_SEARCH_DISTANCE_METERS: f64 = 30.0;

/// Upper bound of the search, in degrees on either axis, whatever
/// `search_distance` asks for -- the old query's `ST_DWithin(..., 0.005)`
/// cap, kept so an absurd distance cannot turn one request into a scan of a
/// whole city.
const MAX_SEARCH_HALF_WIDTH_DEG: f64 = 0.005;

/// Slack between the degree prefilter envelope and the metric test, so a
/// building sitting exactly on the radius is never cut by the prefilter.
const ENVELOPE_SLACK: f64 = 1.01;

/// Metres per degree of longitude and of latitude at `lat_deg` on the WGS84
/// ellipsoid -- the standard truncated series (accurate to ~1 cm per degree).
///
/// Scaling a geometry by these, with the requested point translated to the
/// origin, is a local equirectangular projection: planar `ST_Distance` in it
/// is metric. Over the few hundred metres this endpoint ever searches, that
/// beats both alternatives, measured against the geodesic distance to the
/// true closest point over 32,672 building/point pairs within 30 m, spread
/// over Poland:
///
/// - **`ST_Transform` to EPSG:2180** was 2.5x *less* accurate (max error
///   23 mm vs 9 mm -- CS92's own scale distortion) and ~10x slower on a
///   typical request (10 ms vs 1 ms), almost all of it per-query PROJ setup.
/// - **`ST_Distance_Sphere(ST_ClosestPoint(geom, p), p)`**, the obvious
///   no-PROJ answer (`ST_Distance_Sphere` itself takes only points), picks
///   the closest point in raw degrees, where a degree of longitude is ~0.61
///   of a degree of latitude: errors up to 8.7 m, p99 2.2 m, and a different
///   nearest building for 1.2% of points. This one: none of 11,905.
fn meters_per_degree(lat_deg: f64) -> (f64, f64) {
    let phi = lat_deg.to_radians();
    let per_lon = 111_412.84 * phi.cos() - 93.5 * (3.0 * phi).cos();
    let per_lat = 111_132.954 - 559.822 * (2.0 * phi).cos() + 1.175 * (4.0 * phi).cos();
    (per_lon, per_lat)
}

#[derive(Debug, Deserialize)]
pub struct NearestBuildingParams {
    pub lon: Option<String>,
    pub lat: Option<String>,
    pub search_distance: Option<String>,
    /// Not part of the old API, which served BDOT10k only; that stays the
    /// default so the existing consumer gets what it always got.
    pub source: Option<String>,
}

/// A validated request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NearestBuildingQuery {
    pub lon: f64,
    pub lat: f64,
    pub search_distance: f64,
    pub source: Dataset,
}

fn parse_coordinate(value: Option<&str>, name: &str, limit: f64) -> Result<f64, String> {
    let raw = value
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing required parameter '{name}'"))?;
    let v: f64 = raw
        .parse()
        .map_err(|_| format!("parameter '{name}' must be a number"))?;
    if !v.is_finite() || v.abs() > limit {
        return Err(format!(
            "parameter '{name}' must be within [-{limit}, {limit}]"
        ));
    }
    Ok(v)
}

pub fn parse_params(params: &NearestBuildingParams) -> Result<NearestBuildingQuery, String> {
    let lon = parse_coordinate(params.lon.as_deref(), "lon", 180.0)?;
    let lat = parse_coordinate(params.lat.as_deref(), "lat", 90.0)?;
    let search_distance = match params
        .search_distance
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        None => DEFAULT_SEARCH_DISTANCE_METERS,
        Some(raw) => {
            let v: f64 = raw
                .parse()
                .map_err(|_| "parameter 'search_distance' must be a number".to_string())?;
            if !v.is_finite() || v <= 0.0 {
                return Err("parameter 'search_distance' must be a positive number".to_string());
            }
            v
        }
    };
    let source = match params
        .source
        .as_deref()
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        None | Some("") | Some("bdot10k") | Some("bdot") => Dataset::Bdot10k,
        Some("egib") => Dataset::Egib,
        Some(other) => {
            return Err(format!(
                "unknown source '{other}' (expected 'bdot10k' or 'egib')"
            ));
        }
    };
    Ok(NearestBuildingQuery {
        lon,
        lat,
        search_distance,
        source,
    })
}

/// Half-widths `(dlon, dlat)` in degrees of the RTREE prefilter envelope
/// around the requested point: `search_distance` metres in the same local
/// projection the distance is measured in, capped at
/// `MAX_SEARCH_HALF_WIDTH_DEG`.
fn search_half_widths(q: &NearestBuildingQuery) -> (f64, f64) {
    let (per_lon, per_lat) = meters_per_degree(q.lat);
    let dlat = q.search_distance * ENVELOPE_SLACK / per_lat;
    // per_lon reaches 0 at the poles; the division then yields inf (or a
    // negative a hair short of them), which the cap and `abs` absorb.
    let dlon = (q.search_distance * ENVELOPE_SLACK / per_lon).abs();
    (
        dlon.min(MAX_SEARCH_HALF_WIDTH_DEG),
        dlat.min(MAX_SEARCH_HALF_WIDTH_DEG),
    )
}

/// The full SQL for one request. Every interpolated value is a validated
/// finite `f64` (never request text), formatted in so the envelope is a
/// constant predicate the RTREE can use -- the same pattern as `/package`.
///
/// The `pkg` CTE picks the single nearest building and hands it to the shared
/// tag resolution. Its adjacency read (`nb`) is the search envelope widened
/// by `ADJACENCY_READ_BUFFER_DEG`, so it reaches ~35 m past the search
/// radius: enough for any building whose tags depend on a neighbour count
/// (small residential ones -- see `package::ADJACENCY_READ_BUFFER_DEG`), the
/// same soundness argument `/package` makes at its own request edge.
pub(crate) fn nearest_building_sql(q: &NearestBuildingQuery) -> String {
    let (lon, lat, d) = (q.lon, q.lat, q.search_distance);
    let (dlon, dlat) = search_half_widths(q);
    let (x1, y1, x2, y2) = (lon - dlon, lat - dlat, lon + dlon, lat + dlat);
    let nb_envelope = (
        x1 - ADJACENCY_READ_BUFFER_DEG,
        y1 - ADJACENCY_READ_BUFFER_DEG,
        x2 + ADJACENCY_READ_BUFFER_DEG,
        y2 + ADJACENCY_READ_BUFFER_DEG,
    );
    // The requested point is moved to the origin, so the scaled coordinates
    // stay small and `f64` loses nothing to the translation.
    let (kx, ky) = meters_per_degree(lat);
    let (ox, oy) = (-kx * lon, -ky * lat);
    let dist =
        format!("ST_Distance(ST_Affine(b.geom, {kx}, 0, 0, {ky}, {ox}, {oy}), ST_Point(0, 0))");
    // `near` must stay MATERIALIZED: the shared tag resolution LEFT JOINs
    // downstream of `pkg`, and without it DuckDB re-plans the envelope read
    // into a full sequential scan of the national table (1.3 s vs 18 ms,
    // measured) -- see `tests::the_nearest_building_search_uses_the_rtree_index`.
    // Ties (a point inside two overlapping footprints, both at distance 0)
    // are broken on the record key, so the same request always answers with
    // the same building.
    match q.source {
        Dataset::Bdot10k => bdot10k_building_tags_sql(
            &format!(
                "WITH near AS MATERIALIZED (
                     SELECT b.rowid AS rid, b.geom,
                            ST_X(b.centroid) AS cx, ST_Y(b.centroid) AS cy,
                            b.PRZEWAZAJACAFUNKCJABUDYNKU AS funkcja_szczegolowa,
                            b.FUNKCJAOGOLNABUDYNKU AS funkcja_ogolna,
                            b.LICZBAKONDYGNACJI AS liczba_kondygnacji,
                            {dist} AS dist, b.PRZESTRZENNAZW AS k1, b.LOKALNYID AS k2
                     FROM bdot10k_buildings b
                     WHERE ST_Intersects(b.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
                 )
                 SELECT rid, geom, cx, cy, funkcja_szczegolowa, funkcja_ogolna, liczba_kondygnacji
                 FROM near
                 WHERE dist < {d}
                 ORDER BY dist, k1, k2
                 LIMIT 1"
            ),
            nb_envelope,
        ),
        Dataset::Egib => egib_building_tags_sql(
            &format!(
                "WITH near AS MATERIALIZED (
                     SELECT b.rowid AS rid, b.geom,
                            ST_X(b.centroid) AS cx, ST_Y(b.centroid) AS cy,
                            b.rodzaj_kod, b.kondygnacje_nadziemne,
                            {dist} AS dist, b.id_budynku AS k1
                     FROM egib_buildings b
                     WHERE ST_Intersects(b.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
                 )
                 SELECT rid, geom, cx, cy, rodzaj_kod, kondygnacje_nadziemne
                 FROM near
                 WHERE dist < {d}
                 ORDER BY dist, k1
                 LIMIT 1"
            ),
            nb_envelope,
        ),
        Dataset::Prg => unreachable!("parse_params never selects PRG"),
    }
}

/// The nearest building as `(GeoJSON geometry, OSM tags)`, or `None` when
/// nothing lies within the search distance.
pub fn nearest_building(
    conn: &Connection,
    q: &NearestBuildingQuery,
) -> Result<Option<(String, BTreeMap<String, String>)>> {
    let sql = nearest_building_sql(q);
    let source_label = match q.source {
        Dataset::Egib => SOURCE_BUILDING_EGIB,
        _ => SOURCE_BUILDING_BDOT10K,
    };
    let mut stmt = conn
        .prepare(&sql)
        .context("Failed to prepare nearest building query")?;
    let mut rows = stmt
        .query([])
        .context("Failed to run nearest building query")?;
    let Some(row) = rows.next().context("Failed to read nearest building row")? else {
        return Ok(None);
    };
    let geometry: String = row.get(0)?;
    let resolved: Option<String> = row.get(1)?;
    let levels: Option<i32> = row.get(2)?;
    let tags = with_building_levels(building_tags(resolved.as_deref(), source_label), levels);
    Ok(Some((geometry, tags)))
}

#[derive(Serialize)]
struct TagsProperties {
    tags: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct Feature {
    #[serde(rename = "type")]
    kind: &'static str,
    geometry: Box<RawValue>,
    properties: TagsProperties,
}

#[derive(Serialize)]
struct FeatureCollection {
    #[serde(rename = "type")]
    kind: &'static str,
    features: Vec<Feature>,
}

fn build_response(state: &AppState, q: &NearestBuildingQuery) -> Result<String> {
    let conn = state
        .pool
        .get()
        .context("Failed to acquire pool connection")?;
    let found = nearest_building(&conn, q)?;
    drop(conn);
    let mut features = Vec::new();
    if let Some((geometry, tags)) = found {
        // Logged like a one-building `/package` export, as the old endpoint
        // registered one -- this is a building handed out for import, and
        // `/updates` should show it. The logged area is the building itself.
        log_export(state, &geometry, &[q.source], 0, 1);
        features.push(Feature {
            kind: "Feature",
            geometry: RawValue::from_string(geometry)?,
            properties: TagsProperties { tags },
        });
    }
    Ok(serde_json::to_string(&FeatureCollection {
        kind: "FeatureCollection",
        features,
    })?)
}

/// `GET /josm_plugins/v2/nearest_building`. Left at the router's default
/// `Cache-Control: no-store`, for `/package`'s reason: a cached answer never
/// reaches `log_export`.
pub async fn get_nearest_building(
    State(state): State<AppState>,
    Query(params): Query<NearestBuildingParams>,
) -> Response {
    let q = match parse_params(&params) {
        Ok(q) => q,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    match tokio::task::spawn_blocking(move || build_response(&state, &q)).await {
        Ok(Ok(body)) => {
            let mut resp = body.into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/geo+json"),
            );
            resp
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "nearest building query failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
        Err(e) => {
            tracing::error!(error = %e, "nearest building task panicked");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}
#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;

    /// Raw source tables as `import bdot10k` / `import egib` leave them
    /// (only the columns this endpoint reads, with the loaders' types --
    /// `LICZBAKONDYGNACJI` really is a TINYINT), plus the mapping tables and
    /// the export log. b1 is a 20 m-ish square at 21.0060..21.0062 E; b2 a
    /// second one just east of it. At 52.2 N, 0.00002 deg of longitude is
    /// ~1.37 m.
    const SEED: &str = "
        CREATE TABLE bdot10k_buildings (
            PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
            PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR,
            LICZBAKONDYGNACJI TINYINT);
        CREATE TABLE bdot10k_building_types (
            tier INTEGER, key VARCHAR, min_levels INTEGER, max_levels INTEGER,
            max_neighbours INTEGER, tags VARCHAR);
        CREATE TABLE egib_buildings (
            id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR,
            kondygnacje_nadziemne INTEGER);
        CREATE TABLE egib_building_types (
            tier INTEGER, key VARCHAR, min_levels INTEGER, max_levels INTEGER,
            max_neighbours INTEGER, tags VARCHAR);
        CREATE TABLE package_exports (
            exported_at TIMESTAMP WITH TIME ZONE, area GEOMETRY('epsg:4326'),
            datasets VARCHAR[], address_count INTEGER, building_count INTEGER);

        INSERT INTO bdot10k_buildings
            SELECT 'PL.PZGiK.994.BDOT10k', id, g, ST_Centroid(g), f, NULL, lvl
            FROM (VALUES
                ('b1', ST_MakeEnvelope(21.0060, 52.2060, 21.0062, 52.2062), 'budynek biurowy', 3),
                ('b2', ST_MakeEnvelope(21.0063, 52.2060, 21.0065, 52.2062), 'budynek biurowy', NULL)
            ) t(id, g, f, lvl);
        INSERT INTO bdot10k_building_types VALUES
            (1, 'budynek biurowy', NULL, NULL, NULL, 'building=office');
        INSERT INTO egib_buildings
            SELECT 'e1', g, ST_Centroid(g), 'm', 2
            FROM (SELECT ST_MakeEnvelope(21.0060, 52.2060, 21.0062, 52.2062) AS g);
    ";

    fn seeded_state() -> AppState {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial; SET GLOBAL geometry_always_xy = true;")
            .unwrap();
        conn.execute_batch(SEED).unwrap();
        AppState::for_tests(crate::server::build_pool(conn, 2).unwrap())
    }

    async fn get(state: AppState, uri: &str) -> (StatusCode, String, serde_json::Value) {
        let response = crate::server::build_router(state)
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let content_type = response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .to_string();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        (
            status,
            content_type,
            serde_json::from_slice(&bytes).unwrap(),
        )
    }

    fn params(
        lon: &str,
        lat: &str,
        d: Option<&str>,
        source: Option<&str>,
    ) -> NearestBuildingParams {
        NearestBuildingParams {
            lon: Some(lon.to_string()),
            lat: Some(lat.to_string()),
            search_distance: d.map(str::to_string),
            source: source.map(str::to_string),
        }
    }

    fn export_rows(state: &AppState) -> Vec<(Vec<String>, i32, i32)> {
        let conn = state.pool.get().unwrap();
        let mut stmt = conn
            .prepare("SELECT datasets, address_count, building_count FROM package_exports")
            .unwrap();
        stmt.query_map([], |r| {
            let datasets: duckdb::types::Value = r.get(0)?;
            let datasets = match datasets {
                duckdb::types::Value::List(v) => v
                    .into_iter()
                    .map(|x| match x {
                        duckdb::types::Value::Text(s) => s,
                        other => panic!("unexpected {other:?}"),
                    })
                    .collect(),
                other => panic!("unexpected {other:?}"),
            };
            Ok((datasets, r.get(1)?, r.get(2)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    /// The exact shape josm-plbuildings-server consumes: tags nested under
    /// `properties.tags` and nothing else in `properties` (it pops `tags`
    /// and merges it upward, so a sibling key would leak into OSM tags).
    #[tokio::test]
    async fn a_point_inside_a_building_returns_it_with_tags_nested_under_properties() {
        let state = seeded_state();
        let (status, content_type, json) = get(
            state.clone(),
            "/josm_plugins/v2/nearest_building?lon=21.0061&lat=52.2061&search_distance=1",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type, "application/geo+json");
        assert_eq!(json["type"], "FeatureCollection");
        let features = json["features"].as_array().unwrap();
        assert_eq!(features.len(), 1);
        let f = &features[0];
        assert_eq!(f["type"], "Feature");
        assert_eq!(f["geometry"]["type"], "Polygon");
        let properties = f["properties"].as_object().unwrap();
        assert_eq!(properties.keys().collect::<Vec<_>>(), vec!["tags"]);
        assert_eq!(
            properties["tags"],
            serde_json::json!({
                "building": "office",
                "building:levels": "3",
                "source:building": "BDOT",
            })
        );
        assert_eq!(
            export_rows(&state),
            vec![(vec!["bdot10k".to_string()], 0, 1)],
            "a served building is logged as a one-building export"
        );
    }

    /// No match is an empty *array*, not `null` -- the consumer iterates
    /// `features` unguarded -- and is not logged as an export.
    #[tokio::test]
    async fn nothing_within_the_distance_is_an_empty_feature_array() {
        let state = seeded_state();
        // ~1.37 m east of b1's edge, ~7 m west of b2's.
        let uri = "/josm_plugins/v2/nearest_building?lon=21.00622&lat=52.2061&search_distance=1";
        let (status, _, json) = get(state.clone(), uri).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["features"], serde_json::json!([]));
        assert!(export_rows(&state).is_empty());
    }

    /// Distance is metric and measured to the footprint's edge: the same
    /// point is out of reach at 1 m and in reach at 2 m, and the nearer of
    /// two buildings wins.
    #[tokio::test]
    async fn the_search_distance_is_in_metres_and_the_nearest_building_wins() {
        let conn = seeded_state().pool.get().unwrap();
        let (lon, lat) = ("21.00622", "52.2061");
        let q = parse_params(&params(lon, lat, Some("1"), None)).unwrap();
        assert!(nearest_building(&conn, &q).unwrap().is_none());

        let q = parse_params(&params(lon, lat, Some("2"), None)).unwrap();
        let (_, tags) = nearest_building(&conn, &q).unwrap().unwrap();
        assert_eq!(tags["building:levels"], "3", "b1 (3 storeys), not b2");

        // Closer to b2's west edge (21.0063) than to b1's east edge.
        let q = parse_params(&params("21.00628", lat, Some("30"), None)).unwrap();
        let (_, tags) = nearest_building(&conn, &q).unwrap().unwrap();
        assert!(
            !tags.contains_key("building:levels"),
            "b2 has no storey count"
        );
    }

    #[tokio::test]
    async fn source_egib_reads_the_egib_table_and_labels_it() {
        let state = seeded_state();
        let (status, _, json) = get(
            state.clone(),
            "/josm_plugins/v2/nearest_building?lon=21.0061&lat=52.2061&search_distance=1&source=egib",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let tags = &json["features"][0]["properties"]["tags"];
        assert_eq!(tags["source:building"], "EGiB");
        assert_eq!(tags["building"], "yes", "no egib mapping row is seeded");
        assert_eq!(tags["building:levels"], "2");
        assert_eq!(export_rows(&state), vec![(vec!["egib".to_string()], 0, 1)]);
    }

    #[tokio::test]
    async fn bad_parameters_are_a_400() {
        for uri in [
            "/josm_plugins/v2/nearest_building?lon=21.0",
            "/josm_plugins/v2/nearest_building?lon=abc&lat=52.2",
            "/josm_plugins/v2/nearest_building?lon=21.0&lat=95",
            "/josm_plugins/v2/nearest_building?lon=21.0&lat=52.2&search_distance=-1",
            "/josm_plugins/v2/nearest_building?lon=21.0&lat=52.2&source=prg",
        ] {
            let (status, _, json) = get(seeded_state(), uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            assert!(json["error"].is_string(), "{uri}");
        }
    }

    #[test]
    fn parse_params_defaults_match_the_old_endpoint() {
        let q = parse_params(&params("21.0", "52.2", None, None)).unwrap();
        assert_eq!(q.search_distance, DEFAULT_SEARCH_DISTANCE_METERS);
        assert_eq!(q.source, Dataset::Bdot10k);
        // The old endpoint treated an empty value as absent.
        let q = parse_params(&params("21.0", "52.2", Some(""), Some(""))).unwrap();
        assert_eq!(q.search_distance, DEFAULT_SEARCH_DISTANCE_METERS);
        assert_eq!(q.source, Dataset::Bdot10k);
    }

    /// The degree envelope is only a prefilter; it must never be narrower
    /// than the metric radius, or a building inside the radius is missed.
    #[test]
    fn the_prefilter_envelope_covers_the_radius_and_is_capped() {
        for lat in [49.0, 52.2, 54.9] {
            let q = parse_params(&params("19.0", &lat.to_string(), Some("30"), None)).unwrap();
            let (dlon, dlat) = search_half_widths(&q);
            let (per_lon, per_lat) = meters_per_degree(lat);
            assert!(dlat * per_lat > 30.0);
            assert!(dlon * per_lon > 30.0);
        }
        let q = parse_params(&params("19.0", "52.2", Some("100000"), None)).unwrap();
        assert_eq!(
            search_half_widths(&q),
            (MAX_SEARCH_HALF_WIDTH_DEG, MAX_SEARCH_HALF_WIDTH_DEG)
        );
        // At a pole, a degree of longitude has no length: capped, not inf.
        let q = parse_params(&params("19.0", "90", Some("30"), None)).unwrap();
        assert_eq!(search_half_widths(&q).0, MAX_SEARCH_HALF_WIDTH_DEG);
    }

    /// Pins the series to known values (degree lengths at 0 and 52 N), so a
    /// typo in a coefficient cannot pass as a subtly-wrong distance.
    #[test]
    fn meters_per_degree_matches_the_ellipsoid() {
        let (per_lon, per_lat) = meters_per_degree(0.0);
        assert!((per_lon - 111_319.5).abs() < 1.0, "{per_lon}");
        assert!((per_lat - 110_574.3).abs() < 1.0, "{per_lat}");
        let (per_lon, per_lat) = meters_per_degree(52.0);
        assert!((per_lon - 68_678.0).abs() < 5.0, "{per_lon}");
        assert!((per_lat - 111_267.0).abs() < 5.0, "{per_lat}");
    }

    /// Without the `MATERIALIZED` candidate CTE, the downstream joins of the
    /// shared tag resolution re-plan the source read into a full sequential
    /// scan (measured on the national BDOT10k table: 1.3 s vs 18 ms per
    /// request). Only `EXPLAIN` can see it -- every functional test passes
    /// either way. `RTREE_IN`, not the full name: wide plans truncate labels.
    #[test]
    fn the_nearest_building_search_uses_the_rtree_index() {
        let state = seeded_state();
        let conn = state.pool.get().unwrap();
        conn.execute_batch(
            "CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);
             CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);",
        )
        .unwrap();
        for source in [Dataset::Bdot10k, Dataset::Egib] {
            let q = NearestBuildingQuery {
                lon: 21.0061,
                lat: 52.2061,
                search_distance: 1.0,
                source,
            };
            let plan: String = conn
                .prepare(&format!("EXPLAIN {}", nearest_building_sql(&q)))
                .unwrap()
                .query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            let table = match source {
                Dataset::Egib => "egib_buildings",
                _ => "bdot10k_buildings",
            };
            // Two source reads -- the candidates and the adjacency
            // neighbours -- and both must be index scans. (The mapping
            // table's own sequential scan is expected; it is tiny.)
            assert_eq!(
                plan.matches("RTREE_IN").count(),
                2,
                "{table}: a source read lost its RTREE index:\n{plan}"
            );
        }
    }
}
