//! GET /updates: recent /package export activity as a GeoJSON FeatureCollection,
//! browser-cacheable for 60 seconds.
//!
//! See docs/superpowers/specs/2026-07-20-export-log-updates-endpoint-design.md
//! and docs/duckdb_connection_visibility_investigation.md. Reads run via
//! state.write (not state.read_pool) -- read_pool never observes writes made
//! through write while the server is running.

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::AppState;

#[derive(Serialize)]
pub struct UpdateProperties {
    pub exported_at: String,
    pub datasets: Box<RawValue>,
    pub address_count: i32,
    pub building_count: i32,
}

#[derive(Serialize)]
pub struct UpdateFeature {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub geometry: Box<RawValue>,
    pub properties: UpdateProperties,
}

#[derive(Serialize)]
pub struct UpdatesFeatureCollection {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub features: Vec<UpdateFeature>,
}

/// Recent package_exports rows within the last `minutes`, most recent first.
/// `minutes` is a validated positive integer (see `parse_minutes`), safe to
/// inline into the SQL text.
///
/// Runs on whatever connection it's given -- callers MUST pass a connection
/// derived from `state.write`, not `state.read_pool` (see the module doc
/// comment and docs/duckdb_connection_visibility_investigation.md).
pub fn recent_exports(conn: &Connection, minutes: u64) -> Result<Vec<UpdateFeature>> {
    let sql = format!(
        "SELECT ST_AsGeoJSON(area),
                strftime(exported_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'),
                to_json(datasets), address_count, building_count
         FROM package_exports
         WHERE exported_at >= (now() - INTERVAL '{minutes} minutes')
         ORDER BY exported_at DESC"
    );
    let mut stmt = conn
        .prepare(&sql)
        .context("Failed to prepare /updates query")?;
    let rows = stmt
        .query_map([], |row| {
            let geometry_geojson: String = row.get(0)?;
            let exported_at: String = row.get(1)?;
            let datasets_json: String = row.get(2)?;
            let address_count: i32 = row.get(3)?;
            let building_count: i32 = row.get(4)?;
            Ok((
                geometry_geojson,
                exported_at,
                datasets_json,
                address_count,
                building_count,
            ))
        })
        .context("Failed to run /updates query")?;

    let mut out = Vec::new();
    for row in rows {
        let (geometry_geojson, exported_at, datasets_json, address_count, building_count) =
            row.context("Failed to read /updates row")?;
        out.push(UpdateFeature {
            kind: "Feature",
            geometry: RawValue::from_string(geometry_geojson)?,
            properties: UpdateProperties {
                exported_at,
                datasets: RawValue::from_string(datasets_json)?,
                address_count,
                building_count,
            },
        });
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
pub struct UpdatesParams {
    pub minutes: Option<String>,
}

pub async fn get_updates(
    State(state): State<AppState>,
    Query(params): Query<UpdatesParams>,
) -> Response {
    let minutes = match parse_minutes(
        params.minutes.as_deref(),
        state.config.updates.default_minutes,
        state.config.updates.max_minutes,
    ) {
        Ok(m) => m,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };

    let result = tokio::task::spawn_blocking(move || build_updates(&state, minutes)).await;
    match result {
        Ok(Ok(body)) => {
            let mut resp = body.into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/geo+json"),
            );
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=60"),
            );
            resp
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "updates query failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
        Err(e) => {
            tracing::error!(error = %e, "updates task panicked");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

fn build_updates(state: &AppState, minutes: u64) -> anyhow::Result<String> {
    let features = {
        let conn = state
            .write
            .lock()
            .map_err(|_| anyhow::anyhow!("write mutex poisoned"))?;
        recent_exports(&conn, minutes)?
    };
    let collection = UpdatesFeatureCollection {
        kind: "FeatureCollection",
        features,
    };
    Ok(serde_json::to_string(&collection)?)
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

    use std::path::Path;

    use crate::db::init_db;

    fn setup_db() -> duckdb::Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    #[test]
    fn recent_exports_includes_within_window_excludes_outside() {
        let conn = setup_db();
        conn.execute_batch(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '5 minutes', ST_Point(21.0, 52.0), ['prg'], 3, 4);
             INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '120 minutes', ST_Point(22.0, 53.0), ['bdot10k'], 1, 1);",
        )
        .unwrap();

        let features = recent_exports(&conn, 60).unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(features[0].properties.address_count, 3);
        assert_eq!(features[0].properties.building_count, 4);
    }

    #[test]
    fn recent_exports_orders_most_recent_first() {
        let conn = setup_db();
        conn.execute_batch(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '50 minutes', ST_Point(21.0, 52.0), ['prg'], 1, 0);
             INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '5 minutes', ST_Point(21.0, 52.0), ['prg'], 2, 0);",
        )
        .unwrap();

        let features = recent_exports(&conn, 60).unwrap();
        assert_eq!(features.len(), 2);
        assert_eq!(features[0].properties.address_count, 2, "most recent first");
        assert_eq!(features[1].properties.address_count, 1);
    }

    #[test]
    fn recent_exports_feature_shape_is_correct() {
        let conn = setup_db();
        conn.execute(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now(), ST_Point(21.0, 52.0), ['prg', 'egib'], 7, 2)",
            [],
        )
        .unwrap();

        let features = recent_exports(&conn, 60).unwrap();
        assert_eq!(features.len(), 1);
        let f = &features[0];
        assert_eq!(f.kind, "Feature");
        let geom_json: serde_json::Value = serde_json::from_str(f.geometry.get()).unwrap();
        assert_eq!(geom_json["type"], "Point");
        assert_eq!(f.properties.address_count, 7);
        assert_eq!(f.properties.building_count, 2);
        let datasets: Vec<String> = serde_json::from_str(f.properties.datasets.get()).unwrap();
        assert_eq!(datasets, vec!["prg", "egib"]);
        // exported_at is a UTC ISO-8601 string ending in Z.
        assert!(f.properties.exported_at.ends_with('Z'));
        assert_eq!(f.properties.exported_at.len(), 20); // "YYYY-MM-DDTHH:MM:SSZ"
    }

    #[test]
    fn recent_exports_empty_window_returns_empty_vec() {
        let conn = setup_db();
        assert_eq!(recent_exports(&conn, 60).unwrap().len(), 0);
    }

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::super::AppState;

    fn make_state_with_write(conn: duckdb::Connection) -> (AppState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        // read_pool is unused by /updates but AppState requires it; a
        // throwaway file-backed read-only pool satisfies the type.
        let db_path = dir.path().join("updates_test.duckdb");
        let _ = duckdb::Connection::open(&db_path).unwrap();
        let read_cfg = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .unwrap();
        let manager = duckdb::DuckdbConnectionManager::file_with_flags(&db_path, read_cfg).unwrap();
        let read_pool = r2d2::Pool::builder().max_size(1).build(manager).unwrap();

        let state = AppState {
            write: std::sync::Arc::new(std::sync::Mutex::new(conn)),
            read_pool,
            registry: std::sync::Arc::new(crate::server::jobs::JobRegistry::new_for_tests(vec![])),
            config: std::sync::Arc::new(crate::config::Config::default()),
        };
        (state, dir)
    }

    fn updates_app(state: AppState) -> Router {
        Router::new()
            .route("/updates", axum::routing::get(get_updates))
            .with_state(state)
    }

    #[tokio::test]
    async fn get_updates_returns_recent_exports_with_cache_header() {
        let conn = setup_db();
        conn.execute(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now(), ST_Point(21.0, 52.0), ['prg'], 3, 1)",
            [],
        )
        .unwrap();
        let (state, _dir) = make_state_with_write(conn);

        let response = updates_app(state)
            .oneshot(
                Request::builder()
                    .uri("/updates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/geo+json");
        assert_eq!(response.headers()["cache-control"], "public, max-age=60");
        assert!(
            response.headers().get("content-disposition").is_none(),
            "/updates is not a download, unlike /package"
        );

        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["type"], "FeatureCollection");
        let features = json["features"].as_array().unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(features[0]["properties"]["address_count"], 3);
        assert_eq!(features[0]["properties"]["datasets"][0], "prg");
    }

    #[tokio::test]
    async fn get_updates_respects_minutes_param() {
        let conn = setup_db();
        conn.execute_batch(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '5 minutes', ST_Point(21.0, 52.0), ['prg'], 1, 0);
             INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '120 minutes', ST_Point(21.0, 52.0), ['prg'], 2, 0);",
        )
        .unwrap();
        let (state, _dir) = make_state_with_write(conn);
        let app = updates_app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/updates?minutes=200")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["features"].as_array().unwrap().len(), 2);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/updates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json["features"].as_array().unwrap().len(),
            1,
            "default 60 minutes excludes the 120-minute-old row"
        );
    }

    #[tokio::test]
    async fn get_updates_rejects_over_cap_minutes() {
        let (state, _dir) = make_state_with_write(setup_db());
        let response = updates_app(state)
            .oneshot(
                Request::builder()
                    .uri("/updates?minutes=999999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_updates_empty_window_returns_empty_collection() {
        let (state, _dir) = make_state_with_write(setup_db());
        let response = updates_app(state)
            .oneshot(
                Request::builder()
                    .uri("/updates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["features"].as_array().unwrap().len(), 0);
    }

    use crate::server::package::{get_package, post_package};

    /// Seeds a file-backed read_pool with the government + OSM tables
    /// `/package` needs (one unmatched PRG address, no buildings), and a
    /// write connection with `package_exports` plus the icu extension
    /// `/updates`' interval query requires -- exercising the real
    /// `/package` -> `package_exports` -> `/updates` path end to end.
    fn make_full_state() -> (AppState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("package_then_updates_test.duckdb");
        {
            let conn = duckdb::Connection::open(&db_path).unwrap();
            conn.execute_batch("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true;")
                .unwrap();
            conn.execute_batch(
                "CREATE TABLE prg_addresses (
                     lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                     miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                     geom GEOMETRY);
                 CREATE TABLE osm_addresses (
                     osm_id BIGINT, osm_type VARCHAR, housenumber VARCHAR, street VARCHAR,
                     city VARCHAR, postcode VARCHAR, geom GEOMETRY);
                 CREATE TABLE bdot10k_buildings (lokalnyid VARCHAR, geom GEOMETRY);
                 CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
                 CREATE TABLE osm_buildings (
                     osm_id BIGINT, osm_type VARCHAR, building VARCHAR, geom GEOMETRY);
                 INSERT INTO prg_addresses VALUES
                     ('a1', '12', 'Marszałkowska', 'Warszawa', '00-590', '0918123',
                      ST_Point(21.001, 52.201));",
            )
            .unwrap();
        }
        let read_cfg = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .unwrap();
        let manager = duckdb::DuckdbConnectionManager::file_with_flags(&db_path, read_cfg).unwrap();
        let read_pool = r2d2::Pool::builder().max_size(1).build(manager).unwrap();
        read_pool
            .get()
            .unwrap()
            .execute_batch("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true;")
            .unwrap();

        let write_conn = duckdb::Connection::open_in_memory().unwrap();
        write_conn
            .execute_batch(
                "INSTALL spatial; LOAD spatial; INSTALL icu; LOAD icu;
                 SET geometry_always_xy = true;",
            )
            .unwrap();
        write_conn
            .execute_batch(
                "CREATE TABLE package_exports (
                    exported_at TIMESTAMP WITH TIME ZONE,
                    area GEOMETRY('epsg:4326'),
                    datasets VARCHAR[],
                    address_count INTEGER,
                    building_count INTEGER
                )",
            )
            .unwrap();

        let state = AppState {
            write: std::sync::Arc::new(std::sync::Mutex::new(write_conn)),
            read_pool,
            registry: std::sync::Arc::new(crate::server::jobs::JobRegistry::new_for_tests(vec![])),
            config: std::sync::Arc::new(crate::config::Config::default()),
        };
        (state, dir)
    }

    fn combined_app(state: AppState) -> Router {
        Router::new()
            .route(
                "/package",
                axum::routing::get(get_package).post(post_package),
            )
            .route("/updates", axum::routing::get(get_updates))
            .with_state(state)
    }

    #[tokio::test]
    async fn package_export_appears_in_updates_with_correct_counts_and_geometry() {
        let (state, _dir) = make_full_state();
        let app = combined_app(state);

        let package_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(package_response.status(), axum::http::StatusCode::OK);

        let updates_response = app
            .oneshot(
                Request::builder()
                    .uri("/updates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(updates_response.status(), axum::http::StatusCode::OK);
        let bytes = to_bytes(updates_response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let features = json["features"].as_array().unwrap();
        assert_eq!(features.len(), 1);
        let props = &features[0]["properties"];
        assert_eq!(props["address_count"], 1);
        assert_eq!(props["building_count"], 0);
        assert_eq!(
            props["datasets"],
            serde_json::json!(["prg", "bdot10k", "egib"])
        );
        assert_eq!(features[0]["geometry"]["type"], "Polygon");
    }
}
