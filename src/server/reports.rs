//! `POST /report` — let a user say "stop proposing this object for import".
//!
//! Conventions here are `server::package`'s, deliberately and to the letter:
//! the handler returns a bare `Response` (there is no error enum in this
//! server), the body arrives as a `String` and is parsed by a pure validator
//! returning `Result<_, String>` so it is unit-testable with no DB and no axum,
//! a 400 renders that message through `error_response`, and a 500 logs the real
//! error and tells the client only `"internal error"`.
//!
//! The response is `no-store` without doing anything: `build_router`'s outermost
//! `if_not_present` layer stamps it, and this handler sets no `Cache-Control`.
//!
//! **What this module does not do is decide anything about matching.** It
//! validates a request, hands resolved targets to `crate::reports::insert`, and
//! reports back what was stored. Whether a stored report actually removes an
//! object from `<source>_unmatched` is `compare::rule::reported_sql`'s business,
//! and it takes effect when the `match_refresh` job next drains the cells the
//! insert enqueued.

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::dataset::spec_by_name;
use crate::reports::{self, Outcome, Reason, ReportTarget};
use crate::server::AppState;

/// One object in a request body. `key` is a map rather than a positional array
/// so the wire format is self-describing and order-independent: BDOT10k's key
/// is the composite `(PRZESTRZENNAZW, LOKALNYID)`, and a caller that got the
/// order wrong with an array would silently report the wrong building.
#[derive(Debug, Deserialize)]
struct ObjectBody {
    source: String,
    key: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ReportBody {
    reason: String,
    #[serde(default)]
    note: Option<String>,
    objects: Vec<ObjectBody>,
}

#[derive(Debug, Serialize)]
struct AcceptedItem {
    index: usize,
    source: String,
    report_id: i64,
}

#[derive(Debug, Serialize)]
struct RejectedItem {
    index: usize,
    source: String,
    error: &'static str,
}

#[derive(Debug, Serialize)]
struct ReportResponse {
    accepted: Vec<AcceptedItem>,
    rejected: Vec<RejectedItem>,
    cells_enqueued: i64,
}

/// A request that passed pure validation: every source is known and every key
/// has exactly the columns its source declares, in `key_columns` order.
#[derive(Debug)]
struct ValidRequest {
    targets: Vec<ReportTarget>,
    reason: Reason,
    note: Option<String>,
}

pub const UNKNOWN_RECORD_MESSAGE: &str = "no such record in the current dataset";

/// Pure request validation — no database, no axum, no I/O.
///
/// Everything checkable without touching the source tables is checked here;
/// the one thing that cannot be is whether a key actually names a live record,
/// which `reports::insert` establishes by resolving against the live table (and
/// which is a per-object rejection, not a request-level error).
fn parse_report_body(body: &str, max_objects: usize) -> Result<ValidRequest, String> {
    let parsed: ReportBody =
        serde_json::from_str(body).map_err(|e| format!("invalid JSON body: {e}"))?;

    let reason = Reason::parse(&parsed.reason).ok_or_else(|| {
        format!(
            "unknown reason '{}'; expected one of: {}",
            parsed.reason,
            Reason::vocabulary()
        )
    })?;

    let note = parsed
        .note
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty());
    if reason == Reason::Other && note.is_none() {
        return Err("reason 'other' requires a note".to_string());
    }

    if parsed.objects.is_empty() {
        return Err("objects must not be empty".to_string());
    }
    if parsed.objects.len() > max_objects {
        return Err(format!(
            "too many objects: {} (limit {max_objects})",
            parsed.objects.len()
        ));
    }

    let mut targets = Vec::with_capacity(parsed.objects.len());
    for (i, obj) in parsed.objects.into_iter().enumerate() {
        let spec = spec_by_name(&obj.source)
            .ok_or_else(|| format!("objects[{i}]: unknown source '{}'", obj.source))?;

        // Exact-match the declared key columns rather than merely looking each
        // one up: an extra key means the caller believes the identity includes
        // something it does not, which is worth failing loudly rather than
        // silently ignoring.
        if obj.key.len() != spec.key_columns.len() {
            return Err(format!(
                "objects[{i}]: source '{}' needs exactly the key columns [{}]",
                obj.source,
                spec.key_columns.join(", ")
            ));
        }
        let mut key = Vec::with_capacity(spec.key_columns.len());
        for column in spec.key_columns {
            let value = obj.key.get(*column).ok_or_else(|| {
                format!(
                    "objects[{i}]: source '{}' is missing key column '{column}'",
                    obj.source
                )
            })?;
            if value.trim().is_empty() {
                return Err(format!("objects[{i}]: key column '{column}' is empty"));
            }
            key.push(value.clone());
        }
        targets.push(ReportTarget { spec, key });
    }

    Ok(ValidRequest {
        targets,
        reason,
        note,
    })
}

pub async fn post_report(State(state): State<AppState>, body: String) -> Response {
    if !state.config.reports.enabled {
        // 404 rather than 403: with the feature off the endpoint does not
        // exist as far as a client is concerned, and there is no credential
        // that would make it work.
        return error_response(StatusCode::NOT_FOUND, "reporting is disabled");
    }

    let request = match parse_report_body(&body, state.config.reports.max_objects_per_request) {
        Ok(r) => r,
        Err(msg) => return error_response(StatusCode::BAD_REQUEST, &msg),
    };

    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<ReportResponse> {
        let conn = pool.get()?;
        let sources: Vec<String> = request
            .targets
            .iter()
            .map(|t| t.spec.name.to_string())
            .collect();
        let (outcomes, stats) = reports::insert(
            &conn,
            &request.targets,
            request.reason,
            request.note.as_deref(),
        )?;

        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        for (index, (outcome, source)) in outcomes.into_iter().zip(sources).enumerate() {
            match outcome {
                Outcome::Accepted { report_id } => accepted.push(AcceptedItem {
                    index,
                    source,
                    report_id,
                }),
                Outcome::UnknownRecord => rejected.push(RejectedItem {
                    index,
                    source,
                    error: UNKNOWN_RECORD_MESSAGE,
                }),
            }
        }
        Ok(ReportResponse {
            accepted,
            rejected,
            cells_enqueued: stats.cells_enqueued,
        })
    })
    .await;

    let response = match result {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to store reports");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
        Err(e) => {
            tracing::error!(error = %e, "report task panicked");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    let body = match serde_json::to_string(&response) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, "failed to serialize report response");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        body,
    )
        .into_response()
}

/// Byte-identical to `package::error_response` / `updates::error_response`.
/// Duplicated rather than shared for the same reason those two are: they are
/// four lines and the frontend depends on the `{"error": ...}` shape, which is
/// easier to see stated at each endpoint than to trace to a shared helper.
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
mod handler_tests {
    use super::*;
    use crate::server::AppState;
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    /// Built on `db::init_db` (and therefore on the real `create_schema`), not
    /// on a local DDL constant, so `object_reports` and `match_dirty_cells`
    /// come from the shipping schema. Only the tables their own importers
    /// create have to be declared by hand.
    fn state_with(reports_enabled: bool) -> AppState {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET GLOBAL geometry_always_xy = true".to_string(),
        ];
        let conn = crate::db::init_db(std::path::Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR,
                 geom GEOMETRY, centroid GEOMETRY, WERSJA VARCHAR,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR,
                 LICZBAKONDYGNACJI SMALLINT, KATEGORIAISTNIENIA VARCHAR,
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR,
                 KODKST TINYINT, ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             INSERT INTO bdot10k_buildings (PRZESTRZENNAZW, LOKALNYID, geom) VALUES
                 ('04','bud-1', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let pool = crate::server::build_pool(conn, 2).unwrap();
        let mut state = AppState::for_tests(pool);
        let mut config = crate::config::Config::default();
        config.reports.enabled = reports_enabled;
        config.reports.max_objects_per_request = 2;
        state.config = std::sync::Arc::new(config);
        state
    }

    async fn post(state: AppState, body: &str) -> (axum::http::StatusCode, serde_json::Value) {
        let response = Router::oneshot(
            crate::server::build_router(state),
            Request::builder()
                .method("POST")
                .uri("/report")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn accepts_a_live_key_rejects_a_stale_one_and_enqueues_only_the_live_cell() {
        let state = state_with(true);
        let (status, body) = post(
            state.clone(),
            r#"{"reason":"does_not_exist","objects":[
                 {"source":"bdot10k","key":{"PRZESTRZENNAZW":"04","LOKALNYID":"bud-1"}},
                 {"source":"bdot10k","key":{"PRZESTRZENNAZW":"04","LOKALNYID":"gone"}}]}"#,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["accepted"].as_array().unwrap().len(), 1);
        assert_eq!(body["accepted"][0]["index"], 0);
        assert_eq!(body["rejected"].as_array().unwrap().len(), 1);
        assert_eq!(body["rejected"][0]["index"], 1);
        assert_eq!(body["rejected"][0]["error"], UNKNOWN_RECORD_MESSAGE);
        assert_eq!(body["cells_enqueued"], 1);

        // One stale id in a batch must not discard the rest -- the live one is
        // stored and its cell queued for the drain.
        let conn = state.pool.get().unwrap();
        let stored: i64 = conn
            .query_row("SELECT COUNT(*) FROM object_reports", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stored, 1);
        let queued: i64 = conn
            .query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(queued, 1);
    }

    #[tokio::test]
    async fn a_rejected_request_stores_nothing() {
        for body in [
            r#"{"reason":"nope","objects":[{"source":"bdot10k","key":{"PRZESTRZENNAZW":"04","LOKALNYID":"bud-1"}}]}"#,
            r#"{"reason":"does_not_exist","objects":[{"source":"bdot10k","key":{"LOKALNYID":"bud-1"}}]}"#,
            r#"garbage"#,
        ] {
            let state = state_with(true);
            let (status, json) = post(state.clone(), body).await;
            assert_eq!(status, 400, "body: {body}");
            assert!(json["error"].is_string(), "must use the {{error}} shape");
            let conn = state.pool.get().unwrap();
            let stored: i64 = conn
                .query_row("SELECT COUNT(*) FROM object_reports", [], |r| r.get(0))
                .unwrap();
            assert_eq!(stored, 0, "body: {body}");
        }
    }

    /// The cap is config-driven so an operator can tighten it without a
    /// redeploy; this fixture sets it to 2.
    #[tokio::test]
    async fn over_the_configured_cap_is_a_400() {
        let one = r#"{"source":"bdot10k","key":{"PRZESTRZENNAZW":"04","LOKALNYID":"bud-1"}}"#;
        let body = format!(
            r#"{{"reason":"duplicate","objects":[{},{},{}]}}"#,
            one, one, one
        );
        let (status, json) = post(state_with(true), &body).await;
        assert_eq!(status, 400);
        assert!(
            json["error"].as_str().unwrap().contains("too many objects"),
            "got {json}"
        );
    }

    /// `reports.enabled = false` is the operator's kill switch, and it is the
    /// only thing that stops the endpoint outright -- there is no rate limit.
    #[tokio::test]
    async fn the_config_kill_switch_closes_the_endpoint() {
        let (status, _) = post(
            state_with(false),
            r#"{"reason":"does_not_exist","objects":[
                 {"source":"bdot10k","key":{"PRZESTRZENNAZW":"04","LOKALNYID":"bud-1"}}]}"#,
        )
        .await;
        assert_eq!(status, 404);
    }

    /// A cached report response would be meaningless, and the endpoint sets no
    /// Cache-Control of its own -- it relies on `build_router`'s outermost
    /// `if_not_present` layer, which only covers it because that layer is the
    /// last call in the chain.
    #[tokio::test]
    async fn responses_are_no_store() {
        let response = Router::oneshot(
            crate::server::build_router(state_with(true)),
            Request::builder()
                .method("POST")
                .uri("/report")
                .body(Body::from(
                    r#"{"reason":"does_not_exist","objects":[
                         {"source":"bdot10k","key":{"PRZESTRZENNAZW":"04","LOKALNYID":"bud-1"}}]}"#
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            response.headers().get(axum::http::header::CACHE_CONTROL),
            Some(&axum::http::HeaderValue::from_static("no-store"))
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 100;

    #[test]
    fn accepts_a_well_formed_composite_key() {
        let r = parse_report_body(
            r#"{"reason":"does_not_exist","objects":[
                 {"source":"bdot10k","key":{"PRZESTRZENNAZW":"04","LOKALNYID":"x"}}]}"#,
            MAX,
        )
        .unwrap();
        assert_eq!(r.reason, Reason::DoesNotExist);
        // Ordered by `key_columns`, not by the JSON object's own order.
        assert_eq!(r.targets[0].key, vec!["04".to_string(), "x".to_string()]);
    }

    /// The key is re-ordered into `key_columns` order server-side, so a client
    /// cannot mis-report a building by listing its key fields the other way
    /// round -- the reason `key` is a map and not an array.
    #[test]
    fn key_order_in_the_json_object_does_not_matter() {
        let a = parse_report_body(
            r#"{"reason":"duplicate","objects":[
                 {"source":"bdot10k","key":{"LOKALNYID":"x","PRZESTRZENNAZW":"04"}}]}"#,
            MAX,
        )
        .unwrap();
        assert_eq!(a.targets[0].key, vec!["04".to_string(), "x".to_string()]);
    }

    #[test]
    fn rejects_unknown_source_reason_and_oversized_batches() {
        let cases = [
            (
                r#"{"reason":"does_not_exist","objects":[{"source":"osm","key":{"id":"1"}}]}"#,
                "unknown source",
            ),
            (
                r#"{"reason":"vandalism","objects":[{"source":"prg","key":{"lokalny_id":"a"}}]}"#,
                "unknown reason",
            ),
            (
                r#"{"reason":"duplicate","objects":[]}"#,
                "must not be empty",
            ),
            (r#"{"reason":"duplicate"}"#, "invalid JSON body"),
            (r#"not json at all"#, "invalid JSON body"),
        ];
        for (body, expected) in cases {
            let err = parse_report_body(body, MAX).expect_err("must reject: {body}");
            assert!(err.contains(expected), "got '{err}' for body {body}");
        }

        let many = format!(
            r#"{{"reason":"duplicate","objects":[{}]}}"#,
            std::iter::repeat_n(r#"{"source":"prg","key":{"lokalny_id":"a"}}"#, 3)
                .collect::<Vec<_>>()
                .join(",")
        );
        let err = parse_report_body(&many, 2).expect_err("over the cap");
        assert!(err.contains("too many objects: 3 (limit 2)"), "got '{err}'");
    }

    /// A key with the wrong *arity* is the dangerous case: supplying only
    /// LOKALNYID for a BDOT10k building looks reasonable and is exactly what a
    /// client written against `bdot10k_unmatched`'s old schema would send, but
    /// LOKALNYID is not unique, so accepting it would veto an arbitrary one of
    /// the colliding buildings.
    #[test]
    fn rejects_a_partial_composite_key() {
        let err = parse_report_body(
            r#"{"reason":"does_not_exist","objects":[
                 {"source":"bdot10k","key":{"LOKALNYID":"x"}}]}"#,
            MAX,
        )
        .expect_err("a half key must not be accepted");
        assert!(err.contains("PRZESTRZENNAZW"), "got '{err}'");
    }

    #[test]
    fn rejects_a_key_with_the_right_arity_but_a_wrong_column_name() {
        let err = parse_report_body(
            r#"{"reason":"does_not_exist","objects":[
                 {"source":"bdot10k","key":{"LOKALNYID":"x","lokalnyid":"y"}}]}"#,
            MAX,
        )
        .expect_err("arity alone must not be enough");
        assert!(err.contains("missing key column"), "got '{err}'");
    }

    #[test]
    fn other_requires_a_note_and_whitespace_is_not_one() {
        for body in [
            r#"{"reason":"other","objects":[{"source":"prg","key":{"lokalny_id":"a"}}]}"#,
            r#"{"reason":"other","note":"   ","objects":[{"source":"prg","key":{"lokalny_id":"a"}}]}"#,
        ] {
            let err = parse_report_body(body, MAX).expect_err("must demand an explanation");
            assert!(err.contains("requires a note"), "got '{err}'");
        }
        let ok = parse_report_body(
            r#"{"reason":"other","note":" wrong shape ","objects":[
                 {"source":"prg","key":{"lokalny_id":"a"}}]}"#,
            MAX,
        )
        .unwrap();
        assert_eq!(ok.note.as_deref(), Some("wrong shape"), "note is trimmed");
    }

    #[test]
    fn rejects_an_empty_key_value() {
        let err = parse_report_body(
            r#"{"reason":"duplicate","objects":[{"source":"prg","key":{"lokalny_id":"  "}}]}"#,
            MAX,
        )
        .expect_err("an empty id is not an id");
        assert!(err.contains("is empty"), "got '{err}'");
    }
}
