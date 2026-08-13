//! The one home for every `Cache-Control` value this server ever sends, plus
//! (as of the z14 `ETag`/`304` phase) every `ETag` value it sends too.
//! Nothing outside this module should build a `Cache-Control` or `ETag`
//! `HeaderValue` by hand -- see `CacheHeaders`, built once per `build_router`
//! call from `[cache]` config rather than reformatted per request, and
//! [`weak_etag`]/[`if_none_match_matches`]/[`not_modified`] below.
//!
//! Per-endpoint policy:
//!
//! | endpoint | `Cache-Control` | `ETag` |
//! |---|---|---|
//! | `/health`, `/status` | `no-store` (API default, see `build_router`) | none |
//! | `/package` (GET + POST) | `no-store` -- see below, this one is load-bearing | none |
//! | `/tiles` z14 | `public, max-age=<tile_max_age_seconds>` | weak, from `serving_version::z14_tile_version` |
//! | `/tiles` z5..=z13 | `public, max-age=<agg_tile_max_age_seconds>` | none -- see `tiles::serve_tile`'s comment on why |
//! | `/tiles` out-of-range zoom (204) | `public, max-age=`[`OUT_OF_RANGE_ZOOM_MAX_AGE_SECONDS`] | none |
//! | `/tiles` 500 | none set here -> falls through to the API default `no-store` | none |
//! | `/updates` | `public, max-age=<updates_max_age_seconds>` | none |
//! | `web_dir/fonts/**` | `public, max-age=<font_max_age_seconds>, immutable` | none |
//! | `web_dir/vendor/**` | `public, max-age=<static_max_age_seconds>` | none |
//! | any other file `web_dir` actually served (`index.html`, `app.js`, `style.css`, favicons) | `no-cache` | none |
//! | a path under `web_dir` that resolved to nothing (404) | none set here -> `no-store` | none |
//!
//! `/package` is `no-store` for a **correctness** reason, not just
//! freshness: a cached response never reaches `package::log_export`, so
//! `package_exports` would under-count and `/updates` would silently
//! under-report real exports. Don't loosen it without accounting for that.
//!
//! `/tiles` 500s are deliberately left header-less rather than given an
//! explicit `no-store` here, so that the outer API-default layer in
//! `build_router` is what stamps them -- an errored tile cached for even a
//! minute would turn a transient DB hiccup into an outage that outlives the
//! hiccup itself. The same reasoning is why a 500 never carries an `ETag`
//! either -- see `tiles::finish_tile_response`.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use tower_http::services::ServeDir;

use crate::config::CacheConfig;

/// `Cache-Control: no-store` -- the API default. Applied as the outermost
/// layer in `build_router` via `SetResponseHeaderLayer::if_not_present`, so
/// it only fills in responses that didn't already set their own value (tiles,
/// updates, and the static-asset middleware below all set their own first).
pub const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");

/// `Cache-Control: no-cache` -- always revalidate (via `If-Modified-Since`
/// against `ServeDir`'s `Last-Modified`) rather than skip caching outright.
/// Used for the frontend's own HTML/JS/CSS, which changes on every deploy but
/// costs nothing to revalidate. See `server::mod`'s test
/// `static_html_js_css_revalidate_via_if_modified_since` for the empirical
/// confirmation that tower-http 0.6's `ServeDir` actually answers `304` to
/// `If-Modified-Since` -- if it didn't, this would silently cost a full
/// re-send of `app.js` on every page load and this constant would need to
/// become a short `max-age` instead.
pub const NO_CACHE: HeaderValue = HeaderValue::from_static("no-cache");

/// A tile request outside every served zoom range (z<5 or z>14) always 204s,
/// regardless of database content (see `tiles::serve_tile`'s dispatch) --
/// that's a property of the binary's zoom table, not of the data underneath
/// it, so unlike every other tile response it can be cached far longer than
/// `tile_max_age_seconds`/`agg_tile_max_age_seconds` without going stale.
pub const OUT_OF_RANGE_ZOOM_MAX_AGE_SECONDS: u64 = 86_400;

fn public_max_age(seconds: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("public, max-age={seconds}"))
        .expect("a formatted ASCII integer is always a valid header value")
}

fn public_max_age_immutable(seconds: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("public, max-age={seconds}, immutable"))
        .expect("a formatted ASCII integer is always a valid header value")
}

/// Precomputed `Cache-Control` values derived from `[cache]` config. Built
/// once per `build_router` call (see `AppState::cache_headers`) rather than
/// re-formatted on every request -- `HeaderValue`'s `Bytes` backing makes a
/// clone of one of these cheap.
#[derive(Debug, Clone)]
pub struct CacheHeaders {
    pub tile: HeaderValue,
    pub agg_tile: HeaderValue,
    pub out_of_range_zoom: HeaderValue,
    pub updates: HeaderValue,
    pub vendor: HeaderValue,
    pub font: HeaderValue,
}

impl CacheHeaders {
    pub fn from_config(cache: &CacheConfig) -> Self {
        Self {
            tile: public_max_age(cache.tile_max_age_seconds),
            agg_tile: public_max_age(cache.agg_tile_max_age_seconds),
            out_of_range_zoom: public_max_age(OUT_OF_RANGE_ZOOM_MAX_AGE_SECONDS),
            updates: public_max_age(cache.updates_max_age_seconds),
            vendor: public_max_age(cache.static_max_age_seconds),
            font: public_max_age_immutable(cache.font_max_age_seconds),
        }
    }
}

// --- z14 ETag / conditional 304 -----------------------------------------
//
// Only `tiles::serve_tile`'s z14 branch calls any of these -- see that
// module for why z5..=z13 deliberately don't get an ETag at all. Building
// the primitive here rather than in `tiles.rs` matches this module's own
// "one home for every header value" charter (see the module doc above).

/// A weak `ETag` for a z14 tile, built from `serving_version::z14_tile_version`'s
/// output: `W/"<version>"`.
///
/// Weak, not strong: two responses for the same tile are only guaranteed
/// *semantically* equivalent, never byte-identical -- `ST_AsMVT`'s feature
/// ordering within a layer is not something DuckDB promises to keep stable
/// across queries (no `ORDER BY` backs it), so re-running the exact same
/// query against unchanged data can legally reorder bytes. A strong `ETag`
/// asserts byte-for-byte identity (RFC 9110 SS8.8.1); this doesn't hold here,
/// so the weak indicator is required, not a style choice.
pub fn weak_etag(version: &str) -> HeaderValue {
    HeaderValue::from_str(&format!("W/\"{version}\""))
        .expect("a version string of ASCII digits, dots and dashes is always a valid header value")
}

/// Does the request's `If-None-Match` header already cover `etag` (our own
/// server-computed version, unquoted and un-prefixed -- the same string
/// passed to [`weak_etag`])?
///
/// RFC 9110 SS13.1.2 mandates *weak* comparison for `If-None-Match`
/// specifically (unlike `If-Match`, which requires strong comparison): two
/// entity-tags match if their opaque-tags are identical once any leading
/// `W/` weak indicator is stripped from *both* sides. That's why a client
/// holding a bare `"<version>"` (no `W/`, e.g. hand-typed or from a
/// different server generation) still matches our own `W/"<version>"` here.
///
/// Handles, in order: `*` (matches anything present, RFC 9110 SS13.1.2),
/// a comma-separated list of entity-tags with optional surrounding
/// whitespace around each member, and a malformed/non-UTF-8 header value
/// (`to_str()` fails on either) -- the last of which returns `false` rather
/// than erroring, so a client sending garbage here gets the full tile
/// instead of a spurious `304`. Never risk a false-positive match: worst
/// case here is an extra tile fetch, not a stale one.
pub fn if_none_match_matches(header_value: Option<&HeaderValue>, etag: &str) -> bool {
    let Some(header_value) = header_value else {
        return false;
    };
    let Ok(header_str) = header_value.to_str() else {
        return false;
    };
    let target = format!("\"{etag}\"");
    header_str.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == target
    })
}

/// A `304 Not Modified` for a request whose `If-None-Match` already covers
/// the representation we would otherwise have served.
///
/// RFC 9110 SS15.4.5: a `304` response must not carry a message body, and
/// need not repeat representation metadata like `Content-Type` -- the client
/// is expected to reuse those from its own stored (cached) representation,
/// so resending them here would at best be redundant and at worst
/// contradict what's actually cached. Only `ETag` and `Cache-Control` are
/// set here; this function itself never touches `Content-Type` or
/// `Content-Length`.
///
/// One caveat this function cannot control: axum 0.8's own `Router`
/// unconditionally stamps `Content-Length: 0` on any top-level response
/// whose body reports an exact zero size (`routing::route::set_content_length`,
/// verified against axum 0.8.9's source and against this app's unrelated
/// `/health` route, which gets the same "0") -- that happens downstream of
/// this function returning, is universal across every route in this binary,
/// and is informational rather than a spec violation (RFC 9110 SS15.4.5
/// forbids actual *content* on a 304, not this header). `tiles::serve_tile`'s
/// tests assert on that end-to-end reality, not on what this function alone
/// produces.
pub fn not_modified(etag: HeaderValue, cache_header: HeaderValue) -> Response {
    let mut resp = Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .body(Body::empty())
        .expect("a header-less 304 with an empty body is always a valid response");
    resp.headers_mut().insert(header::ETAG, etag);
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, cache_header);
    resp
}

/// The static-only sub-router `build_router` hangs off its
/// `.fallback_service(...)`. One `ServeDir` (not three nested ones, which
/// would duplicate `web/`'s directory layout in Rust) wrapped in one
/// `from_fn` middleware that classifies every request by path prefix --
/// see the module doc table above.
///
/// Only `/fonts/` and `/vendor/` get a long max-age; everything else that
/// `ServeDir` actually *served* -- the frontend's entry files, the favicons,
/// any stray file dropped into `web_dir` -- gets `no-cache`, i.e. revalidate
/// on every load rather than skip caching outright. The classification is on
/// the response status, not on a hardcoded filename list, so a new asset
/// added to `web_dir` gets sane behaviour without a code change, while a
/// path that resolved to nothing (`ServeDir`'s 404, or a 405/redirect) is
/// left header-less on purpose and inherits the API default (`no-store`)
/// from `build_router`'s outer `SetResponseHeaderLayer::if_not_present`.
pub fn static_router(web_dir: &str, headers: Arc<CacheHeaders>) -> Router {
    Router::new()
        .fallback_service(ServeDir::new(web_dir))
        .layer(axum::middleware::from_fn_with_state(
            headers,
            apply_static_cache_header,
        ))
}

async fn apply_static_cache_header(
    State(headers): State<Arc<CacheHeaders>>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_owned();
    let mut response = next.run(req).await;
    if let Some(value) = classify_static_response(&headers, &path, response.status()) {
        response.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    response
}

/// `status` is what separates "a file `ServeDir` served" from "a path that
/// resolved to nothing": only a 2xx or the `304` `ServeDir` answers to
/// `If-Modified-Since` gets `no-cache`, so a 404 stays header-less and picks
/// up the API-default `no-store` instead. The two long-lived prefixes are
/// matched on the path as received, not percent-decoded -- fine here because
/// only the literal `/fonts/`/`/vendor/` prefix is examined, never a full
/// path, so an encoded segment further along (the space in `Klokantech Noto
/// Sans Regular`) never enters the comparison.
fn classify_static_response(
    headers: &CacheHeaders,
    path: &str,
    status: StatusCode,
) -> Option<HeaderValue> {
    if !(status.is_success() || status == StatusCode::NOT_MODIFIED) {
        return None;
    }
    if path.starts_with("/fonts/") {
        Some(headers.font.clone())
    } else if path.starts_with("/vendor/") {
        Some(headers.vendor.clone())
    } else {
        Some(NO_CACHE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_formats_expected_header_values() {
        let cfg = CacheConfig {
            tile_max_age_seconds: 60,
            agg_tile_max_age_seconds: 300,
            updates_max_age_seconds: 60,
            static_max_age_seconds: 604_800,
            font_max_age_seconds: 31_536_000,
            tile_cache_max_bytes: 268_435_456,
        };
        let headers = CacheHeaders::from_config(&cfg);
        assert_eq!(headers.tile, "public, max-age=60");
        assert_eq!(headers.agg_tile, "public, max-age=300");
        assert_eq!(headers.out_of_range_zoom, "public, max-age=86400");
        assert_eq!(headers.updates, "public, max-age=60");
        assert_eq!(headers.vendor, "public, max-age=604800");
        assert_eq!(headers.font, "public, max-age=31536000, immutable");
    }

    #[test]
    fn classify_static_response_matches_prefixes_and_defaults_to_no_cache() {
        let headers = CacheHeaders::from_config(&CacheConfig::default());
        let ok = StatusCode::OK;
        assert_eq!(
            classify_static_response(
                &headers,
                "/fonts/Klokantech Noto Sans Regular/0-255.pbf",
                ok
            ),
            Some(headers.font.clone())
        );
        assert_eq!(
            classify_static_response(&headers, "/vendor/maplibre-gl/maplibre-gl.mjs", ok),
            Some(headers.vendor.clone())
        );
        // The frontend's entry files and every other served asset (favicons,
        // anything later dropped into web_dir) share `no-cache` -- there is
        // no hardcoded filename list to keep in sync with the directory.
        for p in ["/", "/index.html", "/app.js", "/style.css", "/favicon.svg"] {
            assert_eq!(classify_static_response(&headers, p, ok), Some(NO_CACHE));
        }
        // `ServeDir` still answers 304 to If-Modified-Since under `no-cache`;
        // that response must carry the same policy as the 200 it replaces, or
        // the next load would fall back to the API-default `no-store` and stop
        // revalidating cheaply.
        assert_eq!(
            classify_static_response(&headers, "/app.js", StatusCode::NOT_MODIFIED),
            Some(NO_CACHE)
        );
    }

    /// A path that resolved to nothing is left header-less on purpose, so
    /// `build_router`'s outer `if_not_present` layer stamps the API default
    /// (`no-store`) rather than telling a client to revalidate a 404 body.
    #[test]
    fn classify_static_response_leaves_non_success_alone() {
        let headers = CacheHeaders::from_config(&CacheConfig::default());
        for status in [
            StatusCode::NOT_FOUND,
            StatusCode::METHOD_NOT_ALLOWED,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert_eq!(
                classify_static_response(&headers, "/does/not/exist", status),
                None
            );
            assert_eq!(
                classify_static_response(&headers, "/fonts/x.pbf", status),
                None
            );
        }
    }

    // --- weak_etag / if_none_match_matches / not_modified ------------------

    #[test]
    fn weak_etag_wraps_the_version_in_a_weak_quoted_string() {
        assert_eq!(weak_etag("1-2-3.4-5.6-7.8"), "W/\"1-2-3.4-5.6-7.8\"");
    }

    #[test]
    fn if_none_match_matches_a_verbatim_weak_tag() {
        let header = HeaderValue::from_static("W/\"abc\"");
        assert!(if_none_match_matches(Some(&header), "abc"));
    }

    #[test]
    fn if_none_match_matches_a_member_of_a_comma_separated_list() {
        let header = HeaderValue::from_static("W/\"other\", W/\"abc\", W/\"another\"");
        assert!(if_none_match_matches(Some(&header), "abc"));
    }

    #[test]
    fn if_none_match_star_matches_anything_present() {
        let header = HeaderValue::from_static("*");
        assert!(if_none_match_matches(Some(&header), "anything"));
    }

    /// RFC 9110 SS13.1.2: `If-None-Match` uses *weak* comparison, so a
    /// client's strong `"abc"` still matches our weak `W/"abc"` --
    #[test]
    fn if_none_match_matches_a_strong_client_tag_against_our_weak_tag() {
        let header = HeaderValue::from_static("\"abc\"");
        assert!(if_none_match_matches(Some(&header), "abc"));
    }

    /// -- and the reverse direction too: a client re-sending a weak tag it
    /// got from an earlier weak response still matches.
    #[test]
    fn if_none_match_matches_a_weak_client_tag_against_our_weak_tag() {
        let header = HeaderValue::from_static("W/\"abc\"");
        assert!(if_none_match_matches(Some(&header), "abc"));
    }

    #[test]
    fn if_none_match_rejects_a_mismatched_tag() {
        let header = HeaderValue::from_static("W/\"abc\"");
        assert!(!if_none_match_matches(Some(&header), "xyz"));
    }

    #[test]
    fn if_none_match_rejects_an_absent_header() {
        assert!(!if_none_match_matches(None, "abc"));
    }

    /// A malformed (non-UTF-8) header value must never produce a `304` --
    /// serve the full tile rather than risk matching something we couldn't
    /// actually parse.
    #[test]
    fn if_none_match_rejects_a_malformed_header_value() {
        let header = HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap();
        assert!(!if_none_match_matches(Some(&header), "abc"));
    }

    /// Surrounding whitespace around list members must not defeat a match.
    #[test]
    fn if_none_match_matches_despite_surrounding_whitespace() {
        let header = HeaderValue::from_static("  W/\"other\" ,  W/\"abc\"  ");
        assert!(if_none_match_matches(Some(&header), "abc"));
    }

    #[test]
    fn not_modified_carries_only_etag_and_cache_control() {
        let etag = weak_etag("v1");
        let cache_header = HeaderValue::from_static("public, max-age=60");
        let resp = not_modified(etag.clone(), cache_header.clone());

        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(resp.headers().get(header::ETAG), Some(&etag));
        assert_eq!(
            resp.headers().get(header::CACHE_CONTROL),
            Some(&cache_header)
        );
        assert!(resp.headers().get(header::CONTENT_TYPE).is_none());
        assert!(resp.headers().get(header::CONTENT_LENGTH).is_none());
    }
}
