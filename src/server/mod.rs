mod http_cache;
pub mod jobs;
mod package;
mod tile_cache;
mod tiles;
mod updates;

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::http::header;
use axum::{Router, http::StatusCode};
use duckdb::Connection;
use r2d2::Pool;
use tokio::net::TcpListener;
use tower_http::set_header::SetResponseHeaderLayer;
use tracing::info;

use crate::config::Config as AppConfig;
use crate::shutdown;

/// An `r2d2::ManageConnection` that hands out `try_clone()`s of one shared
/// base connection instead of opening the database file itself.
///
/// `duckdb::DuckdbConnectionManager` (the crate's built-in r2d2 manager)
/// always opens its own independent connection internally
/// (`Connection::open`), which would create a second, unsynchronized DuckDB
/// engine instance on the same file — see
/// docs/duckdb_connection_visibility_investigation.md for the staleness bug
/// this caused. Cloning from the app's one already-open, already-initialized
/// connection instead means every pooled connection shares live MVCC state:
/// a write committed through one pooled connection is immediately visible to
/// every other one.
///
/// A clone inherits write capability (cloning does not downgrade to
/// read-only), so this pool has no engine-enforced guarantee against writes
/// from read-path handlers — same trust level the old single `write` mutex
/// already relied on.
#[derive(Debug)]
pub struct ClonedConnectionManager {
    base: Arc<Mutex<Connection>>,
}

impl ClonedConnectionManager {
    pub fn new(base_conn: Connection) -> Self {
        Self {
            base: Arc::new(Mutex::new(base_conn)),
        }
    }
}

impl r2d2::ManageConnection for ClonedConnectionManager {
    type Connection = Connection;
    type Error = duckdb::Error;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        self.base.lock().unwrap().try_clone()
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.execute_batch("")
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

pub type DbPool = Pool<ClonedConnectionManager>;

/// Builds the shared pool from an already-open, already-initialized base
/// connection (extensions loaded, schema created, `duckdb_init_commands` run
/// — see `db::init_db`). No further per-connection setup is needed: extension
/// loads, custom table/scalar function registrations, and `SET GLOBAL`
/// settings are all instance-wide and already visible to every `try_clone()`
/// (verified empirically — docs/duckdb_connection_visibility_investigation.md).
pub fn build_pool(base_conn: Connection, pool_size: u32) -> Result<DbPool> {
    Pool::builder()
        .max_size(pool_size)
        .build(ClonedConnectionManager::new(base_conn))
        .context("Failed to build DB connection pool")
}

#[derive(Clone)]
pub struct AppState {
    pub pool: DbPool,
    pub registry: Arc<jobs::JobRegistry>,
    pub config: Arc<AppConfig>,
    /// Precomputed `Cache-Control` values derived from `config.cache` (see
    /// `http_cache::CacheHeaders`). `build_router` always rebuilds this from
    /// whatever `config` it was handed right before constructing the router,
    /// so this field's initial value here doesn't need to stay in sync with
    /// `config` by hand -- there's exactly one place
    /// (`CacheHeaders::from_config`, called from `build_router`) that turns
    /// cache config into header bytes.
    pub cache_headers: Arc<http_cache::CacheHeaders>,
    /// Bounded in-process byte cache for z14 `/tiles` responses -- see
    /// `tile_cache` module doc for the design. `TileCache::new(0)` (i.e.
    /// `config.cache.tile_cache_max_bytes == 0`) is a working no-op, so this
    /// field is never `Option` and callers never need to branch on whether
    /// caching is enabled.
    pub tile_cache: Arc<tile_cache::TileCache>,
}

impl AppState {
    /// Test-only constructor. Every Router-building test helper across the
    /// server module tree (`server::tiles`, `server::updates`,
    /// `server::package`, plus this module's own tests) builds its
    /// `AppState` through here rather than writing out the struct literal,
    /// so the next field added to `AppState` only has to be threaded through
    /// one place instead of four. `#[cfg(test)]` here reaches all of them:
    /// this crate has no lib target, so `cargo test` compiles the whole
    /// binary crate with `--cfg test`, not just this module.
    #[cfg(test)]
    pub fn for_tests(pool: DbPool) -> Self {
        let config = Arc::new(AppConfig::default());
        let cache_headers = Arc::new(http_cache::CacheHeaders::from_config(&config.cache));
        let tile_cache = Arc::new(tile_cache::TileCache::new(
            config.cache.tile_cache_max_bytes,
        ));
        Self {
            pool,
            registry: Arc::new(jobs::JobRegistry::new_for_tests(vec![])),
            config,
            cache_headers,
            tile_cache,
        }
    }
}

pub async fn run(
    conn: Connection,
    kv: Arc<crate::osm::kvstore::RocksDB>,
    config: Arc<AppConfig>,
) -> Result<()> {
    let pool = build_pool(conn, config.db_pool_size)?;

    check_startup_conditions(&pool)?;

    // Manual literal, not `JobConfigResolved::from`: `osm_update` is an
    // `OsmUpdateConfig`, not a generic `JobConfig` (it carries the
    // prefetch/batch fields a follow-up task will consume), so the blanket
    // `From<&JobConfig>` impl doesn't apply. Mirrors the `match_refresh`
    // literal below, which has the same shape for the same reason.
    let osm_cfg = jobs::JobConfigResolved {
        enabled: config.jobs.osm_update.enabled,
        interval: std::time::Duration::from_secs(config.jobs.osm_update.interval_seconds),
        timeout: std::time::Duration::from_secs(config.jobs.osm_update.timeout_seconds),
    };
    let export_prune_cfg = jobs::JobConfigResolved {
        enabled: config.jobs.export_log_prune.enabled,
        interval: std::time::Duration::from_secs(config.jobs.export_log_prune.interval_seconds),
        timeout: std::time::Duration::from_secs(config.jobs.export_log_prune.timeout_seconds),
    };
    let job_list: Vec<(Arc<dyn jobs::Job>, jobs::JobConfigResolved)> = vec![
        (
            Arc::new(jobs::osm_update::OsmUpdateJob) as Arc<dyn jobs::Job>,
            osm_cfg,
        ),
        (
            Arc::new(jobs::export_log_prune::ExportLogPruneJob) as Arc<dyn jobs::Job>,
            export_prune_cfg,
        ),
        (
            Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
                &crate::dataset::BDOT10K,
                "bdot10k_update",
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved::from(&config.jobs.bdot10k_update),
        ),
        (
            Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
                &crate::dataset::EGIB,
                "egib_update",
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved::from(&config.jobs.egib_update),
        ),
        (
            Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
                &crate::dataset::PRG,
                "prg_update",
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved::from(&config.jobs.prg_update),
        ),
        (
            Arc::new(jobs::match_refresh::MatchRefreshJob::new(
                config.jobs.match_refresh.batch_size,
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved {
                enabled: config.jobs.match_refresh.enabled,
                interval: std::time::Duration::from_secs(
                    config.jobs.match_refresh.interval_seconds,
                ),
                timeout: std::time::Duration::from_secs(config.jobs.match_refresh.timeout_seconds),
            },
        ),
        // The safety net for a dropped enqueue. Off by default -- see
        // MatchReconcileConfig for the measured reasons. It only appends to
        // match_dirty_cells and lets the per-cell drain rebuild, so unlike
        // `compare reconcile` on the CLI it is safe against a live server.
        (
            Arc::new(jobs::match_reconcile::MatchReconcileJob) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved {
                enabled: config.jobs.match_reconcile.enabled,
                interval: std::time::Duration::from_secs(
                    config.jobs.match_reconcile.interval_seconds,
                ),
                timeout: std::time::Duration::from_secs(
                    config.jobs.match_reconcile.timeout_seconds,
                ),
            },
        ),
        (
            Arc::new(jobs::street_mappings_update::StreetMappingsUpdateJob::new())
                as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved::from(&config.jobs.street_mappings_update),
        ),
        (
            Arc::new(jobs::building_types_update::BuildingTypesUpdateJob::new())
                as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved::from(&config.jobs.building_types_update),
        ),
    ];
    let scheduler = jobs::Scheduler::start(job_list, pool.clone(), kv, config.clone());
    let registry = scheduler.registry.clone();
    let shutdown_notify = scheduler.shutdown_notify();

    let cache_headers = Arc::new(http_cache::CacheHeaders::from_config(&config.cache));
    // Rebuilt again (redundantly) inside `build_router` from whatever
    // `config` it's handed, same as `cache_headers` above -- but unlike
    // `cache_headers` (stateless, cheap to recompute), `build_router`
    // deliberately does NOT rebuild this: it's stateful (the cache's actual
    // contents), and `build_router` is called per-test in some places, so
    // rebuilding it there would silently discard whatever was cached
    // in-between calls. Its struct-update `..state` just carries this Arc
    // through unchanged.
    let tile_cache = Arc::new(tile_cache::TileCache::new(
        config.cache.tile_cache_max_bytes,
    ));
    let state = AppState {
        pool,
        registry,
        config: config.clone(),
        cache_headers,
        tile_cache,
    };

    let app = build_router(state);

    let listener = TcpListener::bind(&config.http_listen_addr).await?;
    info!(addr = %config.http_listen_addr, "HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                if shutdown::is_requested() {
                    shutdown_notify.notify_waiters();
                    break;
                }
            }
        })
        .await?;

    scheduler
        .shutdown(tokio::time::Duration::from_secs(30))
        .await;

    Ok(())
}

/// The single home for the shipping route set. Building it from `state`
/// alone (rather than threading `config` separately) means every caller --
/// production `run()` and every test helper across `server::tiles`,
/// `server::updates`, `server::package` -- goes through the exact same
/// construction, so a test (in particular the static-fallback /
/// path-traversal tests below) exercises the router that actually ships
/// instead of a hand-rolled stand-in that could silently drift from it.
///
/// Always rebuilds `state.cache_headers` from `state.config.cache` before
/// handing `state` to any handler -- the one place `CacheConfig` turns into
/// actual `HeaderValue`s (see `http_cache::CacheHeaders::from_config`), so a
/// caller that hands in a custom `config` never needs to remember to
/// separately rebuild `cache_headers` to match.
///
/// The outer `.layer(SetResponseHeaderLayer::if_not_present(...))` wraps the
/// *entire* router, including the static fallback and axum's own 404/405/
/// extractor-rejection responses (`Router::layer` wraps `path_router`,
/// `fallback_router` and `catch_all_fallback` alike -- verified against
/// axum 0.8's `Router::layer` source, not assumed) -- that is what makes
/// `no-store` the true default for anything that didn't already set its own
/// `Cache-Control` (tiles, `/updates`, and the static-asset middleware in
/// `http_cache::static_router` all set theirs first, so `if_not_present`
/// leaves them alone). It has to be the *last* call in this chain: applying
/// it before `.fallback_service(...)` would only wrap the API routes that
/// existed at that point, not the fallback added afterwards.
pub fn build_router(state: AppState) -> Router {
    let web_dir = state.config.web_dir.clone();
    let cache_headers = Arc::new(http_cache::CacheHeaders::from_config(&state.config.cache));
    let state = AppState {
        cache_headers: cache_headers.clone(),
        ..state
    };
    Router::new()
        .route("/health", axum::routing::get(|| async { StatusCode::OK }))
        .route(
            "/status",
            axum::routing::get(jobs::status_handler::get_status),
        )
        .route("/tiles/{z}/{x}/{y}", axum::routing::get(tiles::serve_tile))
        .route(
            "/package",
            axum::routing::get(package::get_package).post(package::post_package),
        )
        .route("/updates", axum::routing::get(updates::get_updates))
        .with_state(state)
        // Static frontend assets, served from a directory deployed alongside
        // the binary (see Config::web_dir) rather than embedded at compile
        // time. Mounted as a fallback so it never shadows the API routes
        // above; a missing directory just makes every request 404 instead of
        // failing startup. Its own Cache-Control policy (fonts/vendor/known
        // entry files) lives in http_cache::static_router.
        .fallback_service(http_cache::static_router(&web_dir, cache_headers))
        // API default: no-store. Must stay the last call -- see doc comment
        // above.
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            http_cache::NO_STORE,
        ))
}

const REQUIRED_TABLES: &[&str] = &[
    "metadata",
    "osm_addresses",
    "osm_buildings",
    "prg_addresses",
    "bdot10k_buildings",
    "egib_buildings",
];

/// The serving tables `/tiles` and `/package` read directly. Unlike
/// `REQUIRED_TABLES` these always exist (created by `CREATE TABLE IF NOT
/// EXISTS` in `db::create_schema` on every startup), so there is nothing to
/// bail on -- only "empty" is worth flagging, since an in-place upgrade of an
/// existing database gains these tables empty and would otherwise start
/// serving zero features with no indication why (see README).
const UNMATCHED_TABLES: &[&str] = &["bdot10k_unmatched", "egib_unmatched", "prg_unmatched"];

fn check_startup_conditions(pool: &DbPool) -> Result<()> {
    let conn = pool
        .get()
        .context("Failed to acquire a connection for startup checks")?;

    conn.query_row("SELECT 1", [], |_| Ok(()))
        .context("Startup health check failed")?;

    for table in REQUIRED_TABLES {
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = 'main' AND table_name = ?",
                [table],
                |row| row.get(0),
            )
            .with_context(|| format!("Failed to check existence of table '{table}'"))?;

        if exists == 0 {
            anyhow::bail!(
                "Required table '{table}' is missing — run the import commands before starting the server"
            );
        }

        let rows: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .with_context(|| format!("Failed to count rows in table '{table}'"))?;

        info!("Table {} has {} rows.", table, rows);
    }

    for table in UNMATCHED_TABLES {
        let rows: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .with_context(|| format!("Failed to count rows in table '{table}'"))?;

        info!("Table {} has {} rows.", table, rows);
        if rows == 0 {
            tracing::warn!(
                "serving table '{table}' is empty -- /tiles and /package will return no \
                 features for this source until an offline `compare full` populates it \
                 (see README)"
            );
        }
    }

    // osm_former_buildings backs compare::rule's suppression veto (see
    // CLAUDE.md). Like the UNMATCHED_TABLES loop above, it always exists
    // (CREATE TABLE IF NOT EXISTS in db::create_schema) so there is nothing
    // to bail on, only "empty" to flag -- but unlike those tables, a freshly
    // initialized, never-yet-imported database also has it empty, and that
    // is not worth a warning. Gate on osm_buildings being non-empty instead:
    // that is the real signal an operator cares about, "you upgraded to a
    // binary that understands former buildings but have not re-run
    // `import osm` yet", since only a full `import osm` populates this table.
    let osm_buildings_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM osm_buildings", [], |row| row.get(0))
        .context("Failed to count rows in table 'osm_buildings'")?;
    if osm_buildings_rows > 0 {
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM osm_former_buildings", [], |row| {
                row.get(0)
            })
            .context("Failed to count rows in table 'osm_former_buildings'")?;

        info!("Table osm_former_buildings has {} rows.", rows);
        if rows == 0 {
            tracing::warn!(
                "osm_former_buildings is empty even though osm_buildings is not -- this \
                 database predates the former-building suppression veto; run `import osm` \
                 to backfill it (see README)"
            );
        }
    }

    info!("Startup checks passed: pool connection OK, all required tables present");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use duckdb::Connection;
    use tower::ServiceExt;

    use super::jobs::{JobOutcome, JobRegistry, JobState, JobStatus};
    use super::{AppState, build_pool, build_router};

    /// `web_dir` is a parameter (not baked into a fixed default) so the
    /// static-fallback tests below can point it at a tempdir while
    /// non-static tests (`health_returns_200`, `status_returns_jobs_json`)
    /// pass one that is simply never touched.
    fn make_test_state(initial: Vec<JobStatus>, web_dir: &Path) -> AppState {
        let conn = Connection::open_in_memory().unwrap();
        let pool = build_pool(conn, 2).unwrap();
        let mut state = AppState::for_tests(pool);
        state.registry = Arc::new(JobRegistry::new_for_tests(initial));
        state.config = Arc::new(crate::config::Config {
            web_dir: web_dir.to_string_lossy().into_owned(),
            ..crate::config::Config::default()
        });
        state
    }

    #[tokio::test]
    async fn health_returns_200() {
        let dir = tempfile::tempdir().unwrap();
        let app = build_router(make_test_state(vec![], dir.path()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn status_returns_jobs_json() {
        let preset = JobStatus {
            name: "osm_update",
            enabled: true,
            interval_seconds: 60,
            timeout_seconds: 600,
            state: JobState::Idle,
            last_started_at: Some("2026-05-28T12:00:00Z".to_string()),
            last_finished_at: Some("2026-05-28T12:00:03Z".to_string()),
            last_duration_ms: Some(3000),
            last_outcome: Some(JobOutcome::Success),
            next_run_at: Some("2026-05-28T12:01:03Z".to_string()),
            run_count: 7,
        };
        let dir = tempfile::tempdir().unwrap();
        let app = build_router(make_test_state(vec![preset], dir.path()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 8192).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let jobs = v["jobs"].as_array().expect("jobs array");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["name"], "osm_update");
        assert_eq!(jobs[0]["state"], "idle");
        assert_eq!(jobs[0]["run_count"], 7);
        assert_eq!(jobs[0]["last_outcome"]["kind"], "Success");
    }

    /// The static frontend is mounted as a `fallback_service`, so it must
    /// never shadow an API route — a request for `/health` has to keep
    /// hitting the handler even when a file named `health` happens to sit in
    /// `web_dir`. Built via `build_router` (the real, shipping router) rather
    /// than a hand-rolled stand-in, so this pins the actual precedence, not a
    /// copy of it that could drift.
    #[tokio::test]
    async fn static_fallback_does_not_shadow_api_routes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("health"), b"not the api response").unwrap();

        let app = build_router(make_test_state(vec![], dir.path()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert!(
            body.is_empty(),
            "must be the API handler, not the static file"
        );
    }

    /// A request for a real file under `web_dir` is served by the fallback.
    #[tokio::test]
    async fn static_fallback_serves_files_from_web_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), b"<h1>hello</h1>").unwrap();

        let app = build_router(make_test_state(vec![], dir.path()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/index.html")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], b"<h1>hello</h1>");
    }

    /// A missing `web_dir` (or a missing file within it) must 404, not crash
    /// the server — the config doc promises this is not a startup error.
    #[tokio::test]
    async fn static_fallback_404s_when_web_dir_is_missing() {
        let app = build_router(make_test_state(vec![], Path::new("/nonexistent/web/dir")));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/index.html")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// The static fallback must not let a request escape `web_dir`. `ServeDir`
    /// percent-decodes the URI path once and then rejects any component that
    /// is not `Component::Normal`, so `..` is refused before the filesystem is
    /// touched — but that is a property of the dependency, not of our code, so
    /// pin it here rather than trusting a future upgrade to keep it.
    #[tokio::test]
    async fn static_fallback_rejects_path_traversal() {
        let root = tempfile::tempdir().unwrap();
        let web = root.path().join("web");
        std::fs::create_dir(&web).unwrap();
        std::fs::write(web.join("index.html"), b"<h1>hello</h1>").unwrap();
        // The file an attacker would be trying to reach: a sibling of web_dir,
        // standing in for a config file or the DuckDB database next to it.
        std::fs::write(root.path().join("secret.txt"), b"SECRET").unwrap();

        let state = make_test_state(vec![], &web);

        // Raw `..`, single-encoded, mixed-case, encoded separator, and
        // double-encoded. The last one decodes to the literal name `%2e%2e`,
        // which is a normal component and simply does not exist.
        for uri in [
            "/../secret.txt",
            "/%2e%2e/secret.txt",
            "/%2E%2E%2Fsecret.txt",
            "/..%2fsecret.txt",
            "/%252e%252e/secret.txt",
            "/subdir/../../secret.txt",
            "/etc/passwd",
        ] {
            let app = build_router(state.clone());

            let response = app
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();

            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{uri} must not resolve outside web_dir"
            );
            let body = to_bytes(response.into_body(), 1024).await.unwrap();
            assert_ne!(&body[..], b"SECRET", "{uri} leaked a file outside web_dir");
        }
    }

    // --- Phase 2: [cache] config + Cache-Control on every response --------

    #[tokio::test]
    async fn health_and_status_default_to_no_store() {
        let dir = tempfile::tempdir().unwrap();
        let app = build_router(make_test_state(vec![], dir.path()));

        for uri in ["/health", "/status"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.headers()["cache-control"],
                "no-store",
                "{uri} must default to no-store"
            );
        }
    }

    /// `/package` must never be cached: a cached response never reaches
    /// `package::log_export`, so `package_exports` would under-count and
    /// `/updates` would silently under-report real exports (see
    /// `http_cache`'s module doc). Both verbs 400 before touching the DB
    /// here (missing `bbox` / an unparsable body), which is enough to prove
    /// the header without seeding a schema.
    #[tokio::test]
    async fn package_is_no_store_on_both_verbs() {
        let dir = tempfile::tempdir().unwrap();
        let app = build_router(make_test_state(vec![], dir.path()));

        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/package")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(get_response.headers()["cache-control"], "no-store");

        let post_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(post_response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(post_response.headers()["cache-control"], "no-store");
    }

    /// A request that matches no API route and no known static path
    /// (`http_cache::classify_static_path` returns `None` for it) must still
    /// come back `no-store` -- proving the outer
    /// `SetResponseHeaderLayer::if_not_present` really is the default for
    /// "nothing else claimed this response", not just a decoration on the
    /// five API routes. Paired with a 405 (a route that exists, wrong verb)
    /// to cover axum's own rejection path too.
    #[tokio::test]
    async fn unmatched_routes_and_method_rejections_default_to_no_store() {
        let dir = tempfile::tempdir().unwrap();
        let app = build_router(make_test_state(vec![], dir.path()));

        let not_found = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/this/path/does/not/exist/anywhere")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(not_found.status(), StatusCode::NOT_FOUND);
        assert_eq!(not_found.headers()["cache-control"], "no-store");

        let method_not_allowed = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(method_not_allowed.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(method_not_allowed.headers()["cache-control"], "no-store");
    }

    // `/updates` and `/tiles` keeping their own `Cache-Control` through
    // `if_not_present` is pinned where the headers actually originate:
    // `server::updates::tests::get_updates_returns_recent_exports_with_cache_header`
    // and `server::tiles::tests::{z14_tile_carries_the_configured_tile_cache_control,
    // agg_and_points_tiles_carry_the_configured_aggregate_cache_control}` all go
    // through this module's `build_router`, so if the outer no-store layer ever
    // stopped being `if_not_present` (or ran before those handlers set their
    // header instead of after), those assertions -- which check for the
    // *configured* value, not `no-store` -- would fail there instead.

    #[tokio::test]
    async fn fonts_get_immutable_cache_control_including_percent_encoded_space() {
        let dir = tempfile::tempdir().unwrap();
        // The real font path contains a literal space, so a browser request
        // arrives percent-encoded -- exercise that, not a sanitized stand-in.
        let font_dir = dir
            .path()
            .join("fonts")
            .join("Klokantech Noto Sans Regular");
        std::fs::create_dir_all(&font_dir).unwrap();
        std::fs::write(font_dir.join("0-255.pbf"), b"glyph-bytes").unwrap();

        let app = build_router(make_test_state(vec![], dir.path()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/fonts/Klokantech%20Noto%20Sans%20Regular/0-255.pbf")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["cache-control"],
            "public, max-age=31536000, immutable"
        );
    }

    #[tokio::test]
    async fn vendor_assets_get_one_week_cache_control() {
        let dir = tempfile::tempdir().unwrap();
        let vendor_dir = dir.path().join("vendor").join("maplibre-gl");
        std::fs::create_dir_all(&vendor_dir).unwrap();
        std::fs::write(vendor_dir.join("maplibre-gl.mjs"), b"export default {};").unwrap();

        let app = build_router(make_test_state(vec![], dir.path()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/vendor/maplibre-gl/maplibre-gl.mjs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["cache-control"],
            "public, max-age=604800"
        );
    }

    #[tokio::test]
    async fn frontend_entry_files_get_no_cache() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["index.html", "app.js", "style.css"] {
            std::fs::write(dir.path().join(name), b"content").unwrap();
        }

        let app = build_router(make_test_state(vec![], dir.path()));
        for path in ["/index.html", "/app.js", "/style.css"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(response.headers()["cache-control"], "no-cache", "{path}");
        }
    }

    /// The §5 unknown from the Phase 2 plan, settled empirically: tower-http
    /// 0.6's `ServeDir` DOES honour `If-Modified-Since` and answers `304 Not
    /// Modified` rather than re-sending the file -- confirmed by reading
    /// `ServeDir`'s own `open_file.rs`/`future.rs` (an `IfModifiedSince`
    /// check that short-circuits to `StatusCode::NOT_MODIFIED`) and pinned
    /// here end to end. Since this holds, `no-cache` on `app.js`/
    /// `index.html`/`style.css` costs one small conditional round trip per
    /// load, not a full re-send -- had this NOT held, those three would need
    /// a short `max-age` instead of `no-cache`.
    #[tokio::test]
    async fn static_assets_revalidate_via_if_modified_since() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("app.js"), b"console.log('hi');").unwrap();

        let app = build_router(make_test_state(vec![], dir.path()));

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.headers()["cache-control"], "no-cache");
        let last_modified = first
            .headers()
            .get(header::LAST_MODIFIED)
            .expect("ServeDir must set Last-Modified")
            .clone();

        let second = app
            .oneshot(
                Request::builder()
                    .uri("/app.js")
                    .header(header::IF_MODIFIED_SINCE, last_modified)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            second.status(),
            StatusCode::NOT_MODIFIED,
            "tower-http 0.6's ServeDir must honour If-Modified-Since and answer 304"
        );
    }

    /// `build_router` must not panic when `web_dir` doesn't exist -- the
    /// config doc promises this is not a startup error (see
    /// `static_fallback_404s_when_web_dir_is_missing` for the corresponding
    /// per-request behavior). Constructing `http_cache::static_router` --
    /// `ServeDir::new` plus the `from_fn_with_state` middleware -- must stay
    /// lazy about the directory's existence.
    #[test]
    fn build_router_does_not_panic_on_missing_web_dir() {
        let dir = tempfile::tempdir().unwrap();
        let _ = build_router(make_test_state(vec![], &dir.path().join("does-not-exist")));
    }

    /// An in-place upgrade of an existing database gains the `*_unmatched`
    /// tables via `CREATE TABLE IF NOT EXISTS`, empty -- `check_startup_conditions`
    /// must not bail (empty is not a hard requirement), but must warn loudly
    /// enough that an operator can tell why `/tiles`/`/package` are serving
    /// nothing, rather than the server starting silently.
    #[test]
    fn check_startup_conditions_warns_when_a_serving_table_is_empty() {
        use std::io;
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::fmt::MakeWriter;

        #[derive(Clone)]
        struct SharedBuf(Arc<Mutex<Vec<u8>>>);
        impl io::Write for SharedBuf {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl<'a> MakeWriter<'a> for SharedBuf {
            type Writer = SharedBuf;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = crate::db::init_db(std::path::Path::new(":memory:"), &init, None).unwrap();
        // REQUIRED_TABLES also needs these three -- create empty, matching a
        // freshly-upgraded database whose government tables just haven't
        // been (re)compared yet.
        conn.execute_batch(
            "CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        let pool = build_pool(conn, 2).unwrap();

        tracing::subscriber::with_default(subscriber, || {
            super::check_startup_conditions(&pool).unwrap();
        });

        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            out.contains("bdot10k_unmatched") && out.contains("empty"),
            "expected a warning naming the empty serving table, got: {out}"
        );
    }
}
