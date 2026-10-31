//! `GET /stats`, `GET /stats/areas` and `GET /stats/units`: the statistics
//! page's data.
//!
//! All are plain queries run per request; the only caching is HTTP, a
//! `max-age` of `[cache] stats_max_age_seconds`. `/stats` is a handful of
//! counts over whole tables (~0.1 s on the Poland database), `/stats/areas` one
//! aggregation per page (~0.07 s for gminy, ~0.11 s for settlements). Pages
//! read live data, so a recompute landing between two page requests can move
//! an area from one page to the next -- accepted in exchange for having no
//! server-side state at all.
//!
//! `/stats/units` is the whole `teryt_units` table (~2,900 rows). The page's
//! area search filters it in the browser and sends back only the picked
//! unit's code, as `/stats/areas`'s `in`; there is no server-side text search.
//!
//! See docs/superpowers/specs/2026-09-27-statistics-page-design.md.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use duckdb::Connection;
use serde::{Deserialize, Serialize};

use super::AppState;
use super::updates::error_response;
use crate::dataset::{BDOT10K, DatasetSpec, EGIB, PRG};

/// Page order of the dataset rows.
const DATASETS: [&DatasetSpec; 3] = [&PRG, &BDOT10K, &EGIB];

/// How many `building` values the OSM section lists.
const TOP_BUILDING_VALUES: usize = 10;

pub const DEFAULT_PER_PAGE: u64 = 50;
pub const MAX_PER_PAGE: u64 = 200;
/// Far past the last page of either level (52,575 settlements), and small
/// enough that `(page - 1) * per_page` always fits the `OFFSET`'s `BIGINT`.
pub const MAX_PAGE: u64 = 1_000_000;

/// `expr` (a `TIMESTAMPTZ`) as `2026-09-27T11:40:02Z`, the shape `/updates`
/// and the OSM replication stamp already use.
fn utc_sql(expr: &str) -> String {
    format!("strftime(({expr}) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ')")
}

// --- Summary (GET /stats) ------------------------------------------------

#[derive(Serialize, Debug)]
struct Summary {
    computed_at: String,
    osm: OsmSummary,
    datasets: Vec<DatasetSummary>,
    reports: Vec<ReportCount>,
}

#[derive(Serialize, Debug)]
struct OsmSummary {
    replication_timestamp: Option<String>,
    /// Measured at `computed_at`.
    lag_seconds: Option<i64>,
    addresses: BTreeMap<String, i64>,
    buildings: BTreeMap<String, i64>,
    former_buildings: i64,
    top_building_values: Vec<ValueCount>,
}

#[derive(Serialize, Debug)]
struct ValueCount {
    value: Option<String>,
    count: i64,
}

#[derive(Serialize, Debug)]
struct DatasetSummary {
    source: &'static str,
    /// Objects taking part in the comparison: the source table, with the
    /// filter the match rule applies (`compare::totals::comparable_count_sql`).
    comparable: i64,
    unmatched: i64,
    /// `metadata.imported_at:<source>`, set by a successful `import`.
    imported_at: Option<String>,
    /// Newest `dataset_refreshes.finished_at`, set by a successful `update`
    /// (no-op ones included). The client shows the later of the two.
    refreshed_at: Option<String>,
}

#[derive(Serialize, Debug)]
struct ReportCount {
    source: String,
    status: String,
    count: i64,
}

fn count(conn: &Connection, sql: &str) -> Result<i64> {
    conn.query_row(sql, [], |r| r.get(0))
        .with_context(|| format!("stats query failed: {sql}"))
}

fn optional_timestamp(conn: &Connection, sql: &str, param: &str) -> Result<Option<String>> {
    conn.query_row(sql, duckdb::params![param], |r| r.get(0))
        .with_context(|| format!("stats query failed: {sql}"))
}

fn count_by(conn: &Connection, table: &str, column: &str) -> Result<BTreeMap<String, i64>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT coalesce({column}, 'unknown'), count(*) FROM {table} GROUP BY 1"
    ))?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect::<duckdb::Result<BTreeMap<_, _>>>()
        .with_context(|| format!("Failed to count {table} by {column}"))
}

fn dataset_summary(conn: &Connection, spec: &'static DatasetSpec) -> Result<DatasetSummary> {
    Ok(DatasetSummary {
        source: spec.name,
        comparable: count(
            conn,
            &crate::compare::totals::comparable_count_sql(spec.name)?,
        )?,
        unmatched: count(
            conn,
            &format!("SELECT count(*) FROM {}_unmatched", spec.name),
        )?,
        imported_at: optional_timestamp(
            conn,
            &format!(
                "SELECT {} FROM metadata WHERE key = ?",
                utc_sql("max(TRY_CAST(value AS TIMESTAMPTZ))")
            ),
            &crate::import::imported_at_key(spec.name),
        )?,
        refreshed_at: optional_timestamp(
            conn,
            &format!(
                "SELECT {} FROM dataset_refreshes WHERE source = ?",
                utc_sql("max(finished_at)")
            ),
            spec.name,
        )?,
    })
}

fn osm_summary(conn: &Connection) -> Result<OsmSummary> {
    let replication = super::jobs::status_handler::read_osm_replication(conn)?;
    let top_building_values = {
        let mut stmt = conn.prepare(&format!(
            "SELECT building, count(*) AS c FROM osm_buildings
             GROUP BY building ORDER BY c DESC, building LIMIT {TOP_BUILDING_VALUES}"
        ))?;
        let rows = stmt.query_map([], |r| {
            Ok(ValueCount {
                value: r.get(0)?,
                count: r.get(1)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("Failed to read building values")?
    };
    Ok(OsmSummary {
        replication_timestamp: replication.timestamp,
        lag_seconds: replication.lag_seconds,
        addresses: count_by(conn, "osm_addresses", "osm_type")?,
        buildings: count_by(conn, "osm_buildings", "osm_type")?,
        former_buildings: count(conn, "SELECT count(*) FROM osm_former_buildings")?,
        top_building_values,
    })
}

fn report_counts(conn: &Connection) -> Result<Vec<ReportCount>> {
    let mut stmt = conn.prepare(
        "SELECT source, status, count(*) FROM object_reports
         GROUP BY source, status ORDER BY source, status",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(ReportCount {
            source: r.get(0)?,
            status: r.get(1)?,
            count: r.get(2)?,
        })
    })?;
    rows.collect::<duckdb::Result<Vec<_>>>()
        .context("Failed to count reports")
}

/// Every figure of `/stats`, from one read transaction so they all describe
/// the same moment (`now()` is transaction-start-scoped, so `computed_at` and
/// the replication lag agree with it too).
fn build_summary(conn: &Connection) -> Result<Summary> {
    crate::compare::in_transaction(conn, "stats summary", || {
        let computed_at: String =
            conn.query_row(&format!("SELECT {}", utc_sql("now()")), [], |r| r.get(0))?;
        let datasets = DATASETS
            .iter()
            .map(|spec| dataset_summary(conn, spec))
            .collect::<Result<Vec<_>>>()?;
        Ok(Summary {
            computed_at,
            osm: osm_summary(conn)?,
            datasets,
            reports: report_counts(conn)?,
        })
    })
}

// --- Ranking (GET /stats/areas) ------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Gmina,
    Settlement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    Ratio,
    Unmatched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Desc,
    Asc,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Gmina => "gmina",
            Level::Settlement => "settlement",
        }
    }

    /// The group key, which is also the tie-break of every sort. It carries
    /// the names as well as the codes, so no name has to be chosen for a code:
    /// a settlement published under two spellings, or split across two gminy,
    /// is simply two rows, and the 226 PRG addresses with an empty SIMC code
    /// stay apart as the separate places they are instead of merging into one.
    /// (Every gmina code has exactly one name in PRG, so the gmina level loses
    /// nothing by it.) `prg_unmatched` carries the same four columns, copied
    /// at compare time, so both sides group identically.
    fn key_columns(self) -> &'static str {
        match self {
            Level::Gmina => "teryt_gmina, gmina",
            Level::Settlement => "teryt_miejscowosc, miejscowosc, teryt_gmina, gmina",
        }
    }

    /// `teryt, name, teryt_gmina, gmina` of an [`Area`].
    fn area_columns(self) -> &'static str {
        match self {
            Level::Gmina => "teryt_gmina, gmina, teryt_gmina, gmina",
            Level::Settlement => "teryt_miejscowosc, miejscowosc, teryt_gmina, gmina",
        }
    }
}

impl SortKey {
    fn as_str(self) -> &'static str {
        match self {
            SortKey::Ratio => "ratio",
            SortKey::Unmatched => "unmatched",
        }
    }

    /// Integer division in DuckDB yields a `DOUBLE`; equal fractions (1/2,
    /// 2/4) produce the same value, so they tie and fall to the key columns.
    fn sql(self) -> &'static str {
        match self {
            SortKey::Ratio => "coalesce(u.unmatched, 0) / a.total",
            SortKey::Unmatched => "coalesce(u.unmatched, 0)",
        }
    }
}

impl SortOrder {
    fn as_str(self) -> &'static str {
        match self {
            SortOrder::Desc => "desc",
            SortOrder::Asc => "asc",
        }
    }

    fn sql(self) -> &'static str {
        match self {
            SortOrder::Desc => "DESC",
            SortOrder::Asc => "ASC",
        }
    }
}

/// One gmina or settlement of the ranking. At gmina level
/// `teryt_gmina`/`gmina` repeat `teryt`/`name`, so the client needs no
/// per-level branch.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Area {
    pub teryt: String,
    pub name: String,
    pub teryt_gmina: String,
    pub gmina: String,
    pub total: i64,
    pub unmatched: i64,
    /// `[min lon, min lat, max lon, max lat]` over the area's PRG addresses,
    /// for the page's "show on map" link.
    pub bbox: Option<[f64; 4]>,
}

/// The `WHERE` narrowing either PRG table to one unit, or nothing. A unit's
/// code is a prefix of the `teryt_gmina` of everything inside it (2 digits
/// for a województwo, 4 for a powiat, all 7 for a gmina), so one clause serves
/// all three and needs no join to `teryt_units`.
fn scope_sql(scoped: bool) -> &'static str {
    if scoped {
        "WHERE starts_with(teryt_gmina, ?)"
    } else {
        ""
    }
}

/// One page of one level, sorted by the requested key and direction, then by
/// the key columns ascending so the order is total and pages never overlap.
/// The only interpolated text comes from the enums above, never from the
/// request; `LIMIT`/`OFFSET` are parameters.
///
/// On DuckDB 1.5.5 + spatial, `ST_Extent_Agg` corrupted the strings of a
/// `histogram()` over VARCHAR computed alongside it, nondeterministically and
/// with no error (an earlier version of this query lost ~10,250 of 52,560
/// settlement names that way), so this query kept every string a group key.
/// DuckDB 2.0's spatial (`a3dbcd1781`) fixed it: the minimal repro corrupts
/// 679-2,089 of 5,000 names on 1.5.5 and none in five runs on 2.0.
/// `long_names_survive_the_ranking_query_at_scale` still pins the group keys.
///
/// With `scoped`, both sides are narrowed to one unit first, and the query
/// takes its code twice ahead of `LIMIT`/`OFFSET`.
fn areas_sql(level: Level, sort: SortKey, order: SortOrder, scoped: bool) -> String {
    let keys = level.key_columns();
    let area = level.area_columns();
    let (by, dir) = (sort.sql(), order.sql());
    let scope = scope_sql(scoped);
    format!(
        "WITH a AS (
             SELECT {keys}, count(*) AS total, ST_Extent_Agg(geom) AS ext
             FROM prg_addresses {scope}
             GROUP BY {keys}),
         u AS (
             SELECT {keys}, count(*) AS unmatched
             FROM prg_unmatched {scope}
             GROUP BY {keys})
         SELECT {area}, a.total, coalesce(u.unmatched, 0),
                ST_XMin(ext), ST_YMin(ext), ST_XMax(ext), ST_YMax(ext),
                count(*) OVER () AS total_items
         FROM a LEFT JOIN u USING ({keys})
         ORDER BY {by} {dir}, {keys}
         LIMIT ? OFFSET ?"
    )
}

/// How many areas a level has; only needed for a page past the end, which
/// returns no row to read `total_items` from.
fn area_count_sql(level: Level, scoped: bool) -> String {
    format!(
        "SELECT count(*) FROM (SELECT 1 FROM prg_addresses {} GROUP BY {})",
        scope_sql(scoped),
        level.key_columns()
    )
}

#[derive(Serialize, Debug)]
struct AreasPage {
    level: &'static str,
    sort: &'static str,
    order: &'static str,
    /// The unit the ranking is narrowed to, as requested.
    #[serde(rename = "in")]
    within: Option<String>,
    total_items: i64,
    page: u64,
    per_page: u64,
    items: Vec<Area>,
}

fn query_areas(conn: &Connection, q: &AreasQuery) -> Result<AreasPage> {
    let offset = (q.page - 1) * q.per_page;
    let scope = q.within.as_deref();
    let mut stmt = conn
        .prepare(&areas_sql(q.level, q.sort, q.order, scope.is_some()))
        .context("Failed to prepare area query")?;
    let mut total_items = None;
    let mut params: Vec<&dyn duckdb::ToSql> = Vec::new();
    if let Some(code) = &scope {
        params.extend([code as &dyn duckdb::ToSql, code]);
    }
    let (limit, offset) = (q.per_page as i64, offset as i64);
    params.extend([&limit as &dyn duckdb::ToSql, &offset]);
    let rows = stmt
        .query_map(params.as_slice(), |r| {
            let area = Area {
                teryt: r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                name: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                teryt_gmina: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                gmina: r.get::<_, Option<String>>(3)?.unwrap_or_default(),
                total: r.get(4)?,
                unmatched: r.get(5)?,
                bbox: match (r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?) {
                    (Some(x0), Some(y0), Some(x1), Some(y1)) => Some([x0, y0, x1, y1]),
                    _ => None,
                },
            };
            Ok((area, r.get::<_, i64>(10)?))
        })
        .context("Failed to run area query")?;
    let mut items = Vec::new();
    for row in rows {
        let (area, n) = row.context("Failed to read area row")?;
        total_items = Some(n);
        items.push(area);
    }
    let total_items = match total_items {
        Some(n) => n,
        None => {
            let sql = area_count_sql(q.level, scope.is_some());
            conn.query_row(&sql, duckdb::params_from_iter(scope), |r| r.get(0))
                .with_context(|| format!("stats query failed: {sql}"))?
        }
    };
    Ok(AreasPage {
        level: q.level.as_str(),
        sort: q.sort.as_str(),
        order: q.order.as_str(),
        within: q.within.clone(),
        total_items,
        page: q.page,
        per_page: q.per_page,
        items,
    })
}

#[derive(Debug, Deserialize, Default)]
pub struct AreasParams {
    pub level: Option<String>,
    pub sort: Option<String>,
    pub order: Option<String>,
    #[serde(rename = "in")]
    pub within: Option<String>,
    pub page: Option<String>,
    pub per_page: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AreasQuery {
    pub level: Level,
    pub sort: SortKey,
    pub order: SortOrder,
    /// A TERC code to narrow the ranking to: 2, 4 or 7 digits.
    pub within: Option<String>,
    pub page: u64,
    pub per_page: u64,
}

/// A missing or blank parameter takes its default, as `updates::parse_minutes`
/// does.
fn param(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

fn parse_count(name: &str, value: &Option<String>, default: u64, max: u64) -> Result<u64, String> {
    let Some(s) = param(value) else {
        return Ok(default);
    };
    let n: u64 = s
        .parse()
        .map_err(|_| format!("{name} value '{s}' is not a positive integer"))?;
    if n == 0 {
        return Err(format!("{name} must be at least 1"));
    }
    if n > max {
        return Err(format!("{name} {n} exceeds maximum {max}"));
    }
    Ok(n)
}

pub fn parse_areas_params(p: &AreasParams) -> Result<AreasQuery, String> {
    let level = match param(&p.level) {
        None | Some("gmina") => Level::Gmina,
        Some("settlement") => Level::Settlement,
        Some(other) => return Err(format!("level must be gmina or settlement, not '{other}'")),
    };
    let sort = match param(&p.sort) {
        None | Some("ratio") => SortKey::Ratio,
        Some("unmatched") => SortKey::Unmatched,
        Some(other) => return Err(format!("sort must be ratio or unmatched, not '{other}'")),
    };
    let order = match param(&p.order) {
        None | Some("desc") => SortOrder::Desc,
        Some("asc") => SortOrder::Asc,
        Some(other) => return Err(format!("order must be desc or asc, not '{other}'")),
    };
    let within = match param(&p.within) {
        None => None,
        Some(code)
            if matches!(code.len(), 2 | 4 | 7) && code.bytes().all(|b| b.is_ascii_digit()) =>
        {
            Some(code.to_string())
        }
        Some(other) => {
            return Err(format!(
                "in must be a TERC code of 2, 4 or 7 digits, not '{other}'"
            ));
        }
    };
    Ok(AreasQuery {
        level,
        sort,
        order,
        within,
        page: parse_count("page", &p.page, 1, MAX_PAGE)?,
        per_page: parse_count("per_page", &p.per_page, DEFAULT_PER_PAGE, MAX_PER_PAGE)?,
    })
}

// --- Units (GET /stats/units) --------------------------------------------

/// One row of `teryt_units`. The level is not sent: it is the code's length
/// (2 województwo, 4 powiat, 7 gmina).
#[derive(Serialize, Debug, PartialEq)]
struct Unit {
    code: String,
    name: String,
}

#[derive(Serialize, Debug)]
struct Units {
    units: Vec<Unit>,
}

/// Every unit, by code. Empty on a database whose PRG was loaded before
/// `teryt_units` existed; see `import::teryt`.
fn query_units(conn: &Connection) -> Result<Units> {
    let mut stmt = conn.prepare("SELECT code, name FROM teryt_units ORDER BY code")?;
    let rows = stmt.query_map([], |r| {
        Ok(Unit {
            code: r.get(0)?,
            name: r.get(1)?,
        })
    })?;
    Ok(Units {
        units: rows
            .collect::<duckdb::Result<Vec<_>>>()
            .context("Failed to read teryt_units")?,
    })
}

// --- Handlers ------------------------------------------------------------

/// Runs `build` on a pooled connection off the async runtime and wraps its
/// JSON as a 200 carrying the stats `Cache-Control`; any failure is a bare
/// 500, which inherits the outer `no-store`.
async fn json_from_db(
    state: &AppState,
    what: &'static str,
    build: impl FnOnce(&Connection) -> Result<String> + Send + 'static,
) -> Response {
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<String> {
        let conn = pool.get().context("Failed to acquire pool connection")?;
        build(&conn)
    })
    .await;
    match result {
        Ok(Ok(body)) => (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                ),
                (header::CACHE_CONTROL, state.cache_headers.stats.clone()),
            ],
            body,
        )
            .into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %format!("{e:#}"), what, "stats query failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
        Err(e) => {
            tracing::error!(error = %e, what, "stats task panicked");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

pub async fn get_stats(State(state): State<AppState>) -> Response {
    json_from_db(&state, "summary", |conn| {
        Ok(serde_json::to_string(&build_summary(conn)?)?)
    })
    .await
}

pub async fn get_areas(
    State(state): State<AppState>,
    Query(params): Query<AreasParams>,
) -> Response {
    let q = match parse_areas_params(&params) {
        Ok(q) => q,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    json_from_db(&state, "areas", move |conn| {
        Ok(serde_json::to_string(&query_areas(conn, &q)?)?)
    })
    .await
}

pub async fn get_units(State(state): State<AppState>) -> Response {
    json_from_db(&state, "units", |conn| {
        Ok(serde_json::to_string(&query_units(conn)?)?)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::path::Path;
    use tower::ServiceExt;

    const G1: &str = "0000011";
    const G2: &str = "0000022";
    const G3: &str = "0000033";

    /// `(simc, name, terc, gmina, addresses, unmatched)`; each line is one
    /// settlement-level row.
    ///
    /// - 1000001 is published under two spellings, so it is two rows.
    /// - 1000003 spans G2 and G3, so it is two rows as well.
    /// - 1000002 and 1000005 tie on both ratio and unmatched; only the key
    ///   columns order them.
    /// - G1 and G2 share the name "Alfa", as a town and its rural gmina do.
    const ROWS: &[(&str, &str, &str, &str, usize, usize)] = &[
        ("1000001", "Alfa", G1, "Alfa", 3, 2),
        ("1000001", "Alpha", G1, "Alfa", 1, 0),
        ("1000002", "Wieś", G2, "Alfa", 2, 1),
        ("1000003", "Split", G2, "Alfa", 3, 0),
        ("1000003", "Split", G3, "Beta", 1, 1),
        ("1000004", "Pusta", G3, "Beta", 2, 0),
        ("1000005", "Kopia", G3, "Beta", 2, 1),
    ];
    const PRG_TOTAL: i64 = 14;
    const PRG_UNMATCHED: i64 = 5;

    fn seeded_conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = crate::db::init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_addresses (
                 teryt_miejscowosc VARCHAR, miejscowosc VARCHAR,
                 teryt_gmina VARCHAR, gmina VARCHAR, geom GEOMETRY);
             CREATE TABLE bdot10k_buildings (KATEGORIAISTNIENIA VARCHAR);
             CREATE TABLE egib_buildings (id_budynku VARCHAR);
             INSERT INTO bdot10k_buildings VALUES
                 ('eksploatowany'), ('eksploatowany'), ('eksploatowany'), ('w budowie');
             INSERT INTO bdot10k_unmatched (LOKALNYID) VALUES ('b1');
             INSERT INTO egib_buildings VALUES ('e1'), ('e2');
             INSERT INTO osm_addresses (osm_id, osm_type) VALUES
                 (1, 'node'), (2, 'node'), (3, 'way');
             INSERT INTO osm_buildings (osm_id, osm_type, building) VALUES
                 (1, 'way', 'yes'), (2, 'way', 'house'), (3, 'way', 'yes'),
                 (4, 'relation', 'yes');
             INSERT INTO osm_former_buildings (osm_id, osm_type) VALUES (5, 'way');
             INSERT INTO metadata VALUES
                 ('osm_replication_timestamp', '2026-09-27T10:00:00Z'),
                 ('imported_at:prg', '2026-09-20 12:00:00+00');
             INSERT INTO dataset_refreshes VALUES
                 (1, 'bdot10k', '2026-09-26 03:00:00+00', '2026-09-26 03:05:00+00',
                  NULL, 0, 0, 0);
             INSERT INTO object_reports (report_id, source, status) VALUES
                 (1, 'bdot10k', 'active'), (2, 'bdot10k', 'revoked'), (3, 'egib', 'active');",
        )
        .unwrap();
        // Row r's i-th address sits at (20 + r, 50 + i/1000), so every area's
        // extent is known exactly.
        for (r, &(simc, name, terc, gmina, n, unmatched)) in ROWS.iter().enumerate() {
            for i in 0..n {
                conn.execute(
                    "INSERT INTO prg_addresses VALUES (?, ?, ?, ?, ST_Point(?, ?))",
                    duckdb::params![
                        simc,
                        name,
                        terc,
                        gmina,
                        20.0 + r as f64,
                        50.0 + i as f64 / 1000.0
                    ],
                )
                .unwrap();
                if i < unmatched {
                    conn.execute(
                        "INSERT INTO prg_unmatched (teryt_miejscowosc, miejscowosc, teryt_gmina, gmina)
                         VALUES (?, ?, ?, ?)",
                        duckdb::params![simc, name, terc, gmina],
                    )
                    .unwrap();
                }
            }
        }
        conn
    }

    fn summary_json(conn: &Connection) -> serde_json::Value {
        serde_json::to_value(build_summary(conn).unwrap()).unwrap()
    }

    fn q(level: Level, sort: SortKey, order: SortOrder, page: u64, per_page: u64) -> AreasQuery {
        AreasQuery {
            level,
            sort,
            order,
            within: None,
            page,
            per_page,
        }
    }

    /// `(simc, name, terc, gmina, addresses, unmatched)` in two województwa
    /// and three powiaty, added on top of `ROWS` (whose codes all start
    /// `0000`) by `scoped_conn`.
    const SCOPED_ROWS: &[(&str, &str, &str, &str, usize, usize)] = &[
        ("2000001", "Miasto", "0201011", "Miasto", 4, 1),
        ("2000002", "Dwór", "0201022", "Miasto", 2, 2),
        ("2000003", "Folwark", "0201022", "Miasto", 1, 0),
        ("2000004", "Inny", "0202013", "Inny", 3, 0),
        ("2000005", "Daleki", "0401011", "Daleki", 5, 5),
    ];

    fn scoped_conn() -> Connection {
        let conn = seeded_conn();
        for &(simc, name, terc, gmina, n, unmatched) in SCOPED_ROWS {
            for i in 0..n {
                conn.execute(
                    "INSERT INTO prg_addresses VALUES (?, ?, ?, ?, ST_Point(18, 51))",
                    duckdb::params![simc, name, terc, gmina],
                )
                .unwrap();
                if i < unmatched {
                    conn.execute(
                        "INSERT INTO prg_unmatched (teryt_miejscowosc, miejscowosc, teryt_gmina, gmina)
                         VALUES (?, ?, ?, ?)",
                        duckdb::params![simc, name, terc, gmina],
                    )
                    .unwrap();
                }
            }
        }
        conn
    }

    /// `(teryt, total, unmatched)` of every area of `level` inside `code`,
    /// by code.
    fn within(conn: &Connection, level: Level, code: &str) -> Vec<(String, i64, i64)> {
        let mut query = q(level, SortKey::Ratio, SortOrder::Desc, 1, MAX_PER_PAGE);
        query.within = Some(code.into());
        let page = query_areas(conn, &query).unwrap();
        assert_eq!(page.total_items, page.items.len() as i64);
        let mut areas: Vec<_> = page
            .items
            .into_iter()
            .map(|a| (a.teryt, a.total, a.unmatched))
            .collect();
        areas.sort();
        areas
    }

    fn area(teryt: &str, total: i64, unmatched: i64) -> (String, i64, i64) {
        (teryt.into(), total, unmatched)
    }

    #[test]
    fn a_unit_code_narrows_the_ranking_to_what_lies_inside_it() {
        let conn = scoped_conn();
        // Województwo 02: three gminy in two powiaty.
        assert_eq!(
            within(&conn, Level::Gmina, "02"),
            [
                area("0201011", 4, 1),
                area("0201022", 3, 2),
                area("0202013", 3, 0)
            ]
        );
        // Powiat 0201.
        assert_eq!(
            within(&conn, Level::Gmina, "0201"),
            [area("0201011", 4, 1), area("0201022", 3, 2)]
        );
        // A gmina is one row at its own level and its settlements at the other.
        assert_eq!(
            within(&conn, Level::Gmina, "0201022"),
            [area("0201022", 3, 2)]
        );
        assert_eq!(
            within(&conn, Level::Settlement, "0201022"),
            [area("2000002", 2, 2), area("2000003", 1, 0)]
        );
        assert_eq!(
            within(&conn, Level::Settlement, "0201011"),
            [area("2000001", 4, 1)]
        );
        // A code nothing lies inside is an empty ranking, not an error.
        assert_eq!(within(&conn, Level::Gmina, "99"), []);
    }

    #[test]
    fn a_narrowed_ranking_pages_and_counts_only_its_own_areas() {
        let conn = scoped_conn();
        let page = |page| {
            let mut query = q(Level::Gmina, SortKey::Unmatched, SortOrder::Desc, page, 2);
            query.within = Some("02".into());
            query_areas(&conn, &query).unwrap()
        };
        let teryts = |p: &AreasPage| p.items.iter().map(|a| a.teryt.clone()).collect::<Vec<_>>();
        assert_eq!(teryts(&page(1)), ["0201022", "0201011"]);
        assert_eq!(teryts(&page(2)), ["0202013"]);
        // Past the end: no row to read the count from, so it is counted
        // separately -- inside the unit, not nationally.
        let past_end = page(3);
        assert!(past_end.items.is_empty());
        assert_eq!(
            (
                page(1).total_items,
                page(2).total_items,
                past_end.total_items
            ),
            (3, 3, 3)
        );
    }

    /// Every area of a level, in the order of `sort`/`order`.
    fn all(conn: &Connection, level: Level, sort: SortKey, order: SortOrder) -> Vec<Area> {
        query_areas(conn, &q(level, sort, order, 1, MAX_PER_PAGE))
            .unwrap()
            .items
    }

    /// `(teryt, name, teryt_gmina)` of each area, which identifies a row at
    /// either level.
    fn ids(areas: &[Area]) -> Vec<(String, String, String)> {
        areas
            .iter()
            .map(|a| (a.teryt.clone(), a.name.clone(), a.teryt_gmina.clone()))
            .collect()
    }

    fn id(teryt: &str, name: &str, teryt_gmina: &str) -> (String, String, String) {
        (teryt.into(), name.into(), teryt_gmina.into())
    }

    #[test]
    fn the_summary_counts_comparable_and_unmatched_per_source() {
        let v = summary_json(&seeded_conn());
        let datasets = v["datasets"].as_array().unwrap();
        let by_source = |s: &str| datasets.iter().find(|d| d["source"] == s).unwrap();

        assert_eq!(by_source("prg")["comparable"], PRG_TOTAL);
        assert_eq!(by_source("prg")["unmatched"], PRG_UNMATCHED);
        // The `w budowie` row is outside the comparison, as the match rule has it.
        assert_eq!(by_source("bdot10k")["comparable"], 3);
        assert_eq!(by_source("bdot10k")["unmatched"], 1);
        assert_eq!(by_source("egib")["comparable"], 2);
        assert_eq!(by_source("egib")["unmatched"], 0);

        assert_eq!(by_source("prg")["imported_at"], "2026-09-20T12:00:00Z");
        assert!(by_source("prg")["refreshed_at"].is_null());
        assert!(by_source("bdot10k")["imported_at"].is_null());
        assert_eq!(by_source("bdot10k")["refreshed_at"], "2026-09-26T03:05:00Z");
        assert!(by_source("egib")["imported_at"].is_null());
        assert!(by_source("egib")["refreshed_at"].is_null());
    }

    #[test]
    fn the_summary_counts_osm_objects_and_reports() {
        let v = summary_json(&seeded_conn());
        let osm = &v["osm"];
        assert_eq!(osm["addresses"], serde_json::json!({"node": 2, "way": 1}));
        assert_eq!(
            osm["buildings"],
            serde_json::json!({"relation": 1, "way": 3})
        );
        assert_eq!(osm["former_buildings"], 1);
        assert_eq!(
            osm["top_building_values"],
            serde_json::json!([{"value": "yes", "count": 3}, {"value": "house", "count": 1}])
        );
        assert_eq!(osm["replication_timestamp"], "2026-09-27T10:00:00Z");
        assert!(osm["lag_seconds"].as_i64().is_some());
        assert_eq!(
            v["reports"],
            serde_json::json!([
                {"source": "bdot10k", "status": "active", "count": 1},
                {"source": "bdot10k", "status": "revoked", "count": 1},
                {"source": "egib", "status": "active", "count": 1},
            ])
        );
    }

    #[test]
    fn both_levels_sum_to_the_national_prg_figures() {
        let conn = seeded_conn();
        for (level, rows) in [(Level::Gmina, 3), (Level::Settlement, ROWS.len())] {
            let areas = all(&conn, level, SortKey::Ratio, SortOrder::Desc);
            assert_eq!(areas.len(), rows);
            let total: i64 = areas.iter().map(|a| a.total).sum();
            let unmatched: i64 = areas.iter().map(|a| a.unmatched).sum();
            assert_eq!((total, unmatched), (PRG_TOTAL, PRG_UNMATCHED));
        }
    }

    #[test]
    fn a_settlement_split_across_gminy_is_a_row_per_gmina() {
        let conn = seeded_conn();
        let settlements = all(&conn, Level::Settlement, SortKey::Ratio, SortOrder::Desc);
        let split: Vec<_> = settlements
            .iter()
            .filter(|a| a.teryt == "1000003")
            .collect();
        assert_eq!(split.len(), 2);
        let in_g2 = split.iter().find(|a| a.teryt_gmina == G2).unwrap();
        let in_g3 = split.iter().find(|a| a.teryt_gmina == G3).unwrap();
        assert_eq!(
            (in_g2.gmina.as_str(), in_g2.total, in_g2.unmatched),
            ("Alfa", 3, 0)
        );
        assert_eq!(
            (in_g3.gmina.as_str(), in_g3.total, in_g3.unmatched),
            ("Beta", 1, 1)
        );

        // The gmina level counts each half in its own gmina.
        let gminy = all(&conn, Level::Gmina, SortKey::Ratio, SortOrder::Desc);
        let g2 = gminy.iter().find(|a| a.teryt == G2).unwrap();
        let g3 = gminy.iter().find(|a| a.teryt == G3).unwrap();
        assert_eq!((g2.total, g2.unmatched), (5, 1));
        assert_eq!((g3.total, g3.unmatched), (5, 2));
        assert_eq!((g3.name.as_str(), g3.teryt_gmina.as_str()), ("Beta", G3));
    }

    #[test]
    fn an_area_s_extent_covers_all_of_its_addresses() {
        let conn = seeded_conn();
        let settlements = all(&conn, Level::Settlement, SortKey::Ratio, SortOrder::Desc);
        // ROWS[0] has 3 addresses at lon 20.
        let alfa = settlements.iter().find(|a| a.name == "Alfa").unwrap();
        assert_eq!(alfa.bbox, Some([20.0, 50.0, 20.0, 50.002]));
        // G3 holds ROWS[4..=6]; the widest of them has 2 addresses.
        let gminy = all(&conn, Level::Gmina, SortKey::Ratio, SortOrder::Desc);
        let g3 = gminy.iter().find(|a| a.teryt == G3).unwrap();
        assert_eq!(g3.bbox, Some([24.0, 50.0, 26.0, 50.001]));
    }

    #[test]
    fn rankings_order_by_the_chosen_key_then_the_key_columns() {
        let conn = seeded_conn();
        let s = |sort, order| ids(&all(&conn, Level::Settlement, sort, order));
        // Ratios: Split/G3 1/1, Alfa 2/3, Wieś 1/2, Kopia 1/2, then three at 0.
        assert_eq!(
            s(SortKey::Ratio, SortOrder::Desc),
            [
                id("1000003", "Split", G3),
                id("1000001", "Alfa", G1),
                id("1000002", "Wieś", G2),
                id("1000005", "Kopia", G3),
                id("1000001", "Alpha", G1),
                id("1000003", "Split", G2),
                id("1000004", "Pusta", G3),
            ]
        );
        // Only the chosen key flips; ties keep the key columns ascending.
        assert_eq!(
            s(SortKey::Ratio, SortOrder::Asc),
            [
                id("1000001", "Alpha", G1),
                id("1000003", "Split", G2),
                id("1000004", "Pusta", G3),
                id("1000002", "Wieś", G2),
                id("1000005", "Kopia", G3),
                id("1000001", "Alfa", G1),
                id("1000003", "Split", G3),
            ]
        );
        assert_eq!(
            s(SortKey::Unmatched, SortOrder::Desc),
            [
                id("1000001", "Alfa", G1),
                id("1000002", "Wieś", G2),
                id("1000003", "Split", G3),
                id("1000005", "Kopia", G3),
                id("1000001", "Alpha", G1),
                id("1000003", "Split", G2),
                id("1000004", "Pusta", G3),
            ]
        );
        // G1 2/4, G3 2/5, G2 1/5. By count G1 and G3 tie and the code orders
        // them.
        let g = |sort, order| {
            all(&conn, Level::Gmina, sort, order)
                .into_iter()
                .map(|a| a.teryt)
                .collect::<Vec<_>>()
        };
        assert_eq!(g(SortKey::Ratio, SortOrder::Desc), [G1, G3, G2]);
        assert_eq!(g(SortKey::Ratio, SortOrder::Asc), [G2, G3, G1]);
        assert_eq!(g(SortKey::Unmatched, SortOrder::Desc), [G1, G3, G2]);
        assert_eq!(g(SortKey::Unmatched, SortOrder::Asc), [G2, G1, G3]);
    }

    #[test]
    fn pages_partition_every_ranking() {
        let conn = seeded_conn();
        for level in [Level::Gmina, Level::Settlement] {
            for sort in [SortKey::Ratio, SortKey::Unmatched] {
                for order in [SortOrder::Desc, SortOrder::Asc] {
                    let whole = ids(&all(&conn, level, sort, order));
                    for per_page in 1..=4 {
                        let mut paged = Vec::new();
                        for page in 1.. {
                            let p =
                                query_areas(&conn, &q(level, sort, order, page, per_page)).unwrap();
                            assert_eq!(p.total_items, whole.len() as i64);
                            if p.items.is_empty() {
                                break;
                            }
                            paged.extend(ids(&p.items));
                        }
                        assert_eq!(paged, whole, "{level:?} {sort:?} {order:?} by {per_page}");
                    }
                }
            }
        }
    }

    #[test]
    fn a_page_past_the_end_is_empty_but_still_counts_the_areas() {
        let conn = seeded_conn();
        let p = query_areas(
            &conn,
            &q(
                Level::Settlement,
                SortKey::Ratio,
                SortOrder::Desc,
                MAX_PAGE,
                MAX_PER_PAGE,
            ),
        )
        .unwrap();
        assert!(p.items.is_empty());
        assert_eq!(p.total_items, ROWS.len() as i64);
    }

    #[test]
    fn an_area_known_only_from_prg_unmatched_is_left_out() {
        let conn = seeded_conn();
        conn.execute_batch(
            "INSERT INTO prg_unmatched (teryt_miejscowosc, miejscowosc, teryt_gmina, gmina)
             VALUES ('9999999', 'Stale', '9999993', 'Stale')",
        )
        .unwrap();
        let settlements = all(&conn, Level::Settlement, SortKey::Ratio, SortOrder::Desc);
        assert_eq!(settlements.len(), ROWS.len());
        assert!(settlements.iter().all(|a| a.total > 0));
    }

    /// Pins what `areas_sql`'s `ST_Extent_Agg` note relies on: group-key
    /// strings come through it intact. The size is what reproduced the bug
    /// for a `histogram` over the same names (1,176 of these 5,000 wrong):
    /// a parallel aggregation over names longer than DuckDB's 12-byte inline
    /// limit.
    #[test]
    fn long_names_survive_the_ranking_query_at_scale() {
        let conn = seeded_conn();
        conn.execute_batch(
            "SET threads = 4;
             DELETE FROM prg_addresses;
             DELETE FROM prg_unmatched;
             INSERT INTO prg_addresses
             SELECT lpad((i % 5000)::VARCHAR, 7, '0'),
                    'Miejscowość numer ' || (i % 5000) || ' długa nazwa',
                    lpad((i % 50)::VARCHAR, 6, '0') || '2',
                    'Gmina numer ' || (i % 50) || ' długa nazwa',
                    ST_Point(20 + (i % 7) / 10, 50 + (i % 11) / 10)
             FROM range(100000) t(i);",
        )
        .unwrap();
        let mut settlements = Vec::new();
        for page in 1..=25 {
            let p = query_areas(
                &conn,
                &q(
                    Level::Settlement,
                    SortKey::Ratio,
                    SortOrder::Desc,
                    page,
                    MAX_PER_PAGE,
                ),
            )
            .unwrap();
            settlements.extend(p.items);
        }
        assert_eq!(settlements.len(), 5000);
        let wrong: Vec<_> = settlements
            .iter()
            .filter(|s| {
                let n: u64 = s.teryt.parse().unwrap();
                s.name != format!("Miejscowość numer {n} długa nazwa")
                    || s.gmina != format!("Gmina numer {} długa nazwa", n % 50)
            })
            .collect();
        assert!(
            wrong.is_empty(),
            "{} names corrupted, e.g. {:?}",
            wrong.len(),
            wrong.first()
        );
    }

    #[test]
    fn params_default_and_reject_bad_values() {
        assert_eq!(
            parse_areas_params(&AreasParams::default()).unwrap(),
            q(
                Level::Gmina,
                SortKey::Ratio,
                SortOrder::Desc,
                1,
                DEFAULT_PER_PAGE
            )
        );
        let with = |f: fn(&mut AreasParams)| {
            let mut p = AreasParams::default();
            f(&mut p);
            parse_areas_params(&p)
        };
        assert_eq!(
            with(|p| p.level = Some("settlement".into())).unwrap().level,
            Level::Settlement
        );
        assert!(with(|p| p.level = Some("powiat".into())).is_err());
        for code in ["02", "0201", "0201011"] {
            let q = parse_areas_params(&AreasParams {
                within: Some(code.into()),
                ..Default::default()
            });
            assert_eq!(q.unwrap().within.as_deref(), Some(code));
        }
        // Not a TERC length, not digits, or text that would otherwise reach
        // the query.
        for bad in ["0", "020", "02010", "02010111", "ab", "02%", "０２"] {
            let q = parse_areas_params(&AreasParams {
                within: Some(bad.into()),
                ..Default::default()
            });
            assert!(q.is_err(), "{bad}");
        }
        assert_eq!(with(|p| p.within = Some(" ".into())).unwrap().within, None);
        assert!(with(|p| p.sort = Some("name".into())).is_err());
        assert!(with(|p| p.order = Some("up".into())).is_err());
        assert!(with(|p| p.page = Some("0".into())).is_err());
        assert!(with(|p| p.page = Some("x".into())).is_err());
        assert!(with(|p| p.page = Some("1000001".into())).is_err());
        assert!(with(|p| p.per_page = Some("201".into())).is_err());
        assert_eq!(
            with(|p| p.per_page = Some(" ".into())).unwrap().per_page,
            DEFAULT_PER_PAGE
        );
    }

    fn state() -> AppState {
        AppState::for_tests(crate::server::build_pool(seeded_conn(), 4).unwrap())
    }

    async fn get(state: AppState, uri: &str) -> (StatusCode, String, serde_json::Value) {
        let response = crate::server::build_router(state)
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let cache_control = response.headers()[header::CACHE_CONTROL]
            .to_str()
            .unwrap()
            .to_string();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            cache_control,
            serde_json::from_slice(&body).unwrap(),
        )
    }

    #[tokio::test]
    async fn stats_endpoint_serves_the_summary_with_the_stats_max_age() {
        let (status, cache_control, body) = get(state(), "/stats").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cache_control, "public, max-age=300");
        assert_eq!(body["datasets"][0]["source"], "prg");
    }

    #[tokio::test]
    async fn areas_endpoint_pages_the_ranking() {
        let (status, cache_control, body) = get(
            state(),
            "/stats/areas?level=settlement&sort=unmatched&per_page=2&page=2",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cache_control, "public, max-age=300");
        assert_eq!(body["total_items"], 7);
        assert_eq!(body["level"], "settlement");
        let names: Vec<&str> = body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["Split", "Kopia"]);

        let (_, _, past_end) = get(state(), "/stats/areas?page=9").await;
        assert_eq!(past_end["items"], serde_json::json!([]));
        assert_eq!(past_end["total_items"], 3);
    }

    #[tokio::test]
    async fn areas_endpoint_narrows_to_a_unit() {
        let state = AppState::for_tests(crate::server::build_pool(scoped_conn(), 4).unwrap());
        let (status, _, body) = get(state.clone(), "/stats/areas?in=0201").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["in"], "0201");
        assert_eq!(body["total_items"], 2);

        let (_, _, whole) = get(state.clone(), "/stats/areas").await;
        assert!(whole["in"].is_null());
        assert_eq!(whole["total_items"], 7);

        let (status, cache_control, _) = get(state, "/stats/areas?in=020").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(cache_control, "no-store");
    }

    #[tokio::test]
    async fn units_endpoint_serves_the_whole_catalogue_by_code() {
        let conn = seeded_conn();
        conn.execute_batch(
            "INSERT INTO teryt_units VALUES
                 ('0201011', 'gmina', 'Bolesławiec'),
                 ('02', 'wojewodztwo', 'dolnośląskie'),
                 ('0201', 'powiat', 'bolesławiecki')",
        )
        .unwrap();
        let state = AppState::for_tests(crate::server::build_pool(conn, 4).unwrap());
        let (status, cache_control, body) = get(state, "/stats/units").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cache_control, "public, max-age=300");
        assert_eq!(
            body,
            serde_json::json!({"units": [
                {"code": "02", "name": "dolnośląskie"},
                {"code": "0201", "name": "bolesławiecki"},
                {"code": "0201011", "name": "Bolesławiec"},
            ]})
        );
    }

    #[tokio::test]
    async fn units_endpoint_is_an_empty_list_before_the_first_prg_load() {
        let (status, _, body) = get(state(), "/stats/units").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"units": []}));
    }

    #[tokio::test]
    async fn a_bad_parameter_is_a_400_that_is_not_cached() {
        let (status, cache_control, body) = get(state(), "/stats/areas?sort=name").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(cache_control, "no-store");
        assert!(body["error"].as_str().unwrap().contains("sort"));
    }
}
