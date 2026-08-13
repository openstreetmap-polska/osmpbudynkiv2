//! Per-source metadata and the shared row-hash SQL used by both the import
//! and the update paths.
//!
//! The row hash is computed by hashing a whole-row reference over a subquery
//! alias rather than an explicit column list:
//!
//! ```sql
//! SELECT *, hash(s) AS _row_hash FROM (<inner select>) s
//! ```
//!
//! `hash(s)` hashes every column of `s` including `GEOMETRY`, and `s`
//! deliberately does not contain `_row_hash`, so the hash is never
//! self-referential. Because there is no column list to maintain, a source
//! gaining or losing a column cannot silently desynchronize the import and
//! update expressions.

/// Bumped whenever the hashed row content changes for any source.
///
/// That is [`hashed_select`] itself, but also anything feeding *into* it: a
/// source's inner select is part of the hash input, so a change there moves
/// the stored hashes just as surely (version 2 is such a case — PRG's import
/// gained a street-name normalization inside its inner select, see
/// `import::prg::ULICA_PREFIX_STRIP_SQL`). Transformations deliberately
/// wrapped *outside* `hashed_select` — [`DatasetSpec::with_centroid_select`],
/// `mappings::egib::with_rodzaj_kod_select` — do not count.
///
/// The value in force when a live table was built is stamped into
/// `metadata.row_hash_version` by [`stamp_row_hash_version`]. A mismatch
/// against this constant means the stored `_row_hash` values were produced by
/// a different expression, so every row will compare as modified; the refresh
/// warns, rewrites the table wholesale, and re-stamps — so the warning fires
/// once per bump rather than forever. The stamp is global, so a bump made for
/// one source also costs the others one full-rewrite refresh apiece.
pub const ROW_HASH_VERSION: i64 = 2;
pub const ROW_HASH_VERSION_KEY: &str = "row_hash_version";

/// Record that the live dataset tables were built with the current
/// [`ROW_HASH_VERSION`].
///
/// Called by every path that (re)builds a live table's `_row_hash` column
/// wholesale: a full `import`, and the full-rewrite refresh that follows a
/// version bump. Anything that rewrites only a delta must NOT call this — the
/// untouched rows would still carry the old expression's hashes.
pub fn stamp_row_hash_version(conn: &duckdb::Connection) -> anyhow::Result<()> {
    use anyhow::Context;
    // Both interpolated values are compile-time constants of this crate.
    conn.execute_batch(&format!(
        "DELETE FROM metadata WHERE key = '{ROW_HASH_VERSION_KEY}';
         INSERT INTO metadata VALUES ('{ROW_HASH_VERSION_KEY}', '{ROW_HASH_VERSION}');"
    ))
    .context("Failed to record row hash version")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeomKind {
    /// Geometry is already a point; use it directly.
    Point,
    /// Geometry is an area; use its centroid as the representative point.
    Polygon,
}

#[derive(Debug, Clone, Copy)]
pub struct DatasetSpec {
    /// Short source name, used in CLI output, job names and changeset rows.
    pub name: &'static str,
    /// The live table this source owns.
    pub table: &'static str,
    /// Stable per-object identifier. NOT unique — BDOT10k has duplicate IDs,
    /// so the diff compares an ID's whole row-set, never row to row.
    pub id_column: &'static str,
    pub geom_kind: GeomKind,
}

impl DatasetSpec {
    /// SQL for the point that represents this object when assigning it to a
    /// change cell. `alias` is the table alias in the surrounding query
    /// (e.g. `"l"` for the live table, `"s"` for staging). For `Point`
    /// sources the geometry itself is the point; for `Polygon` sources this
    /// reads the persisted `centroid` column (see `with_centroid_select`)
    /// rather than recomputing `ST_Centroid` — the whole reason that column
    /// exists is so this stops being a per-row function call.
    pub fn representative_point_sql(&self, alias: &str) -> String {
        match self.geom_kind {
            GeomKind::Point => format!("{alias}.geom"),
            GeomKind::Polygon => format!("{alias}.centroid"),
        }
    }

    /// Wrap `select_sql` (the output of [`hashed_select`]) so a `Polygon`
    /// source also gains a persisted `centroid GEOMETRY` column, computed
    /// from `geom`. Added OUTSIDE `hashed_select`'s projection deliberately:
    /// `hash(s)` inside `hashed_select` already ran over the inner columns
    /// only, so wrapping here cannot change any row's `_row_hash` and needs
    /// no `ROW_HASH_VERSION` bump. A no-op passthrough for `Point` sources
    /// (PRG), which have no separate centroid to store.
    pub fn with_centroid_select(&self, select_sql: &str) -> String {
        match self.geom_kind {
            GeomKind::Point => select_sql.to_string(),
            GeomKind::Polygon => {
                format!("SELECT *, ST_Centroid(geom) AS centroid FROM ({select_sql}) t")
            }
        }
    }

    /// Name of the transient staging table used during a refresh.
    pub fn staging_table(&self) -> String {
        format!("{}__staging", self.table)
    }
}

pub const BDOT10K: DatasetSpec = DatasetSpec {
    name: "bdot10k",
    table: "bdot10k_buildings",
    id_column: "LOKALNYID",
    geom_kind: GeomKind::Polygon,
};

pub const EGIB: DatasetSpec = DatasetSpec {
    name: "egib",
    table: "egib_buildings",
    id_column: "id_budynku",
    geom_kind: GeomKind::Polygon,
};

pub const PRG: DatasetSpec = DatasetSpec {
    name: "prg",
    table: "prg_addresses",
    id_column: "lokalny_id",
    geom_kind: GeomKind::Point,
};

/// Wrap `inner_select` so its result gains a `_row_hash UBIGINT` column.
///
/// This is the ONLY place the hash expression is written. Both the import
/// and the update path call it; if they ever diverge, every row compares as
/// modified on every refresh forever.
pub fn hashed_select(inner_select: &str) -> String {
    format!("SELECT *, hash(s) AS _row_hash FROM ({inner_select}) s")
}

/// Cap on how many skipped-row ids `filter_invalid_geometry` and
/// `filter_oversized_geometry` each collect as examples -- enough to point
/// an operator at the actual bad records upstream, without holding an
/// unbounded list for a source with many bad rows. The returned count is
/// always the true total regardless of this cap.
pub const MAX_EXAMPLE_IDS: usize = 20;

/// Rows a dataset loader dropped rather than staging, for one of two
/// reasons: geometry that failed `ST_IsValid` (`ST_AsMVTGeom` cannot
/// tolerate invalid geometry, see docs/invalid_geometry_tile_500s.md), or
/// geometry whose bbox spans at least one full z14 cell in either axis (see
/// `filter_oversized_geometry` -- a corrupted merge of two unrelated
/// features, not a real building). Both reasons drop rather than repair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadStats {
    pub skipped_invalid_geometry: i64,
    /// First `MAX_EXAMPLE_IDS` ids of skipped rows, in whatever order the
    /// SELECT below finds them -- not exhaustive, just enough to point an
    /// operator at the actual bad records upstream.
    pub skipped_example_ids: Vec<String>,
    pub skipped_oversized_geometry: i64,
    /// Same cap and ordering caveat as `skipped_example_ids`, for the
    /// oversized-geometry reason.
    pub skipped_oversized_example_ids: Vec<String>,
}

impl LoadStats {
    /// Fold in the oversized-geometry counts from a second filter pass, the
    /// way `import::bdot10k::load_into` / `import::egib::load_into` combine
    /// `filter_invalid_geometry`'s result (`self`) with
    /// `filter_oversized_geometry`'s (`oversized`) into the one `LoadStats`
    /// a loader returns. `oversized`'s own invalid-geometry fields are
    /// ignored -- `filter_oversized_geometry` never sets them.
    pub fn merge_oversized(mut self, oversized: LoadStats) -> Self {
        self.skipped_oversized_geometry = oversized.skipped_oversized_geometry;
        self.skipped_oversized_example_ids = oversized.skipped_oversized_example_ids;
        self
    }
}

/// Render one skip-reason clause for a `job_run_log` summary message, shared
/// by every source's `summarize` (`import::bdot10k`, `import::egib`) and by
/// `update::dataset::summarize_refresh`, so the "N rows, ids: ..., +M more"
/// wording is written once rather than once per reason per source.
/// `reason` reads naturally before "rows", e.g. `"invalid-geometry"` or
/// `"oversized-geometry"`.
pub fn format_skip_clause(reason: &str, count: i64, ids: &[String]) -> String {
    let shown = ids.join(", ");
    let more = (count as usize).saturating_sub(ids.len());
    if more > 0 {
        format!("skipped {count} {reason} rows (ids: {shown}, +{more} more)")
    } else {
        format!("skipped {count} {reason} rows (ids: {shown})")
    }
}

/// Delete invalid-geometry rows from a just-loaded table, capturing example
/// ids before they're gone. Shared by `import::bdot10k::load_into` and
/// `import::egib::load_into` -- the one place both `import` and `update`'s
/// staging load funnel through, so a row filtered out here never reaches
/// `compare::buildings` or `compare::incremental` at all.
pub fn filter_invalid_geometry(
    conn: &duckdb::Connection,
    table: &str,
    id_col: &str,
) -> anyhow::Result<LoadStats> {
    use anyhow::Context;

    let mut skipped_example_ids = Vec::new();
    {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {id_col} FROM {table} WHERE NOT ST_IsValid(geom) LIMIT {MAX_EXAMPLE_IDS}"
            ))
            .with_context(|| format!("Failed to prepare invalid-geometry scan on {table}"))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .with_context(|| format!("Failed to scan invalid-geometry rows in {table}"))?;
        for row in rows {
            skipped_example_ids.push(row.context("Failed to read invalid-geometry id")?);
        }
    }

    let skipped_invalid_geometry = conn
        .execute(
            &format!("DELETE FROM {table} WHERE NOT ST_IsValid(geom)"),
            [],
        )
        .with_context(|| format!("Failed to delete invalid-geometry rows from {table}"))?
        as i64;

    Ok(LoadStats {
        skipped_invalid_geometry,
        skipped_example_ids,
        ..Default::default()
    })
}

/// Delete rows whose bbox spans at least one full z14 cell in either axis --
/// too wide to be a single real building, and in practice a corrupted merge
/// of two unrelated features. Discovered via EGIB row
/// `260208_5.0009.315.1_BUD`: a 2-part MULTIPOLYGON, 10 points total, area
/// ~4,700 m^2, whose envelope spans longitude 19.886->20.507 -- two separate
/// real buildings ~44 km apart glued into one record. `ST_IsValid` returns
/// true for it, so `filter_invalid_geometry` above never catches it; it
/// smears across the map, pollutes any `/package` area it clips, and its
/// centroid (what the match rule compares against OSM, see
/// `DatasetSpec::with_centroid_select`) lands in open countryside between
/// the two buildings, matching neither.
///
/// Measured over the full source tables (BDOT10k 16,351,815 rows, EGIB
/// 17,773,961 rows): this drops 0 BDOT10k rows (the longest genuine BDOT10k
/// building measures 0.696 cells, ~1 km -- ~50% headroom under the
/// threshold) and 85 EGIB rows.
///
/// Threshold is in CELL UNITS, not degrees or metres, because the latitude
/// threshold is not constant in degrees under the Web-Mercator Y projection
/// (0.0135 deg at 52N vs 0.0126 deg at 55N) -- cell units keep it one
/// constant tied to `tile_math::CHANGE_CELL_ZOOM`.
///
/// This is an EXTENT test (bbox width in fractional cell units), not a
/// "reach from the row's own cell" test, deliberately:
///  - it's position-independent -- a bbox's extent doesn't change depending
///    on whether it happens to straddle a cell boundary, where a reach
///    test's answer would (see `tile_math`'s
///    `cell_frac_is_unfloored_across_a_cell_boundary`, which is the reason
///    the SQL below reads `ST_XMin`/`ST_XMax` through
///    `tile_math::cell_x_frac_sql` and never through `cell_x_sql`);
///  - it drops the strictly larger, more obviously-corrupt set (a
///    centroid-relative reach test at >= 2 cells caught only 42 of these 85
///    rows -- the multipolygon ones -- and 0 BDOT10k rows either way);
///  - it yields a statable invariant a later phase depends on: a bbox
///    strictly narrower than one cell straddles at most one cell boundary
///    per axis, so it touches at most 2x2 cells, one of which is the
///    centroid's own cell -- so the row's reach from its own cell is <= 1.
///    A later phase's tile-version query uses a 3x3 cell ring whose radius
///    is exactly that invariant.
///
/// The Y comparison (`YMin - YMax`, not `YMax - YMin`) looks inverted, but
/// isn't: cell-Y grows southward, so `ST_YMin` (the geographically
/// southernmost point of the bbox) has the LARGER fractional cell-Y. Read
/// both terms the same way: "fractional cell coordinate of the bbox's far
/// edge minus that of its near edge >= 1".
pub fn filter_oversized_geometry(
    conn: &duckdb::Connection,
    table: &str,
    id_col: &str,
) -> anyhow::Result<LoadStats> {
    use crate::tile_math::{cell_x_frac_sql, cell_y_frac_sql};
    use anyhow::Context;

    let x_extent = format!(
        "(({}) - ({}))",
        cell_x_frac_sql("ST_XMax(geom)"),
        cell_x_frac_sql("ST_XMin(geom)")
    );
    let y_extent = format!(
        "(({}) - ({}))",
        cell_y_frac_sql("ST_YMin(geom)"),
        cell_y_frac_sql("ST_YMax(geom)")
    );
    let predicate = format!("{x_extent} >= 1 OR {y_extent} >= 1");

    let mut skipped_oversized_example_ids = Vec::new();
    {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {id_col} FROM {table} WHERE {predicate} LIMIT {MAX_EXAMPLE_IDS}"
            ))
            .with_context(|| format!("Failed to prepare oversized-geometry scan on {table}"))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .with_context(|| format!("Failed to scan oversized-geometry rows in {table}"))?;
        for row in rows {
            skipped_oversized_example_ids
                .push(row.context("Failed to read oversized-geometry id")?);
        }
    }

    let skipped_oversized_geometry = conn
        .execute(&format!("DELETE FROM {table} WHERE {predicate}"), [])
        .with_context(|| format!("Failed to delete oversized-geometry rows from {table}"))?
        as i64;

    Ok(LoadStats {
        skipped_oversized_geometry,
        skipped_oversized_example_ids,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashed_select_wraps_inner_query() {
        assert_eq!(
            hashed_select("SELECT 1 AS a"),
            "SELECT *, hash(s) AS _row_hash FROM (SELECT 1 AS a) s"
        );
    }

    #[test]
    fn representative_point_reads_the_stored_centroid_for_polygons() {
        assert_eq!(BDOT10K.representative_point_sql("l"), "l.centroid");
        assert_eq!(EGIB.representative_point_sql("s"), "s.centroid");
    }

    #[test]
    fn representative_point_passes_through_for_points() {
        assert_eq!(PRG.representative_point_sql("l"), "l.geom");
    }

    #[test]
    fn with_centroid_select_wraps_polygon_sources_outside_the_hash() {
        let hashed = hashed_select("SELECT 1 AS a, ST_Point(1, 2) AS geom");
        let wrapped = BDOT10K.with_centroid_select(&hashed);
        assert_eq!(
            wrapped,
            format!("SELECT *, ST_Centroid(geom) AS centroid FROM ({hashed}) t")
        );
    }

    #[test]
    fn with_centroid_select_is_a_noop_for_points() {
        let hashed = hashed_select("SELECT 1 AS a, ST_Point(1, 2) AS geom");
        assert_eq!(PRG.with_centroid_select(&hashed), hashed);
    }

    /// The load-bearing invariant from the module doc: adding `centroid` via
    /// `with_centroid_select` must not change `_row_hash`, since it wraps
    /// `hashed_select`'s output rather than feeding into it. If this ever
    /// regresses, every refresh would compare every row as modified forever
    /// (see `ROW_HASH_VERSION`) without anyone bumping the constant.
    #[test]
    fn with_centroid_select_does_not_change_the_row_hash() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id VARCHAR, geom GEOMETRY);
             INSERT INTO src VALUES
                 ('1', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('2', NULL);",
        )
        .unwrap();

        let inner = "SELECT id, geom FROM src";
        let hashed = hashed_select(inner);
        let with_centroid = BDOT10K.with_centroid_select(&hashed);

        conn.execute_batch(&format!(
            "CREATE TABLE plain AS {hashed};
             CREATE TABLE with_centroid AS {with_centroid};"
        ))
        .unwrap();

        let disagreements: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM plain p JOIN with_centroid c USING (id)
                 WHERE p._row_hash IS DISTINCT FROM c._row_hash",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            disagreements, 0,
            "adding centroid outside hashed_select's wrap must not change _row_hash"
        );

        let has_centroid: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM information_schema.columns
                 WHERE table_name = 'with_centroid' AND column_name = 'centroid'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            has_centroid,
            "the wrapped table must actually carry the centroid column"
        );
    }

    #[test]
    fn staging_table_is_derived_from_live_table() {
        assert_eq!(BDOT10K.staging_table(), "bdot10k_buildings__staging");
        assert_eq!(PRG.staging_table(), "prg_addresses__staging");
    }

    /// The hash must actually be computable over a GEOMETRY column and must
    /// agree between two independent evaluations of the same content. This is
    /// the invariant the whole feature rests on.
    #[test]
    fn hash_agrees_across_independent_evaluations() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id VARCHAR, a VARCHAR, lon DOUBLE, lat DOUBLE);
             INSERT INTO src VALUES ('1','x',20.0,52.0), ('2','y',NULL,NULL);",
        )
        .unwrap();

        let inner = "SELECT id, a, ST_Point(lon, lat) AS geom FROM src";
        let sql = format!(
            "CREATE TABLE t1 AS {};
             CREATE TABLE t2 AS {};",
            hashed_select(inner),
            hashed_select(inner)
        );
        conn.execute_batch(&sql).unwrap();

        let disagreements: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM t1 JOIN t2 USING (id)
                 WHERE t1._row_hash IS DISTINCT FROM t2._row_hash",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(disagreements, 0, "same content must hash identically");

        let nulls: i64 = conn
            .query_row("SELECT COUNT(*) FROM t1 WHERE _row_hash IS NULL", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(nulls, 0, "NULL geometry must still produce a hash");
    }

    #[test]
    fn filter_invalid_geometry_drops_only_invalid_rows_and_returns_stats() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('valid', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('bowtie', ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))'));",
        )
        .unwrap();

        let stats = filter_invalid_geometry(&conn, "t", "id").unwrap();

        assert_eq!(stats.skipped_invalid_geometry, 1);
        assert_eq!(stats.skipped_example_ids, vec!["bowtie".to_string()]);

        let remaining: Vec<String> = {
            let mut s = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(remaining, vec!["valid".to_string()]);
    }

    #[test]
    fn filter_invalid_geometry_caps_example_ids_but_counts_all() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch("CREATE TABLE t (id VARCHAR, geom GEOMETRY);")
            .unwrap();
        for i in 0..25 {
            conn.execute(
                "INSERT INTO t VALUES (?, ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))'))",
                duckdb::params![format!("bad{i}")],
            )
            .unwrap();
        }

        let stats = filter_invalid_geometry(&conn, "t", "id").unwrap();

        assert_eq!(stats.skipped_invalid_geometry, 25);
        assert_eq!(stats.skipped_example_ids.len(), MAX_EXAMPLE_IDS);
    }

    #[test]
    fn filter_invalid_geometry_is_a_noop_when_everything_is_valid() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, geom GEOMETRY);
             INSERT INTO t VALUES ('a', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'));",
        )
        .unwrap();

        let stats = filter_invalid_geometry(&conn, "t", "id").unwrap();

        assert_eq!(stats, LoadStats::default());
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    /// The motivating case: EGIB row `260208_5.0009.315.1_BUD` in miniature --
    /// two small squares ~42 km apart (19.875 and 20.5 degrees longitude,
    /// well past the real row's 19.886->20.507), glued into one valid
    /// MULTIPOLYGON. `ST_IsValid` accepts it (each part is a simple,
    /// non-self-intersecting square), so only the extent filter catches it.
    #[test]
    fn filter_oversized_geometry_drops_a_widely_separated_multipolygon() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('glued', ST_GeomFromText(
                     'MULTIPOLYGON(
                          ((19.875 52.0, 19.876 52.0, 19.876 52.001, 19.875 52.001, 19.875 52.0)),
                          ((20.5 52.0, 20.501 52.0, 20.501 52.001, 20.5 52.001, 20.5 52.0))
                      )'
                 ));",
        )
        .unwrap();

        let stats = filter_oversized_geometry(&conn, "t", "id").unwrap();

        assert_eq!(stats.skipped_oversized_geometry, 1);
        assert_eq!(
            stats.skipped_oversized_example_ids,
            vec!["glued".to_string()]
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    /// The other side of the same measurement: BDOT10k's real longest
    /// building is 0.696 cells (~1 km) wide -- ~50% headroom under the
    /// threshold. A single-part building of comparable width (here ~0.68
    /// cells, 0.015 degrees longitude at 52N) must survive.
    #[test]
    fn filter_oversized_geometry_keeps_a_near_one_km_building() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('big_but_real', ST_GeomFromText(
                     'POLYGON((21.0 52.0, 21.015 52.0, 21.015 52.005, 21.0 52.005, 21.0 52.0))'
                 ));",
        )
        .unwrap();

        let stats = filter_oversized_geometry(&conn, "t", "id").unwrap();

        assert_eq!(
            stats,
            LoadStats::default(),
            "must not drop a real-sized building"
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    /// Pins "extent, not reach": a tiny (~27 m wide) building whose bbox
    /// happens to straddle a real z14 cell boundary must be kept. The
    /// boundary and the coordinates are the same ones pinned in
    /// `tile_math::tests::cell_frac_is_unfloored_across_a_cell_boundary`,
    /// which shows the two edges floor to DIFFERENT cell indices even
    /// though they are ~27 m apart. If this filter were built from
    /// `tile_math::cell_x_sql`/`cell_y_sql` (floored indices) instead of
    /// `cell_x_frac_sql`/`cell_y_frac_sql`, `ST_XMax`'s cell index minus
    /// `ST_XMin`'s would read as 1 -- "spans a full cell" -- and this test
    /// would fail with the row deleted.
    #[test]
    fn filter_oversized_geometry_keeps_small_building_straddling_cell_boundary() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('shed', ST_GeomFromText(
                     'POLYGON((20.9837646484375 52.0, 20.9840087890625 52.0,
                                20.9840087890625 52.0001, 20.9837646484375 52.0001,
                                20.9837646484375 52.0))'
                 ));",
        )
        .unwrap();

        let stats = filter_oversized_geometry(&conn, "t", "id").unwrap();

        assert_eq!(
            stats,
            LoadStats::default(),
            "a boundary-straddling shed must not be dropped just for its position"
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn filter_oversized_geometry_caps_example_ids_but_counts_all() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch("CREATE TABLE t (id VARCHAR, geom GEOMETRY);")
            .unwrap();
        for i in 0..25 {
            conn.execute(
                "INSERT INTO t VALUES (?, ST_GeomFromText(
                     'MULTIPOLYGON(
                          ((19.875 52.0, 19.876 52.0, 19.876 52.001, 19.875 52.001, 19.875 52.0)),
                          ((20.5 52.0, 20.501 52.0, 20.501 52.001, 20.5 52.001, 20.5 52.0))
                      )'
                 ))",
                duckdb::params![format!("bad{i}")],
            )
            .unwrap();
        }

        let stats = filter_oversized_geometry(&conn, "t", "id").unwrap();

        assert_eq!(stats.skipped_oversized_geometry, 25);
        assert_eq!(stats.skipped_oversized_example_ids.len(), MAX_EXAMPLE_IDS);
    }

    /// Like `with_centroid_select_does_not_change_the_row_hash`: dropping
    /// oversized rows runs strictly after `hashed_select` built the table,
    /// so a surviving row's `_row_hash` must be bit-for-bit identical to
    /// what it was before the filter ran -- no `ROW_HASH_VERSION` bump
    /// needed. Computed via `hashed_select` directly (not `load_into`) so
    /// this is independent of the BDOT10k/EGIB parquet shape.
    #[test]
    fn filter_oversized_geometry_does_not_change_surviving_row_hashes() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id VARCHAR, geom GEOMETRY);
             INSERT INTO src VALUES
                 ('keep', ST_GeomFromText(
                     'POLYGON((21.0 52.0, 21.001 52.0, 21.001 52.001, 21.0 52.001, 21.0 52.0))'
                 )),
                 ('drop', ST_GeomFromText(
                     'MULTIPOLYGON(
                          ((19.875 52.0, 19.876 52.0, 19.876 52.001, 19.875 52.001, 19.875 52.0)),
                          ((20.5 52.0, 20.501 52.0, 20.501 52.001, 20.5 52.001, 20.5 52.0))
                      )'
                 ));",
        )
        .unwrap();

        let inner = "SELECT id, geom FROM src";
        conn.execute_batch(&format!("CREATE TABLE t AS {}", hashed_select(inner)))
            .unwrap();
        // `_row_hash` is UBIGINT and can exceed i64::MAX, so snapshot it into
        // a second table and compare with SQL (like
        // `with_centroid_select_does_not_change_the_row_hash` does) instead
        // of round-tripping the value through Rust.
        conn.execute_batch("CREATE TABLE hash_before AS SELECT id, _row_hash FROM t")
            .unwrap();

        let stats = filter_oversized_geometry(&conn, "t", "id").unwrap();
        assert_eq!(stats.skipped_oversized_geometry, 1);

        let disagreements: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM hash_before h JOIN t USING (id)
                 WHERE h._row_hash IS DISTINCT FROM t._row_hash",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            disagreements, 0,
            "filtering must not change a surviving row's _row_hash"
        );

        let survivors: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(survivors, 1, "'keep' must still be the only surviving row");
    }
}
