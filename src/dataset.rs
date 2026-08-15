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

/// `k1 IS NOT NULL AND k2 IS NOT NULL ...`, for use as a `WHERE` clause in a
/// loader's inner select. A record with no identifier cannot be diffed (see
/// `update::diff`, where `ANTI JOIN ... USING (id)` never matches NULL to
/// NULL) or deduplicated, so it is dropped before it is ever written. Paired
/// with [`deduplicate_by_key`], which enforces the other half of the same
/// guarantee -- the two run at different points, deliberately (this one
/// inside the load SELECT, that one after the table exists; see the plan's
/// "Why not do both at insert").
///
/// This filter sits INSIDE `hashed_select`'s input, which [`ROW_HASH_VERSION`]'s
/// doc flags as bump territory ("a source's inner select is part of the hash
/// input"). It still needs no bump: that rule is about expressions that
/// change a row's *value* (`ULICA_PREFIX_STRIP_SQL` is the version-2 case).
/// A row filter changes which rows exist, never the content of a surviving
/// row, so every surviving `_row_hash` is bit-identical.
pub fn non_null_key_sql(key_columns: &[&str]) -> String {
    key_columns
        .iter()
        .map(|k| format!("{k} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// `k1 IS NULL OR k2 IS NULL ...` -- exactly the rows `non_null_key_sql`
/// excludes, for the count query each loader runs to report them (a filtered
/// `CREATE TABLE AS SELECT` never says how many rows its `WHERE` dropped).
///
/// It lives here, beside its complement, rather than in either loader: the
/// two predicates decide the same question from opposite sides, and a
/// composite key makes the relationship easy to get wrong -- the negation of
/// `a IS NOT NULL AND b IS NOT NULL` is `a IS NULL OR b IS NULL`, NOT
/// `a IS NULL AND b IS NULL`. A copy per loader would let BDOT10k's
/// two-column form drift from EGIB's one-column form without anything
/// noticing; `non_null_key_sql_and_null_key_sql_are_complements` pins them
/// together instead.
pub fn null_key_sql(key_columns: &[&str]) -> String {
    key_columns
        .iter()
        .map(|k| format!("{k} IS NULL"))
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Rows a dataset loader dropped rather than staging, for one of four
/// reasons: geometry that failed `ST_IsValid` (`ST_AsMVTGeom` cannot
/// tolerate invalid geometry, see docs/invalid_geometry_tile_500s.md),
/// geometry whose bbox spans at least one full z14 cell in either axis (see
/// `filter_oversized_geometry` -- a corrupted merge of two unrelated
/// features, not a real building), a NULL record key (see
/// [`non_null_key_sql`] -- a record with no identifier cannot be diffed or
/// deduplicated), or a duplicate record key (see [`deduplicate_by_key`]).
/// All four reasons drop rather than repair.
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
    /// Rows dropped for having a NULL record key (`non_null_key_sql`). No
    /// example-ids field, unlike every other reason: the id column *is* what
    /// is missing, so the list would be a column of NULLs.
    pub skipped_null_key: i64,
    pub skipped_duplicate_key: i64,
    /// Same cap and ordering caveat as `skipped_example_ids`, for the
    /// duplicate-key reason.
    pub skipped_duplicate_example_ids: Vec<String>,
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

    /// Fold in the unique-key counts from the dedup pass, the same way
    /// `merge_oversized` folds in the oversized-geometry counts: a loader
    /// combines `filter_invalid_geometry`'s result with
    /// `filter_oversized_geometry`'s, and then with `deduplicate_by_key`'s
    /// (`unique`), into the one `LoadStats` it returns. `skipped_null_key`
    /// is copied here too even though `deduplicate_by_key` itself never sets
    /// it -- NULL-keyed rows are gone before it ever runs -- because the
    /// loader gets that count from a separate query against the source
    /// parquet and needs one field on `unique` to carry it through.
    pub fn merge_unique_key(mut self, unique: LoadStats) -> Self {
        self.skipped_null_key = unique.skipped_null_key;
        self.skipped_duplicate_key = unique.skipped_duplicate_key;
        self.skipped_duplicate_example_ids = unique.skipped_duplicate_example_ids;
        self
    }
}

/// Render one skip-reason clause for a `job_run_log` summary message, shared
/// by every source's `summarize` (`import::bdot10k`, `import::egib`) and by
/// `update::dataset::summarize_refresh`, so the "N rows, ids: ..., +M more"
/// wording is written once rather than once per reason per source.
/// `reason` reads naturally before "rows", e.g. `"invalid-geometry"` or
/// `"oversized-geometry"`.
///
/// `ids` empty renders as a bare "skipped N {reason} rows", with the
/// `(ids: ...)` parenthetical omitted entirely -- needed for the null-key
/// reason, which by design never has example ids (see
/// `LoadStats::skipped_null_key`) and would otherwise render the empty and
/// slightly absurd `(ids: )`.
pub fn format_skip_clause(reason: &str, count: i64, ids: &[String]) -> String {
    if ids.is_empty() {
        return format!("skipped {count} {reason} rows");
    }
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

/// Delete all but one row per duplicate key from a just-loaded table,
/// capturing example ids before they're gone. Sibling to
/// `filter_invalid_geometry` and `filter_oversized_geometry` above, matching
/// their shape exactly: scan for up to `MAX_EXAMPLE_IDS` example ids,
/// `DELETE`, return a `LoadStats`. Among duplicates sharing a key, the row
/// `order_by` ranks first survives (`"WERSJA DESC"` for BDOT10k,
/// `"czas_pozyskania DESC"` for EGIB).
///
/// Two-phase SQL, deliberately: a `dup_keys` `GROUP BY` finds which keys are
/// actually duplicated, and only those rows are fed to the ranking window.
/// Measured on the real 17.77M-row EGIB table (851 duplicate-key groups,
/// ~2.5k rows in `dup_keys`): two-phase 0.60s vs 7.29s for a full-table
/// `QUALIFY row_number()` over every row -- the two-phase structure is worth
/// far more than the choice of ranking function below.
///
/// **No `IS NOT NULL` guard on the key is needed here.** NULL-keyed rows
/// never reached `table` in the first place -- see [`non_null_key_sql`],
/// applied in the load SELECT before this ever runs. Without that
/// precondition a `PARTITION BY` over a nullable key would be actively
/// wrong, not merely redundant: the window would put every NULL-keyed row
/// into one partition and keep exactly one of them, silently discarding the
/// rest as if they were duplicates of each other.
///
/// **`row_number()` rather than `DISTINCT ON` or `arg_max`/`max_by`** --
/// measured, not assumed. In this two-phase shape all three are within
/// noise of each other (0.60s / 0.53s / 0.53s on the real table), because
/// the cost is the `GROUP BY`, not the ranking; `row_number()` wins on
/// shorter SQL, since `DISTINCT ON` yields the *survivors* and this is a
/// DELETE. `arg_max` MUST NOT be used: for a group whose ordering column is
/// all-NULL it returns NULL, that NULL lands in a `NOT IN` anti-predicate,
/// and the predicate then evaluates to NULL for *every* row in the table --
/// the DELETE silently removes nothing at all, table-wide. Latent rather
/// than live today (0 NULL `czas_pozyskania`, 0 NULL `WERSJA` on the real
/// snapshots), which is exactly what makes it dangerous: it would land with
/// a future export, not show up in review.
///
/// **No tiebreak column, deliberately (2026-08-14).** 843 of 851 EGIB
/// duplicate groups tie on `czas_pozyskania`, so which row survives is
/// whatever the scan happened to order first. Cost of that: if scan order
/// for a tying group flips between an import and a later refresh, that
/// group's row reports as *modified* once -- bounded at ~843 rows per EGIB
/// refresh against 17.5M, and self-correcting. If EGIB refresh churn ever
/// shows a persistent ~843-row floor, this is the cause and a tiebreak is
/// the fix.
///
/// **`NULLS LAST` is spelled out** rather than left to DuckDB's default,
/// because `default_null_order` is settable and this project overrides
/// `duckdb_init_commands` wholesale. A NULL version must never win over a
/// dated one.
///
/// **`rowid` is safe here** despite CLAUDE.md's "serving tables store rows,
/// not id references" warning -- that invariant is about *storing* a rowid
/// across a DELETE+INSERT; this one lives and dies inside a single DELETE
/// statement.
///
/// Runs strictly after the table is built and outside `hashed_select`'s
/// projection -- the same argument `filter_oversized_geometry` already
/// relies on -- so no `ROW_HASH_VERSION` bump is needed.
///
/// **Must run after `filter_invalid_geometry` and
/// `filter_oversized_geometry`.** A duplicate pair whose newest member has
/// bad geometry must fall back to the older, valid member instead of being
/// collapsed down to a row one of those filters then deletes -- losing the
/// object entirely.
pub fn deduplicate_by_key(
    conn: &duckdb::Connection,
    table: &str,
    key_columns: &[&str],
    order_by: &str,
    id_column: &str,
) -> anyhow::Result<LoadStats> {
    use anyhow::Context;

    let keys = key_columns.join(", ");
    let predicate = format!(
        "rowid IN (
          WITH dup_keys AS (
            SELECT {keys} FROM {table}
            GROUP BY {keys} HAVING count(*) > 1
          ),
          ranked AS (
            SELECT t.rowid AS rid,
                   row_number() OVER (PARTITION BY {keys} ORDER BY {order_by} NULLS LAST) AS rn
            FROM {table} t JOIN dup_keys USING ({keys})
          )
          SELECT rid FROM ranked WHERE rn > 1
        )"
    );

    let mut skipped_duplicate_example_ids = Vec::new();
    {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {id_column} FROM {table} WHERE {predicate} LIMIT {MAX_EXAMPLE_IDS}"
            ))
            .with_context(|| format!("Failed to prepare duplicate-key scan on {table}"))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .with_context(|| format!("Failed to scan duplicate-key rows in {table}"))?;
        for row in rows {
            skipped_duplicate_example_ids.push(row.context("Failed to read duplicate-key id")?);
        }
    }

    let skipped_duplicate_key = conn
        .execute(&format!("DELETE FROM {table} WHERE {predicate}"), [])
        .with_context(|| format!("Failed to delete duplicate-key rows from {table}"))?
        as i64;

    Ok(LoadStats {
        skipped_duplicate_key,
        skipped_duplicate_example_ids,
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

    #[test]
    fn non_null_key_sql_covers_every_key_column() {
        assert_eq!(
            non_null_key_sql(&["PRZESTRZENNAZW", "LOKALNYID"]),
            "PRZESTRZENNAZW IS NOT NULL AND LOKALNYID IS NOT NULL"
        );
    }

    /// The load select keeps `non_null_key_sql` and the loaders' count query
    /// reports `null_key_sql`, so if the two ever stop partitioning the rows
    /// exactly the count would silently misreport what was dropped. Checked
    /// against the database rather than by string comparison, because the
    /// thing that matters is DuckDB's three-valued logic, not the text: for a
    /// composite key the complement flips `AND` to `OR`, which is the easy
    /// mistake this pins.
    #[test]
    fn non_null_key_sql_and_null_key_sql_are_complements() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (a VARCHAR, b VARCHAR);
             INSERT INTO src VALUES
                 ('x', 'y'), ('x', NULL), (NULL, 'y'), (NULL, NULL);",
        )
        .unwrap();

        let keys = ["a", "b"];
        let (kept, dropped): (i64, i64) = conn
            .query_row(
                &format!(
                    "SELECT count(*) FILTER (WHERE {}), count(*) FILTER (WHERE {}) FROM src",
                    non_null_key_sql(&keys),
                    null_key_sql(&keys)
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();

        assert_eq!(kept, 1, "only the fully-populated key survives the filter");
        assert_eq!(dropped, 3, "every row with any NULL key column is counted");
        assert_eq!(
            kept + dropped,
            4,
            "the two must partition the table exactly"
        );
    }

    /// Mirrors `with_centroid_select_does_not_change_the_row_hash`: applying
    /// `non_null_key_sql` as a `WHERE` on the load select must not change the
    /// `_row_hash` of a row that survives it, even though the filter sits
    /// INSIDE `hashed_select`'s input (see the doc comment on
    /// `non_null_key_sql` for why that's still no-bump territory).
    #[test]
    fn non_null_key_filter_does_not_change_surviving_row_hashes() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id VARCHAR, key VARCHAR, geom GEOMETRY);
             INSERT INTO src VALUES
                 ('1', 'a', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('2', NULL, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));",
        )
        .unwrap();

        let inner = "SELECT id, key, geom FROM src";
        let filtered_inner = format!(
            "SELECT id, key, geom FROM src WHERE {}",
            non_null_key_sql(&["key"])
        );

        conn.execute_batch(&format!(
            "CREATE TABLE plain AS {};
             CREATE TABLE filtered AS {};",
            hashed_select(inner),
            hashed_select(&filtered_inner)
        ))
        .unwrap();

        let disagreements: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM plain p JOIN filtered f USING (id)
                 WHERE p._row_hash IS DISTINCT FROM f._row_hash",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            disagreements, 0,
            "dropping NULL-keyed rows must not change a surviving row's _row_hash"
        );

        let filtered_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM filtered", [], |r| r.get(0))
            .unwrap();
        assert_eq!(filtered_count, 1, "the NULL-keyed row must be gone");
    }

    #[test]
    fn deduplicate_by_key_keeps_the_newest_version() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id_budynku VARCHAR, wersja INTEGER, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('dup', 1, ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('dup', 2, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));",
        )
        .unwrap();

        let stats =
            deduplicate_by_key(&conn, "t", &["id_budynku"], "wersja DESC", "id_budynku").unwrap();

        assert_eq!(stats.skipped_duplicate_key, 1);
        assert_eq!(stats.skipped_duplicate_example_ids, vec!["dup".to_string()]);

        let survivor_wersja: i32 = conn
            .query_row("SELECT wersja FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(survivor_wersja, 2, "the newer version must survive");
    }

    /// Asserts the *count*, not which row survived -- there is deliberately
    /// no determinism guarantee to pin here (see `deduplicate_by_key`'s doc
    /// comment on the tiebreak decision).
    #[test]
    fn deduplicate_by_key_keeps_exactly_one_row_per_key_when_versions_tie() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id_budynku VARCHAR, wersja INTEGER, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('dup', 1, ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('dup', 1, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));",
        )
        .unwrap();

        let stats =
            deduplicate_by_key(&conn, "t", &["id_budynku"], "wersja DESC", "id_budynku").unwrap();
        assert_eq!(stats.skipped_duplicate_key, 1);

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn deduplicate_by_key_leaves_unique_tables_untouched() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id_budynku VARCHAR, wersja INTEGER, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('a', 1, ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('b', 1, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));",
        )
        .unwrap();

        let stats =
            deduplicate_by_key(&conn, "t", &["id_budynku"], "wersja DESC", "id_budynku").unwrap();

        assert_eq!(stats, LoadStats::default());
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    /// Mirrors `filter_oversized_geometry_does_not_change_surviving_row_hashes`:
    /// deleting duplicate rows runs strictly after `hashed_select` built the
    /// table, so a surviving row's `_row_hash` must be bit-for-bit identical
    /// to what it was before the dedup ran -- no `ROW_HASH_VERSION` bump
    /// needed.
    #[test]
    fn deduplicate_by_key_does_not_change_surviving_row_hashes() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id_budynku VARCHAR, wersja INTEGER, geom GEOMETRY);
             INSERT INTO src VALUES
                 ('keep', 1, ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('dup', 1, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')),
                 ('dup', 2, ST_GeomFromText('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))'));",
        )
        .unwrap();

        let inner = "SELECT id_budynku, wersja, geom FROM src";
        conn.execute_batch(&format!("CREATE TABLE t AS {}", hashed_select(inner)))
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE hash_before AS SELECT id_budynku, wersja, _row_hash FROM t",
        )
        .unwrap();

        let stats =
            deduplicate_by_key(&conn, "t", &["id_budynku"], "wersja DESC", "id_budynku").unwrap();
        assert_eq!(stats.skipped_duplicate_key, 1);

        let disagreements: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM hash_before h JOIN t USING (id_budynku, wersja)
                 WHERE h._row_hash IS DISTINCT FROM t._row_hash",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            disagreements, 0,
            "deduplication must not change a surviving row's _row_hash"
        );

        let survivors: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            survivors, 2,
            "'keep' and the surviving 'dup' row must remain"
        );
    }

    /// BDOT10k's two-column key: the same `LOKALNYID` under two different
    /// `PRZESTRZENNAZW` values is not a duplicate and must not be collapsed.
    #[test]
    fn deduplicate_by_key_composite_key() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (przestrzennazw VARCHAR, lokalnyid VARCHAR, wersja INTEGER, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('BUBD', 'same_id', 1, ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('BUBD', 'same_id', 2, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')),
                 ('OT_BUBD', 'same_id', 1, ST_GeomFromText('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))'));",
        )
        .unwrap();

        let stats = deduplicate_by_key(
            &conn,
            "t",
            &["przestrzennazw", "lokalnyid"],
            "wersja DESC",
            "lokalnyid",
        )
        .unwrap();

        assert_eq!(stats.skipped_duplicate_key, 1);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 2,
            "same LOKALNYID under two different PRZESTRZENNAZW values must both survive"
        );
    }

    #[test]
    fn format_skip_clause_omits_the_ids_clause_when_there_are_none() {
        assert_eq!(
            format_skip_clause("null-key", 210080, &[]),
            "skipped 210080 null-key rows"
        );
    }
}
