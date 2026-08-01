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

/// Bumped whenever [`hashed_select`] changes in a way that alters its output.
///
/// The value in force when a live table was built is stamped into
/// `metadata.row_hash_version` by [`stamp_row_hash_version`]. A mismatch
/// against this constant means the stored `_row_hash` values were produced by
/// a different expression, so every row will compare as modified; the refresh
/// warns, rewrites the table wholesale, and re-stamps — so the warning fires
/// once per bump rather than forever.
pub const ROW_HASH_VERSION: i64 = 1;
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

/// Cap on how many skipped-row ids `filter_invalid_geometry` collects as
/// examples -- enough to point an operator at the actual bad records
/// upstream, without holding an unbounded list for a source with many
/// invalid rows. The returned count is always the true total regardless of
/// this cap.
pub const MAX_EXAMPLE_IDS: usize = 20;

/// Rows a dataset loader dropped rather than staging, because their geometry
/// failed `ST_IsValid`. `ST_AsMVTGeom` cannot tolerate invalid geometry (see
/// docs/invalid_geometry_tile_500s.md) -- we drop rather than repair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadStats {
    pub skipped_invalid_geometry: i64,
    /// First `MAX_EXAMPLE_IDS` ids of skipped rows, in whatever order the
    /// SELECT below finds them -- not exhaustive, just enough to point an
    /// operator at the actual bad records upstream.
    pub skipped_example_ids: Vec<String>,
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
}
