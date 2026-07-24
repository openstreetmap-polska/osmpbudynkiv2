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
    /// change cell.
    pub fn representative_point_sql(&self, geom_expr: &str) -> String {
        match self.geom_kind {
            GeomKind::Point => geom_expr.to_string(),
            GeomKind::Polygon => format!("ST_Centroid({geom_expr})"),
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
    fn representative_point_uses_centroid_for_polygons() {
        assert_eq!(
            BDOT10K.representative_point_sql("geom"),
            "ST_Centroid(geom)"
        );
        assert_eq!(EGIB.representative_point_sql("geom"), "ST_Centroid(geom)");
    }

    #[test]
    fn representative_point_passes_through_for_points() {
        assert_eq!(PRG.representative_point_sql("geom"), "geom");
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
}
