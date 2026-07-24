use anyhow::{Context, Result};
use duckdb::Connection;

use crate::dataset::DatasetSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffCounts {
    pub added: i64,
    pub modified: i64,
    pub removed: i64,
}

/// Classify every ID in `spec.table` vs `spec.staging_table()` into the
/// temp tables `diff_added`, `diff_removed` and `diff_modified`.
///
/// The comparison is per-ID, not per-row: an ID's rows are folded into a
/// single order-independent hash via `hash(list_sort(list(_row_hash)))`.
/// IDs are NOT unique in these datasets (BDOT10k ships duplicates), so an
/// ID's whole row-set is replaced as a unit and duplicates cannot drift.
pub fn compute(conn: &Connection, spec: &DatasetSpec) -> Result<DiffCounts> {
    let live = spec.table;
    let staging = spec.staging_table();
    let id = spec.id_column;

    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS diff_live_hashes;
         DROP TABLE IF EXISTS diff_new_hashes;
         DROP TABLE IF EXISTS diff_added;
         DROP TABLE IF EXISTS diff_removed;
         DROP TABLE IF EXISTS diff_modified;

         CREATE TEMP TABLE diff_live_hashes AS
             SELECT {id} AS id, hash(list_sort(list(_row_hash))) AS h
             FROM {live} GROUP BY {id};
         CREATE TEMP TABLE diff_new_hashes AS
             SELECT {id} AS id, hash(list_sort(list(_row_hash))) AS h
             FROM {staging} GROUP BY {id};

         CREATE TEMP TABLE diff_added AS
             SELECT id FROM diff_new_hashes ANTI JOIN diff_live_hashes USING (id);
         CREATE TEMP TABLE diff_removed AS
             SELECT id FROM diff_live_hashes ANTI JOIN diff_new_hashes USING (id);
         CREATE TEMP TABLE diff_modified AS
             SELECT n.id FROM diff_new_hashes n JOIN diff_live_hashes l USING (id)
             WHERE n.h IS DISTINCT FROM l.h;"
    ))
    .with_context(|| format!("Failed to compute diff for {}", spec.name))?;

    let count = |table: &str| -> Result<i64> {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .with_context(|| format!("Failed to count {table}"))
    };

    Ok(DiffCounts {
        added: count("diff_added")?,
        modified: count("diff_modified")?,
        removed: count("diff_removed")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{DatasetSpec, GeomKind};
    use crate::db::init_db;
    use std::path::Path;

    const TEST_SPEC: DatasetSpec = DatasetSpec {
        name: "test",
        table: "live",
        id_column: "id",
        geom_kind: GeomKind::Point,
    };

    /// Live and staging tables covering every classification at once:
    ///   keep     - identical in both            -> unchanged
    ///   mod      - attribute changed            -> modified
    ///   del      - only in live                 -> removed
    ///   add      - only in staging              -> added
    ///   dup      - two rows, one changed        -> modified (whole ID)
    ///   nullgeom - NULL geometry, unchanged     -> unchanged
    fn setup() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let inner_live = "SELECT * FROM (VALUES
             ('keep','v1',20.0,52.0), ('mod','v1',20.1,52.0), ('del','v1',20.2,52.0),
             ('dup','v1',20.3,52.0), ('dup','v2',20.4,52.0), ('nullgeom','v1',NULL,NULL)
           ) t(id, a, lon, lat)";
        let inner_stg = "SELECT * FROM (VALUES
             ('keep','v1',20.0,52.0), ('mod','CHANGED',20.1,52.0), ('add','v1',20.5,52.0),
             ('dup','v1',20.3,52.0), ('dup','CHANGED',20.4,52.0), ('nullgeom','v1',NULL,NULL)
           ) t(id, a, lon, lat)";
        let wrap = |inner: &str| {
            crate::dataset::hashed_select(&format!(
                "SELECT id, a, ST_Point(lon, lat) AS geom FROM ({inner})"
            ))
        };
        conn.execute_batch(&format!(
            "CREATE TABLE live AS {};
             CREATE TABLE live__staging AS {};",
            wrap(inner_live),
            wrap(inner_stg)
        ))
        .unwrap();
        conn
    }

    fn ids(conn: &Connection, table: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!("SELECT id FROM {table} ORDER BY id"))
            .unwrap();
        let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    #[test]
    fn classifies_added_removed_and_modified() {
        let conn = setup();
        let counts = compute(&conn, &TEST_SPEC).unwrap();

        assert_eq!(ids(&conn, "diff_added"), vec!["add"]);
        assert_eq!(ids(&conn, "diff_removed"), vec!["del"]);
        assert_eq!(ids(&conn, "diff_modified"), vec!["dup", "mod"]);
        assert_eq!(
            counts,
            DiffCounts {
                added: 1,
                modified: 2,
                removed: 1
            }
        );
    }

    /// An unchanged row must never appear in any bucket — in particular a row
    /// whose geometry is NULL, which would otherwise hash inconsistently.
    #[test]
    fn unchanged_rows_including_null_geometry_are_not_reported() {
        let conn = setup();
        compute(&conn, &TEST_SPEC).unwrap();
        for table in ["diff_added", "diff_removed", "diff_modified"] {
            let listed = ids(&conn, table);
            assert!(
                !listed.contains(&"keep".to_string()),
                "{table} listed 'keep'"
            );
            assert!(
                !listed.contains(&"nullgeom".to_string()),
                "{table} listed 'nullgeom'"
            );
        }
    }

    /// Re-running the diff against identical content reports nothing.
    #[test]
    fn identical_snapshots_produce_no_changes() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let inner = "SELECT id, a, ST_Point(lon, lat) AS geom FROM (
             SELECT * FROM (VALUES ('a','v1',20.0,52.0), ('b','v2',21.0,53.0)) t(id,a,lon,lat))";
        conn.execute_batch(&format!(
            "CREATE TABLE live AS {};
             CREATE TABLE live__staging AS {};",
            crate::dataset::hashed_select(inner),
            crate::dataset::hashed_select(inner)
        ))
        .unwrap();

        let counts = compute(&conn, &TEST_SPEC).unwrap();
        assert_eq!(
            counts,
            DiffCounts {
                added: 0,
                modified: 0,
                removed: 0
            }
        );
    }
}
