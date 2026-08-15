use anyhow::{Context, Result};
use duckdb::Connection;

use crate::dataset::DatasetSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffCounts {
    pub added: i64,
    pub modified: i64,
    pub removed: i64,
}

/// Classify every record in `spec.table` vs `spec.staging_table()` into the
/// temp tables `diff_added`, `diff_removed` and `diff_modified`, keyed on
/// `spec.key_columns`.
///
/// These are plain equality joins (`ANTI JOIN`, `JOIN ... USING`) — correct
/// ONLY because the key is non-null and unique, a guarantee established at
/// load time by `dataset::non_null_key_sql` + `dataset::deduplicate_by_key`,
/// never re-checked here. The failure mode if that guarantee ever lapses:
/// SQL's `NULL = NULL` is not true, so `ANTI JOIN ... USING (key)` never
/// matches a NULL-keyed record to itself in either direction — the record
/// lands in BOTH `diff_added` and `diff_removed` simultaneously. The apply
/// step's key join then deletes nothing and inserts nothing for it, so the
/// record silently vanishes from the diff's effects while still sitting in
/// both tables (see `update::dataset::refresh`, whose `DELETE`/`INSERT` walk
/// these same key joins). This is not hypothetical: EGIB shipped 210,080 rows
/// (1.2%) with a NULL `id_budynku` before the loaders started dropping them.
/// The `diff_*` temp tables carry the key columns under their own names
/// (not folded into a single `id` column), which is what lets a composite
/// key such as BDOT10k's `(PRZESTRZENNAZW, LOKALNYID)` work with a plain
/// `USING (...)` join at every consumer.
pub fn compute(conn: &Connection, spec: &DatasetSpec) -> Result<DiffCounts> {
    let live = spec.table;
    let staging = spec.staging_table();
    let keys = spec.key_columns.join(", ");
    let changed_predicate = spec.changed_predicate_sql("s", "l");

    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS diff_added;
         DROP TABLE IF EXISTS diff_removed;
         DROP TABLE IF EXISTS diff_modified;

         CREATE TEMP TABLE diff_added AS
             SELECT {keys} FROM {staging} ANTI JOIN {live} USING ({keys});
         CREATE TEMP TABLE diff_removed AS
             SELECT {keys} FROM {live} ANTI JOIN {staging} USING ({keys});
         CREATE TEMP TABLE diff_modified AS
             SELECT {keys} FROM {staging} s JOIN {live} l USING ({keys})
             WHERE {changed_predicate};"
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
        key_columns: &["id"],
        compared_columns: &["a"],
        compare_geometry: true,
        geom_kind: GeomKind::Point,
    };

    /// Live and staging tables covering every classification at once:
    ///   keep     - identical in both            -> unchanged
    ///   mod      - compared column changed      -> modified
    ///   del      - only in live                 -> removed
    ///   add      - only in staging              -> added
    ///   nullgeom - NULL geometry, unchanged     -> unchanged
    ///
    /// Built with plain `CREATE TABLE ... AS SELECT`, not `hashed_select` --
    /// the key-based diff no longer reads `_row_hash` at all.
    fn setup() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let inner_live = "SELECT id, a, ST_Point(lon, lat) AS geom FROM (VALUES
             ('keep','v1',20.0,52.0), ('mod','v1',20.1,52.0), ('del','v1',20.2,52.0),
             ('nullgeom','v1',NULL,NULL)
           ) t(id, a, lon, lat)";
        let inner_stg = "SELECT id, a, ST_Point(lon, lat) AS geom FROM (VALUES
             ('keep','v1',20.0,52.0), ('mod','CHANGED',20.1,52.0), ('add','v1',20.5,52.0),
             ('nullgeom','v1',NULL,NULL)
           ) t(id, a, lon, lat)";
        conn.execute_batch(&format!(
            "CREATE TABLE live AS {inner_live};
             CREATE TABLE live__staging AS {inner_stg};"
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
        assert_eq!(ids(&conn, "diff_modified"), vec!["mod"]);
        assert_eq!(
            counts,
            DiffCounts {
                added: 1,
                modified: 1,
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
            "CREATE TABLE live AS {inner};
             CREATE TABLE live__staging AS {inner};"
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

    /// The whole point of this plan: a record must not be reported as
    /// modified when only a column *outside* `compared_columns` moves. The
    /// spec below deliberately omits `b` from `compared_columns`, changes
    /// only `b` between live and staging, and asserts the record lands in no
    /// bucket at all.
    #[test]
    fn non_compared_column_change_is_not_reported() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE live AS SELECT * FROM (VALUES ('x', 'v1', 'noise1')) t(id, a, b);
             CREATE TABLE live__staging AS SELECT * FROM (VALUES ('x', 'v1', 'noise2')) t(id, a, b);",
        )
        .unwrap();

        const SPEC: DatasetSpec = DatasetSpec {
            name: "test",
            table: "live",
            key_columns: &["id"],
            compared_columns: &["a"],
            compare_geometry: false,
            geom_kind: GeomKind::Point,
        };

        let counts = compute(&conn, &SPEC).unwrap();
        assert_eq!(
            counts,
            DiffCounts {
                added: 0,
                modified: 0,
                removed: 0
            },
            "a change confined to a non-compared column must be invisible to the diff"
        );
    }

    /// Composite-key regression: two records share the FIRST key component
    /// (`k1 = 'ns1'`) but differ in the second (`k2 = 'a'` vs `'b'`), and only
    /// the `'b'` record's value actually changed. A diff that joined on `k1`
    /// alone (dropping `k2` from the `USING` list) would either explode the
    /// join into a cross product of the two `k1='ns1'` rows on each side, or
    /// otherwise misclassify which of the two records changed. Asserting the
    /// exact key pair in `diff_modified`, not just the count, is what catches
    /// that: a `k1`-only join could still land on `modified: 1` by accident
    /// while reporting the wrong row.
    #[test]
    fn composite_key_distinguishes_records_sharing_the_first_component() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE live AS SELECT * FROM (VALUES
                 ('ns1', 'a', 'v1'), ('ns1', 'b', 'v1')
               ) t(k1, k2, val);
             CREATE TABLE live__staging AS SELECT * FROM (VALUES
                 ('ns1', 'a', 'v1'), ('ns1', 'b', 'CHANGED')
               ) t(k1, k2, val);",
        )
        .unwrap();

        const SPEC: DatasetSpec = DatasetSpec {
            name: "test",
            table: "live",
            key_columns: &["k1", "k2"],
            compared_columns: &["val"],
            compare_geometry: false,
            geom_kind: GeomKind::Point,
        };

        let counts = compute(&conn, &SPEC).unwrap();
        assert_eq!(
            counts,
            DiffCounts {
                added: 0,
                modified: 1,
                removed: 0
            }
        );

        let modified: Vec<(String, String)> = {
            let mut stmt = conn
                .prepare("SELECT k1, k2 FROM diff_modified ORDER BY k1, k2")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            modified,
            vec![("ns1".to_string(), "b".to_string())],
            "only the (ns1, b) record changed — (ns1, a) must not be misclassified"
        );
    }
}
