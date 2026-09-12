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
///
/// **`diff_modified` compares `DatasetSpec::content_hash_sql`, projected on
/// each side *before* the join, rather than `changed_predicate_sql` above
/// it** — see that method's doc comment for why, and for the per-key
/// collision bound that makes a 64-bit hash sound here. The two are pinned
/// equivalent by `dataset::tests::signature_changes_exactly_when_the_diff_
/// says_modified`, so `changed_predicate_sql` remains the definition of
/// "modified" and this is only how it is evaluated.
///
/// Three things about that shape are load-bearing:
///
/// 1. **The subqueries must stay inline.** Materializing the two `(key, hash)`
///    projections into temp tables first gives the optimizer nothing and costs
///    two 16M-row temp-table writes: measured on the real BDOT10k pair, 26.4s
///    against 28.7s for the original — i.e. it gives back the entire win.
/// 2. **Only `diff_modified` needs it.** The two `ANTI JOIN`s already project
///    keys alone, so there is no payload to trim and wrapping them would be
///    pure noise.
/// 3. **`__sig` is compared with `IS DISTINCT FROM`, not `<>`.** DuckDB's
///    `hash` never returns NULL today, which makes the two equivalent, but the
///    `(FALSE)`-equivalent arm of `content_hash_sql` and any future NULL-valued
///    digest both depend on the NULL-safe form.
pub fn compute(conn: &Connection, spec: &DatasetSpec) -> Result<DiffCounts> {
    conn.execute_batch(&build_sql(spec))
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

/// The batch [`compute`] runs, as a value.
///
/// A seam, not decoration: it is the only way a test can assert on — or an
/// operator `EXPLAIN` — the SQL that actually runs, rather than a copy of it
/// that can drift. `diff_modified`'s shape in particular is a performance
/// invariant that no functional test can see (the answer is identical either
/// way), so `tests::the_modified_diff_hashes_each_side_before_joining` pins it
/// structurally here.
fn build_sql(spec: &DatasetSpec) -> String {
    let live = spec.table;
    let staging = spec.staging_table();
    let keys = spec.key_columns.join(", ");
    let content_hash = spec.content_hash_sql("t");

    format!(
        "DROP TABLE IF EXISTS diff_added;
         DROP TABLE IF EXISTS diff_removed;
         DROP TABLE IF EXISTS diff_modified;

         CREATE TEMP TABLE diff_added AS
             SELECT {keys} FROM {staging} ANTI JOIN {live} USING ({keys});
         CREATE TEMP TABLE diff_removed AS
             SELECT {keys} FROM {live} ANTI JOIN {staging} USING ({keys});
         CREATE TEMP TABLE diff_modified AS
             SELECT {keys} FROM
                 (SELECT {keys}, {content_hash} AS __sig FROM {staging} t) s
                 JOIN
                 (SELECT {keys}, {content_hash} AS __sig FROM {live} t) l
                 USING ({keys})
             WHERE s.__sig IS DISTINCT FROM l.__sig;"
    )
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

    /// A performance invariant with no functional symptom: the payload-carrying
    /// form (`FROM staging s JOIN live l USING (key) WHERE changed_predicate`)
    /// returns byte-identical results, so every other test in this file passes
    /// either way. What it costs is the whole reason this shape exists — on the
    /// real EGIB pair, at the production `memory_limit = '4GB'` / `threads = 3`,
    /// the three-statement diff measured 21–52s (it runs at the memory ceiling,
    /// so the time tracks how much it spills) against a steady 6.4s, and the
    /// modified-diff alone is an `Out of Memory Error` at a 256 MB limit where
    /// this form finishes in 3.4s. Full table in
    /// `DatasetSpec::content_hash_sql`.
    ///
    /// Asserted on the generated text, since that is the only observable: both
    /// source scans must appear wrapped in a `__sig` projection, and neither
    /// compared column may appear bare at the top level where it would have to
    /// travel through the join as payload. Geometry is the sharpest case, so it
    /// gets its own assertion via EGIB.
    #[test]
    fn the_modified_diff_hashes_each_side_before_joining() {
        let sql = build_sql(&crate::dataset::EGIB);
        let modified = sql
            .split("CREATE TEMP TABLE diff_modified AS")
            .nth(1)
            .expect("build_sql must still create diff_modified");

        assert_eq!(
            modified.matches("AS __sig FROM").count(),
            2,
            "both staging and live must be projected to (key, hash) before the \
             join, not joined and then filtered: {modified}"
        );
        assert!(
            !modified.contains("l.geom") && !modified.contains("s.geom"),
            "geometry must only be reachable inside the hashed subqueries — a \
             `geom` under the join aliases means it is hash-join payload again, \
             which is the 17.6M-polygon cost this shape exists to avoid: \
             {modified}"
        );
        assert!(
            modified.contains("s.__sig IS DISTINCT FROM l.__sig"),
            "the comparison must be the NULL-safe form over the two digests: \
             {modified}"
        );
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
