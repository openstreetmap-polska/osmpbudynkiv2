use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::dataset::{DatasetSpec, GeometryCompare, HASH_ATTRS_FIELD};

/// Stage 1's output for a [`GeometryCompare::Tolerance`] source: every key
/// whose prefilter digest differs, with whether its attributes did. Dropped by
/// [`compute`] once `diff_modified` is built, and by the refresh's scratch
/// guard on the error path.
pub const MODIFIED_CANDIDATES_TABLE: &str = "diff_modified_candidates";

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
///
/// **A [`GeometryCompare::Tolerance`] source runs it in two stages**, because
/// its `content_hash_sql` is only a prefilter (see that method):
///
/// 1. **Prefilter** -- the shape above, unchanged, writing the keys whose
///    digest differs to [`MODIFIED_CANDIDATES_TABLE`] along with whether
///    their `attrs` digest did. Still `(key, hash)` on each side; still no
///    geometry under the join.
/// 2. **Verification** -- rows whose attributes changed are modified with no
///    geometry check. Only the rest, the geometry-only candidates, are joined
///    back to both tables and kept iff `DatasetSpec::geometry_changed_sql`
///    holds. On the 2026-08-15 → 11-09 exports that was 333,340 rows (10% of
///    the 2.1M noise rows, plus the 121,097 real moves); materializing that
///    *key set* is cheap,
///    unlike materializing either full side (point 1 above).
///
/// `tests::the_two_stage_diff_equals_the_definition` pins the result to
/// `changed_predicate_sql` row by row.
pub fn compute(conn: &Connection, spec: &DatasetSpec) -> Result<DiffCounts> {
    conn.execute_batch(&build_sql(spec))
        .with_context(|| format!("Failed to compute diff for {}", spec.name))?;

    let count = |table: &str| -> Result<i64> {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .with_context(|| format!("Failed to count {table}"))
    };

    if let GeometryCompare::Tolerance { metres } = spec.compare_geometry {
        let (candidates, geometry_only): (i64, i64) = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE NOT __attrs_changed)
                     FROM {MODIFIED_CANDIDATES_TABLE}"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .with_context(|| format!("Failed to count {MODIFIED_CANDIDATES_TABLE}"))?;
        let modified = count("diff_modified")?;
        info!(
            source = spec.name,
            prefilter_candidates = candidates,
            geometry_only_candidates = geometry_only,
            confirmed_geometry_only = modified - (candidates - geometry_only),
            tolerance_m = metres,
            "geometry tolerance verification"
        );
        conn.execute_batch(&format!("DROP TABLE {MODIFIED_CANDIDATES_TABLE}"))
            .with_context(|| format!("Failed to drop {MODIFIED_CANDIDATES_TABLE}"))?;
    }

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

    // A separate template rather than a branch inside the one below, so the
    // SQL every other source runs stays byte-identical to what it was before
    // tolerance existed (`tests::exact_and_ignored_geometry_sql_is_unchanged`).
    if let GeometryCompare::Tolerance { .. } = spec.compare_geometry {
        let geom_changed = spec
            .geometry_changed_sql("s.geom", "l.geom")
            .expect("a tolerance spec compares geometry");
        let candidates = MODIFIED_CANDIDATES_TABLE;
        let attrs = HASH_ATTRS_FIELD;
        return format!(
            "DROP TABLE IF EXISTS diff_added;
         DROP TABLE IF EXISTS diff_removed;
         DROP TABLE IF EXISTS diff_modified;
         DROP TABLE IF EXISTS {candidates};

         CREATE TEMP TABLE diff_added AS
             SELECT {keys} FROM {staging} ANTI JOIN {live} USING ({keys});
         CREATE TEMP TABLE diff_removed AS
             SELECT {keys} FROM {live} ANTI JOIN {staging} USING ({keys});
         CREATE TEMP TABLE {candidates} AS
             SELECT {keys}, s.__sig.{attrs} IS DISTINCT FROM l.__sig.{attrs} AS __attrs_changed FROM
                 (SELECT {keys}, {content_hash} AS __sig FROM {staging} t) s
                 JOIN
                 (SELECT {keys}, {content_hash} AS __sig FROM {live} t) l
                 USING ({keys})
             WHERE s.__sig IS DISTINCT FROM l.__sig;
         CREATE TEMP TABLE diff_modified AS
             SELECT {keys} FROM {candidates} WHERE __attrs_changed
             UNION ALL
             SELECT {keys} FROM
                 (SELECT {keys} FROM {candidates} WHERE NOT __attrs_changed) c
                 JOIN {staging} s USING ({keys})
                 JOIN {live} l USING ({keys})
             WHERE {geom_changed};"
        );
    }

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
    use crate::dataset::{DatasetSpec, GeomKind, GeometryCompare};
    use crate::db::init_db;
    use std::path::Path;

    const TEST_SPEC: DatasetSpec = DatasetSpec {
        name: "test",
        table: "live",
        key_columns: &["id"],
        compared_columns: &["a"],
        compare_geometry: GeometryCompare::Exact,
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
    /// gets its own assertion -- via PRG (`Exact`, one stage) and EGIB
    /// (`Tolerance`, where stage 1 writes the candidate table).
    ///
    /// For EGIB the property also has a second half: stage 2 may join
    /// geometry, but only for the geometry-only *candidates*, never the full
    /// tables against each other.
    #[test]
    fn the_modified_diff_hashes_each_side_before_joining() {
        let stage_one = |sql: &str, table: &str| -> String {
            sql.split(&format!("CREATE TEMP TABLE {table} AS"))
                .nth(1)
                .unwrap_or_else(|| panic!("build_sql must still create {table}"))
                .split("CREATE TEMP TABLE")
                .next()
                .unwrap()
                .to_string()
        };
        let prg = build_sql(&crate::dataset::PRG);
        let egib = build_sql(&crate::dataset::EGIB);
        for prefilter in [
            stage_one(&prg, "diff_modified"),
            stage_one(&egib, MODIFIED_CANDIDATES_TABLE),
        ] {
            assert_eq!(
                prefilter.matches("AS __sig FROM").count(),
                2,
                "both staging and live must be projected to (key, hash) before the \
                 join, not joined and then filtered: {prefilter}"
            );
            assert!(
                !prefilter.contains("l.geom") && !prefilter.contains("s.geom"),
                "geometry must only be reachable inside the hashed subqueries — a \
                 `geom` under the join aliases means it is hash-join payload again, \
                 which is the 17.6M-polygon cost this shape exists to avoid: \
                 {prefilter}"
            );
            assert!(
                prefilter.contains("s.__sig IS DISTINCT FROM l.__sig"),
                "the comparison must be the NULL-safe form over the two digests: \
                 {prefilter}"
            );
        }

        let verification = stage_one(&egib, "diff_modified");
        assert!(
            verification.contains(&format!(
                "(SELECT id_budynku FROM {MODIFIED_CANDIDATES_TABLE} WHERE NOT __attrs_changed) c"
            )),
            "stage 2 must drive its geometry join from the geometry-only \
             candidates: {verification}"
        );
        assert_eq!(
            verification.matches("JOIN").count(),
            2,
            "stage 2 joins the candidates to staging and live, nothing else: {verification}"
        );
    }

    fn tolerance_spec() -> DatasetSpec {
        DatasetSpec {
            name: "test",
            table: "live",
            key_columns: &["id"],
            compared_columns: &["a"],
            compare_geometry: GeometryCompare::Tolerance {
                metres: crate::dataset::EGIB_GEOMETRY_TOLERANCE_M,
            },
            geom_kind: GeomKind::Polygon,
        }
    }

    /// The two-stage diff and `changed_predicate_sql` must agree row by row --
    /// the property `dataset::tests::signature_changes_exactly_when_the_diff_
    /// says_modified` gives the one-stage sources. The fixture includes rows
    /// the prefilter lets through and verification must drop, rows it
    /// verifies, rows modified on attributes alone, and NULL geometry in
    /// every position.
    #[test]
    fn the_two_stage_diff_equals_the_definition() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(&crate::dataset::tolerance_fixture::create_sql(
            "live",
            "live__staging",
            true,
        ))
        .unwrap();
        let spec = tolerance_spec();

        let counts = compute(&conn, &spec).unwrap();
        let defined: Vec<String> = {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT l.id FROM live__staging s JOIN live l USING (id) WHERE {} ORDER BY l.id",
                    spec.changed_predicate_sql("s", "l")
                ))
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert!(!defined.is_empty());
        assert_eq!(ids(&conn, "diff_modified"), defined);
        assert_eq!(counts.modified, defined.len() as i64);
        assert_eq!((counts.added, counts.removed), (0, 0));
        assert!(
            !table_exists(&conn, MODIFIED_CANDIDATES_TABLE),
            "the candidate table is scratch and must not outlive compute"
        );
    }

    /// Our own pipeline is deterministic, so the same file staged twice must
    /// diff 0/0/0 under the tolerance form too -- not merely "nothing over
    /// the tolerance", but nothing reaching verification at all.
    #[test]
    fn tolerance_identical_snapshots_produce_no_changes() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(&crate::dataset::tolerance_fixture::create_sql(
            "live", "other", true,
        ))
        .unwrap();
        conn.execute_batch("CREATE TABLE live__staging AS SELECT * FROM live")
            .unwrap();

        let counts = compute(&conn, &tolerance_spec()).unwrap();
        assert_eq!(
            counts,
            DiffCounts {
                added: 0,
                modified: 0,
                removed: 0
            }
        );
    }

    fn table_exists(conn: &Connection, name: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?",
            [name],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
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
            compare_geometry: GeometryCompare::None,
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
            compare_geometry: GeometryCompare::None,
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

    /// PRG's and BDOT10k's diff batch must be byte-identical to what ran
    /// before `GeometryCompare::Tolerance` existed (captured from the
    /// pre-change `build_sql`): the two-stage form is EGIB's alone.
    #[test]
    fn exact_and_ignored_geometry_sql_is_unchanged() {
        assert_eq!(
            build_sql(&crate::dataset::BDOT10K),
            r#"DROP TABLE IF EXISTS diff_added;
         DROP TABLE IF EXISTS diff_removed;
         DROP TABLE IF EXISTS diff_modified;

         CREATE TEMP TABLE diff_added AS
             SELECT PRZESTRZENNAZW, LOKALNYID FROM bdot10k_buildings__staging ANTI JOIN bdot10k_buildings USING (PRZESTRZENNAZW, LOKALNYID);
         CREATE TEMP TABLE diff_removed AS
             SELECT PRZESTRZENNAZW, LOKALNYID FROM bdot10k_buildings ANTI JOIN bdot10k_buildings__staging USING (PRZESTRZENNAZW, LOKALNYID);
         CREATE TEMP TABLE diff_modified AS
             SELECT PRZESTRZENNAZW, LOKALNYID FROM
                 (SELECT PRZESTRZENNAZW, LOKALNYID, hash(t.WERSJA, t.KATEGORIAISTNIENIA, t.PRZEWAZAJACAFUNKCJABUDYNKU, t.FUNKCJAOGOLNABUDYNKU, t.LICZBAKONDYGNACJI, t.NAZWA, t.FSBUD, t.INFORMACJADODATKOWA, t.KODKST, t.ZRODLODANYCHGEOMETRYCZNYCH) AS __sig FROM bdot10k_buildings__staging t) s
                 JOIN
                 (SELECT PRZESTRZENNAZW, LOKALNYID, hash(t.WERSJA, t.KATEGORIAISTNIENIA, t.PRZEWAZAJACAFUNKCJABUDYNKU, t.FUNKCJAOGOLNABUDYNKU, t.LICZBAKONDYGNACJI, t.NAZWA, t.FSBUD, t.INFORMACJADODATKOWA, t.KODKST, t.ZRODLODANYCHGEOMETRYCZNYCH) AS __sig FROM bdot10k_buildings t) l
                 USING (PRZESTRZENNAZW, LOKALNYID)
             WHERE s.__sig IS DISTINCT FROM l.__sig;"#
        );
        assert_eq!(
            build_sql(&crate::dataset::PRG),
            r#"DROP TABLE IF EXISTS diff_added;
         DROP TABLE IF EXISTS diff_removed;
         DROP TABLE IF EXISTS diff_modified;

         CREATE TEMP TABLE diff_added AS
             SELECT lokalny_id FROM prg_addresses__staging ANTI JOIN prg_addresses USING (lokalny_id);
         CREATE TEMP TABLE diff_removed AS
             SELECT lokalny_id FROM prg_addresses ANTI JOIN prg_addresses__staging USING (lokalny_id);
         CREATE TEMP TABLE diff_modified AS
             SELECT lokalny_id FROM
                 (SELECT lokalny_id, hash(t.numer_porzadkowy, t.ulica, t.miejscowosc, t.kod_pocztowy, t.teryt_miejscowosc, ST_AsWKB(t.geom)) AS __sig FROM prg_addresses__staging t) s
                 JOIN
                 (SELECT lokalny_id, hash(t.numer_porzadkowy, t.ulica, t.miejscowosc, t.kod_pocztowy, t.teryt_miejscowosc, ST_AsWKB(t.geom)) AS __sig FROM prg_addresses t) l
                 USING (lokalny_id)
             WHERE s.__sig IS DISTINCT FROM l.__sig;"#
        );
    }
}
