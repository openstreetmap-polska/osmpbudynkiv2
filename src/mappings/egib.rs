//! The EGIB `rodzaj` -> KŚT-letter cascade (Appendix B of
//! `docs/building_type_mappings.md`).
//!
//! Consumed by `import::egib::load_into` to precompute
//! `egib_buildings.rodzaj_kod` at import time. An earlier draft of the
//! building-type-mappings design ran this cascade in the serve query instead;
//! that was reversed once measurement showed the cascade costs +1.0 s when
//! run over the neighbour-candidate bbox slice at serve time (see
//! `docs/superpowers/specs/2026-08-03-building-type-mappings-design.md`,
//! "One new source-table column"). Living here rather than inline in
//! `import::egib` keeps the cascade in exactly one place, the way
//! `dataset::hashed_select` is the one place the row-hash expression lives.

/// SQL `CASE` expression resolving `rodzaj` to a single KŚT letter, or NULL
/// when unresolved (an expected outcome for this source -- the cascade is
/// empirical, not backed by a closed schema enumeration the way BDOT10k's
/// tier-1 column is, so `ELSE NULL` is normal drift rather than an error).
///
/// Order matters: the tier-1 bare-letter prefix match runs last, since it
/// would otherwise swallow tier-3/tier-4 strings that also start with a KŚT
/// letter.
pub const RODZAJ_KOD_CASE_SQL: &str = r"CASE
  WHEN regexp_matches(lower(trim(rodzaj)), '^[a-z] - .* \(\d+\)$')
      THEN (lower(trim(rodzaj)))[1:1]
  WHEN lower(trim(rodzaj)) LIKE 'budynki mieszkalne%'          THEN 'm'
  WHEN lower(trim(rodzaj)) LIKE 'budynki biurowe%'             THEN 'b'
  WHEN lower(trim(rodzaj)) LIKE 'budynki handlowo-usługowe%'   THEN 'h'
  WHEN lower(trim(rodzaj)) LIKE 'budynki przemysłowe%'         THEN 'p'
  WHEN lower(trim(rodzaj)) LIKE 'budynki transportu%'          THEN 't'
  WHEN lower(trim(rodzaj)) LIKE 'budynki szpitali%'            THEN 'z'
  WHEN lower(trim(rodzaj)) LIKE 'budynki oświaty%'             THEN 'k'
  WHEN lower(trim(rodzaj)) LIKE 'budynki produkcyjne%'         THEN 'g'
  WHEN lower(trim(rodzaj)) LIKE 'zbiorniki, silosy%'           THEN 's'
  WHEN lower(trim(rodzaj)) LIKE 'pozostałe budynki niemieszkalne%' THEN 'i'
  WHEN lower(trim(rodzaj)) IN ('mieszkalny', 'budynekmieszkalny')  THEN 'm'
  WHEN lower(trim(rodzaj)) = 'biurowy'                              THEN 'b'
  WHEN lower(trim(rodzaj)) = 'handlowouslugowy'                     THEN 'h'
  WHEN lower(trim(rodzaj)) = 'przemyslowy'                          THEN 'p'
  WHEN lower(trim(rodzaj)) = 'transportuilacznosci'                 THEN 't'
  WHEN lower(trim(rodzaj)) = 'szpitalaiinnebudynkiopiekizdrowotnej'  THEN 'z'
  WHEN lower(trim(rodzaj)) = 'oswiatynaukiikulturyorazsportu'       THEN 'k'
  WHEN lower(trim(rodzaj)) = 'produkcyjnyuslugowyigospodarczy'      THEN 'g'
  WHEN lower(trim(rodzaj)) = 'zbiorniksilosibudynekmagazynowy'      THEN 's'
  WHEN lower(trim(rodzaj)) = 'budynekniemieszkalny'                 THEN 'i'
  WHEN regexp_matches(lower(trim(rodzaj)), '^[mgithpskbz][a-z]?[0-9]?\.?$')
      THEN (lower(trim(rodzaj)))[1:1]
  ELSE NULL
END";

/// Wrap `select_sql` (typically the output of
/// `DatasetSpec::with_centroid_select`) so it also gains a `rodzaj_kod
/// VARCHAR` column. Added OUTSIDE `hashed_select`'s projection, the same way
/// `centroid` is (see `DatasetSpec::with_centroid_select`), so it never
/// affects `_row_hash` and needs no `ROW_HASH_VERSION` bump.
pub fn with_rodzaj_kod_select(select_sql: &str) -> String {
    format!("SELECT *, ({RODZAJ_KOD_CASE_SQL}) AS rodzaj_kod FROM ({select_sql}) t")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> duckdb::Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    /// One example per cascade tier, drawn straight from Appendix B, plus the
    /// unresolved and NULL cases.
    #[test]
    fn resolves_one_example_per_tier() {
        let c = conn();
        c.execute_batch(
            "CREATE TABLE src (id VARCHAR, rodzaj VARCHAR);
             INSERT INTO src VALUES
                 ('tier1_bare', 'mj'),
                 ('tier1_dot',  'b.'),
                 ('tier2_camel', 'produkcyjnyUslugowyIGospodarczy'),
                 ('tier3_full', 'budynki transportu i łączności'),
                 ('tier4_combined', 'm - budynki mieszkalne (110)'),
                 ('unresolved', 'x'),
                 ('is_null', NULL);",
        )
        .unwrap();

        let select = with_rodzaj_kod_select("SELECT id, rodzaj FROM src");
        conn_query(&c, &select, "tier1_bare", Some("m"));
        conn_query(&c, &select, "tier1_dot", Some("b"));
        conn_query(&c, &select, "tier2_camel", Some("g"));
        conn_query(&c, &select, "tier3_full", Some("t"));
        conn_query(&c, &select, "tier4_combined", Some("m"));
        conn_query(&c, &select, "unresolved", None);
        conn_query(&c, &select, "is_null", None);
    }

    fn conn_query(c: &duckdb::Connection, select: &str, id: &str, expected: Option<&str>) {
        let got: Option<String> = c
            .query_row(
                &format!("SELECT rodzaj_kod FROM ({select}) t WHERE id = ?"),
                duckdb::params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(got.as_deref(), expected, "id={id}");
    }

    /// Every one of the ten letter->KŚT pairings in `docs/building_type_mappings.md`'s
    /// EGIB table resolves from its bare-letter form.
    #[test]
    fn resolves_every_kst_letter() {
        let c = conn();
        c.execute_batch(
            "CREATE TABLE src (id VARCHAR, rodzaj VARCHAR);
             INSERT INTO src VALUES
                 ('p','p'), ('t','t'), ('h','h'), ('k','k'), ('g','g'),
                 ('s','s'), ('b','b'), ('z','z'), ('i','i'), ('m','m');",
        )
        .unwrap();
        let select = with_rodzaj_kod_select("SELECT id, rodzaj FROM src");
        for letter in ["p", "t", "h", "k", "g", "s", "b", "z", "i", "m"] {
            conn_query(&c, &select, letter, Some(letter));
        }
    }

    /// `with_rodzaj_kod_select` must not disturb the row hash: it wraps
    /// `hashed_select`'s output rather than feeding into it, the same
    /// invariant `dataset::with_centroid_select` carries -- see
    /// `dataset::tests::with_centroid_select_does_not_change_the_row_hash`.
    #[test]
    fn does_not_change_the_row_hash() {
        let c = conn();
        c.execute_batch("CREATE TABLE src (id VARCHAR, rodzaj VARCHAR); INSERT INTO src VALUES ('1','m'), ('2', NULL);")
            .unwrap();
        let inner = "SELECT id, rodzaj FROM src";
        let hashed = crate::dataset::hashed_select(inner);
        let with_rodzaj = with_rodzaj_kod_select(&hashed);

        c.execute_batch(&format!(
            "CREATE TABLE plain AS {hashed};
             CREATE TABLE with_rodzaj AS {with_rodzaj};"
        ))
        .unwrap();

        let disagreements: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM plain p JOIN with_rodzaj w USING (id)
                 WHERE p._row_hash IS DISTINCT FROM w._row_hash",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(disagreements, 0);
    }
}
