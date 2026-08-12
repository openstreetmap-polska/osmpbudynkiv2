//! One home for the OSM lifecycle-prefixed building key list.
//!
//! `import osm` reads this set as a DuckDB list literal (`key_list_sql`,
//! `has_any_key_sql`, `matched_key_sql`) and `update osm` reads it as plain
//! Rust over changeset tags (`key_of`). A key present in one spelling but not
//! the other would let one path create or delete rows the other can never
//! see, so `matched_key_sql_agrees_with_key_of` below pins the two together.

/// Lifecycle-prefixed building keys (https://wiki.osm.org/Lifecycle_prefix).
/// Order is priority order: 24 Polish ways carry two of these, so the first
/// present here is the one recorded, and import and `update osm` agree on the
/// stored value. Adding a key changes which objects `import osm` collects but
/// NOT what an existing table holds — it only takes effect after a re-import.
pub const LIFECYCLE_BUILDING_KEYS: [&str; 8] = [
    "demolished:building",
    "destroyed:building",
    "abandoned:building",
    "was:building",
    "razed:building",
    "removed:building",
    "disused:building",
    "ruins:building",
];

/// `LIFECYCLE_BUILDING_KEYS` as a DuckDB list literal, e.g.
/// `['demolished:building', 'destroyed:building', ...]`.
pub fn key_list_sql() -> String {
    let items: Vec<String> = LIFECYCLE_BUILDING_KEYS
        .iter()
        .map(|k| format!("'{k}'"))
        .collect();
    format!("[{}]", items.join(", "))
}

/// True when the `MAP(VARCHAR, VARCHAR)` expression `tags_expr` carries any
/// lifecycle-prefixed building key. NULL for a NULL map (an untagged way),
/// which the caller's `WHERE` then filters out like any other NULL.
pub fn has_any_key_sql(tags_expr: &str) -> String {
    format!("list_has_any(map_keys({tags_expr}), {})", key_list_sql())
}

/// The highest-priority lifecycle key present in `tags_expr`, or NULL if
/// none is. Uses `lambda k:`, not the `->` arrow — the arrow form is
/// deprecated in the bundled DuckDB (1.5.5) and emits a warning.
/// `list_filter` is documented order-preserving, so `[1]` picks the
/// highest-priority match per `LIFECYCLE_BUILDING_KEYS`' order.
pub fn matched_key_sql(tags_expr: &str) -> String {
    format!(
        "list_filter({}, lambda k: map_contains({tags_expr}, k))[1]",
        key_list_sql()
    )
}

/// The import-side row filter: `tags_expr` carries a lifecycle-prefixed
/// building key **and** no live `building` key. Both halves of the rule in
/// one expression, on purpose — the second half is the disjointness decision
/// (an object that also carries `building` is a standing building OSM has
/// retagged in part, so it stays a normal `osm_buildings` row), and folding
/// it in here is what lets `matched_key_sql_agrees_with_key_of` compose
/// exactly what `import osm` evaluates rather than a hand-copied
/// approximation of it. `key_of` returning `Some` is the Rust twin of this
/// whole predicate; splitting the two halves back apart at the call sites
/// would leave the disjointness rule spelled three times (both import passes
/// plus `key_of`) with nothing pinning them together.
pub fn is_former_building_sql(tags_expr: &str) -> String {
    format!(
        "({} AND element_at({tags_expr}, 'building')[1] IS NULL)",
        has_any_key_sql(tags_expr)
    )
}

/// Rust twin of `matched_key_sql`. Returns the canonical `&'static str` from
/// `LIFECYCLE_BUILDING_KEYS`, never a copy of the changeset's own `String`,
/// so the two producers (`import osm`'s SQL and `update osm`'s Rust) can only
/// ever write byte-identical values. Returns `None` when a live `building`
/// key is present (see the disjointness decision in Step 3 of the plan) —
/// such an object stays a normal `osm_buildings` row, not a former one.
pub fn key_of(tags: &[(String, String)]) -> Option<&'static str> {
    if tags.iter().any(|(k, _)| k == "building") {
        return None;
    }
    LIFECYCLE_BUILDING_KEYS
        .iter()
        .find(|&&key| tags.iter().any(|(k, _)| k == key))
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_of_returns_the_highest_priority_key_when_several_are_present() {
        // was:building is lower priority than demolished:building in
        // LIFECYCLE_BUILDING_KEYS, so demolished:building must win.
        let tags = vec![
            ("was:building".to_string(), "yes".to_string()),
            ("demolished:building".to_string(), "house".to_string()),
        ];
        assert_eq!(key_of(&tags), Some("demolished:building"));
    }

    #[test]
    fn key_of_is_none_when_no_lifecycle_key_is_present() {
        let tags = vec![("addr:housenumber".to_string(), "12".to_string())];
        assert_eq!(key_of(&tags), None);
    }

    #[test]
    fn key_of_is_none_when_a_live_building_key_coexists() {
        // building=service + disused:building=train_station: a building that
        // still stands and is no longer a station — not a former building.
        let tags = vec![
            ("building".to_string(), "service".to_string()),
            ("disused:building".to_string(), "train_station".to_string()),
        ];
        assert_eq!(key_of(&tags), None);
    }

    /// Turns a `&[(String, String)]` fixture into a DuckDB list literal of
    /// quoted strings, e.g. `['a', 'b']` — used to build a MAP literal for
    /// the parity test below.
    fn sql_string_list(items: &[String]) -> String {
        let quoted: Vec<String> = items
            .iter()
            .map(|s| format!("'{}'", s.replace('\'', "''")))
            .collect();
        format!("[{}]", quoted.join(", "))
    }

    /// The anti-drift test: for the same tag set, the SQL `import osm`
    /// evaluates and the Rust `update osm` evaluates must agree on what an
    /// object's `lifecycle_key` is, or the two producers could write
    /// different values for the same object — or one could create a row the
    /// other can never delete. Needs no PBF, just a DuckDB MAP built from the
    /// fixture `key_of` sees.
    ///
    /// The SQL side composes both halves the import statement applies —
    /// `is_former_building_sql` (its WHERE) around `matched_key_sql` (its
    /// projection) — rather than the projection alone. That is the whole
    /// point: pairing only the projection against `key_of` would leave the
    /// domains mismatched (an object carrying `building` *and* a lifecycle
    /// key: the projection yields the key, `key_of` yields `None`), forcing
    /// the fixtures to dodge exactly the case worth pinning. Composing the
    /// real predicate means deleting the disjointness half from
    /// `is_former_building_sql` breaks this test, because `key_of` still
    /// applies it.
    #[test]
    fn matched_key_sql_agrees_with_key_of() {
        let fixtures: Vec<Vec<(String, String)>> = vec![
            // single key
            vec![("demolished:building".to_string(), "yes".to_string())],
            // multiple keys: priority order decides
            vec![
                ("was:building".to_string(), "yes".to_string()),
                ("demolished:building".to_string(), "house".to_string()),
            ],
            vec![
                ("ruins:building".to_string(), "yes".to_string()),
                ("razed:building".to_string(), "yes".to_string()),
                ("abandoned:building".to_string(), "yes".to_string()),
            ],
            // The disjointness case, ~378 objects nationally: a live
            // `building` key alongside a lifecycle one (a station building
            // that still stands but is no longer a station). Neither producer
            // may treat this as a former building.
            vec![
                ("building".to_string(), "service".to_string()),
                ("disused:building".to_string(), "train_station".to_string()),
            ],
            vec![
                ("building".to_string(), "construction".to_string()),
                ("demolished:building".to_string(), "retail".to_string()),
                ("was:building".to_string(), "yes".to_string()),
            ],
            // no lifecycle key at all
            vec![("addr:housenumber".to_string(), "12".to_string())],
            vec![],
        ];

        let conn = duckdb::Connection::open_in_memory().unwrap();
        for tags in fixtures {
            let expected = key_of(&tags);

            let keys: Vec<String> = tags.iter().map(|(k, _)| k.clone()).collect();
            let values: Vec<String> = tags.iter().map(|(_, v)| v.clone()).collect();
            let sql = format!(
                "SELECT CASE WHEN {} THEN {} END
                 FROM (SELECT map({}::VARCHAR[], {}::VARCHAR[]) AS tags) t",
                is_former_building_sql("tags"),
                matched_key_sql("tags"),
                sql_string_list(&keys),
                sql_string_list(&values),
            );
            let got: Option<String> = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
            assert_eq!(got.as_deref(), expected, "tags = {tags:?}");
        }
    }
}
