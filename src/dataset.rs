//! Per-source metadata ([`DatasetSpec`]) plus the shared load-time helpers
//! used by both `import` and `update`'s staging load: [`non_null_key_sql`] /
//! [`null_key_sql`] (record-identity filtering), [`deduplicate_by_key`],
//! [`filter_invalid_geometry`] and [`filter_oversized_geometry`].
//!
//! [`DatasetSpec::changed_predicate_sql`] is the single home for the
//! comparison a refresh uses to decide whether a record is "modified" — see
//! its doc comment and `docs/superpowers/plans/2026-08-14-key-based-diff.md`
//! for the measurements behind each source's `compared_columns` /
//! `compare_geometry` choice.

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
    /// Unique, non-null record identity, guaranteed at load by
    /// `dataset::non_null_key_sql` + `dataset::deduplicate_by_key`.
    pub key_columns: &'static [&'static str],
    /// Columns compared to decide "modified". Never volatile export metadata.
    pub compared_columns: &'static [&'static str],
    /// Whether geometry participates in the comparison.
    pub compare_geometry: bool,
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

    /// Wrap `select_sql` so a `Polygon` source also gains a persisted
    /// `centroid GEOMETRY` column, computed from `geom`. Safe to add outside
    /// the diff's view: `centroid` is simply not one of the names in
    /// `compared_columns`, so it cannot affect what `changed_predicate_sql`
    /// compares.
    ///
    /// That has a consequence, though. Because `centroid` sits outside the
    /// compared set, its value on an *unmodified* record never self-heals —
    /// it is recomputed for every row a refresh *stages*, so a record
    /// rewritten for some other reason (e.g. its `WERSJA` bumped) picks up a
    /// corrected expression for free, but a record the diff never touches
    /// keeps whatever `centroid` its last import or full rewrite computed,
    /// forever. **Editing this expression requires a re-import, not a
    /// refresh**, to reach every row.
    ///
    /// A no-op passthrough for `Point` sources (PRG), which have no separate
    /// centroid to store.
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

    /// SQL predicate that is true when the record under alias `a` differs
    /// from the record under alias `b` in a way this source cares about.
    ///
    /// This is the ONE place the comparison is written; see
    /// `docs/superpowers/plans/2026-08-14-key-based-diff.md` for the
    /// measurements behind each source's `compared_columns` /
    /// `compare_geometry` choice. Shape: a row-wise
    /// `(a.c1, a.c2, ...) IS DISTINCT FROM (b.c1, b.c2, ...)` over
    /// `compared_columns`, plus `OR ST_AsWKB(a.geom) IS DISTINCT FROM
    /// ST_AsWKB(b.geom)` when `compare_geometry` is set. The whole result is
    /// parenthesized so a caller can safely append ` AND ...` to it.
    ///
    /// Row-wise `IS DISTINCT FROM` is used specifically because it is
    /// NULL-safe in DuckDB (`(NULL, 1) IS DISTINCT FROM (NULL, 1)` is false,
    /// `(NULL, 1) IS DISTINCT FROM (2, 1)` is true) — EGIB depends on this,
    /// since 617,207 of its records have all three compared attributes NULL
    /// and would otherwise compare as permanently "distinct".
    ///
    /// Geometry MUST be compared via `ST_AsWKB(...)`, never the bare
    /// `a.geom IS DISTINCT FROM b.geom` — measured on the real 17.5M-row
    /// EGIB table: native GEOMETRY comparison took 24.18s against 2.50s for
    /// `ST_AsWKB`, for the identical answer.
    pub fn changed_predicate_sql(&self, a: &str, b: &str) -> String {
        let attrs = if self.compared_columns.is_empty() {
            None
        } else {
            let lhs = self
                .compared_columns
                .iter()
                .map(|c| format!("{a}.{c}"))
                .collect::<Vec<_>>()
                .join(", ");
            let rhs = self
                .compared_columns
                .iter()
                .map(|c| format!("{b}.{c}"))
                .collect::<Vec<_>>()
                .join(", ");
            Some(format!("({lhs}) IS DISTINCT FROM ({rhs})"))
        };

        let geom = if self.compare_geometry {
            Some(format!(
                "ST_AsWKB({a}.geom) IS DISTINCT FROM ST_AsWKB({b}.geom)"
            ))
        } else {
            None
        };

        match (attrs, geom) {
            (Some(attrs), Some(geom)) => format!("({attrs} OR {geom})"),
            (Some(attrs), None) => format!("({attrs})"),
            (None, Some(geom)) => format!("({geom})"),
            // Nothing is compared, so nothing can ever be modified.
            (None, None) => "(FALSE)".to_string(),
        }
    }

    /// SQL for this source's record key under `alias`, as a `VARCHAR[]` list
    /// literal in `key_columns` order: `[a.k1::VARCHAR, a.k2::VARCHAR]`.
    ///
    /// The list shape exists because BDOT10k's key is the composite
    /// `(PRZESTRZENNAZW, LOKALNYID)` while EGIB's and PRG's are single columns,
    /// and `object_reports` stores all three in one table. The `::VARCHAR`
    /// casts are not decoration: every key column happens to be `VARCHAR`
    /// today, so an uncast list would work by accident, and a future source
    /// with a numeric key would silently produce a list of a different type
    /// that compares equal to nothing.
    ///
    /// **DuckDB's list `=` treats NULL as equal to NULL** -- verified, not
    /// assumed: `[NULL] = [NULL]` is `true` and `[NULL,'x'] = ['a','x']` is
    /// `false`, i.e. the `EXCEPT`/`IS NOT DISTINCT FROM` semantics, not scalar
    /// `=`'s. So a NULL-keyed report *would* match a NULL-keyed live record
    /// rather than harmlessly matching nothing, and the thing preventing a
    /// silent wrong-row veto is that neither can exist: `non_null_key_sql`
    /// drops NULL-keyed records at load, and `reports::insert` resolves every
    /// report against a live row before storing it. Do not relax either on the
    /// assumption that NULL is inert here.
    pub fn key_list_sql(&self, alias: &str) -> String {
        let items = self
            .key_columns
            .iter()
            .map(|c| format!("{alias}.{c}::VARCHAR"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{items}]")
    }

    /// A digest of everything this source treats as *content*, for detecting
    /// that a record has changed since some earlier point in time without
    /// having a second copy of it to diff against.
    ///
    /// Built from the same `compared_columns` + `compare_geometry` as
    /// [`changed_predicate_sql`], and that pairing is the whole point: "did
    /// this record change" must have one answer, whether it is asked by the
    /// refresh diff (which has both versions in hand) or by
    /// `reports::reconcile_source` (which has only a stored signature). If the
    /// two ever disagree, a user report either lapses while its object is
    /// unchanged or survives a change that should have revived the object.
    /// `dataset::tests::signature_changes_exactly_when_the_diff_says_modified`
    /// pins them together.
    ///
    /// **Do not widen this to hash every column.** Hashing a whole row cannot
    /// tell a record changing from its *serialization* changing: one BDOT10k
    /// re-export rewrote all 16,344,762 rows byte-for-byte and would enqueue
    /// every z14 cell in Poland. Hashing only the curated compared set
    /// (BDOT10k's excludes geometry for exactly that reason) is what keeps that
    /// from happening. Evaluated only for rows that carry a report --
    /// `O(active reports)`, never `O(source table)`.
    ///
    /// NULL handling is explicit because DuckDB's `concat` *skips* NULL inputs
    /// rather than propagating them, which would make `(NULL, 'x')` and
    /// `('x', NULL)` hash identically. Each element is `COALESCE`d to a
    /// sentinel and elements are separated, so neither can happen.
    ///
    /// Geometry is digested as `hex(ST_AsWKB(...))` rather than the bare
    /// `GEOMETRY`, matching `changed_predicate_sql`'s reason for `ST_AsWKB`:
    /// native geometry comparison measured 24.18s against 2.50s on the real
    /// 17.5M-row EGIB table.
    pub fn content_signature_sql(&self, alias: &str) -> String {
        // chr(30) = record separator, chr(31) = unit separator. Both are
        // control characters that cannot occur in these sources' text columns
        // or in hex output, so neither a value containing the separator nor a
        // value equal to the NULL sentinel is reachable.
        const NULL_SENTINEL: &str = "chr(30)";
        const SEPARATOR: &str = "chr(31)";

        let mut parts: Vec<String> = self
            .compared_columns
            .iter()
            .map(|c| format!("COALESCE({alias}.{c}::VARCHAR, {NULL_SENTINEL})"))
            .collect();

        if self.compare_geometry {
            parts.push(format!(
                "COALESCE(hex(ST_AsWKB({alias}.geom)), {NULL_SENTINEL})"
            ));
        }

        // Nothing is compared, so nothing can ever change -- mirroring
        // `changed_predicate_sql`'s `(FALSE)` arm. `concat()` with no arguments
        // is a syntax error, so this case needs its own constant.
        if parts.is_empty() {
            return "md5('')".to_string();
        }

        format!("md5(concat({}))", parts.join(&format!(", {SEPARATOR}, ")))
    }
}

/// Look a source up by the name it is known by everywhere else -- CLI
/// arguments, `match_dirty_cells.source`, `object_reports.source`, job names.
///
/// Returns `None` rather than panicking because the one caller that matters
/// (`server::reports`) resolves a string straight out of an HTTP request body,
/// where an unknown source is a 400 and not a bug.
pub fn spec_by_name(name: &str) -> Option<&'static DatasetSpec> {
    match name {
        "bdot10k" => Some(&BDOT10K),
        "egib" => Some(&EGIB),
        "prg" => Some(&PRG),
        _ => None,
    }
}

/// Every source, in a stable order, for callers that sweep all of them
/// (`reports::reconcile_all`, `compare::reconcile::enqueue_all`'s shape).
pub const ALL_SPECS: &[&DatasetSpec] = &[&BDOT10K, &EGIB, &PRG];

pub const BDOT10K: DatasetSpec = DatasetSpec {
    name: "bdot10k",
    table: "bdot10k_buildings",
    key_columns: &["PRZESTRZENNAZW", "LOKALNYID"],
    // WERSJA plus the retained attributes; geometry deliberately excluded.
    // BDOT10k periodically re-serializes every geometry wholesale: 0.94% of
    // rows differed between 03-15 and 04-19, 4.5% between 04-19 and 08-01,
    // and 100% (16,344,762 of 16,344,762) in the 2026-08-10 export.
    // Re-serialized bytes are indistinguishable from real movement, so
    // comparing geometry would mean one refresh per re-serialization event
    // rewriting the whole table and dirtying every z14 cell in Poland. Cost
    // of excluding it: a geometry-only edit with no WERSJA bump and no
    // attribute change is missed, self-healing whenever that record next
    // changes for another reason.
    //
    // The comparison must be IS DISTINCT FROM, never > or >=: attributes
    // change WITHOUT a WERSJA bump (6,395 records in the 03-15 -> 04-19
    // pair, including 64 KATEGORIAISTNIENIA transitions, the column
    // rule::BDOT10K_EKSPLOATOWANY_FILTER gates on), so a > predicate would
    // miss them outright. A symmetric predicate also means a staged record
    // whose WERSJA went backwards replaces the live one rather than freezing
    // it -- upstream's latest export is treated as the truth, not the
    // highest version number. Measured 2026-08-15: after deduplicate_by_key
    // (which orders WERSJA DESC), backwards movement is 0 across all three
    // national pairs, so that second case is theoretical; the first is not.
    // See docs/superpowers/plans/2026-08-14-key-based-diff.md.
    compared_columns: &[
        "WERSJA",
        "KATEGORIAISTNIENIA",
        "PRZEWAZAJACAFUNKCJABUDYNKU",
        "FUNKCJAOGOLNABUDYNKU",
        "LICZBAKONDYGNACJI",
        "NAZWA",
        "FSBUD",
        "INFORMACJADODATKOWA",
        "KODKST",
        "ZRODLODANYCHGEOMETRYCZNYCH",
    ],
    compare_geometry: false,
    geom_kind: GeomKind::Polygon,
};

pub const EGIB: DatasetSpec = DatasetSpec {
    name: "egib",
    table: "egib_buildings",
    key_columns: &["id_budynku"],
    // `czas_pozyskania` (99.7% churn per export) and `pozostale_atrybuty`
    // (32.8%, carries a per-export gml_id) are excluded as pure export
    // noise. Geometry is NOT optional here, unlike BDOT10k: 617,207 records
    // have all three of these attributes NULL, so geometry is their only
    // signal of change. See docs/superpowers/plans/2026-08-14-key-based-diff.md.
    compared_columns: &["rodzaj", "kondygnacje_nadziemne", "kondygnacje_podziemne"],
    compare_geometry: true,
    geom_kind: GeomKind::Polygon,
};

pub const PRG: DatasetSpec = DatasetSpec {
    name: "prg",
    table: "prg_addresses",
    key_columns: &["lokalny_id"],
    // `wersja_id` and `poczatek_wersji_obiektu` are deliberately absent.
    // PRG bulk-republishes by gmina: over four consecutive snapshot pairs,
    // `wersja_id` moved 34-147x more often than any content changed
    // (149,198 version bumps vs 1,012 content changes in the 4-day pair).
    // Downstream that is 1,549 z14 dirty cells instead of 182.
    // `poczatek_wersji_obiektu` moves in exact lockstep with `wersja_id` (0
    // disagreements in 8.6M records), so it is a second version-metadata
    // column, not content -- including it would silently reinstate
    // version-only churn. See docs/superpowers/plans/2026-08-14-key-based-diff.md.
    compared_columns: &[
        "numer_porzadkowy",
        "ulica",
        "miejscowosc",
        "kod_pocztowy",
        "teryt_miejscowosc",
    ],
    compare_geometry: true,
    geom_kind: GeomKind::Point,
};

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
/// This filter sits INSIDE the load SELECT, alongside expressions that
/// rewrite a row's *value* (e.g. `import::prg::ULICA_PREFIX_STRIP_SQL`). A
/// row filter is a different kind of thing from those: it changes which rows
/// exist, never the content of a surviving row, which is what makes it safe
/// to sit wherever it does relative to the other load-time steps — none of
/// them can observe a row this filter already dropped, and this filter
/// cannot alter what they see from a row it lets through.
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
/// Runs strictly after the table is built. Like [`non_null_key_sql`], this is
/// a row filter -- it changes which rows exist, never the content of a
/// surviving row -- so it needs nothing further to stay correct as the
/// load-time steps around it change.
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

/// Drop `column` from `table` immediately after [`deduplicate_by_key`] has
/// consumed it as the ranking window's `order_by`. Shared by
/// `import::egib::load_into` (`czas_pozyskania`) and
/// `import::prg::materialize_into` (`wersja_id`) -- one mechanism for both
/// sources, per
/// `docs/superpowers/plans/2026-08-14-column-trimming.md`'s "the
/// ordering-column problem".
///
/// `deduplicate_by_key`'s `order_by` is interpolated into a window function
/// running against the table *after* it already exists (`row_number() OVER
/// (PARTITION BY {keys} ORDER BY {order_by} ...)`), so the ordering column
/// has to be a real column of the table at dedup time -- it cannot be
/// projected away by the `CREATE TABLE AS SELECT` that builds the table, the
/// way e.g. PRG's `dlugosc_geograficzna` is consumed to build `geom` and
/// simply never appears in the output column list. This runs the moment
/// after dedup no longer needs it, so nothing downstream (the diff, the
/// serving tables, `/tiles`) ever sees a column nothing reads.
///
/// This is the one place in the codebase that runs `ALTER TABLE` -- but it
/// is not the migration path several other gotchas explain does not exist:
/// it runs against a table the caller's own `CREATE TABLE AS SELECT` just
/// built moments earlier in the same load, never against a pre-existing
/// live database.
pub fn drop_ordering_column(
    conn: &duckdb::Connection,
    table: &str,
    column: &str,
) -> anyhow::Result<()> {
    use anyhow::Context;

    conn.execute_batch(&format!("ALTER TABLE {table} DROP COLUMN {column};"))
        .with_context(|| format!("Failed to drop ordering column {column} from {table}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the whole report-expiry mechanism rests on:
    /// `content_signature_sql` must differ *exactly* when
    /// `changed_predicate_sql` says the record was modified.
    ///
    /// If the signature were more sensitive, reports would expire on churn the
    /// diff deliberately ignores -- reinstating exactly the PRG-version and
    /// BDOT10k-re-serialization noise `compared_columns` was tuned to exclude.
    /// If it were less sensitive, a corrected record would stay vetoed forever.
    /// Neither failure produces an error; both are silent, which is why this is
    /// a property test over a matrix rather than a couple of examples.
    #[test]
    fn signature_changes_exactly_when_the_diff_says_modified() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        // Pairs of (a, b, geom) values covering: identical rows, each compared
        // column moving alone, geometry moving alone, NULL appearing and
        // disappearing, NULL-vs-NULL (which must NOT read as changed), and the
        // ('x', NULL) / (NULL, 'x') transposition -- the case a naive
        // `concat` would hash identically, because DuckDB's concat skips NULLs
        // rather than propagating them.
        conn.execute_batch(
            "CREATE TABLE l (id VARCHAR, a VARCHAR, b VARCHAR, geom GEOMETRY);
             CREATE TABLE s (id VARCHAR, a VARCHAR, b VARCHAR, geom GEOMETRY);
             INSERT INTO l VALUES
                 ('same',      'x',  'y',  ST_Point(1,1)),
                 ('a_moved',   'x',  'y',  ST_Point(1,1)),
                 ('b_moved',   'x',  'y',  ST_Point(1,1)),
                 ('geom_moved','x',  'y',  ST_Point(1,1)),
                 ('to_null',   'x',  'y',  ST_Point(1,1)),
                 ('from_null', NULL, 'y',  ST_Point(1,1)),
                 ('both_null', NULL, NULL, ST_Point(1,1)),
                 ('transposed','x',  NULL, ST_Point(1,1)),
                 ('null_geom', 'x',  'y',  NULL);
             INSERT INTO s VALUES
                 ('same',      'x',  'y',  ST_Point(1,1)),
                 ('a_moved',   'X!', 'y',  ST_Point(1,1)),
                 ('b_moved',   'x',  'Y!', ST_Point(1,1)),
                 ('geom_moved','x',  'y',  ST_Point(2,2)),
                 ('to_null',   NULL, 'y',  ST_Point(1,1)),
                 ('from_null', 'x',  'y',  ST_Point(1,1)),
                 ('both_null', NULL, NULL, ST_Point(1,1)),
                 ('transposed',NULL, 'x',  ST_Point(1,1)),
                 ('null_geom', 'x',  'y',  NULL);",
        )
        .unwrap();

        // Both polarities of `compare_geometry`, since BDOT10k excludes
        // geometry while EGIB and PRG include it -- the signature has to track
        // that choice, not make its own.
        for compare_geometry in [true, false] {
            let spec = DatasetSpec {
                name: "test",
                table: "l",
                key_columns: &["id"],
                compared_columns: &["a", "b"],
                compare_geometry,
                geom_kind: GeomKind::Point,
            };
            let sql = format!(
                "SELECT l.id, {changed}, {sig_s} IS DISTINCT FROM {sig_l}
                 FROM l JOIN s USING (id) ORDER BY l.id",
                changed = spec.changed_predicate_sql("s", "l"),
                sig_s = spec.content_signature_sql("s"),
                sig_l = spec.content_signature_sql("l"),
            );
            let mut stmt = conn.prepare(&sql).unwrap();
            let rows: Vec<(String, bool, bool)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(rows.len(), 9, "every fixture row must be compared");
            for (id, diff_says_changed, signature_differs) in rows {
                assert_eq!(
                    diff_says_changed, signature_differs,
                    "compare_geometry={compare_geometry}, row '{id}': the diff and the \
                     signature disagree about whether this record changed"
                );
            }
        }
    }

    /// The specific trap the NULL sentinel and separator exist for: without
    /// them, `concat` would skip NULLs and hash ('x', NULL) and (NULL, 'x')
    /// identically, so a record whose two attributes swapped would keep its
    /// report alive forever. Asserted directly rather than left implicit in the
    /// matrix above, because it is the one case where a plausible simpler
    /// implementation is silently wrong.
    #[test]
    fn signature_distinguishes_a_null_from_a_shifted_value() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let spec = DatasetSpec {
            name: "test",
            table: "t",
            key_columns: &["id"],
            compared_columns: &["a", "b"],
            compare_geometry: false,
            geom_kind: GeomKind::Point,
        };
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, a VARCHAR, b VARCHAR);
             INSERT INTO t VALUES ('l', 'x', NULL), ('r', NULL, 'x');",
        )
        .unwrap();
        let differ: bool = conn
            .query_row(
                &format!(
                    "SELECT (SELECT {sig} FROM t WHERE id = 'l')
                         IS DISTINCT FROM (SELECT {sig} FROM t WHERE id = 'r')",
                    sig = spec.content_signature_sql("t"),
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(differ, "('x', NULL) and (NULL, 'x') must not hash alike");
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
    fn with_centroid_select_wraps_polygon_sources() {
        let select = "SELECT 1 AS a, ST_Point(1, 2) AS geom";
        let wrapped = BDOT10K.with_centroid_select(select);
        assert_eq!(
            wrapped,
            format!("SELECT *, ST_Centroid(geom) AS centroid FROM ({select}) t")
        );
    }

    #[test]
    fn with_centroid_select_is_a_noop_for_points() {
        let select = "SELECT 1 AS a, ST_Point(1, 2) AS geom";
        assert_eq!(PRG.with_centroid_select(select), select);
    }

    /// The load-bearing invariant from `with_centroid_select`'s doc: adding
    /// `centroid` must not change what `changed_predicate_sql` compares,
    /// since `centroid` is not one of `compared_columns`. Uses a
    /// locally-defined `DatasetSpec` with a smaller `compared_columns` than
    /// `BDOT10K`'s real ten columns -- this pins the general mechanism
    /// `with_centroid_select` relies on, not BDOT10K's specific column list,
    /// so the test data stays small. Mirrors
    /// `mappings::egib::tests::does_not_change_what_the_diff_compares`, the
    /// equivalent test for `with_rodzaj_kod_select`.
    #[test]
    fn with_centroid_select_does_not_change_what_the_diff_compares() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id VARCHAR, a VARCHAR, geom GEOMETRY);
             INSERT INTO src VALUES
                 ('1', 'x', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('2', NULL, NULL);",
        )
        .unwrap();

        let spec = DatasetSpec {
            name: "test",
            table: "t",
            key_columns: &["id"],
            compared_columns: &["a"],
            compare_geometry: false,
            geom_kind: GeomKind::Polygon,
        };

        let inner = "SELECT id, a, geom FROM src";
        let with_centroid = spec.with_centroid_select(inner);

        conn.execute_batch(&format!(
            "CREATE TABLE plain AS {inner};
             CREATE TABLE with_centroid AS {with_centroid};"
        ))
        .unwrap();

        let differing: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM plain p JOIN with_centroid c USING (id)
                     WHERE {}",
                    spec.changed_predicate_sql("p", "c")
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            differing, 0,
            "adding centroid outside compared_columns must not change what the diff sees"
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
    fn drop_ordering_column_removes_the_column_and_keeps_the_rest() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, ordering_col INTEGER, kept VARCHAR);
             INSERT INTO t VALUES ('a', 1, 'x');",
        )
        .unwrap();

        drop_ordering_column(&conn, "t", "ordering_col").unwrap();

        let cols: Vec<String> = {
            let mut stmt = conn
                .prepare(
                    "SELECT column_name FROM duckdb_columns()
                     WHERE table_name = 't' ORDER BY column_index",
                )
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(cols, vec!["id".to_string(), "kept".to_string()]);

        let kept: String = conn
            .query_row("SELECT kept FROM t WHERE id = 'a'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(kept, "x", "surviving rows and columns must be untouched");
    }

    #[test]
    fn format_skip_clause_omits_the_ids_clause_when_there_are_none() {
        assert_eq!(
            format_skip_clause("null-key", 210080, &[]),
            "skipped 210080 null-key rows"
        );
    }

    #[test]
    fn changed_predicate_sql_single_column_no_geometry() {
        let spec = DatasetSpec {
            name: "test",
            table: "t",
            key_columns: &["id"],
            compared_columns: &["rodzaj"],
            compare_geometry: false,
            geom_kind: GeomKind::Point,
        };
        assert_eq!(
            spec.changed_predicate_sql("s", "l"),
            "((s.rodzaj) IS DISTINCT FROM (l.rodzaj))"
        );
    }

    #[test]
    fn changed_predicate_sql_multi_column_with_geometry() {
        let spec = DatasetSpec {
            name: "test",
            table: "t",
            key_columns: &["id"],
            compared_columns: &["rodzaj", "kondygnacje_nadziemne", "kondygnacje_podziemne"],
            compare_geometry: true,
            geom_kind: GeomKind::Polygon,
        };
        assert_eq!(
            spec.changed_predicate_sql("s", "l"),
            "((s.rodzaj, s.kondygnacje_nadziemne, s.kondygnacje_podziemne) IS DISTINCT FROM \
             (l.rodzaj, l.kondygnacje_nadziemne, l.kondygnacje_podziemne) \
             OR ST_AsWKB(s.geom) IS DISTINCT FROM ST_AsWKB(l.geom))"
        );
    }

    /// Silent-regression territory: BDOT10k's geometry churn (100% in the
    /// 2026-08-10 export) means comparing geometry would rewrite every
    /// z14 cell in Poland on that kind of refresh. `compare_geometry: false`
    /// is what prevents that, so pin that the predicate text never mentions
    /// `geom` -- a future reader "fixing" `compare_geometry` back to `true`
    /// on the const should fail this test.
    #[test]
    fn bdot10k_predicate_does_not_mention_geometry() {
        assert!(!BDOT10K.changed_predicate_sql("s", "l").contains("geom"));
    }

    /// PRG's version columns (`wersja_id`, `poczatek_wersji_obiektu`) are
    /// pure bulk-republication noise -- see the comment on `PRG` -- and must
    /// never appear in the comparison. Geometry, unlike BDOT10k, IS compared
    /// for PRG, so the predicate must mention `ST_AsWKB`.
    #[test]
    fn prg_predicate_omits_version_columns_and_uses_st_aswkb() {
        let sql = PRG.changed_predicate_sql("s", "l");
        assert!(!sql.contains("wersja_id"));
        assert!(!sql.contains("poczatek_wersji_obiektu"));
        assert!(sql.contains("ST_AsWKB"));
    }

    /// `IS DISTINCT FROM` must be NULL-safe on the row-wise tuple form, since
    /// EGIB depends on it (617,207 records with all three compared
    /// attributes NULL). Driven through `changed_predicate_sql` on a local
    /// spec rather than hand-written SQL, so this pins the actual generated
    /// predicate, not just DuckDB's general behaviour.
    #[test]
    fn changed_predicate_sql_is_null_safe() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let spec = DatasetSpec {
            name: "test",
            table: "t",
            key_columns: &["id"],
            compared_columns: &["a", "b"],
            compare_geometry: false,
            geom_kind: GeomKind::Point,
        };

        conn.execute_batch(
            "CREATE TABLE s (id VARCHAR, a INTEGER, b INTEGER);
             CREATE TABLE l (id VARCHAR, a INTEGER, b INTEGER);
             INSERT INTO s VALUES ('same', NULL, 1), ('diff', NULL, 1);
             INSERT INTO l VALUES ('same', NULL, 1), ('diff', 2, 1);",
        )
        .unwrap();

        let predicate = spec.changed_predicate_sql("s", "l");
        let changed: Vec<String> = {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT s.id FROM s JOIN l USING (id) WHERE {predicate} ORDER BY s.id"
                ))
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };

        assert_eq!(
            changed,
            vec!["diff".to_string()],
            "(NULL, 1) vs (NULL, 1) must be NOT distinct, (NULL, 1) vs (2, 1) must be distinct"
        );
    }

    /// Geometry participation must be driven by `compare_geometry`,
    /// exercised through real SQL rather than just string-inspecting the
    /// predicate: two rows with equal attributes and different geometry
    /// compare as changed when `compare_geometry: true`, and unchanged when
    /// `compare_geometry: false`.
    #[test]
    fn changed_predicate_sql_geometry_participation_follows_compare_geometry() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        conn.execute_batch(
            "CREATE TABLE s (id VARCHAR, a INTEGER, geom GEOMETRY);
             CREATE TABLE l (id VARCHAR, a INTEGER, geom GEOMETRY);
             INSERT INTO s VALUES
                 ('moved', 1, ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'));
             INSERT INTO l VALUES
                 ('moved', 1, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));",
        )
        .unwrap();

        let with_geom = DatasetSpec {
            name: "test",
            table: "t",
            key_columns: &["id"],
            compared_columns: &["a"],
            compare_geometry: true,
            geom_kind: GeomKind::Polygon,
        };
        let without_geom = DatasetSpec {
            compare_geometry: false,
            ..with_geom
        };

        let changed_with: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM s JOIN l USING (id) WHERE {}",
                    with_geom.changed_predicate_sql("s", "l")
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            changed_with, 1,
            "geometry-only change must count as modified when compare_geometry is true"
        );

        let changed_without: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM s JOIN l USING (id) WHERE {}",
                    without_geom.changed_predicate_sql("s", "l")
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            changed_without, 0,
            "geometry-only change must be invisible when compare_geometry is false"
        );
    }
}
