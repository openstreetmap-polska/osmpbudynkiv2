//! Carried columns: raw fields copied from a live building table into
//! `*_unmatched` at compare time, so `server::package::building_tags` can
//! resolve an OSM building tag at serve time, and `/tiles` can display them as
//! attributes, without joining back to the live table (barred by the
//! serving-table invariant -- see `CLAUDE.md`). Despite the name of this
//! struct/module, not every carried column is used for classification --
//! existence status, free-text names/descriptions, cartographic codes, and
//! geometry provenance are along for the ride too, purely for `/tiles`
//! display (see `docs/vector_tile_attributes.md`).
//!
//! Defined once, beside `rule.rs`, because `compare::buildings` and
//! `compare::incremental` both build the buildings serving-table INSERT and
//! must agree on it column-for-column -- `compare::full_vs_incremental_equivalence`
//! pins the two together. If they drifted, one path would keep serving stale
//! or absent columns while the other did not.

/// `dest_names`/`source_exprs` are positional, comma-separated, and must list
/// the same number of columns in the same order: the INSERT lists explicit
/// destination column names, and the SELECT is matched to them by position,
/// not by alias.
pub struct ClassificationColumns {
    /// Destination column names in `*_unmatched`.
    pub dest_names: &'static str,
    /// Source expressions read off the live table alias `b`.
    pub source_exprs: &'static str,
}

/// `source_table` is always one of the two building live tables -- a
/// code-level constant at every call site, never external input.
pub fn classification_columns(source_table: &str) -> ClassificationColumns {
    match source_table {
        // PRZESTRZENNAZW is carried for a different reason from every other
        // column here, and it is not display: it is the *other half of
        // BDOT10k's record key*. `dataset::BDOT10K.key_columns` is the
        // composite (PRZESTRZENNAZW, LOKALNYID) and LOKALNYID alone is not
        // unique, so without this column the serving table -- and therefore
        // `/tiles` and `/package`, which read only the serving table -- cannot
        // express a complete identity for a BDOT10k building. `POST /report`
        // needs one: a report keyed on a non-unique id would veto an arbitrary
        // one of the colliding buildings. EGIB (`id_budynku`) and PRG
        // (`lokalny_id`) have single-column keys and already carry them.
        "bdot10k_buildings" => ClassificationColumns {
            dest_names: "PRZESTRZENNAZW, funkcja_szczegolowa, funkcja_ogolna, liczba_kondygnacji, \
                         KATEGORIAISTNIENIA, NAZWA, FSBUD, INFORMACJADODATKOWA, KODKST, \
                         ZRODLODANYCHGEOMETRYCZNYCH",
            source_exprs: "b.PRZESTRZENNAZW, b.PRZEWAZAJACAFUNKCJABUDYNKU, b.FUNKCJAOGOLNABUDYNKU, \
                           b.LICZBAKONDYGNACJI, \
                           b.KATEGORIAISTNIENIA, b.NAZWA, b.FSBUD, b.INFORMACJADODATKOWA, b.KODKST, \
                           b.ZRODLODANYCHGEOMETRYCZNYCH",
        },
        "egib_buildings" => ClassificationColumns {
            dest_names: "rodzaj_kod, kondygnacje_nadziemne, kondygnacje_podziemne, rodzaj",
            source_exprs: "b.rodzaj_kod, b.kondygnacje_nadziemne, b.kondygnacje_podziemne, b.rodzaj",
        },
        other => panic!("classification_columns: unknown source table {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bdot10k_and_egib_column_lists_have_matching_arity() {
        for table in ["bdot10k_buildings", "egib_buildings"] {
            let cc = classification_columns(table);
            let dest_count = cc.dest_names.split(',').count();
            let src_count = cc.source_exprs.split(',').count();
            assert_eq!(
                dest_count, src_count,
                "{table}: dest_names and source_exprs must list the same number of columns"
            );
        }
    }

    #[test]
    #[should_panic(expected = "unknown source table")]
    fn unknown_source_table_panics_rather_than_silently_returning_nothing() {
        classification_columns("osm_buildings");
    }
}
