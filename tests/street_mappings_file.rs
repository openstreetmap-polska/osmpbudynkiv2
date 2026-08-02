//! Structural invariants of the committed mapping file. The repo has no CI
//! workflow, so these run as part of `cargo test` instead.

use std::collections::HashSet;

const PATH: &str = "mappings/street_names_mappings.csv";

/// Split one CSV line into 3 fields, honouring `""`-escaped quoted fields.
fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => fields.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    fields.push(cur);
    fields
}

fn rows() -> Vec<Vec<String>> {
    let text = std::fs::read_to_string(PATH).expect("mapping file must exist");
    let mut lines = text.lines();
    let header = lines.next().expect("file must have a header");
    assert_eq!(
        header, "teryt_simc_code,prg_street_name,osm_street_name",
        "unexpected header"
    );
    lines
        .filter(|l| !l.is_empty())
        .map(split_csv_line)
        .collect()
}

#[test]
fn every_row_has_three_fields_and_a_non_empty_mapping() {
    for (i, r) in rows().iter().enumerate() {
        assert_eq!(r.len(), 3, "row {} has {} fields", i + 2, r.len());
        assert!(!r[1].is_empty(), "row {} has empty prg_street_name", i + 2);
        assert!(!r[2].is_empty(), "row {} has empty osm_street_name", i + 2);
    }
}

#[test]
fn no_field_has_leading_or_trailing_whitespace() {
    for (i, r) in rows().iter().enumerate() {
        for (col, v) in r.iter().enumerate() {
            assert_eq!(
                v.trim(),
                v,
                "row {} col {} is not trimmed: {v:?}",
                i + 2,
                col
            );
        }
    }
}

#[test]
fn keys_are_unique_case_insensitively() {
    let mut seen = HashSet::new();
    for (i, r) in rows().iter().enumerate() {
        let key = (r[0].clone(), r[1].to_lowercase());
        assert!(seen.insert(key), "row {} duplicates an earlier key", i + 2);
    }
}

#[test]
fn file_is_sorted_for_stable_diffs() {
    let all = rows();
    let mut sorted = all.clone();
    sorted.sort_by_key(|r| (r[1].to_lowercase(), r[0].clone()));
    assert_eq!(all, sorted, "file must be sorted by (lower(name), simc)");
}

#[test]
fn a_known_global_and_a_known_settlement_row_are_present() {
    let all = rows();
    assert!(
        all.iter()
            .any(|r| r[0].is_empty() && r[1] == "Kościuszki" && r[2] == "Tadeusza Kościuszki"),
        "expected the global Kościuszki row"
    );
    assert!(
        all.iter().any(|r| r[0] == "0212529"
            && r[1] == "Kościuszki"
            && r[2] == "Generała Tadeusza Kościuszki"),
        "expected the Dobieszowice exception row"
    );
}
