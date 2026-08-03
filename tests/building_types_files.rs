//! Structural invariants of the committed building-type mapping files. The
//! repo has no CI workflow, so these run as part of `cargo test` instead —
//! mirrors `tests/street_mappings_file.rs`. The DB-backed validation (the
//! full loader, including drift against a real `bdot10k_buildings`/
//! `egib_buildings`) is unit-tested in `src/mappings/building_types.rs`; this
//! file only needs plain string parsing, no DuckDB, so it stays fast.

use std::collections::HashSet;

const BDOT10K_PATH: &str = "mappings/bdot10k_building_types.csv";
const EGIB_PATH: &str = "mappings/egib_building_types.csv";

/// Split one CSV line honouring `""`-escaped quoted fields (the same routine
/// as `tests/street_mappings_file.rs`, since `k`'s `fixme` value embeds a
/// comma inside quotes).
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

/// (header fields, data rows) for `path`.
fn rows(path: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let text = std::fs::read_to_string(path).expect("mapping file must exist");
    let mut lines = text.lines();
    let header = split_csv_line(lines.next().expect("file must have a header"));
    let rows = lines
        .filter(|l| !l.is_empty())
        .map(split_csv_line)
        .collect();
    (header, rows)
}

fn tags_index(header: &[String]) -> usize {
    header
        .iter()
        .position(|c| c == "tags")
        .expect("header must have a 'tags' column")
}

fn tier_index(header: &[String]) -> usize {
    header
        .iter()
        .position(|c| c == "tier")
        .expect("header must have a 'tier' column")
}

fn key_index(header: &[String]) -> usize {
    header
        .iter()
        .position(|c| c == "key")
        .expect("header must have a 'key' column")
}

fn specificity_index(header: &[String]) -> [usize; 3] {
    ["min_levels", "max_levels", "max_neighbours"].map(|c| {
        header
            .iter()
            .position(|h| h == c)
            .unwrap_or_else(|| panic!("header must have a '{c}' column"))
    })
}

fn assert_file_is_well_formed(path: &str, expected_cols: usize) {
    let (header, data) = rows(path);
    assert_eq!(
        header.len(),
        expected_cols,
        "{path}: expected {expected_cols} header columns, got {:?}",
        header
    );
    let tags_i = tags_index(&header);
    let tier_i = tier_index(&header);
    let key_i = key_index(&header);
    let spec_i = specificity_index(&header);

    let mut seen = HashSet::new();
    for (i, r) in data.iter().enumerate() {
        let line = i + 2;
        assert_eq!(
            r.len(),
            expected_cols,
            "{path}: row {line} has {} fields, expected {expected_cols}",
            r.len()
        );

        // Every tags value must parse as ';'-separated k=v and include
        // 'building' -- the hard invariant from CLAUDE.md/building_type_mappings.md.
        let mut has_building = false;
        for part in r[tags_i].split(';') {
            let part = part.trim();
            assert!(
                !part.is_empty(),
                "{path}: row {line} has an empty tag segment"
            );
            let (k, v) = part
                .split_once('=')
                .unwrap_or_else(|| panic!("{path}: row {line} tag '{part}' is not k=v"));
            assert!(
                !k.trim().is_empty() && !v.trim().is_empty(),
                "{path}: row {line} tag '{part}' has an empty key or value"
            );
            if k.trim() == "building" {
                has_building = true;
            }
        }
        assert!(
            has_building,
            "{path}: row {line} (key={:?}) has no 'building' tag",
            r[key_i]
        );

        // No duplicate (tier, lower(key), specificity) -- would tie in the
        // serve query's precedence ORDER BY.
        let tier = r[tier_i].trim();
        let key = r[key_i].trim().to_lowercase();
        let specificity = spec_i.iter().filter(|&&c| !r[c].trim().is_empty()).count();
        assert!(
            seen.insert((tier.to_string(), key.clone(), specificity)),
            "{path}: row {line} (tier={tier}, key={key:?}) ties an earlier row on specificity"
        );

        // min_levels <= max_levels when both present.
        let (min_i, max_i, nb_i) = (spec_i[0], spec_i[1], spec_i[2]);
        if !r[min_i].trim().is_empty() && !r[max_i].trim().is_empty() {
            let min: i64 = r[min_i]
                .trim()
                .parse()
                .expect("min_levels must be an integer");
            let max: i64 = r[max_i]
                .trim()
                .parse()
                .expect("max_levels must be an integer");
            assert!(min <= max, "{path}: row {line} has min_levels > max_levels");
        }
        if !r[nb_i].trim().is_empty() {
            let n: i64 = r[nb_i]
                .trim()
                .parse()
                .expect("max_neighbours must be an integer");
            assert!(n >= 0, "{path}: row {line} has negative max_neighbours");
        }
    }
}

#[test]
fn bdot10k_file_is_well_formed() {
    assert_file_is_well_formed(BDOT10K_PATH, 6);
}

#[test]
fn egib_file_is_well_formed() {
    assert_file_is_well_formed(EGIB_PATH, 7);
}

/// The two keys this repo's `mappings::building_types` treats as the
/// adjacency class must actually be present and carry a `max_neighbours=0`
/// row, or the `detached`/`house` (resp. EGIB's storey-refined) split never
/// fires for anyone.
#[test]
fn adjacency_keys_have_a_zero_neighbour_row() {
    let (header, data) = rows(BDOT10K_PATH);
    let key_i = key_index(&header);
    let nb_i = specificity_index(&header)[2];
    assert!(
        data.iter()
            .any(|r| r[key_i].trim() == "budynek jednorodzinny" && r[nb_i].trim() == "0"),
        "bdot10k file must have a max_neighbours=0 row for 'budynek jednorodzinny'"
    );

    let (header, data) = rows(EGIB_PATH);
    let key_i = key_index(&header);
    let nb_i = specificity_index(&header)[2];
    assert!(
        data.iter()
            .any(|r| r[key_i].trim() == "m" && r[nb_i].trim() == "0"),
        "egib file must have a max_neighbours=0 row for 'm'"
    );
}
