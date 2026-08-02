# Map PRG street names to OSM street names

Written 2026-08-01. PRG publishes abbreviated street names (`gen. Romualda
Traugutta`, `św. Jerzego`, `plac Wolności`) while OSM Poland overwhelmingly
uses expanded ones (`Generała …`, `Świętego …`, `Plac …`). `/package` currently
emits `addr:street` verbatim from `prg_addresses.ulica`, so an importer has to
expand every abbreviation by hand in JOSM before uploading. This adds a
curated mapping, applied at serving time, so the GeoJSON a user downloads is
importable as-is.

The legacy gugik2osm deployment carried a 12,221-row mapping file
(`teryt_simc_code, teryt_ulic_code, teryt_street_name, osm_street_name`). That
file is the input to the migration described below, not the shipped artifact.

## Scope

**In scope:** `addr:street` on the `/package` GeoJSON address features.

**Out of scope, deliberately:**

- **Matching.** `compare::addresses::compare_addresses` joins on normalised
  housenumber and a spatial grid key; street names play no part. This feature
  therefore cannot change which addresses are unmatched, only how the ones
  already selected are labelled. No `compare` path changes.
- **`/tiles`.** Its address layer emits `lokalny_id`, `housenumber` and `city`
  — never the street.
- **`addr:city` / `addr:place`.** These keep coming from `miejscowosc`
  untouched. Settlement-name normalisation is a separate problem.
- **`teryt_ulica` as a lookup key.** Confirmed with the user: ULIC codes are
  not stable over time. The column is not read, and is dropped from the file
  entirely.

## Lookup semantics

Keyed on `(teryt_simc_code, prg_street_name)`, resolved in priority order for
a PRG row with settlement `S` and street `N`:

1. row with `teryt_simc_code = S` matching `N` → its `osm_street_name`
2. else row with `teryt_simc_code IS NULL` matching `N` → its `osm_street_name`
3. else emit `N` unchanged

The settlement row always wins, so a per-town exception never has to be
coordinated with the global row it overrides.

Matching is **case-insensitive on a trimmed name** (`lower(trim(...))` on both
sides). Every row in the shipped file matches current PRG byte-for-byte, so
exact matching would work today — but PRG's capitalisation has already drifted
once (see "Migration", review 2), and an exact match degrades silently: rows
stop firing with no error, just quietly fewer rewrites. `prg_unmatched.ulica`
is also not guaranteed trimmed, since it comes from the GML parser.

Per-town mappings genuinely differ, which is why a name-only map is
insufficient: `Kościuszki` → `Tadeusza Kościuszki` in most towns but
`Generała Tadeusza Kościuszki` in Dobieszowice; `Dąbrowskiego` → `Henryka`
in Orzesze and `Jarosława` in Żory — different people.

## Storage

New table in `db.rs::create_schema`, so it always exists and an empty table is
a valid state:

```sql
CREATE TABLE IF NOT EXISTS street_name_mappings (
    teryt_simc_code VARCHAR,   -- NULL = global rule
    prg_street_name VARCHAR,
    osm_street_name VARCHAR
);
```

The table is exactly the CSV — no derived columns. A stored
`lower(trim(prg_street_name))` key column was designed and then rejected on
measurement: 26.57 ms vs 27.07 ms per max-size package query (20 queries, best
of 3), i.e. noise on a query dominated by the spatial scan. This is *not* the
`centroid` situation from `docs/centroid_index_measured.md`: there the function
wrapped a 15M-row indexed geometry column and defeated an RTREE scan; here it
sits on the 3,272-row build side of a hash join, and the address side is
unindexable for this predicate either way.

Because the table is created by `create_schema`, **no migration path is needed**
— unlike the `centroid` column, an existing database gains it on next startup
and serves raw names until a file is loaded.

## Serving

One change, in `package.rs::unmatched_addresses` — the only place PRG street
names reach the outside world:

```sql
SELECT ST_AsGeoJSON(a.geom), a.numer_porzadkowy,
       COALESCE(loc.osm_street_name, gl.osm_street_name, a.ulica),
       a.miejscowosc, a.kod_pocztowy, a.teryt_miejscowosc
FROM prg_unmatched a
LEFT JOIN street_name_mappings loc
       ON lower(trim(loc.prg_street_name)) = lower(trim(a.ulica))
      AND loc.teryt_simc_code = a.teryt_miejscowosc
LEFT JOIN street_name_mappings gl
       ON lower(trim(gl.prg_street_name)) = lower(trim(a.ulica))
      AND gl.teryt_simc_code IS NULL
WHERE ...
```

The `COALESCE` chain **is** the priority rule. An empty table misses both joins
and falls through to `a.ulica`, so "a missing mapping file must not break the
app" is a property of the query rather than a code path that can rot.
`address_tags` is untouched — it still receives an `AddressRow` and does not
know a mapping exists.

Note `gl` rather than `glob` as the alias: `glob` is a DuckDB operator and
parses as a syntax error there.

## Loading

New `src/mappings.rs`:

- `load_from_path(conn, path) -> Result<LoadStats>` — parse, validate, then
  `DELETE` + `INSERT` in one transaction so readers never observe a
  half-loaded table.
- Validation **rejects the whole load**, leaving the previous table intact:
  duplicate `(lower(trim(prg_street_name)), teryt_simc_code)`, empty name on
  either side, unexpected column set. Values are trimmed defensively with a
  warning rather than rejected, as belt-and-braces alongside the CI check.
- `LoadStats` reports rows loaded plus **how many reference a
  `prg_street_name` absent from `prg_addresses`** — the staleness signal that
  drove review 2 below, available for free because the database is at hand.

Entry points:

- **CLI:** `import street-mappings [--file <path>] [--url <url>]`, dispatched
  from `import/mod.rs` alongside the other sources, self-reporting to
  `job_run_log` under `import:street-mappings`.
- **Config:** `download_urls.street_mappings`, defaulting to this repo's raw
  GitHub URL on `main`.
- **Background job:** `[jobs.street_mappings_update]`, disabled by default,
  `interval_seconds = 86400`. A HEAD request compares the ETag against
  `metadata['street_mappings_etag']` and returns early when unchanged. It uses
  `metadata` rather than `dataset_refreshes`, whose columns are shaped for the
  geospatial snapshots and their dirty-cell bookkeeping — none of which
  applies here, since a mapping change alters no geometry and enqueues no
  cells.

## The committed file

`mappings/street_names_mappings.csv`, three columns
(`teryt_simc_code, prg_street_name, osm_street_name`), empty `teryt_simc_code`
meaning global. Sorted by `(lower(prg_street_name), teryt_simc_code)` so a
name's global row and its exceptions sit together and diffs stay stable.

Contributors edit it by hand in a PR. The structural invariants — three
columns, no leading or trailing whitespace, no duplicate
`(lower(name), simc)`, file sorted — are enforced by a **Rust test that reads
the committed file**, not by a CI workflow: the repository has no CI
configuration, and a `#[test]` gets the same protection out of the existing
`cargo test` run with no new infrastructure. Should CI arrive later, that test
is already covered by it.

## Migration from the legacy file

Performed by `scripts/migrate_legacy_street_mappings.py`. This is a **one-shot
tool that has already been run** — the shipped CSV is its output, and future
edits are hand-made PRs. It is committed for provenance, because it encodes
~50 curation decisions together with the evidence for each, and it fails
loudly if any decision goes stale (an ambiguous key with no recorded decision,
a decision referencing a row absent from the source, a correction matching
nothing).

Result: **12,221 rows → 3,272**, while rewriting *more* addresses than the
legacy file (15,675 vs 14,580 on the current `prg_unmatched`).

Its inputs are not in the repo: the legacy file lives under the gitignored
`example_data/`, and the two PRG vocabulary exports come from a populated
DuckDB database. That is acceptable for a tool whose purpose is provenance
rather than repeated execution, but it does mean the script cannot be re-run
from a clean checkout — the committed CSV is the artifact of record.

### Review 1 — collapse to global rows

Keyed on `lower(trim(name))`, 3,933 of the legacy file's 3,975 keys had
exactly one OSM form across every settlement and became global rows (3,932
emitted — one key's only row was a typo dropped outright, see below). Only 42
keys were ambiguous.
Of those, 12 had a dominant form (≥80% of rows) where the minority was usually
a single typo; the rest were resolved individually against OSM frequency in
`osm_addresses`.

Two findings shaped the method:

- **Frequency alone is unsafe.** `H. Derdowskiego` → `Henryka Dąbrowskiego`
  has 1,657 OSM addresses against `Hieronima Derdowskiego`'s 749, but
  Dąbrowski is a different person — the legacy row was simply wrong. The
  guard is that the OSM name must plausibly expand the PRG name (surname stem
  must match).
- **There is no national convention to codify.** For nicknames OSM Poland
  carries `Fieldorfa "Nila"` (128), `Fieldorfa „Nila”` (66), `Fieldorfa-Nila`
  (41), `Fieldorfa Nila` (20) and plain `Fieldorfa` (244) simultaneously. Such
  keys stay settlement-only rather than having a coin-flip global imposed on
  every unlisted town.

Five keys remain settlement-only: `dąbrowskiego` and `księdza pojdy`
(different people), `mjr hubala` / `mjra hubala` (no majority among four
renderings), and `gen. fieldorfa` (PRG gives only the surname; expanding would
invent three words). `gen. romualda traugutta` was dropped entirely at the
user's direction.

### Review 2 — retarget onto current PRG spelling

The legacy file was built against an older PRG export. Checking every row
against the live `prg_addresses`:

| group | rows | action |
| --- | --- | --- |
| already exact in current PRG | 2,931 | keep as-is |
| case differs only (`ks.` → PRG now `Ks.`) | 306 | re-key |
| punctuation drift (`płk.` → PRG now `płk`) | 41 | re-key |
| PRG dropped the honorific entirely | 331 | **drop** |
| street-type prefix artefact (`osiedle Os. Modrzewiowe`) | 83 | drop |
| no trace in PRG | 276 | drop |
| no-op (PRG spelling already equals the OSM target) | 26 | drop |
| duplicate after re-keying | 8 | drop |
| settlement row whose name is gone from that town | 2 | drop |

The 331 "dropped honorific" rows are the one judgment call: the file keys
`dr. Adama Próchnika` but PRG now publishes `Adama Próchnika`. Re-keying would
build a global rule prepending `Doktora` to a name where PRG mentions no title
at all, in every town in the country — the same reasoning that keeps
`gen. Fieldorfa` settlement-only. They are dropped.

**Known gap:** the 83 street-type-prefix artefacts are dropped rather than
re-keyed, because the migration implements only case- and punctuation-
insensitive re-keying, not prefix stripping. These are recoverable — the file
keys `osiedle Os. Modrzewiowe`, PRG now publishes `Os. Modrzewiowe`, and the
target `Osiedle Modrzewiowe` is a legitimate `Os.` → `Osiedle` expansion.
Recovering them is a follow-up worth ~83 rows of coverage, deliberately left
out of this pass rather than silently lost.

### Review 3 — redundancy and spelling

No redundancy found: zero duplicate keys, zero settlement rows repeating their
own global, zero exact no-ops, and no settlement-only key whose rows now agree
well enough to be promoted.

Spelling was audited by splitting every OSM target into tokens and keeping
those appearing nowhere in OSM's or PRG's street vocabulary — 18 hits, 15 of
them real and corrected: 9 misspellings (`rroku`, `Wielkpolskiej`,
`Kaziemierza`, `Malchiora`, `Zeglugi`, `Rzeczpospolitej`, and
`Doktora Michała Doktoraozdowicza` — a find-and-replace that expanded the `Dr`
*inside* `Drozdowicza`) and 6 nominatives where Polish street names require
the genitive. Each was confirmed against real OSM usage or is a plain
grammatical fix.

The DB-dependent audits from reviews 2 and 3 are not automatable in CI, which
has no database. They are recorded here so the next curation round starts from
them rather than rediscovering them.

## Testing

| test | pins |
| --- | --- |
| empty table leaves `addr:street` as the raw PRG name | the missing-file requirement |
| a global row rewrites the name | the basic path |
| a settlement row beats the global for the same key | the priority rule |
| lookup is case-insensitive | PRG capitalisation drift cannot silently disable rows |
| duplicate `(key, simc)` rejects the load, table unchanged | the uniqueness invariant |
| `/package` end-to-end: load file, request area, expect expanded name | the whole feature |

Unit tests live inline in `src/mappings.rs` and `src/server/package.rs`; the
end-to-end test goes in `tests/` following the existing `assert_cmd` +
`tempfile` pattern.
