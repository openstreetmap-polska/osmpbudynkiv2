# Plan 2 — Diff on record identity, remove `_row_hash` entirely

**Status: implemented 2026-08-15.** `DatasetSpec` carries `key_columns` / `compared_columns` /
`compare_geometry` and the single-home `changed_predicate_sql`; `update::diff::compute` is a key diff;
`_row_hash`, `hashed_select`, `ROW_HASH_VERSION` and `stamp_row_hash_version` are deleted, replaced by
`update::dataset::check_column_shapes_match`. PRG got the *PRG needs Plan 1 too* treatment below.
**The migration in the next section is still outstanding** — this landed as a code change only; no
database has been re-imported. See *Verification* for the re-measured numbers.

**[Plan 1](2026-08-14-dataset-deduplication.md) had already landed** for BDOT10k and EGIB when this was
written (that document's own "not started" header is stale) — `non_null_key_sql`, `null_key_sql` and
`deduplicate_by_key` are in `src/dataset.rs`, and all three loaders now call them.

Best executed **before** [Plan 3](2026-08-14-column-trimming.md), though either order works. Both need
the same full re-import, so doing them together saves one rebuild.

## Context

The government-dataset refresh identifies change by hashing whole rows. `hashed_select`
(`src/dataset.rs:137`) stamps `hash(s)` over every column into `_row_hash`; `update::diff::compute`
(`src/update/diff.rs:20`) folds each id's rows into `hash(list_sort(list(_row_hash)))` and compares.
`ROW_HASH_VERSION` (`src/dataset.rs:34`) exists to self-heal when the hashed content changes, and its
global stamp means a bump for one source costs the other two a full-rewrite refresh each.

The mechanism does two jobs at once — establishing identity and detecting change — and it is bad at the
second, because it cannot distinguish a record changing from its *serialization* changing.

### Measurement 1 — the hash marks nearly everything as modified

Measured on the real national snapshots, comparing as the loaders actually store the data (NULL keys
dropped, duplicates collapsed, geometry transformed to EPSG:4326):

| snapshot pair | common records | "modified" by whole-row hash | modified by a signal anyone cares about |
|---|---|---|---|
| EGIB 08-01 → 08-10 | 17,552,215 | ~17,552,000 (**~100%**) | 270,138 (**1.54%**) |
| BDOT10k 08-01 → 08-10 | 16,344,762 | 16,344,762 (**100%**) | 32,422 (**0.20%**) |
| PRG 08-10 → 08-14 | 8,607,150 | 149,198 (1.7%) | 1,012 (**0.012%**) |

Three independent causes, one per source — and each source's noise column is a different one, which is
why no single rule fixes all three:

- **EGIB — a harvest timestamp.** `czas_pozyskania` moves for **17,507,375 rows (99.7%)** on every
  export. `pozostale_atrybuty` adds another 5,757,724 (32.8%), carrying a per-export `gml_id`
  (`budynki.162268`) that changes even when the building does not.
- **BDOT10k — periodic wholesale geometry re-serialization.** The 2026-08-10 export re-serialized
  **every** geometry: 16,344,762 of 16,344,762 common records have different WKB bytes.
- **PRG — bulk re-versioning by gmina.** 149,198 records got a new `wersja_id` in four days with no
  content change whatsoever.

So a routine EGIB or BDOT10k refresh today deletes and reinserts the entire table and enqueues every z14
cell in Poland into `match_dirty_cells`, for changes affecting under 2% of records.

### Measurement 2 — each source's noise is a *different* column

This is the finding that shapes the whole design. There is no uniform "compare content" or "trust the
version" rule; each source lies in its own way, and the fix is to configure the comparison per source.

**BDOT10k geometry is normally stable — the 08-10 export was a one-off event, not the norm:**

| pair | common | geometry bytes differ | share |
|---|---|---|---|
| 03-15 → 04-19 | 16,246,929 | 152,143 | 0.94% |
| 04-19 → 08-01 | 16,216,598 | 728,817 | 4.5% |
| 08-01 → 08-10 | 16,344,762 | **16,344,762** | **100%** |

Comparing BDOT10k geometry would therefore be cheap *most* of the time and catastrophic occasionally —
one export re-serializes the country and rewrites 16.3M rows. Since the re-serialized bytes are
indistinguishable from real movement, there is no predicate that keeps the geometry signal and survives
that export.

**PRG's version column is the noise, the exact mirror image.** Over four consecutive snapshot pairs:

| pair | days | `wersja_id` moved | content changed | ratio | missed by version-only |
|---|---|---|---|---|---|
| 01-10 → 03-15 | 64 | 1,159,284 | 30,614 | **37.9×** | 20 |
| 03-15 → 08-01 | 139 | 1,210,621 | 33,865 | **35.7×** | 77 |
| 08-01 → 08-10 | 9 | 246,648 | 7,199 | **34.3×** | 1 |
| 08-10 → 08-14 | 4 | 149,198 | 1,012 | **147×** | 0 |

("Content changed" here compares *every* PRG column, which is the conservative framing — it makes
content's number as large as it can be and the ratio as favourable to version-only as possible. The
predicate this plan actually ships is narrower still: 958 rather than 1,012 for the 4-day pair. See
*PRG's compared set*.)

Systematic in every pair, and *worse* at short intervals: content changes accrue with elapsed time
(~250/day) while version bumps do not (37k/day over four days, 18k/day over 64 — the same records get
re-versioned repeatedly, which a pairwise diff counts once). At a daily refresh cadence the ratio is
therefore worse than any row above.

The mechanism is **bulk republication by gmina**: in the 4-day pair, 24 gminas re-versioned ≥99% of
their records, accounting for 147,893 of the 148,186 content-free bumps (99.8%). It recurs rather than
rotating to completion — of the gminas that fully republished in some pair, 62 did it in two of the four
pairs, 42 in three, and **12 in all four**. It also cannot be filtered out by timestamp: Warsaw's 125,226
re-versioned records carry **11,020 distinct** new `wersja_id` values spread across the month, so the
bumps look individually organic and only reveal themselves as bulk at gmina granularity.

**EGIB geometry, unlike BDOT10k's, is byte-stable and load-bearing:** 10,986 of 17.55M changed in nine
days (0.06%), and **10,397 of those changed geometry with all three attributes unchanged**. 617,207
records (3.5%) have `rodzaj`, `kondygnacje_nadziemne` and `kondygnacje_podziemne` all NULL, so geometry
is their only possible signal. Dropping geometry from EGIB's predicate would freeze those footprints.

### Measurement 3 — the hash silently cannot handle NULL ids

EGIB had **210,080 rows with a NULL `id_budynku`** (1.2%). `GROUP BY id` collapses them into one group;
`ANTI JOIN ... USING (id)` never matches NULL to NULL, so the group lands in both `diff_added` and
`diff_removed`; then the apply's `WHERE id IN (SELECT id FROM diff_removed)` evaluates to NULL, which is
not true, so **nothing is deleted and nothing is inserted**.

Plan 1 has fixed the staging half of this — those rows are dropped at load now. What remains is that a
key-based diff must never reintroduce NULL tolerance, and that legacy NULL-keyed rows already in a live
table can only be shed by the re-import this plan requires anyway.

## Goal

Identity comes from the record identifier; change detection compares the columns that actually carry
signal for that source. `_row_hash`, `hashed_select`, `ROW_HASH_VERSION` and its metadata stamp are
deleted.

| source | key | modified when |
|---|---|---|
| BDOT10k | `(PRZESTRZENNAZW, LOKALNYID)` | `WERSJA` moved, or a retained attribute differs. **Geometry not compared.** |
| EGIB | `id_budynku` | `rodzaj` / `kondygnacje_nadziemne` / `kondygnacje_podziemne` differ, **or geometry differs** |
| PRG | `lokalny_id` | any retained column differs, **geometry included**. **Version not compared.** |

One mechanism, three configurations. "Version-only" and "content-only" are not modes — they are just
which column names appear in one list.

## Decision records

Each source's change signal is a measured choice, and each looks like an oversight from the outside.
Record the reasoning next to the spec, or it will be "cleaned up" into a uniform rule.

### BDOT10k — `WERSJA` **plus** the retained attributes, geometry excluded

Measured over exactly the columns [Plan 3](2026-08-14-column-trimming.md) retains (so these are the
numbers the shipped predicate will produce, not an upper bound over columns that get dropped):

| pair | common | `WERSJA` alone | `WERSJA` + retained attrs | missed by version-only | of those, `KATEGORIAISTNIENIA` |
|---|---|---|---|---|---|
| 03-15 → 04-19 | 16,246,929 | 304,061 | 310,456 | 6,395 | 64 |
| 04-19 → 08-01 | 16,216,598 | 798,551 | 800,486 | 1,935 | 0 |
| 08-01 → 08-10 | 16,344,762 | 32,422 | 32,422 | 0 | 0 |

**Adding the attributes to `WERSJA` costs at most +2.1% modified rows and closes the blind spot.** That
includes 64 `KATEGORIAISTNIENIA` transitions (44 of them `w budowie → eksploatowany`) — the column
`rule::BDOT10K_EKSPLOATOWANY_FILTER` gates on, so a missed one leaves a building excluded from
comparison entirely, or proposed for import when it should not be. `LICZBAKONDYGNACJI` and
`PRZEWAZAJACAFUNKCJABUDYNKU` changes also alter exported tags and adjacency class.

> **Departure from the original instruction, flagged deliberately.** The brief was version-only for
> BDOT10k, explicitly accepting missed rows as an upstream problem. The measurement says that trade buys
> essentially nothing: the blind spot closes for +2.1% in the worst observed pair and +0% in the most
> recent one. If you would still rather have the strictly cheaper predicate, **delete every entry but
> `"WERSJA"` from `BDOT10K.compared_columns`** — that is the entire change, and everything else in this
> plan is unaffected.

**Geometry is excluded** because of Measurement 2: normally 0.94–4.5% churn, but the 08-10 export moved
100% of it. Keeping geometry would mean one refresh per re-serialization event that rewrites the whole
table and dirties every cell in Poland — reintroducing the exact failure this plan exists to remove.
Cost of excluding it: a geometry-only edit with no `WERSJA` bump and no attribute change is missed
(224 and 1,935 in the two normal pairs), self-healing whenever that record's version or attributes next
move. This is the one place the accepted blind spot genuinely remains.

**Two corrections to earlier drafts of this plan**, both from re-measurement — do not restore the old
claims:

- The sentinel `WERSJA = 2023-12-31 13:00:00+01` covers **4,717,784 rows (28.9%)**, not 5,393,846 /
  33.1%.
- The claim that **100% of version-only misses carry that sentinel is false** — **0** of the 1,935 missed
  rows in 04-19 → 08-01 do. The sentinel is not the explanation for the blind spot, so do not build any
  logic on it.

**The predicate must be `IS DISTINCT FROM`, never `>` or `>=`** — and the reason changed on
re-measurement (2026-08-15), so the conclusion is unchanged but its justification is not:

- **The load-bearing reason** is the table above: 6,395 records changed a retained attribute *without*
  a `WERSJA` bump in the 03-15 → 04-19 pair, 64 of them `KATEGORIAISTNIENIA` transitions. A `>`
  predicate misses every one of them.
- **The backwards-movement reason is now theoretical.** Earlier drafts said `WERSJA` moves backwards
  for ~2 records per pair. Re-measured as the loaders actually store the data: **0 backwards moves
  across all three national pairs.** The old figure was measured *before* `deduplicate_by_key` and is
  exactly the 2 duplicate-key groups each snapshot carries — where an arbitrary pick between the two
  rows could land on the older one. `deduplicate_by_key(..., "WERSJA DESC", ...)` already removes that
  case. Do not restore the old claim.

A consequence worth stating plainly, since `IS DISTINCT FROM` is symmetric: **a staged record whose
`WERSJA` went backwards still replaces the live one.** The apply is an unconditional DELETE-then-INSERT
of the staging row for everything in `diff_modified`. That is the intended semantics — the model is
"the latest published snapshot is the truth", the same model that makes "absent from the new dump" mean
deletion. It is a mirror, not a version-tracking archive, so an upstream correction that lowers a
version number is reproduced rather than rejected.

### EGIB — three attributes plus geometry

No usable version column, and `czas_pozyskania` / `pozostale_atrybuty` are pure export noise
(99.7% / 32.8% churn). Once those are excluded, everything the source ships is signal — the table is
only 8 columns wide. Measured as loaded, 08-01 → 08-10:

```
n_old 17,589,371   n_new 17,566,060
added 13,845   removed 37,156   common 17,552,215
attrs differ 259,741   geometry differs 10,986   modified total 270,138 (1.54%)
```

Geometry is not optional here — see Measurement 2 for the 10,397 geometry-only changes and the 617,207
all-NULL-attribute records.

**Compare geometry as `ST_AsWKB(...)`, not natively.** Measured on the real 17.5M-row transformed table:

| predicate | elapsed |
|---|---|
| attributes only | 1.13 s |
| attributes + `a.geom IS DISTINCT FROM b.geom` (native GEOMETRY) | **24.18 s** |
| attributes + `ST_AsWKB(a.geom) IS DISTINCT FROM ST_AsWKB(b.geom)` | **2.50 s** |
| attributes + a stored `hash(geom)` column | 2.03 s |

Native GEOMETRY comparison is ~10× slower for the same answer. A stored geometry-hash column buys 0.5 s
for 8 bytes × 17.5M rows and works directly against Plan 3 — rejected.

### PRG — content, **not** version

The 34–147× measurement above. Version-only would take PRG from ~1,012–33,865 modified records per
refresh to 149,198–1,210,621, and the downstream cost is real work, not just a bigger number — measured
on the 4-day pair, counting both the old and new position of each changed record:

| signal | modified records | z14 cells enqueued into `match_dirty_cells` |
|---|---|---|
| version-only | 149,198 | **1,549** |
| content | 1,012 | **182** |

8.5× the drain work per refresh (less than the 147× record ratio, because bulk republication is
spatially concentrated) in exchange for the 0–77 records version-only would catch and content
comparison would not.

Those misses are also the least consequential kind: in the 4.5-month pair, **76 of the 77 are street
renames and 1 is a city change** — zero position moves, zero housenumber changes. `compare::addresses`
joins on `UPPER(TRIM(numer_porzadkowy))` + grid key + distance and never reads street names, so a missed
one cannot change which addresses are unmatched; it leaves a stale `addr:street` in `prg_unmatched`
until that record's content next moves. A tag-quality gap, not a correctness one.

**Also exclude `poczatek_wersji_obiektu`.** It moves in exact lockstep with `wersja_id` — 0
disagreements across 8.6M records — so it is a second version-metadata column, not content. Leaving it
in `compared_columns` would silently reinstate version-only behaviour with all of its churn.

#### PRG's compared set

"Content" for PRG means *what is served*, not every column the source ships. The serving surface is
exactly seven columns — `server::package::unmatched_addresses` (`src/server/package.rs:422-424`) and
`server::tiles`'s `ADDRESSES_MVT_SQL` read the same set — and `compared_columns` is six of them plus
geometry. Marginal cost of each candidate, measured as rows it catches that the others do not:

| column | serves | 01-10→03-15 | 03-15→08-01 | 08-01→08-10 | 08-10→08-14 |
|---|---|---|---|---|---|
| `numer_porzadkowy` | `addr:housenumber` | 28,770 (combined) | 29,848 | 7,097 | 958 |
| `ulica` | `addr:street` | ” | ” | ” | ” |
| `miejscowosc` | `addr:city` / `addr:place` | ” | ” | ” | ” |
| `kod_pocztowy` | `addr:postcode` | ” | ” | ” | ” |
| geometry | position | ” | ” | ” | ” |
| `teryt_miejscowosc` | `addr:city:simc` **+ mapping join key** | +193 | +42 | +0 | +0 |
| `wazny_od_lub_data_nadania` | a `/tiles` attribute only | *+1,511* | *+3,128* | *+102* | *+54* |

**`teryt_miejscowosc` is in the set for a non-obvious reason.** It is exported as `addr:city:simc`, but
more importantly it is the join key selecting which `street_name_mappings` row applies (settlement row →
global row → raw name, see CLAUDE.md's street-name-mapping gotcha). A change to it therefore changes the
exported `addr:street` **even when `ulica` is byte-identical** — which is why comparing `ulica` alone is
not enough. It costs essentially nothing.

**`wazny_od_lub_data_nadania` is deliberately excluded (2026-08-15).** It is display-only — a `/tiles`
attribute, never an OSM tag and never a match input — and it is the most expensive candidate by a wide
margin, adding ~5% churn on the long pairs. Cost of excluding it: that tile attribute reads stale until
the record next changes for another reason. Reinstate it only if the attribute becomes load-bearing.

Four columns were checked and rejected on measurement, recorded so they are not re-proposed:

- **`czesc_miejscowosci` — 0 non-null rows in all five snapshots.** The column is entirely empty.
  Plan 3's drop-list rates it "medium — could feed `addr:suburb`"; it cannot, and that entry should be
  corrected.
- **`status` and `wazny_do` — still 0 non-null in all five snapshots**, confirming Plan 3's note. Keep
  its "re-check if PRG starts populating it" flag, but note this is *not* a `compared_columns` question:
  a populated `wazny_do` marks an expired address that should not be proposed at all, which is a change
  to the compare rule, not to change detection.
- **`teryt_ulica`** — populated for 64% of records and genuinely changing (3,895 and 9,075 in the two
  long pairs), but nothing reads it. It joins `compared_columns` on the day an `addr:street:ulic` tag
  lands, not before.
- **`teryt_gmina`** and the TERC-derived admin names — 0 changes across all four pairs.

Note the two tiers within the set: `numer_porzadkowy` and geometry are the only *correctness*-critical
entries, since `compare::addresses` matches on housenumber + grid key + distance. The rest are tag
quality — missing one leaves a stale tag in an export, never a wrong matched/unmatched verdict.

## PRG needs Plan 1 too

New finding, not in Plan 1's scope when it was written: **`lokalny_id` is not reliably unique.** It is
unique in four of the five snapshots but not in `2026-01-10`, where
`49ba6299-04f9-48c1-8c50-56737d64927e` appears twice — the same Tuchola address at two versions, one
carrying `wazny_od_lub_data_nadania = 0200-07-15` (year 200, a typo) and the other the corrected
`2003-07-15` at a later `wersja_id`. A stale version shipped alongside its correction.

A key-based diff is only correct once the key is unique, so PRG must get the same treatment BDOT10k and
EGIB already have, in `import::prg::materialize_into` (`src/import/prg.rs:370`):

- `non_null_key_sql(&["lokalny_id"])` in the load SELECT (alongside the existing coordinate `IS NOT
  NULL` guard), plus the `null_key_sql` count query for the skip report.
- `deduplicate_by_key(conn, target, &["lokalny_id"], "wersja_id DESC", "lokalny_id")` after the table
  exists. **`wersja_id DESC` is the right order** and the duplicate above is why: the two rows differ
  only in version and in a field the newer one corrects. This is also the one place `wersja_id` earns
  its keep for PRG — it is useless as a change signal and useful as a tiebreak.
- PRG runs no geometry filters, so there is no "must come after `filter_oversized_geometry`" ordering
  constraint here — but keep the dedup after the table is built, matching the other two loaders.

`LoadStats` already carries `skipped_null_key` / `skipped_duplicate_key`, and `summarize_refresh`
(`src/update/dataset.rs:271`) already reports both, so `job_run_log` picks this up with no changes.

This makes `wersja_id` load-bearing for PRG even though nothing serves it — the one job it is good at.
[Plan 3](2026-08-14-column-trimming.md) drops it from the stored table, so the two plans interact:
see that plan's **The ordering-column problem**, which covers both this column and EGIB's
`czas_pozyskania`. The short version is that `deduplicate_by_key` runs as a statement against the
already-created table, so its ordering column cannot be projected away by the `CREATE TABLE AS SELECT`
that builds it; it needs an explicit `ALTER TABLE ... DROP COLUMN` afterwards, or (PRG only, since PRG
runs no geometry filters) a `QUALIFY` in the inner select instead. If Plan 3 lands first, that must be
resolved there before this plan's PRG dedup can work at all.

`przestrzen_nazw` stays out of PRG's key: it has exactly **one** distinct value in all five snapshots
(`PL.PZGIK.200`), so it contributes nothing but a wider join. BDOT10k's `PRZESTRZENNAZW` is in its key
because Plan 1 already put it there (16 distinct values) — leave that alone.

## Design

### `src/dataset.rs`

Replace `id_column` with the key and the comparison configuration, so identity and change detection sit
together and cannot drift:

```rust
pub struct DatasetSpec {
    pub name: &'static str,
    pub table: &'static str,
    /// Unique, non-null record identity, guaranteed at load by
    /// `dataset::non_null_key_sql` + `dataset::deduplicate_by_key`.
    pub key_columns: &'static [&'static str],
    /// Columns compared to decide "modified". Never volatile export metadata.
    /// See each spec's comment — the choice is measured, not stylistic.
    pub compared_columns: &'static [&'static str],
    /// Whether geometry participates. False for BDOT10k, and that is
    /// deliberate — see its comment.
    pub compare_geometry: bool,
    pub geom_kind: GeomKind,
}
```

Specs (drawing only on columns Plan 3 retains, so the two plans compose in either order):

- **`BDOT10K`** — keys `["PRZESTRZENNAZW", "LOKALNYID"]`; compared `["WERSJA", "KATEGORIAISTNIENIA",
  "PRZEWAZAJACAFUNKCJABUDYNKU", "FUNKCJAOGOLNABUDYNKU", "LICZBAKONDYGNACJI", "NAZWA", "FSBUD",
  "INFORMACJADODATKOWA", "KODKST", "ZRODLODANYCHGEOMETRYCZNYCH"]`; `compare_geometry: false`.
- **`EGIB`** — keys `["id_budynku"]`; compared `["rodzaj", "kondygnacje_nadziemne",
  "kondygnacje_podziemne"]`; `compare_geometry: true`.
- **`PRG`** — keys `["lokalny_id"]`; compared `["numer_porzadkowy", "ulica", "miejscowosc",
  "kod_pocztowy", "teryt_miejscowosc"]`; `compare_geometry: true`. See *PRG's compared set* below —
  the list is exactly what `/package` turns into tags plus what selects the street-name mapping,
  and `teryt_miejscowosc`'s presence is not for the reason it looks like.

`BDOT10K.compare_geometry: false` and the absence of `wersja_id` from `PRG.compared_columns` are the two
lines most likely to be "fixed" by a future reader. Both need a comment carrying the measurement and a
pointer to this document.

Add one builder — the single home for the comparison text:

```rust
/// `(a.c1, a.c2, ...) IS DISTINCT FROM (b.c1, b.c2, ...)`, plus geometry when
/// `compare_geometry`. Row-wise `IS DISTINCT FROM` is NULL-safe in DuckDB
/// (verified: `(NULL,1) IS DISTINCT FROM (NULL,1)` is false, `(NULL,1) IS
/// DISTINCT FROM (2,1)` is true), which EGIB depends on — 617,207 of its
/// records have all three compared attributes NULL.
pub fn changed_predicate_sql(&self, a: &str, b: &str) -> String
```

Geometry, when compared, appends `OR ST_AsWKB({a}.geom) IS DISTINCT FROM ST_AsWKB({b}.geom)`. Never the
bare `{a}.geom IS DISTINCT FROM {b}.geom` — 10× slower for the same answer, measured above.

**Delete:** `hashed_select` (`:137`), `ROW_HASH_VERSION` (`:34`), `ROW_HASH_VERSION_KEY` (`:35`),
`stamp_row_hash_version` (`:44`), `id_column` (`:70`), and the module doc's row-hash explanation
(`:1-15`).

**Also update, don't just delete, the two `ROW_HASH_VERSION` caveats that survive as comments:**
`non_null_key_sql`'s doc (`:157-162`) and `deduplicate_by_key`'s (`:466-468`) each explain why they need
no bump. That reasoning becomes moot, but the underlying property — these run outside the compared
projection — is still worth one line, since it is what makes them safe to reorder.

### `src/update/diff.rs`

Rewrite `compute` around the key. The per-id `hash(list_sort(list(_row_hash)))` fold disappears; it
existed only to tolerate duplicate ids, which Plan 1 removed.

```sql
CREATE TEMP TABLE diff_added AS
    SELECT {keys} FROM {staging} ANTI JOIN {live} USING ({keys});
CREATE TEMP TABLE diff_removed AS
    SELECT {keys} FROM {live} ANTI JOIN {staging} USING ({keys});
CREATE TEMP TABLE diff_modified AS
    SELECT {keys} FROM {staging} s JOIN {live} l USING ({keys})
    WHERE {spec.changed_predicate_sql("s", "l")};
```

The temp tables now carry the key columns under their own names rather than a single `id` column, which
is what lets BDOT10k's composite key work; every consumer joins `USING ({keys})`.

`compute`'s doc comment must state that these are **plain equality joins, correct only because the key
is non-null** — a future source with a nullable key would classify every such record as both added and
removed, which is exactly Measurement 3's bug. That is the only remaining guard against reintroducing it.

`ScratchGuard` (`src/update/dataset.rs:22`) drops two fewer temp tables — `diff_live_hashes` and
`diff_new_hashes` are gone. Its doc comment's "~16M rows for PRG" hash-table rationale goes with them.

### `src/update/dataset.rs`

- Delete `check_row_hash_version` (`:372`), the `RowHashVersion` enum (`:353`), the call at `:101`, and
  the conditional `stamp_row_hash_version` at `:181-183`. The `bump_serving_epoch` call at `:192` stays
  exactly where it is — unconditional, inside the transaction — and its comment already explains why it
  differs from the stamp beside it; that contrast disappears, so reword rather than delete.
- The apply's `WHERE {id} IN (...)` (`:167-172`) becomes a key join:

```sql
DELETE FROM {live} WHERE EXISTS (
    SELECT 1 FROM (SELECT * FROM diff_removed UNION ALL SELECT * FROM diff_modified) d
    WHERE {live}.k1 = d.k1 AND {live}.k2 = d.k2);
INSERT INTO {live} SELECT s.* FROM {staging} s SEMI JOIN (
    SELECT * FROM diff_added UNION ALL SELECT * FROM diff_modified) d USING ({keys});
```

- **Add the safety net `ROW_HASH_VERSION` used to provide.** Removing the stamp removes the automatic
  "the shape changed, recompare everything" self-heal. Its replacement is a loud failure rather than a
  silent full rewrite: at the start of `refresh`, compare the staging and live column lists via
  `duckdb_columns()` and `bail!` with an explicit "column set changed — re-run `import <source>`"
  message if they differ. Strictly better than today, where a shape change either silently rewrites the
  table or fails deep inside `INSERT ... SELECT *` with an arity error.

### `src/update/changeset.rs`

`insert_change_areas` (`:27`) and `insert_dirty_cells` (`:83`) join `s.{id} = d.id` four times each.
Convert all eight to key joins over `spec.key_columns`.

The fan-out caveat at `:22-26` ("a modified object that did NOT move contributes its cell twice") is
about live-vs-staging double counting, not duplicate ids, so it **stays**. What can go is any implication
that one object may contribute more than those two rows.

### Import loaders

- Remove `hashed_select` wrapping from `src/import/bdot10k.rs` (`:58`), `src/import/egib.rs` (`:35`) and
  `src/import/prg.rs` (`:382`). `with_centroid_select` and `with_rodzaj_kod_select` keep wrapping the
  plain inner select.
- Replace both loaders' local `KEY_COLUMNS` consts (`src/import/bdot10k.rs:19`,
  `src/import/egib.rs:16`) with `spec.key_columns` — both already carry a "Plan 2 moves this onto
  `DatasetSpec::key_columns`" comment marking the intent.
- Add PRG's non-null filter and dedup per *PRG needs Plan 1 too*, above.
- Remove `stamp_row_hash_version` from the `import` dispatch — four call sites
  (`src/import/mod.rs:25,30,41,183`) plus the local helper at `:232`. Leave every `bump_serving_epoch`
  alone.

**`src/import/prg.rs:361-369`** — the comment explaining that `ULICA_PREFIX_STRIP_SQL` sits *inside*
`hashed_select` and therefore requires a `ROW_HASH_VERSION` bump must be **replaced, not deleted**. The
reason the transform is safe changes from "the hash version catches an edit" to "`ulica` is in
`compared_columns`, so an edit to the expression surfaces as an ordinary modification on the next
refresh and self-heals per record". Worth stating explicitly, because the *inside/outside `hashed_select`*
distinction that comment teaches is about to stop existing.

**The new trap this creates, and it must be written down somewhere durable (CLAUDE.md):** values derived
*outside* `compared_columns` no longer self-heal at all. `centroid` (`with_centroid_select`) and
`rodzaj_kod` (`RODZAJ_KOD_CASE_SQL`) are recomputed for every staged row, so a record that is modified
for some other reason picks up the new expression — but an *unmodified* record keeps its old value
forever. Under `ROW_HASH_VERSION` a bump forced a full rewrite that fixed those columns as a side
effect; nothing does that now. Editing either expression requires a re-import, not a refresh. BDOT10k is
the sharpest case: with geometry excluded from its predicate, a future value-rewriting transform on a
BDOT10k column outside `compared_columns` would never propagate to an unmodified record.

### `src/mappings/egib.rs`

`with_rodzaj_kod_select`'s doc (`:52-54`) and the `does_not_change_the_row_hash` test (`:128-149`) are
both built on `hashed_select`. The test's *property* — that wrapping does not alter what the diff sees —
is still worth pinning; rewrite it against `changed_predicate_sql` instead of `_row_hash`, and rename it.
The module doc's "`dataset::hashed_select` is the one place the row-hash expression lives" (`:12`)
becomes "`DatasetSpec::changed_predicate_sql` is the one place the comparison lives".

## Files

- `src/dataset.rs` — spec fields, `changed_predicate_sql`; delete the hash machinery
- `src/update/diff.rs` — rewrite `compute`
- `src/update/dataset.rs` — apply SQL, `ScratchGuard`, delete version checks, add the column-shape guard
- `src/update/changeset.rs` — key joins
- `src/import/{bdot10k,egib}.rs` — drop `hashed_select`, switch `KEY_COLUMNS` to `spec.key_columns`
- `src/import/prg.rs` — drop `hashed_select`, add non-null filter + dedup, rewrite the
  `ULICA_PREFIX_STRIP_SQL` comment
- `src/import/mod.rs` — remove four `stamp_row_hash_version` calls and the helper
- `src/mappings/egib.rs` — retarget the doc and the wrapping test
- `src/serving_version.rs` — three doc cross-references to `ROW_HASH_VERSION` (`:37,112,124`) and one to
  `stamp_row_hash_version` (`:48,52`); the *bump-site* rules themselves are unaffected
- `src/job_log.rs:8` — one doc reference to `stamp_row_hash_version`'s delete-then-insert convention
- `tests/cli_import_{bdot10k,egib,prg}.rs` — the `_row_hash` assertions and the
  `metadata.row_hash_version == "2"` assertion (`cli_import_bdot10k.rs:117,130-135`) go away
- `src/compare/mod.rs` — see Testing
- `CLAUDE.md` — the "Gotcha — row-hash version" section is deleted and replaced by one describing the
  per-source comparison configuration and the derived-column trap above

## Migration

**Requires a full re-import of all three sources** (`import full`), then `compare full`. `_row_hash` is
being dropped from the live tables, `INSERT INTO {live} SELECT * FROM {staging}` is positional and
arity-strict, and no `ALTER TABLE` path exists in this codebase. Combine with
[Plan 3](2026-08-14-column-trimming.md)'s re-import if executing both — one rebuild, not two.

That re-import is also what sheds the legacy NULL-keyed EGIB rows and any duplicate-keyed PRG rows: the
loaders stop staging them, but no refresh can delete rows already in a live table whose key is NULL.

## Testing

- **`src/update/diff.rs`** — extend the inline `setup()` (`:84`): keep / modified / added / removed, plus
  a composite-key case, plus **a record where only a non-compared column moves, asserting no
  modification**. That last one is the entire point of the plan and nothing else covers it. The `dup`
  fixture row (two rows sharing an id) no longer represents a reachable state — Plan 1 makes it
  impossible — so replace it rather than adapting it.
- **`src/dataset.rs`** — `changed_predicate_sql` shape per source; assert **BDOT10k's predicate does not
  mention `geom`** and **PRG's does not mention `wersja_id`**, since both are silent-regression
  territory. Keep a NULL-semantics test for the row-wise `IS DISTINCT FROM` form.
- **`src/update/dataset.rs`** — the existing ~25 tests need their `LIVE_ROWS`/`NEW_ROWS` helpers
  reshaped; the whole `row_hash_version` stamp/restamp matrix is deleted and replaced with
  column-shape-guard tests (matching shapes proceed, differing shapes `bail!`).
- **`src/import/prg.rs`** — a `load_into`-level test that a duplicate `lokalny_id` collapses to the
  higher `wersja_id`, mirroring the existing BDOT10k/EGIB dedup tests.
- **`src/compare/mod.rs`** — `full_vs_incremental_equivalence` and `drain_refresh_concurrency` build
  their fixture with `BDOT10K.with_centroid_select(&hashed_select(&rows_sql(n, tag)))` (`:417`, `:482`)
  and weave `tag` into a `'{tag}' AS wersja` column (`:370-372`) purely to move the row hash. Two edits:
  drop the `hashed_select` wrapping, and **add a `PRZESTRZENNAZW` column** to `rows_sql`, which the
  fixture currently lacks and the new composite key requires. The `wersja` column keeps working as the
  change trigger — `WERSJA` is in `compared_columns`, and DuckDB identifiers are case-insensitive — but
  say so in a comment, because it now works for a specific reason rather than incidentally.
- No NULL-key test belongs here: Plan 1's `load_into_drops_null_keyed_rows` pins that at the loader.

## Verification

```bash
cargo test && cargo clippy && cargo fmt -- --check
grep -rn "_row_hash\|hashed_select\|ROW_HASH_VERSION\|row_hash_version\|id_column" src/ tests/
# expect no hits
```

Then the real-data check of the whole point of this plan — refresh churn should collapse:

```bash
cargo run --release -- --config example_config.toml import egib \
  --file example_data/EGiB/0_budynki_2026-08-01.parquet
cargo run --release -- --config example_config.toml update egib \
  --file example_data/EGiB/0_budynki_2026-08-10.parquet
duckdb -readonly osmpbudynkiv2.duckdb -c \
  "SELECT source, added, modified, removed FROM dataset_refreshes ORDER BY snapshot_id DESC LIMIT 1;"
```

Expected, from the measurements above:

| source | pair | added | modified | removed |
|---|---|---|---|---|
| EGIB | 08-01 → 08-10 | 13,845 | **270,138** | 37,156 |
| BDOT10k | 08-01 → 08-10 | 7,059 | **32,422** | 5,235 |
| PRG | 08-10 → 08-14 | 1,678 | **958** | 108 |

Against ~17.5M / ~16.3M / 149,198 modified today. (PRG's 958 is the shipped six-column predicate; a
comparison over *every* PRG column gives 1,012 for the same pair — the difference is
`wazny_od_lub_data_nadania`, excluded above.)

**Read `modified` as approximate, `added`/`removed` as exact (2026-08-15).** Re-measured against the
predicate `changed_predicate_sql` actually generates: PRG reproduced exactly (1,678 / 958 / 108), and
BDOT10k reproduced `added` and `removed` exactly but gave **32,418**, four short of the 32,422 above.
The gap is the invalid-geometry rows `filter_invalid_geometry` deletes at load, which a predicate-only
reproduction against the raw parquet does not drop — it is not a predicate disagreement. Ruled out as
causes: duplicate-key ties on the dedup ordering column (each BDOT10k snapshot has exactly 2
duplicate-key groups, neither tied on `WERSJA`, so `row_number()` has nothing to break arbitrarily).
Treat a handful of rows' divergence as expected; treat a divergence in `added`/`removed`, or an order
of magnitude anywhere, as a real miss. Diagnosing a miss:

- `modified` in the millions for EGIB → a volatile column (`czas_pozyskania`, `pozostale_atrybuty`)
  leaked into `compared_columns`.
- `modified` = 16,344,762 for BDOT10k → `compare_geometry` was left on.
- `modified` ≈ 149,198 for PRG → `wersja_id` or `poczatek_wersji_obiektu` leaked into
  `compared_columns`.
- `modified` = 0 across the board → the predicate is inverted or the key join is matching nothing;
  check `added`/`removed` are also not equal to the full table size.

The PRG numbers need the 2026-08-10 and 2026-08-14 snapshots, which exist as
`example_data/PRG/prg_*.parquet` (converted via the `prg_convert` CLI at `/mnt/nvme/git/prg_convert`,
using `--download-teryt` with the credentials in `.env`).
