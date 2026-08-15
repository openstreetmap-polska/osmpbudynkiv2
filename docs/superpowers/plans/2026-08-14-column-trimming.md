# Plan 3 — Store only the columns that are actually used

**Status:** not started (this plan). [Plan 1](2026-08-14-dataset-deduplication.md) and
[Plan 2](2026-08-14-key-based-diff.md) have both landed — `src/dataset.rs` already carries `DatasetSpec`,
`key_columns`, `compared_columns` and `changed_predicate_sql`, and `ROW_HASH_VERSION` is gone. That
resolves the ordering question this document used to pose: this plan executes strictly after Plan 2, so
*Ordering and migration* below no longer has a "before Plan 2" branch to choose.

## Context

All three government tables are built with `CREATE TABLE AS SELECT *` from the source
(`src/import/bdot10k.rs:53`, `src/import/egib.rs:31`, `src/import/prg.rs:372`), so every column the
publisher ships is stored forever whether or not anything reads it. A full audit of `src/`, `web/`,
`tests/` and `mappings/` found 7 unread columns in BDOT10k, 2 in EGIB and 17 in PRG.

**The rationale narrows once Plan 2 lands, and that is worth stating up front.** Today an unread column
still costs *churn*, because it sits inside `_row_hash` and can force a full-table rewrite on refresh —
which is exactly what `czas_pozyskania` and `pozostale_atrybuty` do to EGIB. After Plan 2, churn is
decided entirely by `DatasetSpec::compared_columns`, so a stored-but-uncompared column costs nothing but
disk. Both orderings are still worth doing; just do not justify this plan with the churn argument if
Plan 2 has already landed.

### Measured share of source storage (compressed parquet bytes, 2026-08-10)

| source | total | droppable | share |
|---|---|---|---|
| BDOT10k | 1,628 MB | **162 MB** | 9.9% (`IDEGIB` alone is 148 MB / 9.1%) |
| EGIB | 2,310 MB | **422 MB** | 18.3% (`pozostale_atrybuty` is 422 MB / 18.3%) |
| PRG | — | **~1,110 MB** uncompressed payload | 4 unused doubles ≈ 263 MB, versioning ≈ 460 MB, admin names ≈ 255 MB, codes ≈ 133 MB (not re-measured after keeping `teryt_gmina`/`gmina` — the actual PRG saving is somewhat below ~1,110 MB by whatever slice of "admin names"/"codes" those two columns are) |

Plan 2's removal of `_row_hash` adds a further 8 bytes/row — about 340 MB across the three tables.

## Goal

Each loader projects an explicit column list instead of `SELECT *`. Dropped PRG columns are recorded in
a doc so they can be restored deliberately.

**Stored ⊋ compared.** The keep-lists below are a superset of Plan 2's `compared_columns`, and the two
serve different questions: *what does anything read* versus *what decides a record changed*. A column
can legitimately be kept and not compared (PRG's `wazny_od_lub_data_nadania`), but the reverse is always
a bug — a compared column that is not stored cannot be compared. Pin it with a test (see *Testing*).

## Retained and dropped columns

### `bdot10k_buildings` — keep 14, drop 7

**Keep:** `PRZESTRZENNAZW`, `LOKALNYID`, `WERSJA`, `KATEGORIAISTNIENIA`, `PRZEWAZAJACAFUNKCJABUDYNKU`,
`FUNKCJAOGOLNABUDYNKU`, `LICZBAKONDYGNACJI`, `NAZWA`, `FSBUD`, `INFORMACJADODATKOWA`, `KODKST`,
`ZRODLODANYCHGEOMETRYCZNYCH`, `geom`, `centroid`.

**Drop:** `TERYT`, `POCZATEKWERSJIOBIEKTU`, `OZNACZENIEZMIANY`, `UWAGI`, `KODKARTO10K`,
`SKROTKARTOGRAFICZNY`, `IDEGIB`.

`PRZESTRZENNAZW` and `WERSJA` read as unused today but are **not** droppable — Plan 1 made them the
dedup key and ordering column, Plan 2 makes them the identity and part of the change signal. If Plan 3
is executed first, keep them anyway; a comment should say why, or a later reader will "clean them up".

This keep-list is exactly the set Plan 2's BDOT10k churn figures were measured over, so the two compose
without re-measurement. `OZNACZENIEZMIANY` is dropped despite moving in near-perfect lockstep with
`WERSJA` (32,422 vs 32,422 rows in the 08-01 → 08-10 pair) — it is a redundant second witness of a
signal `WERSJA` already carries, not an independent one.

### `egib_buildings` — keep 7, drop 2

**Keep:** `id_budynku`, `rodzaj`, `kondygnacje_nadziemne`, `kondygnacje_podziemne`, `geom`, `centroid`,
`rodzaj_kod`.

**Drop:** `pozostale_atrybuty`, `czas_pozyskania`.

Every retained non-derived column is also one of Plan 2's `compared_columns` — EGIB is only 8 columns
wide at source, so "keep what is read" and "compare what is kept" coincide here. `czas_pozyskania` needs
the treatment described in *The ordering-column problem*, below: Plan 1's dedup orders by it, so it
cannot simply be left out of the projection.

### `prg_addresses` — keep 10, drop 15

**Keep:** `lokalny_id`, `numer_porzadkowy`, `ulica`, `miejscowosc`, `kod_pocztowy`, `teryt_miejscowosc`,
`teryt_gmina`, `gmina`, `wazny_od_lub_data_nadania`, `geom`.

**`teryt_gmina`/`gmina` are kept despite having no current reader** (2026-08-15 revision to this plan,
at the user's request) — the same "kept but not compared" shape as `wazny_od_lub_data_nadania` below,
except that column *is* read by `/tiles`' `addresses_all` legend layer and these two, for now, are not.
Keeping them costs whatever share of the "codes ≈ 133 MB" / "admin names ≈ 255 MB" storage estimate below
is theirs specifically (not separately measured) and buys the natural per-gmina key without a future
re-import to get it back.

**`wazny_od_lub_data_nadania` is kept but deliberately NOT compared** (Plan 2, *PRG's compared set*).
Three sites read it out of `prg_addresses` — `compare::addresses` (`src/compare/addresses.rs:102`),
`compare::incremental` (`src/compare/incremental.rs:90`), both carrying it into `prg_unmatched`, and
`ALL_ADDRESSES_MVT_SQL` (`src/server/tiles.rs:230`) reading the live table directly for the
`addresses_all` legend layer — so dropping it would break `/tiles`. It stays out of `compared_columns`
because it is display-only and the most expensive candidate there (~5% extra churn). This is the
canonical "kept but not compared" case; do not let the two lists be unified.

**Drop** — this is the table for `docs/prg_dropped_columns.md`:

| column | what it holds | why dropped | restore value |
|---|---|---|---|
| `przestrzen_nazw` | INSPIRE namespace, `PL.PZGIK.200` | **exactly 1 distinct value** in all five 2026 snapshots; not part of PRG's key for that reason | none |
| `wersja_id` | record version id | unread by serving, and useless as a change signal (see Plan 2's 34–147× measurement) — **but consumed at import as the dedup ordering column**, see below | low |
| `poczatek_wersji_obiektu` | timestamp this version began | unread; moves in exact lockstep with `wersja_id` (0 disagreements in 8.6M rows), so it is a second version-metadata column | none |
| `wazny_do` | validity end date | **0 non-null rows in all five snapshots** (2026-01-10 … 2026-08-14) | none while empty; **re-check if PRG starts populating it** — see the note below |
| `status` | record status | **0 non-null rows in all five snapshots** | same as above |
| `teryt_wojewodztwo` | voivodeship TERYT code | unread | low |
| `wojewodztwo` | voivodeship name (TERC-derived) | unread except by one test | low — see Testing |
| `teryt_powiat` | county TERYT code | unread | low |
| `powiat` | county name (TERC-derived) | unread | low |
| `czesc_miejscowosci` | locality-part name | **0 non-null rows in all five snapshots — the column is entirely empty** | **none** (corrected 2026-08-15; previously rated "medium, could feed `addr:suburb`" — it cannot, there is nothing in it) |
| `teryt_ulica` | street ULIC code | unread, though populated for 64% of records and genuinely changing (3,895 and 9,075 in the two long pairs) | **highest** — the obvious source for an `addr:street:ulic` tag, and a stabler street join key than the name |
| `x_epsg_2180` | easting, PUWG 1992 | redundant with `geom` | none |
| `y_epsg_2180` | northing, PUWG 1992 | redundant with `geom` | none |
| `dlugosc_geograficzna` | longitude | consumed in the inner select to build `geom`, then redundant | none |
| `szerokosc_geograficzna` | latitude | same | none |

Restoring any of these is a one-line addition to the projection in `materialize_into` plus a re-import
of PRG — no schema migration exists, and none is needed. **Two of them come with a rider:**

- **`teryt_ulica`** — restoring it for an `addr:street:ulic` tag means adding it to Plan 2's
  `PRG.compared_columns` in the same change. A served column that is not compared silently serves stale
  values.
- **`wazny_do` / `status`** — if PRG starts populating either, that is *not* merely a compared-column
  question. A record with an end date is an expired address that should not be proposed for import at
  all, which is a change to `compare::addresses`' rule, not to change detection.

## The ordering-column problem

**This is a correction to the previous version of this plan**, which said `czas_pozyskania` should be
"consumed in the dedup window and projected away, exactly as `dlugosc_geograficzna` is consumed to build
PRG's `geom`". That analogy does not hold, and the mechanism it implies does not exist.

`dlugosc_geograficzna` is consumed *inside the same SELECT* that projects it away. `deduplicate_by_key`
(`src/dataset.rs:475`) is a different shape entirely: it runs as a later statement against the table that
already exists, and its `order_by` is interpolated into a window function over that table
(`row_number() OVER (PARTITION BY {keys} ORDER BY {order_by} NULLS LAST)`). The ordering column must
therefore be a real column of the created table at dedup time. It cannot be projected away by the
statement that creates the table.

That affects two columns: EGIB's `czas_pozyskania` (Plan 1, landed) and PRG's `wersja_id` (Plan 2's new
PRG dedup). Options, recommendation first:

1. **`ALTER TABLE {target} DROP COLUMN {ordering_column}` immediately after the dedup, inside
   `load_into`.** Verified to work in DuckDB. One mechanism for both sources, and it keeps
   `deduplicate_by_key` untouched. **Caveat to handle explicitly:** CLAUDE.md states "there is no
   `ALTER TABLE` anywhere in this codebase", a claim several gotchas lean on when explaining why no
   migration path exists. This does not actually create one — it is a load-time projection inside a
   table this function just built, not a mutation of an existing database's schema — but the CLAUDE.md
   sentence must be qualified in the same change, or the next reader will believe a migration path is
   now available.
2. **PRG only: dedup in the inner select** via
   `QUALIFY row_number() OVER (PARTITION BY lokalny_id ORDER BY wersja_id DESC NULLS LAST) = 1`, so
   `wersja_id` never becomes a column of the table at all. Legal for PRG **only** because PRG runs no
   geometry filters — `deduplicate_by_key`'s doc requires the dedup to run *after*
   `filter_invalid_geometry` and `filter_oversized_geometry`, so a duplicate pair whose newest member has
   bad geometry falls back to the older valid one instead of vanishing. EGIB and BDOT10k cannot use this.
   Cleaner for PRG, at the cost of PRG's loader no longer looking like the other two, and of losing the
   `skipped_duplicate_key` count that `deduplicate_by_key` reports into `job_run_log`.
3. **Retain the ordering columns.** Costs 422 MB (EGIB) and a share of PRG's ~460 MB versioning payload —
   most of this plan's EGIB saving. Not recommended, but it is the zero-risk option.

Recommendation: **(1) for both**, so one mechanism covers both sources and the `skipped_duplicate_key`
reporting is preserved. Whichever is chosen, `BDOT10K`'s `WERSJA` is unaffected — it is retained anyway
as part of `compared_columns`.

## Design

Replace the `SELECT *` forms with explicit lists, in the shared loaders so import and update staging
both follow:

- `src/import/bdot10k.rs:53` — `SELECT * EXCLUDE(GEOM), ...` becomes an explicit 12-column list plus the
  transformed `geom`.
- `src/import/egib.rs:31` — likewise; `czas_pozyskania` stays in the projection through the dedup and is
  removed afterwards per *The ordering-column problem*.
- `src/import/prg.rs:372` — the `SELECT * REPLACE (...)` becomes an explicit list.
  `dlugosc_geograficzna`/`szerokosc_geograficzna` remain in the inner select (they build `geom` and feed
  the `WHERE ... IS NOT NULL` guard) and are dropped by the outer projection; `wersja_id` is handled per
  *The ordering-column problem*.

Prefer an explicit list over `EXCLUDE(...)`: a source that adds a column should not silently start
storing it.

Keep each list next to its `DatasetSpec` `compared_columns` (Plan 2) or cross-reference them, so the two
cannot drift — and add the subset assertion described in *Testing*, which makes drift a test failure
rather than a code-review responsibility.

## Consumers to re-check (expected: no change needed)

The audit says every dropped column has zero readers, but confirm after editing:

- `/tiles`' `ALL_BUILDINGS_MVT_SQL` and `ALL_ADDRESSES_MVT_SQL` (`src/server/tiles.rs:220-268`) read the
  **live** tables directly. `ALL_ADDRESSES_MVT_SQL` reads `wazny_od_lub_data_nadania` (`:230`), which is
  retained — this is the one that would break if the keep-list were derived from Plan 2's
  `compared_columns` instead of from an audit of readers.
- The adjacency `nb` CTEs (`server/package.rs:606-609`, `server/tiles.rs:120-124,162-167`) read
  `PRZEWAZAJACAFUNKCJABUDYNKU` / `rodzaj_kod` / `centroid` / `geom` — all retained.
- `compare::columns::classification_columns` (`src/compare/columns.rs:30-46`) — all retained.
- `mappings::building_types` drift checks (`src/mappings/building_types.rs:44-58`) — all retained.
- The `*_unmatched` serving tables are **out of scope**: they already carry only what they need.

## Ordering and migration

**Requires a full re-import** (`import full`) followed by `compare full` — `INSERT INTO {live} SELECT *
FROM {staging}` (`src/update/dataset.rs:170`) is positional and arity-strict, and no `ALTER TABLE`
migration path exists for an already-built database. (The `ALTER TABLE ... DROP COLUMN` this plan's own
loaders run to solve *The ordering-column problem* is not a counterexample — it runs against a table the
same function just built with `CREATE TABLE AS SELECT`, never against a pre-existing live one.)

Plan 2 has already landed: no row hash exists to version, and the churn question is already settled by
`compared_columns`. This plan needs exactly one re-import of its own — the "executed before Plan 2"
branch this section used to describe (bump `ROW_HASH_VERSION`, solve *The ordering-column problem*
eagerly) no longer applies and has been dropped from this revision rather than kept as a dead option.

## Files

- `src/import/bdot10k.rs`, `src/import/egib.rs`, `src/import/prg.rs` — explicit projections, plus the
  ordering-column handling
- `docs/prg_dropped_columns.md` — **new**, the table above verbatim
- `tests/cli_import_prg.rs:83` — see Testing
- `CLAUDE.md` — state that the loaders project an explicit column list, that adding a consumer for a new
  column means adding it to that list plus a re-import, and — if option (1) is taken — qualify the
  "there is no `ALTER TABLE` anywhere in this codebase" claim

## Testing

Add one test per source asserting the stored column set exactly, so an accidental `SELECT *`
reintroduction fails loudly:
`SELECT column_name FROM duckdb_columns() WHERE table_name = 'bdot10k_buildings'` compared against a
constant list.

**Add the subset assertion** (once Plan 2 has landed): for each spec, every entry in `key_columns` and
`compared_columns` appears in that source's stored column list. A pure unit test over the two constants
plus the loader's list — no database needed — and it is the only thing that catches "compared column
silently dropped", which produces a binder error at refresh time rather than at build time.

Also assert the ordering columns are **gone** from the stored tables (`czas_pozyskania`,
`wersja_id`) — under option (1) their removal is a separate statement from the projection, so it is
exactly the kind of step that gets lost in a refactor while every other test still passes.

**One real coverage loss to resolve:** `tests/cli_import_prg.rs:83` asserts on `wojewodztwo` to verify
the TERC mapping was applied. TERC is consumed inside `prg_convert`'s parser, so the `--terc-file` flag
stays required regardless — only its *output columns* are being discarded. Options, recommendation
first:

1. Move the assertion to a unit test in `src/import/prg.rs` that inspects the raw table produced by
   `stream_gml_into` **before** `materialize_into` projects it away. Keeps the coverage and the storage
   saving.
2. Retain `wojewodztwo` alone (~60 MB) purely to keep the CLI assertion.

The stale note in the previous version of this plan — "a live concern for EGIB until Plan 2's surrogate
key lands" — is **removed**. Plan 2 evaluated and rejected the geometry-derived surrogate key; Plan 1
landed the actual fix, dropping NULL-keyed rows in the load SELECT, so `filter_invalid_geometry`'s
`r.get::<_, String>(0)` can no longer meet a NULL id.

## Verification

```bash
cargo test && cargo clippy && cargo fmt -- --check
grep -n "SELECT \*" src/import/*.rs   # only the inner PRG select should remain

cargo run --release -- --config example_config.toml import full
duckdb -readonly osmpbudynkiv2.duckdb -c "
  SELECT table_name, count(*) cols FROM duckdb_columns()
  WHERE table_name IN ('bdot10k_buildings','egib_buildings','prg_addresses')
  GROUP BY 1 ORDER BY 1;"
```

Expect `bdot10k_buildings` 14, `egib_buildings` 7, `prg_addresses` 10 — `_row_hash` is already gone
(Plan 2 landed first), and the PRG count includes the retained `teryt_gmina`/`gmina`.

Then confirm the database shrank and the service still works end to end:

```bash
ls -la osmpbudynkiv2.duckdb
cargo run --release -- --config example_config.toml compare full
cargo run --release -- --config example_config.toml run > server.log 2>&1 &
curl -s localhost:8080/status | head -40
curl -sI localhost:8080/tiles/14/9075/5363     # expect 200 or 204, never 500
```

A 500 from `/tiles` after this change almost certainly means a dropped column is still referenced by one
of the `*_all` layers or an adjacency CTE — `wazny_od_lub_data_nadania` in `ALL_ADDRESSES_MVT_SQL` is
the most likely culprit, since it is the one retained-but-uncompared column.
