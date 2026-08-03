# Map BDOT10k / EGIB building classes to OSM building tags

Written 2026-08-03. `/package` currently emits `building=yes` for every
government building, discarding classification both sources already carry.
`docs/building_type_mappings.md` established *what* the mapping should be and
measured each decision; the two CSV files in `mappings/` are its output. This
document is the implementation plan for wiring them in.

It deliberately follows the shape of
`2026-08-01-prg-street-name-mappings-design.md` and of the commits that
implemented it (`e1faead`, `ed02954`, `39c1819`, `7016ab8`, `041aeab`):
a serving-time transform, a validate-and-swap CSV loader, an `import`
subcommand, a `download_urls` entry, and an ETag-gated background job.

## Scope

**In scope:** the `properties` map on `/package` building features, for both
`bdot10k_unmatched` and `egib_unmatched`.

**Out of scope, deliberately:**

- **Matching.** `compare::rule::unmatched_buildings_sql` tests centroid
  containment and reads no classification column. This feature cannot change
  *which* buildings are unmatched, only how the ones already selected are
  labelled — the same property that makes the street-name mapping safe. No
  change to `rule.rs`, no reconcile, no drain.
- **`/tiles`.** Surfacing the type in `BUILDINGS_MVT_SQL` becomes cheap once
  the columns are carried and is worth doing for review, but it is a separate
  change with its own tile-cache implications.
- **`source:building`.** Stays a code constant. The CSV supplies OSM *type*
  tags; provenance is not a mapping decision.

## Where the mapping is applied

**Carry the raw classification columns into `*_unmatched` at compare time;
resolve the mapping at serve time.** The reasoning is in
`docs/building_type_mappings.md#where-the-mapping-is-applied`; the operative
consequence is that editing a CSV requires no `compare`, no reconcile, no
drain and no redeploy.

Resolving a `*_unmatched` row's *own* attributes by joining back to
`bdot10k_buildings` is barred outright by the serving-table invariant in
`CLAUDE.md` — `LOKALNYID` is not unique and rowids are not stable across the
DELETE+INSERT every recompute performs. Hence the columns are copied.

The [adjacency](#adjacency) CTE does read `bdot10k_buildings` at serve time,
and that is not a violation: it is a *spatial* read of a bbox, asking "what
same-class buildings are here", never "which row is this row". Nothing is keyed
by id, so there is nothing to go stale.

## Storage

### Mapping tables

Two tables, identical shape, created in `db.rs::create_schema` so an empty
table is a valid state and no migration is needed:

```sql
CREATE TABLE IF NOT EXISTS bdot10k_building_types (
    tier           INTEGER,   -- 1 = PRZEWAZAJACAFUNKCJABUDYNKU, 2 = FUNKCJAOGOLNABUDYNKU
    key            VARCHAR,   -- stored lower(trim(...))
    min_levels     INTEGER,   -- inclusive; NULL = unconstrained
    max_levels     INTEGER,
    max_neighbours INTEGER,
    tags           VARCHAR    -- ';'-separated k=v pairs, verbatim from the CSV
);
CREATE TABLE IF NOT EXISTS egib_building_types (...);  -- same columns, tier 1 = rodzaj letter
```

`note` is **read and discarded**. It exists for whoever maintains the file and
is never emitted, so storing it would only invite someone to serve it. This is
also why the loader must accept the column being absent — as it is in
`bdot10k_building_types.csv` and present in `egib_building_types.csv`.

No derived `specificity` column: it is three `IS NOT NULL` tests over a
~180-row build side, computed inline in the `ORDER BY`. Same reasoning that
rejected a stored `lower(trim(prg_street_name))` key for the street mapping.

### Serving-table columns

| table | new columns | source |
| --- | --- | --- |
| `bdot10k_unmatched` | `funkcja_szczegolowa VARCHAR` | `PRZEWAZAJACAFUNKCJABUDYNKU` |
| | `funkcja_ogolna VARCHAR` | `FUNKCJAOGOLNABUDYNKU` |
| | `liczba_kondygnacji SMALLINT` | `LICZBAKONDYGNACJI` |
| `egib_unmatched` | `rodzaj_kod VARCHAR` | `egib_buildings.rodzaj_kod` |
| | `kondygnacje_nadziemne INTEGER` | `kondygnacje_nadziemne` |

**There is no `neighbours` column.** Adjacency is computed inside the serve
query — see [Adjacency](#adjacency), which measures it at ~+0.1 s on a
worst-case request.

`liczba_kondygnacji` is not used by any current BDOT10k mapping row (0 of 178
carry a level constraint) — it is carried for `building:levels` (step 6) and
because carrying it later would mean a second schema change.

### One new source-table column: `egib_buildings.rodzaj_kod`

Appendix B's `rodzaj` cascade is **precomputed at import**, as a `VARCHAR`
column on `egib_buildings` added outside `hashed_select`'s projection exactly
the way `centroid` is — so it never affects `_row_hash` and needs no
`ROW_HASH_VERSION` bump. `egib_unmatched` then carries the resolved letter
rather than the raw string.

This reverses an earlier draft of this document, which kept the raw `rodzaj`
and ran the cascade in the serve query on the grounds that the cascade is
empirical and a fix should not need a recompare. Measurement killed it: the
serve query has to apply the class filter to the **neighbour candidate set**,
which is a 96,827-row bbox slice of `egib_buildings`, and the regexp cascade
costs **1.0 s** there (0.41 s → 1.39 s for the same 39,201 rows; an `IN` list
over the identical values is free). Precomputing is the difference between a
0.25 s request and a 1.4 s one.

Having paid that, the letter must be precomputed *consistently*: if the serve
query resolved the mapping key from raw `rodzaj` while the adjacency filter
read a stored column, a cascade change between deploy and re-import would make
the two disagree. One column, read by both.

The cascade itself still lives in one place — a `pub const` in
`src/mappings/egib.rs` — but it is now consumed by the import projection rather
than by the serve query. A cascade change becomes an `import egib` re-run,
which is the same contract `centroid` already has, and a cascade change is a
code change and therefore a redeploy anyway.

Scope note: this is EGIB-only. BDOT10k's class filter is already a plain
equality on `PRZEWAZAJACAFUNKCJABUDYNKU` and needs no precomputation.

**Three places create the serving tables, not two:** `db.rs::create_schema`,
the `SEED` constant in `server/package.rs` (used by `make_seeded_state`), and
`server/updates.rs:456`. All three need the columns.

As with `centroid`, **there is no migration path** for `rodzaj_kod`: a database
built before step 5 must re-run `import egib`. The `*_unmatched` columns come
from `create_schema` and appear on next startup, but stay NULL until `compare`
runs.

## Serving

`unmatched_buildings` returns `(geometry, tags)` instead of `geometry`;
`building_tags` becomes a function of the resolved string. The query has three
stages: read the package rows, count same-class neighbours, then resolve the
mapping.

```sql
WITH pkg AS (                       -- the existing /package read, plus columns
    SELECT b.rowid AS rid, b.geom,
           ST_X(ST_Centroid(b.geom)) AS cx, ST_Y(ST_Centroid(b.geom)) AS cy,
           b.funkcja_szczegolowa, b.funkcja_ogolna, b.liczba_kondygnacji
    FROM bdot10k_unmatched b
    WHERE ST_Intersects(b.geom, ST_MakeEnvelope(<bbox>))
      AND ST_Intersects(ST_Centroid(b.geom), ST_GeomFromGeoJSON(?))
), nb AS (                          -- same-class neighbour candidates
    SELECT geom, ST_X(centroid) AS cx, ST_Y(centroid) AS cy
    FROM bdot10k_buildings
    WHERE ST_Intersects(geom, ST_MakeEnvelope(<bbox buffered by 0.0005>))
      AND lower(trim(PRZEWAZAJACAFUNKCJABUDYNKU)) = 'budynek jednorodzinny'
), cnt AS (
    SELECT p.rid, count(*) AS neighbours
    FROM pkg p JOIN nb
      ON (p.cx <> nb.cx OR p.cy <> nb.cy)
     AND ST_Intersects(p.geom, nb.geom)
    GROUP BY p.rid
)
SELECT ST_AsGeoJSON(pkg.geom), t.tags
FROM pkg
LEFT JOIN cnt USING (rid)
LEFT JOIN LATERAL (
    SELECT m.tags
    FROM bdot10k_building_types m
    WHERE ((m.tier = 1 AND m.key = lower(trim(pkg.funkcja_szczegolowa)))
        OR (m.tier = 2 AND m.key = lower(trim(pkg.funkcja_ogolna))))
      AND (m.min_levels     IS NULL OR pkg.liczba_kondygnacji >= m.min_levels)
      AND (m.max_levels     IS NULL OR pkg.liczba_kondygnacji <= m.max_levels)
      AND (m.max_neighbours IS NULL OR coalesce(cnt.neighbours, 0) <= m.max_neighbours)
    ORDER BY m.tier ASC,
             (m.min_levels     IS NOT NULL)::INT
           + (m.max_levels     IS NOT NULL)::INT
           + (m.max_neighbours IS NOT NULL)::INT DESC
    LIMIT 1
) t ON TRUE
```

EGIB is the same shape with `rodzaj_kod = 'm'` as the `nb` filter and
`m.key = pkg.rodzaj_kod` as the tier-1 predicate.

Load-bearing properties:

- **`ORDER BY tier, specificity DESC LIMIT 1` is the precedence rule**, the
  way the street mapping's `COALESCE` chain is. Tier 1 always beats tier 2
  because it is a cascade over two different key columns, not a tie. Within a
  tier, most constraints wins.
- **A constrained row cannot match an unknown value.** `NULL >= 0` is NULL, not
  true, so a `min_levels` row simply drops out where the storey count is
  missing — which is how EGIB's 3-storey-or-unknown case reaches
  `building=residential`.
- **`neighbours` is never NULL** — `coalesce(cnt.neighbours, 0)` makes "no
  matching rows in `cnt`" mean zero neighbours, which is what it is. This is
  the difference from the earlier stored-column design, where NULL meant "not
  computed yet"; see [Adjacency](#adjacency).
- **An empty mapping table misses the join** and falls through to
  `building=yes`, so "a missing mapping file must not break the app" is a
  property of the query rather than a code path that can rot.
- **The `nb` bbox must be buffered** (0.0005° ≈ 35 m) so a party wall across
  the request edge is not missed. Measured maximum centroid separation between
  two intersecting residential buildings is 0.000833°, so the buffer covers
  reading the neighbour; it does not need to cover the neighbour's own extent.

`building_tags(Option<&str>) -> BTreeMap<String, String>` splits on `;`, then
on the first `=`, and always inserts `source:building`. `None` or an empty
string yields `building=yes`. It never warns: the loader has the whole
database and reports drift once per load (see below), which is strictly more
useful than a warning per packaged feature.

## Loading

`src/mappings.rs` becomes `src/mappings/` with `street_names.rs` (moved
verbatim) and `building_types.rs`. Same all-or-nothing contract: parse into
`<table>__staging`, validate, `DELETE` + `INSERT` in one transaction, and on
any failure leave the previous table intact.

```rust
pub struct BuildingTypeStats {
    pub rows_loaded: usize,
    /// Keys in the file matching no row in the source table.
    pub keys_absent_from_source: i64,
    /// Distinct source keys the file does not cover, and how many rows they
    /// account for. For BDOT10k tier 1 this must be 0.
    pub source_keys_uncovered: i64,
    pub source_rows_uncovered: i64,
}
```

Rejections (each fails the whole load):

- a `tags` value that does not parse as `;`-separated `k=v`, or **any row
  without a `building` key** — this turns the hard invariant from the design
  doc into something enforced at load;
- two rows matching with **equal specificity** for the same `(tier, key)`;
- `min_levels > max_levels`, or `max_neighbours < 0`;
- **a `max_neighbours` constraint on a key outside the code-level adjacency
  set** (below). Without this check, a CSV edit could reference a column that
  is NULL for that class and the row would silently never fire;
- **a column count other than 6 or 7.** Not paranoia: while generating these
  files a raw string replace inserted a comma into an unquoted `note` field
  and DuckDB's sniffer accepted the result as a single-column table without
  complaint. Assert the shape; do not trust the sniffer.

Drift detection, in both directions, is the analogue of
`rows_absent_from_prg` and more useful here because the BDOT10k key space is
closed by `OT_FunSzczegolowaBudynkuType`. Measured against the current
production database: **0 tier-1 and 0 tier-2 values uncovered**, and 2 file
keys absent from the data — the 167-in-schema vs 165-in-data gap. Those are
the numbers a healthy load should report. Both counts go into the `job_log`
message so `/status` surfaces them.

Like `rows_absent_from_prg`, the drift queries must tolerate the source table
not existing — `import building-types` may legitimately run first.

## Adjacency

**Computed in the serve query, not stored.** Definition, per
`docs/building_type_mappings.md#adjacency`:

```sql
NOT ST_Equals(a.geom, b.geom) AND ST_Intersects(a.geom, b.geom)
```

The `>= 3 m` shared-boundary refinement is **dropped** — measured at ~1% of
buildings and statistically tied on accuracy, with no tuned constant to
justify.

Neighbours are counted **only among rows of the same class**, so an abutting
garage never suppresses `detached`. The class set is a code-level constant,
not a CSV column:

```rust
// bdot10k: tier-1 key; egib: resolved rodzaj letter
const BDOT10K_ADJACENCY_KEYS: &[&str] = &["budynek jednorodzinny"];
const EGIB_ADJACENCY_KEYS:    &[&str] = &["m"];
```

The loader check above is what keeps that constant and the CSV honest: a row
carrying `max_neighbours` for some other key would be counting neighbours the
`nb` CTE never reads.

### Why not a stored column

An earlier draft of this document put `neighbours` on `bdot10k_buildings` /
`egib_buildings`, computed at import by a whole-table grid-key pass and
maintained incrementally from `dataset_change_areas` plus a buffer. That
carried the entire cost of the feature: a new source column with no migration
path, a second full-vs-incremental algorithm pair, a refresh hook, a
neighbour-of-a-changed-row buffer that nothing would notice getting wrong, and
a NULL state that changed what the CSV's `max_neighbours` rows meant before it
landed.

**Measurement removed all of it.** Computing adjacency inside the serve query
costs about **+0.1 s per dataset** on a worst-case request — see
[Measured request cost](#measured-request-cost). The stored column bought
roughly a tenth of a second and cost most of the machinery in this document.

The consequence for the CSV is worth stating plainly: `max_neighbours` is now
live-editable like every other column, and the `detached` / `house` split works
from the first release rather than waiting on a later step.

### Four things the query must get right

Each was measured; the naive form of each is 3–20× slower, and one is wrong.

1. **The class filter must be a plain equality.** Applying the `rodzaj` regexp
   cascade to the 96,827-row bbox slice costs **1.0 s** (0.41 s → 1.39 s for
   the same 39,201 output rows). This is the whole reason
   `egib_buildings.rodzaj_kod` is precomputed.
2. **No grid key.** DuckDB runs a real spatial join between the two
   materialised sets: plain `ST_Intersects` takes **0.021 s**, against 0.058 s
   for a 0.0005° grid key and 0.368 s for 0.002°. The grid-key trick is both
   slower and, at 0.0005°, **wrong** — it misses a pair and flips one verdict,
   because two intersecting residential buildings' centroids sit up to
   0.000833° apart.
   The "O(n²), killed at 590 s" warning in `docs/building_type_mappings.md`
   describes a bbox-wide self-join over a whole 425k-row table staged through a
   temp table. It does not apply to a 39k × 13k join between two sets the
   optimiser can see.
3. **Identity by centroid coordinates, not `ST_Equals`.** `NOT ST_Equals`
   doubles the pair-test cost (0.357 s → 0.793 s) for an identical result —
   3,002 pairs either way, zero verdict differences. Two distinct buildings
   sharing an exact centroid would be a duplicate polygon, which is not a
   neighbour anyway.
4. **Aggregate the counts, then join back.** Putting the `LEFT JOIN` and
   `ST_AsGeoJSON` in one grouped query costs **5.2 s** against 0.25 s for
   counting first and joining the counts to the package rows.
   `ST_AsGeoJSON` must run once per output feature, never once per join row.

### Measured request cost

Production database, `threads = 8`, `memory_limit = '4GB'` (the configured
serving settings), on the worst-case windows in Poland at the configured
`max_area_sq_deg = 0.04` (0.2° × 0.2°). Warm page cache; times are the median
of three consecutive runs.

| window | features | today | with adjacency |
| --- | --- | --- | --- |
| EGIB, most unmatched — (18.6, 50.2) | 13,218 | 0.155 s | **0.247 s** |
| BDOT10k, densest `jednorodzinny` — (21.0, 52.2) | 3,049 | 0.047 s | **0.157 s** |
| BDOT10k, most unmatched — (19.4, 51.6) | 4,892 | 0.054 s | **0.141 s** |

A both-datasets max-size package therefore goes from roughly 0.20 s to 0.40 s.
Windows were chosen by scanning the whole country on a 0.2° grid for the
maximum `budynek jednorodzinny` count and the maximum `*_unmatched` count, so
these are worst cases rather than representative ones.

Two caveats on the numbers:

- **Warm cache.** The database is 20 GB against 31 GB of RAM on the machine
  measured, so a hot server matches these figures and a cold start does not.
- **CPU, not latency, is what grows.** Per-request user time goes from ~0.3 s
  to ~1.1–1.9 s — a 4–6× increase. With `db_pool_size = 8`, eight concurrent
  max-size requests will contend. `/package` is a heavyweight download endpoint
  under an area cap, so this is a thing to watch rather than a blocker, but it
  is the real cost of the design and belongs in any later capacity discussion.

## Entry points

Mirroring the street-mapping commits one for one:

- **CLI:** `import building-types [--bdot10k-file <p>] [--egib-file <p>]
  [--bdot10k-url <u>] [--egib-url <u>]`, dispatched from `import/mod.rs`,
  self-reporting to `job_run_log` under `import:building-types`. One
  subcommand for both files: they are always wanted together, and a single
  `--source` selector would be an option nobody passes.
- **Config:** `download_urls.bdot10k_building_types` and
  `download_urls.egib_building_types`, defaulting to this repo's raw GitHub
  URLs on `main`, alongside `street_mappings`.
- **Background job:** `[jobs.building_types_update]`, disabled by default,
  `interval_seconds = 86400`. One job, two independent ETags in `metadata`
  (`bdot10k_building_types_etag`, `egib_building_types_etag`) — a failure or
  no-op on one file must not gate the other. `metadata` rather than
  `dataset_refreshes` for the same reason as the street job: a mapping change
  alters no geometry and enqueues no cells.
- Downloaded files are cleaned up per `cleanup_downloaded_files`, matching
  `57bb6ac`.

## Sequencing

Each step is independently shippable and verifiable.

1. **Carry the classification columns.** Add them to `bdot10k_unmatched` /
   `egib_unmatched` in all three creation sites, and to the select lists in
   `compare::buildings` and `compare::incremental`. No behaviour change; verify
   by re-running `compare` and checking the columns populate. **Note the select
   list is built twice** — extracting it into a shared constant beside `rule.rs`
   is the right move while touching it, since the two must agree by
   `compare::full_vs_incremental_equivalence`.
2. **The loader.** `mappings/building_types.rs` with the validation above, plus
   the schema and the committed-file structural test. Nothing consumes it yet.
3. **Entry points.** CLI, config, background job.
4. **BDOT10k serving, including adjacency.** Turn `building_tags` into a lookup
   and add the `nb`/`cnt` CTEs. Adjacency is no longer a separate step: without
   a stored column it is three CTEs in the same query, and deferring it would
   mean shipping `budynek jednorodzinny` as `house` for no gain. This step alone
   covers 98.14% of the BDOT10k import.
5. **EGIB serving.** Precompute `egib_buildings.rodzaj_kod` at import, carry it
   through `compare`, add the letter table and EGIB's `nb`/`cnt` CTEs. Larger
   volume and lower confidence than BDOT10k, so it is worth its own reviewable
   change. Needs an `import egib` re-run to populate `rodzaj_kod`.
6. **`building:levels`.** Independent of everything above and the largest win
   per line of code, but blocked on the open item: BDOT10k has a single
   `LICZBAKONDYGNACJI` while OSM's `building:levels` counts above-ground
   storeys only. Confirm against the XSD before emitting.

Both CSV files ship as committed. The earlier plan deferred
`egib_building_types.csv`'s two 1–2 storey `m` rows, because with a stored
`neighbours` column they would have fired against a NULL and emitted `house`
where `docs/building_type_mappings.md` promises `residential`. Serve-time
adjacency has no such state — the count is always real — so the rows are
correct from the first release.

## Testing

| test | pins |
| --- | --- |
| empty mapping table → `building=yes` | the missing-file requirement |
| tier-1 hit | the basic path |
| tier-1 miss → tier-2 hit | the cascade |
| neither tier → `building=yes` | the fallthrough |
| a `man_made` pair round-trips into `properties` | multi-tag values |
| `max_neighbours=0` fires at 0, not at 1 | the adjacency branch |
| an isolated building counts 0, not NULL | `coalesce(cnt.neighbours, 0)` |
| a neighbour of a *different* class does not count | the class restriction |
| a neighbour just outside the request bbox still counts | the `nb` buffer |
| each EGIB cascade tier resolves to its letter | Appendix B |
| unresolvable `rodzaj` → `building=yes` | the `ELSE NULL` branch |
| missing `building` key rejects the load, table unchanged | the hard invariant |
| duplicate specificity rejects the load | precedence being total |
| `min_levels > max_levels` rejects the load | constraint sanity |
| `max_neighbours` on a non-adjacency key rejects the load | the code/CSV seam |
| committed CSVs: column count, every row has `building`, no duplicate specificity | a bad PR |
| `/package` end-to-end: load files, request area, expect mapped tags | the whole feature |

Do **not** pin all 178 rows — that makes the test a copy of the CSV and fails
for any addition rather than for any regression. Pin decision *classes*.

Unit tests inline in `src/mappings/building_types.rs` and
`src/server/package.rs`; end-to-end in `tests/cli_import_building_types.rs`,
following `tests/cli_import_street_mappings.rs`.

`building_tags_are_fixed` in `server/package.rs` is deleted by step 4.

The adjacency tests are cheap to write against a handful of hand-placed
polygons and are the ones worth having: every serve-time-adjacency bug is a
wrong tag on real data rather than a crash.

## Verified against production while writing this

Against `osmpbudynkiv2.duckdb` (read-only), `threads = 8`,
`memory_limit = '4GB'`:

**Key coverage**

- BDOT10k tier 1: **0** distinct `PRZEWAZAJACAFUNKCJABUDYNKU` values uncovered
  by the CSV. Tier 2: **0** uncovered. 2 CSV keys absent from the data.
- EGIB: the Appendix B cascade leaves **666,244 / 17,797,836 rows (3.74%)**
  unresolved → `building=yes`.
- `bdot10k_building_types.csv`: 178 rows, 1 with a neighbour constraint, 0 with
  a level constraint, 27 carrying `fixme`.

**Serve-time adjacency** (worst-case 0.2° × 0.2° windows, warm cache, median of
three runs — full table in [Measured request cost](#measured-request-cost))

| what | measured |
| --- | --- |
| EGIB worst window, 13,218 features | 0.155 s → **0.247 s** |
| BDOT10k densest window, 3,049 features | 0.047 s → **0.157 s** |
| the `ST_Intersects` join itself (39,201 × 13,218) | 0.021 s |
| `rodzaj` regexp cascade on the candidate read | +1.0 s — hence `rodzaj_kod` |
| `NOT ST_Equals` vs centroid inequality | 0.793 s vs 0.357 s, identical results |
| grid key 0.0005° / 0.002° vs no grid key | 0.058 s / 0.368 s vs 0.021 s |
| grid key 0.0005° correctness | **1 pair missed, 1 verdict flipped** |
| max centroid separation of two intersecting buildings | 0.000833° |
| counts-then-join vs one grouped `LEFT JOIN` | 0.25 s vs 5.2 s |

Benchmark scripts are not committed — they are throwaway SQL against a database
that is not in the repository, the same provenance situation as the street
mapping's DB-dependent audits. The numbers are recorded here so the next round
starts from them.
