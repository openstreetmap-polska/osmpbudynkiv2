#!/usr/bin/env bash
# Generate "v2" fixture snapshots for the update/diff integration tests.
#
# Each v2 file differs from its v1 counterpart by exactly:
#   - 1 row removed  (the row with the lexicographically smallest id)
#   - 1 row modified (storey count bumped on the largest id)
#   - 1 row added    (a copy of the largest id's row under a synthetic id)
#
# Those exact counts are asserted by tests/cli_update_*.rs — if you change
# this script, change those assertions too.
#
# On top of that, each v2 file also gains two more rows that exercise
# `dataset::load_into`'s NULL-key filter and duplicate-key dedup
# (docs/superpowers/plans/2026-08-14-dataset-deduplication.md):
#   - 1 NULL-key row  (a copy of an existing row with the key column nulled
#                      out -- LOKALNYID only for BDOT10k's composite key,
#                      which is enough to fail non_null_key_sql and exercises
#                      the composite OR form of null_key_sql)
#   - 1 duplicate-key row (a second copy of an existing surviving row, under
#                          the SAME key, with a strictly OLDER version value)
#
# Both of these are dropped by `load_into` before the staging table is ever
# diffed against the live table, so they do NOT change the 1/1/1 added/
# modified/removed counts above -- the NULL-key row never lands in the
# staging table at all, and the duplicate-key row is deleted by
# `deduplicate_by_key` before `update::diff` runs. That is also why the
# duplicate's version must be the OLDER of the two: `deduplicate_by_key`
# keeps the row with the greatest version, so an older duplicate loses the
# tie-break and the row that survives is the pre-existing, untouched one --
# byte-identical to what v1 produced. A NEWER duplicate would instead survive
# in the duplicate's place, and its differing version column would make that
# key report as an extra `modified`, breaking the 1/1/1 assertion.
set -euo pipefail
cd "$(dirname "$0")/.."

duckdb -c "
SET enable_geoparquet_conversion = false;
COPY (
  WITH ranked AS (SELECT *, row_number() OVER (ORDER BY LOKALNYID) rn,
                         count(*) OVER () n FROM 'bdot10k.parquet')
  SELECT * EXCLUDE (rn, n) FROM ranked WHERE rn > 1 AND rn < n
  UNION ALL
  SELECT * EXCLUDE (rn, n) REPLACE (
      COALESCE(LICZBAKONDYGNACJI, 0) + 1 AS LICZBAKONDYGNACJI)
    FROM ranked WHERE rn = n
  UNION ALL
  SELECT * EXCLUDE (rn, n) REPLACE (LOKALNYID || '_ADDED' AS LOKALNYID)
    FROM ranked WHERE rn = n
  UNION ALL
  -- NULL-key row: a copy of the rn=3 row with LOKALNYID nulled out. Dropped
  -- by non_null_key_sql inside load_into's load SELECT, before the staging
  -- table exists -- never reaches the diff.
  SELECT * EXCLUDE (rn, n) REPLACE (CAST(NULL AS VARCHAR) AS LOKALNYID)
    FROM ranked WHERE rn = 3
  UNION ALL
  -- Duplicate-key row: a second copy of the rn=2 row (already surviving via
  -- the rn > 1 AND rn < n branch above), same (PRZESTRZENNAZW, LOKALNYID)
  -- key, WERSJA moved a day OLDER so deduplicate_by_key's 'WERSJA DESC'
  -- ordering keeps the original, untouched row and deletes this one.
  SELECT * EXCLUDE (rn, n) REPLACE (WERSJA - INTERVAL 1 DAY AS WERSJA)
    FROM ranked WHERE rn = 2
) TO 'bdot10k_v2.parquet' (FORMAT PARQUET);
"

# EGIB, unlike BDOT10k, is read back as real GeoParquet: egib.parquet carries
# valid GeoParquet metadata and src/import/egib.rs relies on the geometry
# column auto-detecting as GEOMETRY so it can call ST_Transform on it. Writing
# v2 with conversion disabled would emit a plain BLOB column with no `geo`
# metadata, and the EGIB loader would fail with "No function matches
# ST_Transform(BLOB, ...)". So load spatial and leave conversion on here.
duckdb -c "
LOAD spatial;
COPY (
  WITH ranked AS (SELECT *, row_number() OVER (ORDER BY id_budynku) rn,
                         count(*) OVER () n FROM 'egib.parquet')
  SELECT * EXCLUDE (rn, n) FROM ranked WHERE rn > 1 AND rn < n
  UNION ALL
  SELECT * EXCLUDE (rn, n) REPLACE (
      COALESCE(kondygnacje_nadziemne, 0) + 1 AS kondygnacje_nadziemne)
    FROM ranked WHERE rn = n
  UNION ALL
  SELECT * EXCLUDE (rn, n) REPLACE (id_budynku || '_ADDED' AS id_budynku)
    FROM ranked WHERE rn = n
  UNION ALL
  -- NULL-key row: a copy of the rn=3 row with id_budynku nulled out. Dropped
  -- by non_null_key_sql inside load_into's load SELECT, before the staging
  -- table exists -- never reaches the diff.
  SELECT * EXCLUDE (rn, n) REPLACE (CAST(NULL AS VARCHAR) AS id_budynku)
    FROM ranked WHERE rn = 3
  UNION ALL
  -- Duplicate-key row: a second copy of the rn=2 row (already surviving via
  -- the rn > 1 AND rn < n branch above), same id_budynku key,
  -- czas_pozyskania set to an OLDER string so deduplicate_by_key's
  -- 'czas_pozyskania DESC' ordering (lexicographic == chronological for this
  -- 'YYYY-MM-DD HH:MM' format) keeps the original, untouched row and deletes
  -- this one.
  SELECT * EXCLUDE (rn, n) REPLACE (CAST('2020-01-01 00:00' AS VARCHAR) AS czas_pozyskania)
    FROM ranked WHERE rn = 2
) TO 'egib_v2.parquet' (FORMAT PARQUET);
"

echo "Wrote bdot10k_v2.parquet and egib_v2.parquet"
