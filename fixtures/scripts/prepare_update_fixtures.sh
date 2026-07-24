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
) TO 'bdot10k_v2.parquet' (FORMAT PARQUET);
"

duckdb -c "
SET enable_geoparquet_conversion = false;
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
) TO 'egib_v2.parquet' (FORMAT PARQUET);
"

echo "Wrote bdot10k_v2.parquet and egib_v2.parquet"
