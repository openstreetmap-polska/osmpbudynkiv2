# The Poland database stopped being able to CHECKPOINT

Written 2026-07-30. Hit while verifying the `/tiles` index work. Recovered; the
database is healthy again. Recorded because the failure mode is non-obvious, the
recovery is not, and one of the causes was a change made the same day.

---

## Symptom

Any write session against `osmpbudynkiv2.duckdb` died on close:

```
FATAL Error: Failed to create checkpoint because of error:
FATAL Error: Failed to create checkpoint:
INTERNAL Error: GetChildStats not implemented for ColumnData of type GEOMETRY
```

An explicit `CHECKPOINT` or `FORCE CHECKPOINT` failed the same way; one run
produced a different internal assertion instead
(`Operation requires a flat vector but a non-flat vector was encountered`).

**Reads were entirely unaffected.** Every row count, every query, every
`/tiles` and `/package` response was correct throughout. This is not data
corruption — it is DuckDB failing to *write* the WAL back into the main file.

Consequences while it lasted:

- the WAL (`osmpbudynkiv2.duckdb.wal`) never shrank and grew with every write;
- the application checkpointed no better than the CLI — a clean shutdown still
  left the WAL behind;
- **any DDL that forces a checkpoint became fatal**, which is what turned this
  from a slow leak into an outage (below).

## How it became an outage

`CREATE INDEX` forces a checkpoint. The `/tiles` work added three
`CREATE INDEX IF NOT EXISTS` statements to `db::create_schema`.

While the indexes already existed, `IF NOT EXISTS` made them a no-op, no
checkpoint was forced, and the server started fine — the broken checkpoint sat
there latent. Dropping those indexes (during an experiment to find out whether
they were the cause) meant the next startup had to actually *build* them, which
forced a checkpoint, which was fatal:

```
Error: Failed to create schema
Caused by: FATAL Error: Failed to create checkpoint ...
```

The server would not boot at all.

**Fix applied:** `create_schema` no longer creates these indexes inside the
fatal schema batch. `db::create_serving_indexes` runs them separately and
*warns* on failure instead of propagating the error. Serving unindexed is
strictly better than not serving. A database in this state now starts, logs a
warning naming this document, and answers queries with sequential scans.

## What did NOT cause it

- **Not the new serving-table indexes.** `CHECKPOINT` still failed with all
  three dropped.
- **Not GEOMETRY + RTREE in general.** A fresh database with a GEOMETRY table
  and an RTREE index over it, built and closed the same way, checkpoints
  cleanly. Reproduced at 5,000 rows; the failure needs something about this
  particular large database.

The precise trigger was not identified. Given reads stayed correct and a
straight copy of every table produced a healthy file, the damage looks confined
to checkpoint-time column statistics rather than to stored data.

## Recovery

Copy every table into a fresh database. Reads work, so this is reliable.

`ATTACH` the broken file read-only, recreate each table from its exact DDL
(`SELECT sql FROM duckdb_tables()` — do not rely on `CREATE TABLE AS`, which
loses `package_exports.area`'s `GEOMETRY('epsg:4326')` type and the
`dataset_refreshes` primary key), `INSERT ... SELECT *` each one, recreate the
import-time RTREE indexes, then `CHECKPOINT`.

On this dataset (~60M rows across 12 tables) the rebuild took **62 seconds** and
checkpointed cleanly. Verified afterwards: every table's row count identical,
`/tiles` byte-for-byte identical responses (26,819 B for `z14/9148/5394` before
and after), `/package` identical (790,259 B), and a clean shutdown leaving no
WAL.

One surprise worth expecting: **the rebuilt file was substantially larger** —
9.0 GB → 14.2 GB. Same rows, same types. The original was written by the import
path, which inserts in an order that evidently compresses far better than a bulk
`INSERT ... SELECT`. If size matters, re-importing beats rebuilding.

The broken original was kept alongside as `osmpbudynkiv2.duckdb.broken-backup`
(plus its `.wal`); delete both once the rebuilt database has proven itself.

## If you hit this again

1. Don't panic about the data — check a few `COUNT(*)`s read-only first. Reads
   being correct is the normal case here.
2. Don't run DDL against it. `CREATE INDEX`, and anything else that forces a
   checkpoint, will fail fatally and can take startup down with it.
3. Rebuild by copying tables out, as above. Keep the original until the
   replacement has served real traffic.
