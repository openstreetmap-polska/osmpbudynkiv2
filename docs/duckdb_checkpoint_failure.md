# The Poland database stopped being able to CHECKPOINT

Written 2026-07-30. Hit while verifying the `/tiles` index work. Recovered; the
database was healthy again. Recorded because the failure mode is non-obvious,
the recovery is not, and one of the causes was a change made the same day.

> **It recurred on 2026-09-09, and the upstream cause is now known** --
> duckdb-spatial#861. Everything below is the original July write-up and its
> "precise trigger was not identified" conclusion is superseded; jump to
> [Update 2026-09-09](#update-2026-09-09-it-recurred-and-the-upstream-cause-is-now-identified)
> for the mechanism, why the patched-extension fix is unavailable on v1.5.5,
> and the CLI version-skew hazard. The July **recovery procedure is still the
> right one** and is unchanged.

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
3. **Use a DuckDB CLI whose version matches the server's exactly** — see the
   version-skew hazard below before opening the file with anything.
4. Rebuild by copying tables out, as above. Keep the original until the
   replacement has served real traffic.

---

# Update 2026-09-09: it recurred, and the upstream cause is now identified

The July occurrence's "precise trigger was not identified" note above is
answered. It is
**[duckdb-spatial#861](https://github.com/duckdb/duckdb-spatial/issues/861)**,
fixed by **[PR#862](https://github.com/duckdb/duckdb-spatial/pull/862)**.

The tell is the error string this document already recorded as the odd variant:
*"Operation requires a flat vector but a non-flat vector was encountered"*. That
is #861's signature, and in September it became the *primary* error rather than
the occasional one:

```
osmpbudynkiv2 D checkpoint;
FATAL Error: Failed to create checkpoint because of error:
Operation requires a flat vector but a non-flat vector was encountered
```

## The mechanism

WAL replay of inserts into a **pre-existing RTREE index**. When the database
closes without checkpointing while the WAL holds such inserts, the next open
buffers the replay, and `BoundIndex::ApplyBufferedReplays` builds a dictionary
vector over a selection vector which `ConvertToEntries` then reads as if it were
flat. PR#862 fixes it in `rtree_index.cpp` by reading row IDs through
`UnifiedVectorFormat`, plus a `SetCardinality(count)` before `Flatten()` in
`Insert`/`Delete`.

This explains why July's investigation could not reproduce it on a small fresh
database ("a fresh database with a GEOMETRY table and an RTREE index over it,
built and closed the same way, checkpoints cleanly"). The reproducer needs an
*uncheckpointed close with pending index inserts*, not size — the test built the
index after the inserts, which is precisely the ordering #861 says works.

**Still not data corruption.** The stored data is fine; the replay code path
mishandles a vector layout. July's finding that a straight table copy produces a
healthy file remains the reliable recovery, and remains correct for the same
reason.

## Why this server is unusually exposed

The precondition is met continuously. `osm_buildings`, `osm_addresses`,
`osm_former_buildings` (`src/import/osm.rs:722`) and all three `*_unmatched`
serving tables carry RTREE indexes on `geom`, and two enabled jobs write to them
constantly: `osm_update` inserting replication diffs every 60 s, and
`match_refresh` doing DELETE-then-INSERT per drained cell every 30 s. Any
unclean close arms the bug.

In September the unclean close came from memory pressure: the process was at
9.7 GB RSS with 6 GB swapped out, shutdown took longer than the unit's
`TimeoutStopSec=60`, and systemd `SIGKILL`ed it. `example.service` now sets
`TimeoutStopSec=300` for this reason. See
`docs/memory_growth_investigation.md`.

## Worse than July: reads are affected this time

July recorded "reads were entirely unaffected". That does not hold here. #861's
second manifestation is index-bound queries failing during buffered replay, and
every `/tiles` zoom is index-bound:

```
ERROR osmpbudynkiv2::server::tiles: tile query failed
  error=INTERNAL Error: Operation requires a flat vector but a non-flat vector
  was encountered   z=14 x=8952 y=5285
```

So you cannot ride this one out serving traffic while planning a rebuild, the
way July's occurrence allowed.

## The patched-extension route does not work on v1.5.5

Tried and ruled out: `FORCE INSTALL spatial FROM core_nightly` **has no build
for DuckDB v1.5.5**. `core_nightly` publishes against the current development
version, and this server is pinned to v1.5.5
(`cmake/duckdb_version.cmake`'s `OVERRIDE_GIT_DESCRIBE`, which must move in
lockstep with the `duckdb` tag in `Cargo.toml` — see CLAUDE.md).

So picking up #862 means a DuckDB version bump, not an extension swap. Until
then the recovery is the table copy documented above, and the prevention is
avoiding unclean shutdowns.

## Version-skew hazard when poking at the file

The September session reached the checkpoint error through
`~/.duckdb/cli/latest/duckdb`, which was **v2.1.0-alpha40775** — two major
versions ahead of the server's v1.5.5, and an alpha.

DuckDB can upgrade storage format on write. Had that `CHECKPOINT` succeeded, it
could have rewritten the file in a format the v1.5.5 server binary cannot open,
converting a fixable replay bug into an unreadable production database. It
failed, so nothing was written — but that was luck, not safety.

Two rules when investigating this file by hand:

- **Match the CLI version to the server's.** `latest` is not a safe default.
- **Copy the file and its `.wal` aside first.** Both, together: the WAL is where
  the un-checkpointed work lives, and separating them changes what the database
  is.

A mismatched CLI also loads a spatial extension from a *different* extension
directory than the server's
(`/opt/osmpbudynkiv2/.duckdb/extensions/v1.5.5/linux_amd64/`), so anything it
tells you about extension behaviour is about that other stack, not production's.

## Before any rebuild or re-import

Run `reports export`. `object_reports` is the only table in this database that
cannot be reconstructed from an external source — everything else is an
`import full` away. See CLAUDE.md.
