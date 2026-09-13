# PRG refresh leaked the whole parsed snapshot, every day

Found 2026-09-13 by heap profiling production (v0.2.1 + `heap-profiling`).
Closes the leak in `docs/memory_growth_investigation.md`.

## Symptom

Server memory rose by roughly 2.6–2.9 GiB once a day and never came back,
from v0.1.2 onwards. Between refreshes it was flat.

## Measurement

One-minute RSS + swap samples across the first daily job window after a
restart (jobs fire relative to process start, here ~12:15 UTC):

| time (UTC) | event | RSS + swap |
|---|---|---|
| Sep 12 14:00 → Sep 13 11:59 | idle serving, 22 h | 4.1–4.3 GiB, flat |
| 12:16–12:18 | `update bdot10k` (0 changes) | dipped to 2.3, back to 4.3 |
| 12:20–12:25 | `update egib` (+31,140 / ~29,800 / −9,866) | 4.35, flat |
| **12:25:32–12:30:37** | **`update prg`, 16 GML entries streamed** | **4.34 → 7.22** |
| 16:16 | idle | 7.15, flat |

The climb starts at PRG entry 1 and stops at entry 16. Both building refreshes
finished and released their memory.

Live bytes on the unprefixed (Rust / RocksDB / GEOS) jemalloc, from `jeprof`:

| dump | time | live |
|---|---|---|
| `i308` | 12:25:05, before PRG | 481 MiB |
| `i411` | 12:31:41, PRG streamed | 3,248 MiB |
| `i835` | 16:11, 4 h after PRG finished | 3,299 MiB |

So the memory was still *referenced* hours later: a leak, not allocator
retention. The `i308 → i411` diff was 97.5% `realloc` under one stack:

```
arrow_array::builder::GenericByteBuilder<Utf8>::append_value
osmpbudynkiv2::import::prg::stream_gml_into   (prg_convert model2021.rs)
osmpbudynkiv2::update::run                    (src/import/prg.rs)
DatasetUpdateJob::run
```

## Cause

`stream_gml_into` passed each 2,048-row batch to DuckDB with
`duckdb::vtab::arrow::arrow_recordbatch_to_query_params` and
`INSERT INTO ... SELECT * FROM arrow(?, ?)`. That function pushes the batch
into a process-global `Vec<Arc<RecordBatch>>` that is never cleared. Its own
doc comment says so:

> Each call permanently retains one RecordBatch allocation in a process-global
> arena that is never freed […] Memory grows monotonically. Do not call this
> per row or per query.

About 4,200 batches per load kept all 8.6M parsed addresses alive for the life
of the process. `import prg` leaked the same bytes too, but it exits right
after, so nobody noticed.

This was never GEOS, which the investigation had ranked as the likelier
suspect.

## Fix

Batches now go through `Appender::append_record_batch` (the `appender-arrow`
feature). It copies each batch into DuckDB data chunks and drops it. The
table is created up front by `create_table_for_schema`, whose column types
come from `to_duckdb_logical_type_for_field`. That is the same function the
Appender uses to type its chunks, so the table and the chunks match by
construction. `arrow()` is still registered in `db.rs` but nothing calls it.

Verified on the national `PRG-punkty_adresowe_2026-01-10.zip` (8,530,862
parsed rows) with `import prg`, v0.2.0 code against the fix, same config
(`memory_limit = '4GB'`, 8 threads), fresh database each:

| | peak RSS | streaming step | total |
|---|---|---|---|
| v0.2.0 (`arrow()` per batch) | 4.58 GiB | 1m 42s | 1m 48s |
| fix (`Appender`) | **1.97 GiB** | **1m 06s** | **1m 11s** |

The two `prg_addresses` tables are identical: all 10 column types match, and
`EXCEPT ALL` finds 0 rows either way across 8,530,861 addresses (geometry
compared as WKB). The CLI peak shows the leak only up to process exit. In the
server those 2.6 GiB were never released, and each daily run added more.

**Do not reintroduce `arrow_recordbatch_to_query_params`,
`arrow_arraydata_to_query_params` or `arrow_ffi_to_query_params` on any
repeated path.** All three use the same never-freed store.

## Reading the profiles (what worked)

- `jeprof` is not installed with the crate, but
  `tikv-jemalloc-sys-*/jemalloc/bin/jeprof.in` is. It needs only two
  substitutions: `@jemalloc_version@` and `@JEMALLOC_PREFIX@` → `_rjem_`.
- The profile names the binary by its **deployed** path
  (`/opt/osmpbudynkiv2/osmpbudynkiv2`). Off-host, `jeprof` silently leaves
  every frame as a raw address. Rewrite that path in the `.heap` files'
  `MAPPED_LIBRARIES` section to the local copy of the identical binary
  (check the md5), and the frames resolve.
- `ls --time-style=+%H:%M:%S` hides the date. Two small dumps that looked
  like they were inside the window were from the day before.
