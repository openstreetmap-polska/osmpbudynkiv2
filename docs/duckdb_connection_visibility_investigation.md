# DuckDB read/write connection visibility — investigation notes

Status: **unresolved, deferred**. Found while designing the export-log `/updates`
feature (2026-07-20). Not fixed yet — the feature works around it (see bottom).
This file exists so the investigation can be picked up again without redoing it.

## The problem

`server/mod.rs` holds two separate things pointed at the same DuckDB file:

- `write: Arc<Mutex<Connection>>` — one connection, opened read-write in `main.rs`
  via `db::init_db`, used by background jobs (`OsmUpdateJob`) via `JobContext.write`.
- `read_pool: Pool<DuckdbConnectionManager>` — built by `build_read_pool()`, which
  calls `Connection::open_with_flags(db_path, AccessMode::ReadOnly)` **independently**
  of `write`, once, at server startup.

**Verified bug:** once the server is running, `read_pool` never sees anything
written via `write` afterward, for the lifetime of the process. This means:

- `/tiles` and `/package` never reflect data applied by the background OSM
  update job while the server keeps running (existing latent issue, not
  introduced by the export-log feature — just discovered while working on it).
- A naive "log an export via `write`, then read it back via `read_pool`"
  design for `/updates` would always return an empty `FeatureCollection`,
  forever, regardless of how many exports actually happened.

Restarting the server "fixes" it (a fresh `read_pool` opened at the next
startup sees everything committed up to that point) — the staleness only
affects writes made *while* a given server process is running.

## Root cause

Confirmed against DuckDB's own concurrency docs
(`https://duckdb.org/docs/current/connect/concurrency`, cached locally via
`duckdb-skills:duckdb-docs`):

> In in-process mode, DuckDB has two configurable options for concurrency:
> 1. **Read-write mode:** one process can both read and write to the database.
> 2. **Read-only mode:** multiple processes can read from the database, but no
>    processes can write.

There is no third documented mode of "one live writer plus separate long-lived
readers in the same process, staying in sync." Each `Connection::open()` call —
even to the same file path, even from the same process — creates its own
independent DuckDB engine instance (own buffer manager, own catalog cache, own
transaction view). It is **not** shared memory the way e.g. Postgres backends
share `shared_buffers`. A read-only connection reads the file's committed state
once, at open time, and has no mechanism to notice the file changed underneath
it afterward — not even after an explicit `CHECKPOINT` on the writer side
(tested: checkpointing does nothing for an already-open separate reader).

Verified empirically (`cargo run --example`, scratch probes, not committed):

- Two independent `Connection::open()` calls to the same file, one read-write
  and one read-only, in the same process: the read-only side never observes
  writes made by the other, with or without `CHECKPOINT`.
- A **freshly-opened** `Connection::open_with_flags(path, ReadOnly)`, opened
  *after* the write, in the same process, while the writer connection is still
  open: sees the write immediately. So it's specifically about *reusing* an
  already-open connection object (which is exactly what a long-lived
  `read_pool` does), not about read-only mode itself.
- r2d2 pools reuse already-opened connection objects rather than reopening per
  checkout, so a persistent `Pool<DuckdbConnectionManager>` never self-heals
  this even under load.

## The `try_clone()` mechanism (the actual fix, once someone wants to do it)

`duckdb::Connection::try_clone()` (crate source:
`~/.cargo/registry/.../duckdb-1.10502.0/src/lib.rs` and `inner_connection.rs`)
does **not** reopen the file. It clones the `Arc<Mutex<DatabaseHandle>>` (the
same underlying `duckdb_database` C handle) and calls `duckdb_connect()` again
on it — a new "cursor"/client context bound to the *same* shared engine
instance. This is the officially-supported mechanism for the "one process,
multiple threads, MVCC" mode described above.

Interestingly, `DuckdbConnectionManager`'s own `r2d2::ManageConnection::connect()`
already calls `try_clone()` internally — so connections *within* today's
`read_pool` already share live state *with each other*. The bug is narrower
than it first looks: `build_read_pool` clones from a connection it opened
itself (independently of `write`), not from `write`. If it cloned from `write`
instead, the whole pool would share live state with the writer too.

Verified properties of `try_clone()`:

- **Shares live state.** A clone made *before* a write, queried again
  *after* the write with no reopening, correctly saw the new row. A second
  clone made *after* the write also saw it. Confirms the fix works.
- **Inherits write capability.** A clone of a read-write connection can
  itself execute writes — cloning does **not** downgrade to read-only.
  Switching `read_pool` to clone-based construction would lose the
  OS/DuckDB-enforced "this pool cannot write" guarantee that
  `AccessMode::ReadOnly` currently provides; it would become a coding
  convention instead (same trust level `write` already relies on).
- **Extensions are database-instance-wide, not per-connection.** A clone can
  call `ST_Point()` immediately with no `LOAD spatial` needed — confirmed via
  `duckdb_extensions()` showing `spatial` as loaded from the clone's own view
  too. Extension loading only needs to happen once per database instance.
- **Session-level `SET` config is per-connection and does NOT propagate to
  clones.** `SET geometry_always_xy = true` on the original connection is
  invisible to a clone (`current_setting(...)` errors as "not in the
  catalog" on the fresh clone). Any new clone needs the relevant `SET`
  statements reapplied — i.e. `config.duckdb_init_commands` re-run against it,
  same loop `build_read_pool` already does today, just aimed at a clone
  instead of a freshly-opened file. This matters a lot here specifically,
  since `geometry_always_xy` affects coordinate order in every spatial query
  this app runs.
- **Cheap.** ~73µs/clone in a debug build, ~3.3µs/clone in release
  (1000 clone+drop cycles timed directly). Negligible next to actual query
  time or HTTP request overhead — a "clone a fresh cursor per request, apply
  init_commands, run the query, drop it" pattern is practical, not just a
  pooled long-lived pattern.

## Candidate fixes (not implemented — pick up later)

1. **Pool of clones sourced from `write`.** Replace `build_read_pool`'s
   `Connection::open_with_flags(path, ReadOnly)` with a small custom
   `r2d2::ManageConnection` impl whose `connect()` calls
   `write.lock().unwrap().try_clone()` instead of opening the path itself.
   Keeps the existing `AppState.read_pool: Pool<...>` shape. Loses the
   read-only enforcement property (see above).
2. **Clone-per-request, no persistent pool.** Drop `read_pool` from
   `AppState` entirely. Any handler that needs to read calls a small helper —
   lock `write` briefly → `try_clone()` → unlock → re-run
   `config.duckdb_init_commands` on the fresh clone → run the actual query
   with no lock held → drop the clone at the end of the request. Simpler
   (no r2d2/custom manager, no pool-size tuning), and per-clone cost is
   negligible per the measurements above. No built-in backpressure under a
   traffic spike (a non-concern at this app's actual scale, but worth noting).

Either fixes `/tiles` and `/package`'s existing staleness as a side effect,
not just enables `/updates`.

## Current status / how the export-log feature avoids this

The export-log `/updates` feature (see
`docs/superpowers/specs/2026-07-20-export-log-updates-endpoint-design.md`) does
**not** fix this. Per explicit decision, it works around it narrowly: `/updates`
reads `package_exports` via `state.write` (locked briefly), the same connection
already used for the export-log insert and the retention job, instead of via
`state.read_pool`. This sidesteps the staleness for this one new endpoint
without touching the shared `read_pool` architecture. `/tiles` and `/package`'s
pre-existing staleness (reading OSM data through `read_pool`, never seeing the
background job's updates) remains unfixed and out of scope for that feature.
