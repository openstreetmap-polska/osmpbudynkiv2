# DuckDB read/write connection visibility — investigation notes

Status: **resolved, implemented (2026-07-21)**. Found while designing the
export-log `/updates` feature (2026-07-20); root-caused and fixed the same
week. `AppState.write` and `AppState.read_pool` are gone — replaced by a
single `AppState.pool: DbPool`, backed by `server::ClonedConnectionManager`
(clones of one base connection, so every pooled connection shares live MVCC
state) and a configurable `db_pool_size`. `duckdb_init_commands` now use `SET
GLOBAL` (see the "Better fix" section below) so no per-connection replay
machinery was needed. Verified against a real running server, not just tests:
started `run` against a fixture-backed DB, hit `/package` (writes an export
log row) then `/updates` (reads it back through a separate HTTP request/pool
checkout) and the write was visible immediately. This file is kept for the
investigation history and reasoning; no further action needed unless new
staleness-shaped symptoms appear.

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

## duckdb-rs README's "Thread safety" section (2026-07-21 follow-up)

Prompted by a re-read of the [duckdb-rs README](https://github.com/duckdb/duckdb-rs).
Its "Thread safety" section recommends exactly the r2d2 pattern already in use
here (`DuckdbConnectionManager` + `r2d2::Pool`), with one pool shared by every
thread/worker in the example:

```rust
let manager = DuckdbConnectionManager::file("file.db")?;
let pool = r2d2::Pool::new(manager)?;
let conn = pool.get()?;
conn.execute("INSERT INTO foo (bar) VALUES (?)", params![1])?;
```

Confirmed against the actual vendored source
(`~/.cargo/registry/.../duckdb-1.10502.0/src/r2d2.rs`, not just the README
prose) that this is not new information but a direct confirmation of what was
already reasoned out above:

```rust
pub struct DuckdbConnectionManager {
    connection: Arc<Mutex<Connection>>,
}
impl DuckdbConnectionManager {
    pub fn file<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self { connection: Arc::new(Mutex::new(Connection::open(path)?)) })
    }
}
impl r2d2::ManageConnection for DuckdbConnectionManager {
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connection.lock().unwrap();
        conn.try_clone()   // <- every pooled connection is a clone of ONE base connection
    }
    ...
}
```

So `DuckdbConnectionManager` always opens exactly one base connection itself
(`Connection::open(path)` inside `file()`) and every connection handed out by
the pool is a `try_clone()` of that single base connection — there is no
public constructor that wraps an already-open `Connection`/`Arc<Mutex<Connection>>`
you already have. This is exactly why `build_read_pool`'s use of
`DuckdbConnectionManager::file(...)` opens an independent base connection
that never shares state with `write`: it's the manager's own design, not a
misuse of it. It also confirms the earlier finding that pool members share
live state *with each other* (same rationale as `try_clone()`, see above) —
the README example's multi-threaded INSERTs all landing correctly is exactly
that property in action.

**Does this mean we should switch to one shared pool for everything (reads
*and* writes), like the README example does?** Checked this against the app's
actual write paths before answering. `log_export()` in `src/server/package.rs`
(package.rs:539-571) locks `state.write` and runs an `INSERT INTO
package_exports` on **every** `/package` request — and `/package` requests can
arrive concurrently from multiple HTTP clients. Today that's safe only because
`write` is a single `Mutex<Connection>` serializing every writer in the
process (HTTP handlers *and* background jobs) at the application level.
Collapsing `write` and `read_pool` into one `r2d2::Pool<DuckdbConnectionManager>`
and having writers call `pool.get()` per the README pattern would hand out
*independent* checked-out connections to concurrent `/package` requests —
each one a real DuckDB transaction — reintroducing the possibility of a
DuckDB write-write conflict that the current Mutex makes structurally
impossible. Nothing in this codebase currently retries on conflict, so this
would be a behavior regression, not just a refactor, unless retry logic were
added too.

**Conclusion: relevant, but only for the read side.** It corroborates
Candidate fix 1 above (a pool of clones sourced from `write`) and now gives us
the exact reference implementation to copy (the `impl r2d2::ManageConnection`
block quoted above, ~15 lines, pointed at the app's existing
`Arc<Mutex<Connection>>` instead of a freshly-`open()`ed one). It does **not**
support literally adopting the README's single-shared-pool-for-everything
shape here — `write` should stay a distinct, Mutex-serialized path for
writers; only `read_pool`'s manager should change to clone from it.

## Reanalysis driven by the actual goal: concurrency, not read/write separation (2026-07-21)

The framing above ("keep `write` Mutex-serialized, only fix `read_pool`") was
optimizing for *minimal change*, not for the app's actual requirement: don't
let the DB become a bottleneck under concurrent HTTP requests. Revisited with
that goal explicit, using DuckDB's own concurrency docs
(`duckdb-skills:duckdb-docs`, `/docs/current/connect/concurrency`) instead of
assumption.

**The single-Mutex design is itself the bigger bottleneck.** A `Connection` is
`Send`, not `Sync` — nothing can run two queries on it at once. Every request
that goes through `write.lock()` (or would go through a single shared
read connection) queues up and runs strictly one-at-a-time in whatever order
threads win the lock, *regardless* of how many CPU cores DuckDB's own
`threads` setting could otherwise use for that query. Concurrent `/tiles`,
`/package`, and `/updates` requests cannot overlap at all under that model. A
pool doesn't just fix staleness — it's what lets DuckDB's own MVCC and
internal parallelism actually engage for concurrent callers instead of the
app serializing everything ahead of the database. Clone cost is ~3.3µs
(measured earlier in this doc), so pooling has no meaningful downside if
queries themselves are cheap — the question is what happens when they're not
cheap and several land at once, which a pool handles and a single connection
by construction cannot.

**The write-conflict worry above was wrong — checked against the docs
instead of assuming.** [`/docs/current/connect/concurrency#handling-concurrency`](https://duckdb.org/docs/current/connect/concurrency#handling-concurrency):

> As long as there are no write conflicts, multiple concurrent writes will
> succeed. **Appends will never conflict, even on the same table.** Multiple
> threads can also simultaneously update separate tables or separate subsets
> of the same table. Optimistic concurrency control comes into play when two
> threads attempt to edit (update or delete) the same row at the same time.

Checked this app's actual writers against that rule:
- `log_export()` (`package.rs:539`, runs on every `/package` request,
  concurrently) — pure `INSERT`. Never conflicts, by the doc's explicit
  guarantee, no matter how many run at once.
- `export_log_prune` — `DELETE` on old rows past a retention cutoff, disjoint
  from whatever `log_export` is inserting concurrently (new rows). Different
  row subsets of the same table — documented as non-conflicting.
- OSM update job — writes to OSM tables, a different table entirely from
  `package_exports`. Non-conflicting by construction.

None of today's writers ever edit the same row concurrently, so none of them
can hit DuckDB's documented conflict case. The earlier conclusion ("collapsing
`write` into the pool reintroduces conflict risk, don't do it") does not hold
up. It was a plausible-sounding guess that turned out to be wrong once
checked against the actual rule instead of intuition — worth remembering: if
a future write path ever does concurrent `UPDATE`/`DELETE` on overlapping
rows, *that* specific path would need retry-on-conflict logic, not the
architecture as a whole.

**New bug found while testing this, unrelated to visibility.** While
verifying pool behavior empirically (`cargo run --example`, scratch probe,
not committed — same pattern as the earlier probes in this doc), tested
whether `build_read_pool`'s current approach actually configures every
pooled connection. It doesn't. `Pool::builder().max_size(4).build(manager)`
has no explicit `min_idle`, and r2d2's default is
`min_idle.unwrap_or(max_size)` — confirmed in
`~/.cargo/registry/.../r2d2-0.8.10/src/lib.rs` (`wait_for_initialization`) —
so `build()` eagerly creates all 4 connections up front, each an independent
`try_clone()` via `DuckdbConnectionManager::connect()`. `build_read_pool` then
does a single `pool.get()` and runs `duckdb_init_commands` (including `SET
geometry_always_xy = true`) against *only* that one connection.

Reproduced directly: built a 4-connection pool, ran the init commands on one
checked-out connection, then held all 4 pool connections open simultaneously
and queried `current_setting('geometry_always_xy')` on each:

```
checkout 0: geometry_always_xy = Ok("true")
checkout 1: geometry_always_xy = Err(... "not in the catalog" ...)
checkout 2: geometry_always_xy = Err(... "not in the catalog" ...)
checkout 3: geometry_always_xy = Err(... "not in the catalog" ...)
```

So today, in production, only 1 of `READ_POOL_SIZE = 4` read connections
actually has `geometry_always_xy` (and the other session-scoped init
commands) set. Roughly 3 out of 4 concurrent `/tiles`/`/package` requests are
served by a connection where this was never applied — a live correctness bug
(wrong geometry axis order on most spatial reads under concurrency),
independent of the staleness bug this whole document is about. r2d2 has an
official hook for exactly this — `CustomizeConnection::on_acquire(&self, conn:
&mut C)`, called once per connection right after `manager.connect()` creates
it (`r2d2-0.8.10/src/lib.rs:244-245`) — which the current code doesn't use at
all.

**Revised recommendation.** Given the goal is concurrency, not preserving a
read/write split for its own sake, and the write-conflict concern doesn't
apply to this app's actual write patterns:

- Drop `write: Arc<Mutex<Connection>>` and `read_pool` as two separate
  `AppState` fields. Replace both with **one** `r2d2::Pool<DuckdbConnectionManager>`,
  sized for real concurrency (e.g. matching CPU count / expected concurrent
  request count, not the arbitrary `READ_POOL_SIZE = 4`), whose manager's base
  connection is opened read-write once. Every handler and job calls
  `pool.get()` for both reads and writes — matches the README's canonical
  shape and fixes the original staleness bug as a side effect (every
  connection is a `try_clone()` of the same base, sharing live MVCC state).
- Attach a `CustomizeConnection` whose `on_acquire` re-runs
  `config.duckdb_init_commands` on every connection the pool ever creates —
  fixes the newly-found bug and makes it structurally impossible to regress
  (no more "only the first connection someone happened to `.get()`" pattern).
- Explicitly not adding retry-on-conflict logic for now, since no current
  writer can conflict — but note it as a requirement if a future write path
  ever does concurrent `UPDATE`/`DELETE` on overlapping rows.

This is a larger change than "Candidate fix 1" above (removes `write`
entirely rather than just repointing `read_pool`'s source), but is simpler in
shape (one pool, one code path, one place init commands are guaranteed to
run) and directly serves the actual goal of not letting the DB serialize
concurrent requests.

## Does re-running `duckdb_init_commands` per connection risk network calls? (2026-07-21, cont.)

Raised concern: if `CustomizeConnection::on_acquire` reruns the full
`duckdb_init_commands` list (`INSTALL spatial`, `LOAD spatial`, plus several
`SET`s) on every new pooled connection, could `INSTALL`/`LOAD` trigger a
network round-trip (version check, re-download) each time — a real latency
risk under load, since new connections get created whenever the pool grows or
replaces a broken one.

**Checked the docs first**
(`/docs/current/extensions/overview#installing-more-extensions`,
`.../installing_extensions#force-installing-to-upgrade-extensions`): `INSTALL`
copies the extension to a local cache directory once; every subsequent
`INSTALL` of the same extension "ignores the statement if it is already
installed" and uses the local copy, no re-download, unless `FORCE INSTALL` is
used. `LOAD` similarly "ignores the statement if it is already loaded."
Neither is documented as doing a background version/metadata check on repeat.

**Verified empirically, not just from docs prose.** Timed `INSTALL spatial;
LOAD spatial;` run 1) once, cold, 2) five more times on five different fresh
`try_clone()`s, 3) three more times on the same connection object:

```
first INSTALL+LOAD (on base conn): 105ms   <- local extension-dir read/parse, not network
clone 0..4: ~1.0-1.4ms each
same-conn repeat 0..2: ~1.0ms each
```

Ran the whole probe under `strace -f -e trace=network` (connect/sendto/
getaddrinfo): **zero matches**, including on the very first call. So on this
machine, with the extension already cached locally, `INSTALL`/`LOAD spatial`
never touches the network at all, cold or repeated — confirms the docs.
Repeating it on every new pooled connection costs ~1ms, not an HTTP round
trip. (First-run-ever-on-a-fresh-machine cold-download cost wasn't measured
here — that only happens once per deployment/cache directory, not per
connection, regardless of pooling strategy.)

**Then checked which of the app's other init commands actually need
per-connection replay**, since extensions are instance-wide (established
earlier) but individual `SET` options can be `GLOBAL` or `SESSION` scoped per
DuckDB's docs (`/docs/current/sql/statements/set#scopes`: "for most options
this is GLOBAL"). Ran all of the default `duckdb_init_commands`
(`config.rs:149`) on one pool connection, then read `current_setting(...)` on
a genuinely distinct sibling `try_clone()` held open at the same time (first
attempt at this got a false "it propagated!" result by sequentially
`get()`/`drop()`-ing the *same* pooled connection twice — same mistake
avoided earlier in this doc by holding all checkouts simultaneously):

| Setting | Propagates to a fresh sibling clone? |
|---|---|
| `enable_progress_bar` | yes (GLOBAL) |
| `preserve_insertion_order` | yes (GLOBAL) |
| `temp_directory` | yes (GLOBAL) |
| `memory_limit` | yes (GLOBAL) |
| `threads` | yes (GLOBAL) |
| `geometry_always_xy` | **no** (SESSION) — same "not in the catalog" error as before |

So of today's default init commands, `geometry_always_xy` is the *only* one
that actually needs to run again on every new connection. Everything else —
including both extension statements — only needs to run once, on the pool
manager's base connection, before any clones are handed out.

**Two ways to act on this, tradeoff not yet resolved:**

1. **Precise split.** Run the full `duckdb_init_commands` list once on the
   base connection (covers extension install/load + all `GLOBAL` settings).
   Separately, have `on_acquire` replay only the one known session-scoped
   statement (`SET geometry_always_xy = true`). Zero redundant work per new
   connection. Fragile if the configured command list ever gains another
   session-scoped `SET` in the future — nothing would catch it short of
   re-running this exact probe, since `duckdb_init_commands` is a freeform
   `Vec<String>` from TOML and scope isn't something the app can introspect
   generically.
2. **Replay everything, every time.** `on_acquire` reruns the entire
   `duckdb_init_commands` list on every new connection, as originally
   proposed. Robust against future config changes (any session-scoped
   setting added later is handled automatically, no special-casing needed).
   Cost is the ~1-2ms measured above, and only pays on connection *creation*
   (pool warmup, or replacing a broken connection) — not per checkout or per
   query. Given the measurement, this is not a meaningful cost at this app's
   scale.

Leaning toward option 2 given the measured cost is negligible and it doesn't
require correctly classifying arbitrary future config entries by DuckDB
scope, but this wasn't decided — pick up here.

## Better fix: `SET GLOBAL`, found before implementing either option above (2026-07-21, cont.)

Question raised: DuckDB's `SET` syntax supports an explicit scope keyword
(`/docs/current/sql/statements/set#examples` shows `SET GLOBAL threads = 4`,
`SET GLOBAL sort_order = 'desc'`) — does forcing `geometry_always_xy` to
`GLOBAL` scope instead of relying on its default (`SESSION`) make it
propagate to clones like the other settings do, sidestepping the whole
replay-per-connection problem?

Verified empirically: `conn_a.execute_batch("SET GLOBAL geometry_always_xy =
true")` succeeds, and a sibling `try_clone()` (`conn_b`, held open at the same
time, never touched directly) immediately reads `current_setting('geometry_always_xy')
= "true"` — same propagation behavior as the settings that were already
`GLOBAL` by default.

**This obsoletes both options above.** Neither a `CustomizeConnection`
`on_acquire` hook nor a split "run once vs. run per-connection" command list
is needed. The fix is a one-line config change: write
`SET GLOBAL geometry_always_xy = true` instead of `SET geometry_always_xy =
true` in `duckdb_init_commands` (`config.rs:155`, `example_config.toml:35`).
With that change, the entire `duckdb_init_commands` list — extensions and
every `SET` — can run exactly once, on whichever connection seeds the pool,
before any clones are created. This is already the shape `build_read_pool`
uses today (`pool.get()` once, run `init_commands` on it) — the only things
that actually need to change are (a) what connection seeds the pool (for the
staleness fix — still requires sourcing from `write` rather than an
independent `Connection::open()`, see Candidate fixes above) and (b) this one
config line. No new r2d2 machinery required for the settings problem at all.

Worth double-checking `SET GLOBAL` against the rest of the configured
settings too before implementing (docs suggest it's fine for anything that
isn't hard-restricted to session-only, but that wasn't verified per-setting
the way `geometry_always_xy` was) — low risk, cheap to confirm same as above.

**Checked before implementing — one exception found.** Ran each configured
`SET GLOBAL <option>` against the `duckdb` CLI individually.
`preserve_insertion_order`, `temp_directory`, `max_temp_directory_size`,
`memory_limit`, `threads`, and `geometry_always_xy` all accept `GLOBAL` scope
without complaint. `enable_progress_bar` does not:

```
$ duckdb :memory: -c "SET GLOBAL enable_progress_bar = false;"
Catalog Error: option "enable_progress_bar" cannot be set globally
```

`enable_progress_bar` only exists in `example_config.toml`, not in
`config.rs`'s Rust default, and stayed a bare `SET` there — it only affects
whether DuckDB prints a progress bar for long queries, not query results, so
it doesn't need to be visible on every pooled connection the way
`geometry_always_xy` does. Every other configured setting uses `SET GLOBAL`.
Caught by actually running the compiled binary against a real config file
(`cargo run -- --config <path> compare ...`) before calling the change done —
it failed loudly (`Failed to execute DuckDB init command: SET GLOBAL
enable_progress_bar = false`) rather than silently, which is why this is a
one-line fix instead of an undiscovered regression.

## Implementation (2026-07-21)

Implemented in full:

- `src/server/mod.rs`: new `ClonedConnectionManager` (`r2d2::ManageConnection`
  impl cloning from one shared `Arc<Mutex<Connection>>` base — the ~15-line
  manager reasoned about throughout this doc, copied conceptually from
  `duckdb-rs`'s own `r2d2.rs`), a `DbPool` type alias, and `build_pool()`.
  `AppState` now has a single `pool: DbPool` field; `write` and `read_pool`
  are gone. `check_startup_conditions` and `build_read_pool` collapsed into
  one pool-based path. `READ_POOL_SIZE` constant removed in favor of
  `config.db_pool_size` (new `Config` field, default `8`).
- `src/config.rs` / `example_config.toml`: `duckdb_init_commands` use `SET
  GLOBAL` (except `enable_progress_bar`, see above), with a comment on both
  explaining why. Added `db_pool_size`.
- `src/server/jobs/mod.rs` and both job implementations
  (`osm_update.rs`, `export_log_prune.rs`): `JobContext.write` →
  `JobContext.pool`; jobs call `ctx.pool.get()` instead of
  `ctx.write.lock()`.
- `src/server/{package,tiles,updates}.rs`: every handler (including
  `log_export`) now goes through `state.pool` — no more split between a
  write path and a read-only pool.
- Verified: full test suite (142 unit + all integration tests) passing;
  `cargo clippy`/`cargo fmt --check` clean (pre-existing unrelated clippy
  warnings in `import/osm.rs` untouched); and a real end-to-end run — built a
  fixture-backed DB via the CLI import commands, started `run`, called
  `/package` (writes an export row) then `/updates` (reads it back via a
  separate HTTP request) and confirmed the row appeared immediately, which is
  the exact symptom this whole document is about.
- Confirmed custom scalar UDFs (`register_scalar_function_with_state`, used
  by `resolve_node_coords`/`resolve_way_coords`) and the `arrow` table
  function (`register_table_function::<ArrowVTab>`) are instance-wide and
  visible on `try_clone()`'d siblings too, the same way extensions are —
  checked empirically before relying on it, since the background OSM update
  job depends on those UDFs and now runs against a pooled clone rather than
  the literal connection they were registered on.
- Not done: no retry-on-conflict logic was added for writers. Per the
  concurrency reanalysis above, no current writer (`log_export`'s INSERTs,
  the retention job's DELETEs on a disjoint row range, the OSM update job's
  writes to different tables) can hit DuckDB's documented write-write
  conflict case, so this wasn't needed — but if a future write path does
  concurrent `UPDATE`/`DELETE` on overlapping rows, it would need one.
- Incidental finding, out of scope for this change but fixed separately right
  after: `/tiles` returned 500 for every z=14 request —
  `ST_AsMVTGeom(GEOMETRY, GEOMETRY, INTEGER_LITERAL, INTEGER_LITERAL,
  BOOLEAN)` doesn't match any overload (the function wants `BOX_2D`, not
  `GEOMETRY`, for the second argument), plus a second, independent bug in
  `BUILDINGS_MVT_SQL`'s `UNION ALL` branches (`geom` ambiguous between the
  source table and the `bbox` CTE). Confirmed via `git diff` that
  `tiles.rs`'s SQL text was untouched by the pooling change itself — both
  bugs pre-existing, unrelated to pooling. Fixed in the same session:
  `ST_Extent(ST_MakeEnvelope(...))` narrows to `BOX_2D`, and the ambiguous
  columns are qualified (`bdot10k_buildings.geom` / `egib_buildings.geom`).
  `tiles.rs` had zero tests before this — added four, and confirmed they
  catch the regression by temporarily reverting the fix and watching them
  fail with 500s.
