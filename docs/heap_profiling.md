# Heap profiling the server

Written 2026-09-12, for the memory leak in `docs/memory_growth_investigation.md`.
Read that first for why this is the remaining step: the jemalloc arena-retention
hypothesis was tested in production and came back **negative**, so what is left
is a genuine leak and profiling is how it gets located.

---

## What this profiles, and what it cannot

This process contains **two jemallocs** (CLAUDE.md's jemalloc gotcha):

| allocator | symbols | serves | profiled here |
|---|---|---|---|
| `rust-rocksdb`'s, via `tikv-jemalloc-sys` | **unprefixed** `malloc`/`free` | the Rust heap, RocksDB, **and GEOS inside `spatial.duckdb_extension`** | **yes** |
| DuckDB's bundled copy | `duckdb_je_*` | DuckDB only | no |

Linux is not in `rust-librocksdb-sys`'s `NO_JEMALLOC_TARGETS`, so its jemalloc
is built unprefixed and its `malloc`/`free` override glibc's process-wide. The
spatial extension is a runtime-loaded shared object that calls plain
`malloc`/`free`, so its GEOS allocations bind to that same allocator and **do**
appear in these profiles.

**The blind spot is deliberate and convenient.** DuckDB's own allocations go
through `duckdb_je_*` and will not appear — but DuckDB's usage is already known
from its own accounting (3.7 GiB, pinned at `memory_limit`, reported in the
`Out of Memory Error` text and by `duckdb_memory()`). The unexplained ~5.2 GiB
is precisely the part that *is* in scope here. Do not read an absent DuckDB in
the profile as evidence about DuckDB.

## Building

**The binary must be built in the Ubuntu 22.04 container.** The deploy host
(budynki.openstreetmap.org.pl) is Ubuntu 22.04 / **glibc 2.35**; a binary built
on a newer distro links newer glibc symbol versions and dies at startup with a
loader error, before any of this matters.

```bash
./docker/build-profiling.sh          # builds osmpb-build:22.04 workload
```

That runs `cargo build --profile profiling --features heap-profiling` inside the
image. Two things it relies on:

- **`--profile profiling`** (already in `Cargo.toml`) is release optimisation
  with `debug = "line-tables-only"` and `strip = "none"`, so `jeprof` can
  resolve symbols. A plain `--release` build is stripped enough to make profiles
  unreadable.
- **`--features heap-profiling`** adds `tikv-jemalloc-sys` with its `profiling`
  feature as a *direct* dependency. It does not add a second allocator:
  `rust-rocksdb` already depends on that exact crate, and cargo unifies features
  onto the one build — so the effect is purely to pass `--enable-prof` to the
  jemalloc that is already linked.

Verify the result before shipping it:

```bash
# must print nothing above GLIBC_2.35
objdump -T /mnt/nvme/osmpb-build/target/profiling/osmpbudynkiv2 \
  | grep -oE 'GLIBC_[0-9.]+' | sort -V -u | tail -3

# proof --enable-prof took effect on the RIGHT allocator (expect ~119, not 0)
nm /mnt/nvme/osmpb-build/target/profiling/osmpbudynkiv2 | grep -cE '_rjem.*prof'
```

The second check is the one that matters, and it has to be spelled exactly like
that. Two ways to get a false pass:

- **`nm -D` finds nothing either way.** These are local symbols (`t`/`b`/`d`),
  not dynamic ones, so the dynamic table is the wrong place to look.
- **Grepping for a bare `prof_dump` matches the wrong jemalloc.** DuckDB's
  bundled copy ships with profiling compiled in already, so `duckdb_je_prof_*`
  symbols are present regardless of what this feature did. The `_rjem` prefix is
  what distinguishes `rust-rocksdb`'s allocator — the one backing the Rust heap
  and GEOS — from DuckDB's.

`--enable-prof` failing is otherwise silent: you get a working binary that
ignores `prof:true` at runtime, and you find out only after spending a deploy
window.

**Both jemallocs read the same `MALLOC_CONF`.** Since DuckDB's also has
profiling compiled in, `prof:true` activates *both*, and both write dumps using
the same `<prefix>.<pid>.<seq>.i<iseq>.heap` naming from the same PID. That is
mostly a bonus — it puts DuckDB's 3.7 GiB in reach too — but expect the dump
files to interleave, and confirm which allocator a given profile came from by
whether its frames carry `_rjem_` or `duckdb_je_` prefixes before drawing any
conclusion from it.

## Running it

Profiling is compiled in but **inert until activated by `MALLOC_CONF`**, so the
binary is safe to deploy before you decide to profile. Activate by adding to the
systemd unit:

```ini
Environment=MALLOC_CONF=narenas:4,background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:0,prof:true,prof_active:true,prof_prefix:/mnt/osmpbudynkiv2/jeprof/jeprof,lg_prof_interval:32
```

- `lg_prof_interval:32` dumps a profile every 4 GiB *allocated* (not resident).
  Tune down to 30 (1 GiB) for a faster first sample.

**Time the profiling window against the daily dataset refreshes, not against
idle.** Measured 2026-09-12 over 23 minutes of ordinary serving, total anonymous
memory was **flat** (-6.2 MiB; RSS fell 184 MiB while swap rose 178 MiB, which
is the kernel migrating pages under host pressure, not the process doing
anything). A continuous leak at the rate the totals imply would have shown
~+40 MiB in that window. So the accumulation is **event-driven**, and the
arithmetic points at the daily `*_update` jobs: ~5.2 GiB unaccounted over two
uptime days is ~2.6 GiB per refresh cycle, and the earlier v0.1.2 run's 4-day
figure lands on the same per-cycle number.

Those jobs fire at roughly 09:00 UTC (`egib` 08:59, `bdot10k` 09:02, `prg`
09:08). Profiling a plateau will show nothing but steady-state caches, so the
dumps have to straddle that window. Note also that `bdot10k` and `egib`
currently **fail** there with DuckDB `Out of Memory Error` while `prg` succeeds
— a diff dying partway through is a better leak candidate than one that
completes, so do not "fix" the OOM by raising `memory_limit` before profiling
has had a chance to see it.
- `prof_prefix` must point at a **directory that exists and is writable by the
  service user**, and must be on `/mnt` — `/` on this host is 84% full.
  `mkdir -p /mnt/osmpbudynkiv2/jeprof && chown osmpbudynkiv2 ...` first.
- Keep the existing `narenas`/decay settings. Changing them at the same time
  would confound the result against the baseline already measured.

Overhead is a sampling backtrace per ~512 KiB allocated by default — real but
modest. `prof_leak:true,prof_final:true` additionally dumps at exit; not used
here because the process is long-lived and we want the *growth*, not the end
state.

## Reading the profiles

`jeprof` is a perl script shipped with jemalloc; perl is already on the server.

The useful output is not a single profile but a **diff between two**, which
isolates what grew rather than what is merely large:

```bash
jeprof --show_bytes --text \
  /opt/osmpbudynkiv2/osmpbudynkiv2 \
  --base=jeprof.<early>.heap \
  jeprof.<later>.heap \
  | head -40
```

A single profile is dominated by legitimately-large steady-state allocations
(caches, buffers) and will send you chasing them. The diff shows only the delta
between two points hours apart, which is the leak.

## What to look for

From the investigation so far, in rough order of prior probability:

1. **GEOS allocations inside the spatial extension** — the same component that
   produced duckdb-spatial#861. Tile rendering calls `ST_AsMVTGeom` constantly
   (`tile_refresh` every 10 s), and `/package` calls `ST_Intersection`.
2. **The Rust heap** — anything accumulating per request or per job tick.

Neither is confirmed. The point of the diff is to decide between them, not to
confirm a guess.
