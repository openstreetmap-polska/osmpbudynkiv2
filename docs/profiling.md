## 1. Add a profiling release profile to `Cargo.toml`

This keeps your normal `--release` untouched but adds a `profiling` profile that's release-optimized and has debug symbols (so samply can resolve function names in the flamegraph).

```toml
# Release-optimized build with debug symbols kept for profilers (samply, perf).
# Use via `cargo run --profile profiling -- ...` or `cargo build --profile profiling`.
[profile.profiling]
inherits = "release"
debug = "line-tables-only"
strip = "none"
```

`debug = "line-tables-only"` is the sweet spot: you get function names + line numbers in the flamegraph without paying the ~2× binary-size cost of full `debug = true`.

## 2. Allow samply to record without root

samply uses Linux's `perf_event_open`. Check your kernel setting:

```bash
cat /proc/sys/kernel/perf_event_paranoid
```

If it's `> 1`, lower it for this session (reverts on reboot):

```bash
sudo sysctl kernel.perf_event_paranoid=1
```

(Or `=-1` if samply still complains about missing kernel samples — for profiling user-space Rust code, `1` is enough.)

## 3. Build the profiling binary

```bash
cargo build --profile profiling
```

The binary lands at `./target/profiling/osmpbudynkiv2`.

## 4. Run under samply

Instead of `cargo run`, invoke the binary directly under `samply record`:

```bash
samply record \
  ./target/profiling/osmpbudynkiv2 \
  --config ./example_config.toml \
  import osm --file ./example_data/OSM/poland-latest.osm.pbf
```

When the import finishes (or you Ctrl-C), samply auto-opens your browser on `http://localhost:3000` with the profile loaded in Firefox Profiler. The data is also saved to `profile.json.gz` in the CWD — keep it if you want to compare a before/after run.

### Useful options

- `-r 1000` — sampling rate in Hz. Default is 1000 (good). Lower to `250` for very long runs to keep the profile size manageable.
- `--save-only` — write `profile.json.gz` without launching the UI. Useful for headless machines; open later with `samply load profile.json.gz`.
- `-o my_run.json.gz` — custom output path.
- `--` — separator before your binary's args, if any of them clash with samply flags.

For a long run (expect 30–40 min), I'd suggest:

```bash
samply record --save-only -o osm_import_before.json.gz \
  ./target/profiling/osmpbudynkiv2 \
  --config ./example_config.toml \
  import osm --file ./example_data/OSM/poland-latest.osm.pbf
```

Then `samply load osm_import_before.json.gz` to inspect.

## 5. What to look at in the profile

Once it's open in Firefox Profiler:

- **Call tree / Flame Graph tab** — look for:
  - `stream_nodes_to_rocksdb` / `stream_ways_to_rocksdb` — how much time is in DuckDB row decoding (`Value::get`, `value_to_i64_list`) vs `rocksdb::…::write` vs `encoding::…`.
  - `resolve_node_coords` UDF inside the SQL passes — this is where per-way RocksDB point lookups live.
  - `ST_ReadOSM` — if a big fraction of CPU is in PBF parsing, that confirms the 6-pass scan is expensive and worth collapsing.
- **Marker Chart** isn't automatic for Rust, but the `tracing` spans' `info!` logs will appear in stderr so you can correlate wall-clock steps with the flame graph ranges.

Tip: use the **Inverted call tree** view to find leaf hotspots quickly (e.g. "how much total time is in `rocksdb::write`?") before drilling into their callers.

## Sanity check — quick run first

Before burning 40 minutes, do a tiny profiled run against the fixture to confirm the tooling works:

```bash
samply record ./target/profiling/osmpbudynkiv2 \
  --config ./example_config.toml \
  import osm --file ./fixtures/osm.pbf
```

If the flamegraph opens with resolved Rust symbols (you should see `osmpbudynkiv2::import::osm::stream_nodes_to_rocksdb` etc., not hex addresses), you're good to run the full Poland import.
