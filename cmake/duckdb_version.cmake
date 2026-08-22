# Pins the version DuckDB stamps into itself when built via `bundled-cmake`.
#
# WHY THIS FILE EXISTS
# --------------------
# DuckDB's CMakeLists derives its version from `git describe --tags --long` run
# inside `duckdb-sources`. Cargo checks that submodule out at a bare commit with
# **no tags fetched**, so `git describe` fails and DuckDB silently falls back to
# the dummy version `v0.0.1` (CMakeLists.txt: "likely due to shallow clone …
# Continuing with dummy version v0.0.1").
#
# That is not cosmetic. The version string is the extension repository path, so
# every `INSTALL <ext>` resolves to
#   http://extensions.duckdb.org/v0.0.1/linux_amd64/<ext>.duckdb_extension.gz
# which 404s, and locally installed extensions under ~/.duckdb/extensions/v1.5.5/
# are never found. `INSTALL spatial` is in this project's default
# `duckdb_init_commands`, so with the wrong version *every* command fails at
# startup and 387 of 600 tests fail with a single root cause.
#
# CMake cache variables cannot be injected through `libduckdb-sys`'s build
# script — it forwards only a fixed set of env vars and has no generic `-D`
# passthrough — so this is delivered as a toolchain file, which CMake reads
# before the project's own version logic runs. `.cargo/config.toml` points
# CMAKE_TOOLCHAIN_FILE here.
#
# KEEP IN SYNC WITH Cargo.toml
# ----------------------------
# This must match the DuckDB version behind the `duckdb`/`libduckdb-sys` tag
# pinned in Cargo.toml (`tag = "v1.10505.0"` -> DuckDB v1.5.5). Nothing checks
# the two against each other, and a mismatch fails the same silent way: DuckDB
# reports a version whose extension directory holds the wrong binaries.
# `db::tests::duckdb_reports_a_real_version_so_extensions_resolve` asserts the
# built library's actual version, so a drift here fails one named test instead of
# several hundred unrelated-looking ones.
set(OVERRIDE_GIT_DESCRIBE "v1.5.5" CACHE STRING "DuckDB version stamp" FORCE)
