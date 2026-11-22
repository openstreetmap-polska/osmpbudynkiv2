#!/usr/bin/env bash
# Build a production-compatible binary with jemalloc heap profiling compiled in.
#
# Runs in the Ubuntu 24.04 image, the same distro the release workflow builds on.
# A binary built on a distro newer than the deploy host (Ubuntu 26.04, glibc
# 2.43) will not start there. See build-ubuntu2404.Dockerfile, which also has
# the `docker build` command for the image.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE="${OSMPB_BUILD_CACHE:-/mnt/nvme/osmpb-build}"
# One target dir per image. Cargo's fingerprints do not include the distro, so a
# dir shared with the old 22.04 image would link its compiled DuckDB/RocksDB
# objects instead of rebuilding them, the same trap the release workflow's
# `ImageOS` cache key exists for.
TARGET="$CACHE/target-24.04"
mkdir -p "$CACHE/cargo-home" "$TARGET"
exec docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e CARGO_HOME=/cargo-home \
  -v "$REPO":/src \
  -v "$CACHE/cargo-home":/cargo-home \
  -v "$TARGET":/target \
  osmpb-build:24.04 \
  cargo build --profile profiling --features heap-profiling "$@"
