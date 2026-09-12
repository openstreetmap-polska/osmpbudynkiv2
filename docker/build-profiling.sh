#!/usr/bin/env bash
# Build a production-compatible binary with jemalloc heap profiling compiled in.
#
# Must run in the Ubuntu 22.04 image: the deploy host is glibc 2.35 and a binary
# built on a newer distro will not start there. See build-ubuntu2204.Dockerfile.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE="${OSMPB_BUILD_CACHE:-/mnt/nvme/osmpb-build}"
mkdir -p "$CACHE/cargo-home" "$CACHE/target"
exec docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e CARGO_HOME=/cargo-home \
  -v "$REPO":/src \
  -v "$CACHE/cargo-home":/cargo-home \
  -v "$CACHE/target":/target \
  osmpb-build:22.04 \
  cargo build --profile profiling --features heap-profiling "$@"
