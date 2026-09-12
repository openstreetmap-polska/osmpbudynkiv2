# Build environment matching the production host.
#
# The deploy target (budynki.openstreetmap.org.pl) is **Ubuntu 22.04, glibc
# 2.35**. A binary built on a newer distro links against newer glibc symbol
# versions and fails to start there with a `GLIBC_2.3x not found` loader error
# -- so every binary shipped to production must be built in here, not on the
# developer's machine. Check what a built binary actually demands with:
#
#   objdump -T target/... | grep -oE 'GLIBC_[0-9.]+' | sort -V -u | tail -1
#
# Anything above GLIBC_2.35 will not run on the server.
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# cmake + ninja are required by the DuckDB `bundled-cmake` backend (see
# CLAUDE.md). libclang-dev is required by `bindgen`, which both `libduckdb-sys`
# and `rust-librocksdb-sys` run -- the `clang` package alone does NOT provide
# libclang.so and the build fails late, after ~2 minutes of compiling, with
# "couldn't find any valid shared libraries matching: ['libclang.so']".
# perl is needed by `jeprof`, which ships with jemalloc and reads the heap
# profiles this build can emit.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        curl \
        git \
        libclang-dev \
        ninja-build \
        perl \
        pkg-config \
        python3 \
    && rm -rf /var/lib/apt/lists/*

# Ubuntu 22.04 ships a rustc far too old for this crate's `edition = "2024"`
# (needs >= 1.85), so install via rustup rather than apt.
#
# Installed to /usr/local rather than /root so the image can be run with
# `--user $(id -u)`. That matters: the build bind-mounts the source tree to pick
# up Cargo.lock changes, and running as root would leave root-owned files in the
# developer's working tree.
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path \
    && chmod -R a+rwX /usr/local/rustup /usr/local/cargo

# Kept out of the bind-mounted source tree: host and container builds must not
# share a target dir, since they resolve different toolchains and glibc.
ENV CARGO_TARGET_DIR=/target

WORKDIR /src
