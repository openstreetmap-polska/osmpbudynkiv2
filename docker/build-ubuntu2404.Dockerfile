# Build environment matching the production host.
#
# Same distro as the release workflow's `runs-on: ubuntu-24.04` (glibc 2.39,
# GLIBCXX_3.4.33), so a profiling build links what a release build links. The
# deploy target (budynki.openstreetmap.org.pl) is **Ubuntu 26.04, glibc 2.43**
# since 2026-10-25 (it was 22.04 / glibc 2.35 before). glibc and libstdc++ are
# backward- but not forward-compatible: a binary built on a distro newer than
# the host links newer symbol versions and fails to start with a
# `GLIBC_2.xx not found` loader error. So a binary shipped to production is
# built in here, not on the developer's machine, whose distro may be newer.
# Check what a built binary actually demands with:
#
#   objdump -T target/... | grep -oE 'GLIBC_[0-9.]+' | sort -V -u | tail -1
#
# Anything above the host's glibc (2.43) will not run on the server.
#
# Build the image with:
#
#   docker build -t osmpb-build:24.04 -f docker/build-ubuntu2404.Dockerfile docker
FROM ubuntu:24.04

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

# Ubuntu's packaged rustc lags whatever `rust-toolchain.toml` asks for (22.04's
# was too old even for this crate's `edition = "2024"`, which needs >= 1.85),
# so install via rustup rather than apt.
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
