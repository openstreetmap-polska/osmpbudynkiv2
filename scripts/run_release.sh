#!/usr/bin/env bash

set -euo pipefail

set -o allexport && source .env && set +o allexport

cargo build --release

# ./target/release/osmpbudynkiv2 --config example_config.toml import full
# ./target/release/osmpbudynkiv2 --config example_config.toml compare full

# ./target/release/osmpbudynkiv2 --config example_config.toml update osm
# ./target/release/osmpbudynkiv2 --config example_config.toml queue drain

./target/release/osmpbudynkiv2 --config example_config.toml init
