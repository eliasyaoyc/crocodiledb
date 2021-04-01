#!/usr/bin/env bash

set -euo pipefail

cargo build --release --bin toydb

for ID in 1 2 3; do
    (cargo run -q --release -- -c toydb-$ID/toydb.yaml 2>&1 | sed -e "s/\\(.*\\)/node-$ID \\1/g") &
done

trap 'kill $(jobs -p)' EXIT
wait < <(jobs -p)