#!/usr/bin/env bash

# Regenerate expected.adiff files for adiff tests by running the current
# binary against each fixture's input.osm + edits.osc.

set -euo pipefail

REPO=$(git rev-parse --show-toplevel)
BIN="$REPO/target/debug/osmx-rs"
TESTS="$REPO/bin/tests/adiff"

cargo build --manifest-path "$REPO/Cargo.toml" >&2

update_test() {
  local name=$1
  local dir="$TESTS/$name"
  if [[ ! -f "$dir/input.osm" || ! -f "$dir/edits.osc" ]]; then
    echo "skipping $name (no input.osm/edits.osc)" >&2
    return
  fi

  local tmp
  tmp=$(mktemp -d)
  trap "rm -rf '$tmp'" RETURN

  osmium cat "$dir/input.osm" -o "$tmp/in.pbf" -f pbf
  "$BIN" expand "$tmp/in.pbf" "$tmp/test.osmx"

  if [[ -d "$dir/expected" ]]; then
    rm -rf "$dir/expected"
    mkdir -p "$dir/expected"
    "$BIN" augmented-diff --split "$tmp/test.osmx" "$dir/edits.osc" "$dir/expected"
    echo "wrote $dir/expected/*.adiff"
  fi

  if [[ -f "$dir/expected.adiff" ]]; then
    "$BIN" augmented-diff "$tmp/test.osmx" "$dir/edits.osc" "$dir/expected.adiff"
    echo "wrote $dir/expected.adiff"
  fi
}

for d in "$TESTS"/*/; do
  update_test "$(basename "$d")"
done
