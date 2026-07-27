#!/usr/bin/env bash
# Gate the module's exported API surface against ci/exported.txt.
#
#   scripts/export-census.sh            # check; exits non-zero on any drift
#   scripts/export-census.sh --write    # regenerate the allowlist from the tree
#
# Both additions and removals fail the check: the allowlist is the record of
# which exports the census approved, so a deletion wave has to land exactly the
# symbols it said it would.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

allowlist="ci/exported.txt"

case "${1:-}" in
  --write)
    [[ "$#" == 1 ]] || {
      echo "usage: export-census.sh [--write]" >&2
      exit 2
    }
    go run ./ci/exportcensus -o "$allowlist"
    ;;
  "")
    go run ./ci/exportcensus -check "$allowlist"
    ;;
  *)
    echo "usage: export-census.sh [--write]" >&2
    exit 2
    ;;
esac
