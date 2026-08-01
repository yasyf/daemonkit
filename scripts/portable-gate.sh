#!/usr/bin/env bash
# Gate the module's linux-portable subset against ci/portable.txt.
#
#   scripts/portable-gate.sh            # check; exits non-zero on drift either way
#   scripts/portable-gate.sh --write    # record an undeclared gain in the manifest
#
# daemonkit is macOS-only and structurally so, but a handful of packages reach
# for no darwin seam, and fleet consumers that ship linux build exactly those.
# The manifest is the declared boundary, and the gate fails in both directions:
# a declared package that no longer builds and vets under GOOS=linux is a
# regression, and an undeclared package that does is a boundary nobody reviewed.
#
# The two directions do not share a remedy. --write records a gain; it refuses
# while anything is regressed, because regenerating the manifest over a
# regression drops the regressed package from it and launders a broken boundary
# into an approved one in a single command. A regression is fixed in the
# package, or given up in a separate change whose diff says so.
#
# The package set is enumerated under GOOS=darwin: a darwin-tagged package drops
# out of `go list ./...` entirely under any other GOOS, and this gate runs on a
# linux runner.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

manifest="ci/portable.txt"

usage() {
  echo "usage: portable-gate.sh [--write]" >&2
  exit 2
}

# portable prints the module-relative directory of every package that both
# builds and vets for linux, sorted. `-o /dev/null` keeps a main package from
# dropping a binary into the tree.
portable() {
  local module pkg dir
  module="$(go list -m)"
  GOOS=darwin GOARCH=arm64 go list ./... | while read -r pkg; do
    dir="${pkg#"$module"}"
    dir="${dir#/}"
    if GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /dev/null "$pkg" >/dev/null 2>&1 &&
      GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go vet "$pkg" >/dev/null 2>&1; then
      echo "${dir:-.}"
    fi
  done | sort
}

declared() {
  grep -Ev '^[[:space:]]*(#|$)' "$manifest" | sort
}

section() {
  local label="$1" sign="$2" lines="$3"
  [[ -n "$lines" ]] || return 0
  printf 'portable gate: %s (%d):\n' "$label" "$(printf '%s\n' "$lines" | wc -l | tr -d ' ')" >&2
  printf '%s\n' "$lines" | sed "s/^/  $sign /" >&2
}

# explain re-runs the linux build and vet of each named package with the output
# shown. Discovery has to swallow that output — most of the module is darwin-only
# by design and fails there on purpose — so without this a regression reaches CI
# as a bare package name with no compiler diagnostic and no way in.
explain() {
  local module dir pkg
  module="$(go list -m)"
  while read -r dir; do
    if [[ "$dir" == "." ]]; then
      pkg="$module"
    else
      pkg="$module/$dir"
    fi
    printf 'portable gate: %s under GOOS=linux:\n' "$pkg" >&2
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /dev/null "$pkg" >&2 || true
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go vet "$pkg" >&2 || true
    printf "  reproduce: GOOS=linux GOARCH=amd64 CGO_ENABLED=0 sh -c 'go build -o /dev/null %s && go vet %s'\n" \
      "$pkg" "$pkg" >&2
  done <<<"$1"
}

case "${1:-}" in
  --write) [[ "$#" == 1 ]] || usage ;;
  "") ;;
  *) usage ;;
esac

got="$(mktemp)"
want="$(mktemp)"
trap 'rm -f "$got" "$want"' EXIT
portable >"$got"
declared >"$want"

regressed="$(comm -13 "$got" "$want")"
undeclared="$(comm -23 "$got" "$want")"

if [[ -n "$regressed" ]]; then
  section "declared portable, but no longer builds and vets under GOOS=linux" "-" "$regressed"
  section "builds and vets under GOOS=linux, but is not declared portable" "+" "$undeclared"
  explain "$regressed"
  printf 'portable gate: a regression is fixed in the package, not recorded — --write refuses while one stands. Giving the package up is a separate change that drops its line from %s and says why.\n' \
    "$manifest" >&2
  exit 1
fi

if [[ "${1:-}" == "--write" ]]; then
  cp "$got" "$manifest"
  printf 'portable gate: %d packages recorded in %s\n' \
    "$(wc -l <"$manifest" | tr -d ' ')" "$manifest" >&2
  exit 0
fi

if [[ -n "$undeclared" ]]; then
  section "builds and vets under GOOS=linux, but is not declared portable" "+" "$undeclared"
  echo "portable gate: an undeclared gain needs review. Once approved: scripts/portable-gate.sh --write" >&2
  exit 1
fi

printf 'portable gate: %d packages build and vet under GOOS=linux, %s matches\n' \
  "$(wc -l <"$got" | tr -d ' ')" "$manifest" >&2
