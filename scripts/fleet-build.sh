#!/usr/bin/env bash
# Compile every fleet consumer against the daemonkit working tree, so a
# breaking surface change goes red BEFORE a release exists.
#
# Each consumer is built under its own generated go.work (in a temp dir) whose
# `use` directives point at this tree and at that consumer. The `use` directive
# is what overrides the consumer's daemonkit pin — daemonkit never carries a
# committed `replace` or a committed go.work, and the consumer checkouts are
# never mutated (AGENTS.md § Repository Structure).
#
# Usage:
#   scripts/fleet-build.sh                # every consumer
#   scripts/fleet-build.sh --only cc-notes [--only binrun]
#   FLEET_REPOS="binrun synckit" scripts/fleet-build.sh
#
# Environment:
#   FLEET_REPOS  space-separated consumer list, overriding the default fleet
#   FLEET_SRC    directory holding consumer checkouts (default: daemonkit's parent)
#
# Exit: 0 all consumers built; 1 a consumer failed; 2 a consumer's source was
# unreachable (nothing failed, but the gate did not cover the whole fleet).

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

owner="yasyf"
default_repos="synckit reposync captain-hook cc-notes cc-patch cc-orchestrate cc-interact binrun cc-present cc-review"

fail() {
  echo "fleet-build: $*" >&2
  exit 1
}

only=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --only)
      [[ "$#" -ge 2 ]] || fail "usage: fleet-build.sh [--only REPO]..."
      only="$only $2"
      shift 2
      ;;
    -h | --help)
      sed -n '2,22p' "${BASH_SOURCE[0]}" | cut -c3-
      exit 0
      ;;
    *)
      fail "unknown argument $1 (usage: fleet-build.sh [--only REPO]...)"
      ;;
  esac
done

repos="${only:-${FLEET_REPOS:-$default_repos}}"
src_dir="${FLEET_SRC:-$(dirname "$root")}"

# The trailing slash macOS leaves on TMPDIR would put a `//` in every cloned
# consumer's path, and go.work `use` resolution mis-resolves that into a
# relative "../../.." it cannot open.
tmp_base="${TMPDIR:-/tmp}"
tmp="$(mktemp -d "${tmp_base%/}/fleet-build.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

go_directive() {
  awk '$1 == "go" {print $2; exit}' "$1/go.mod"
}

pinned_daemonkit() {
  awk '$1 == "github.com/yasyf/daemonkit" {print $2; exit}' "$1/go.mod"
}

describe_checkout() {
  local dir="$1" rev dirty
  rev="$(git -C "$dir" rev-parse --short HEAD 2>/dev/null)" || {
    echo "no-git"
    return
  }
  dirty=""
  [[ -z "$(git -C "$dir" status --porcelain 2>/dev/null)" ]] || dirty="-dirty"
  echo "$rev$dirty"
}

# A workspace build treats the consumer as a main module, so the go command may
# add missing hashes to its go.sum. The gate must leave checkouts byte-identical.
snapshot_manifest() {
  local dir="$1" work="$2" file
  for file in go.mod go.sum; do
    [[ -f "$dir/$file" ]] && cp "$dir/$file" "$work/$file.orig"
  done
  return 0
}

restore_manifest() {
  local dir="$1" work="$2" repo="$3" file
  for file in go.mod go.sum; do
    [[ -f "$work/$file.orig" ]] || continue
    cmp -s "$work/$file.orig" "$dir/$file" && continue
    cp "$work/$file.orig" "$dir/$file"
    echo "  note: $repo/$file was rewritten by the build; restored"
  done
}

# Prints the consumer's source directory, cloning it when no checkout is present.
resolve_source() {
  local repo="$1"
  if [[ -f "$src_dir/$repo/go.mod" ]]; then
    echo "$src_dir/$repo"
    return 0
  fi
  local dest="$tmp/clone/$repo"
  mkdir -p "$(dirname "$dest")"
  git clone --quiet --depth 1 "https://github.com/$owner/$repo" "$dest" >&2 || return 1
  echo "$dest"
}

failed_repos=""
skipped_repos=""
dk_rev="$(describe_checkout "$root")"
dk_go="$(go_directive "$root")"

echo "fleet-build: daemonkit $root ($dk_rev)"
echo "fleet-build: consumer checkouts under $src_dir"
echo

for repo in $repos; do
  if ! source_dir="$(resolve_source "$repo")"; then
    printf '  %-6s %-15s %s\n' "SKIP" "$repo" "no checkout at $src_dir/$repo and clone failed"
    skipped_repos="$skipped_repos $repo"
    continue
  fi

  origin="local"
  [[ "$source_dir" == "$src_dir/$repo" ]] || origin="clone"

  work="$tmp/work/$repo"
  mkdir -p "$work"
  printf 'go %s\n\nuse (\n\t%s\n\t%s\n)\n' \
    "$(printf '%s\n%s\n' "$dk_go" "$(go_directive "$source_dir")" | sort -V | tail -1)" \
    "$root" "$source_dir" >"$work/go.work"

  snapshot_manifest "$source_dir" "$work"
  if (cd "$source_dir" && GOWORK="$work/go.work" CGO_ENABLED=0 go build ./...) >"$tmp/$repo.log" 2>&1; then
    verdict="PASS"
  else
    verdict="FAIL"
    failed_repos="$failed_repos $repo"
  fi
  restore_manifest "$source_dir" "$work" "$repo"
  printf '  %-6s %-15s %-6s pin %-8s %s (%s)\n' \
    "$verdict" "$repo" "$origin" "$(pinned_daemonkit "$source_dir")" \
    "$source_dir" "$(describe_checkout "$source_dir")"
done

if [[ -n "$failed_repos" ]]; then
  echo
  echo "==================== compiler errors ===================="
  for repo in $failed_repos; do
    echo
    echo "---- $repo ----"
    cat "$tmp/$repo.log"
  done
  echo "========================================================="
fi

words() { echo "$1" | wc -w | tr -d ' '; }
total="$(words "$repos")"
failed="$(words "$failed_repos")"
skipped="$(words "$skipped_repos")"

echo
echo "fleet-build: $((total - failed - skipped))/$total built, $failed failed, $skipped unreachable"

if [[ -n "$skipped_repos" ]]; then
  echo
  echo "WARNING: the gate did not cover the whole fleet. Unreachable consumers:" >&2
  for repo in $skipped_repos; do
    echo "  - $repo (clone denied? a private consumer needs a token with read access)" >&2
  done
fi

[[ -z "$failed_repos" ]] || exit 1
[[ -z "$skipped_repos" ]] || exit 2
