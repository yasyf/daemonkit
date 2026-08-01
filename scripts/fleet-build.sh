#!/usr/bin/env bash
# Compile every fleet consumer against the daemonkit working tree, so a
# breaking surface change goes red BEFORE a release exists.
#
# Each consumer is built under its own generated go.work (in a temp dir) whose
# `use` directives point at this tree, at that consumer, and at every fleet
# substrate the consumer requires that has a checkout of its own. Without that
# last cone a leg resolves cc-interact, cc-notes, reposync and friends from
# their published tags and reports a stale peer's break as daemonkit's. The
# `use` directive is what overrides the consumer's daemonkit pin — daemonkit
# never carries a committed `replace` or a committed go.work, and the consumer
# checkouts are never mutated (AGENTS.md § Repository Structure).
#
# A checkout counts only when its own go.mod declares the module the leg is for,
# so a directory that merely carries the name cannot stand in for it; a consumer
# with no such checkout fails its leg instead of being cloned from remote main,
# which during a migration is the unmigrated tree and a false green.
#
# ci/fleet-refs.txt declares each leg's ref and GOOS set; `--ref REPO` prints
# that ref and `--cone REPO` the substrate it needs, so the workflow checks out
# exactly the trees this script expects to build.
#
# Usage:
#   scripts/fleet-build.sh                # every consumer
#   scripts/fleet-build.sh --only cc-notes [--only binrun]
#   scripts/fleet-build.sh --ref binrun   # print binrun's declared ref
#   scripts/fleet-build.sh --cone cc-orchestrate  # print its fleet substrate
#   FLEET_REPOS="binrun synckit" scripts/fleet-build.sh
#
# Environment:
#   FLEET_REPOS  space-separated consumer list, overriding the default fleet
#   FLEET_SRC    directory holding consumer checkouts (default: daemonkit's parent)
#
# Exit: 0 all consumers built; 1 a consumer failed or has no checkout.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

owner="yasyf"
default_repos="synckit reposync captain-hook cc-notes cc-patch cc-orchestrate cc-interact binrun cc-present cc-review"
refs_file="$root/ci/fleet-refs.txt"
default_goos="darwin"

fail() {
  echo "fleet-build: $*" >&2
  exit 1
}

only=""
ref_query=""
cone_query=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --only)
      [[ "$#" -ge 2 ]] || fail "usage: fleet-build.sh [--only REPO]..."
      only="$only $2"
      shift 2
      ;;
    --ref)
      [[ "$#" -ge 2 ]] || fail "usage: fleet-build.sh --ref REPO"
      ref_query="$2"
      shift 2
      ;;
    --cone)
      [[ "$#" -ge 2 ]] || fail "usage: fleet-build.sh --cone REPO"
      cone_query="$2"
      shift 2
      ;;
    -h | --help)
      sed -n '2,34p' "${BASH_SOURCE[0]}" | cut -c3-
      exit 0
      ;;
    *)
      fail "unknown argument $1 (usage: fleet-build.sh [--only REPO]... | --ref REPO | --cone REPO)"
      ;;
  esac
done

# Prints column $2 of $1's ci/fleet-refs.txt line; empty when it declares none.
declared() {
  awk -v repo="$1" -v col="$2" '
    /^[[:space:]]*(#|$)/ { next }
    $1 == repo { print $col; exit }
  ' "$refs_file"
}

check_refs() {
  local repo ref rest seen=""
  while read -r repo ref _ rest; do
    case " $default_repos " in
      *" $repo "*) ;;
      *) fail "ci/fleet-refs.txt declares $repo, which is no fleet consumer" ;;
    esac
    case " $seen " in
      *" $repo "*) fail "ci/fleet-refs.txt declares $repo twice" ;;
    esac
    seen="$seen $repo"
    [[ -n "$ref" ]] || fail "ci/fleet-refs.txt leaves $repo without a ref"
    [[ -z "$rest" ]] || fail "ci/fleet-refs.txt gives $repo more than \"<repo> <ref> [goos,...]\""
  done < <(grep -Ev '^[[:space:]]*(#|$)' "$refs_file")
  for repo in $default_repos; do
    case " $seen " in
      *" $repo "*) ;;
      *) fail "ci/fleet-refs.txt leaves $repo undeclared, so its leg would build a branch nobody chose" ;;
    esac
  done
}

repos="${only:-${FLEET_REPOS:-$default_repos}}"
src_dir="${FLEET_SRC:-$(dirname "$root")}"

go_directive() {
  awk '$1 == "go" {print $2; exit}' "$1/go.mod"
}

highest_go() {
  local dir
  for dir in "$@"; do go_directive "$dir"; done | sort -V | tail -1
}

pinned_daemonkit() {
  awk -v module="github.com/$owner/daemonkit" '
    {
      for (i = 1; i < NF; i++) {
        if ($i == module && substr($(i + 1), 1, 1) == "v") { print $(i + 1); exit }
      }
    }
  ' "$1/go.mod"
}

module_of() {
  [[ -f "$1/go.mod" ]] || return 0
  awk '$1 == "module" {print $2; exit}' "$1/go.mod"
}

# Prints $src_dir's checkout of module $1, and only when that checkout's own
# go.mod declares it: a directory carrying the name is not evidence of identity.
checkout_of() {
  local module="$1" named="$src_dir/${1##*/}"
  [[ "$(module_of "$named")" == "$module" ]] || return 1
  echo "$named"
}

# Every github.com/<owner> module the consumer requires, direct or indirect.
fleet_requires() {
  awk -v prefix="github.com/$owner/" '
    $1 == "replace" || $1 == "exclude" { next }
    {
      for (i = 1; i < NF; i++) {
        if (index($i, prefix) == 1 && substr($(i + 1), 1, 1) == "v") { print $i }
      }
    }
  ' "$1/go.mod" | sort -u
}

# The fleet substrate the consumer checked out at $1 sits on, named by repo:
# every module it requires under the owner's prefix but itself and daemonkit.
cone_of() {
  local self required
  self="$(module_of "$1")"
  for required in $(fleet_requires "$1"); do
    [[ "$required" != "github.com/$owner/daemonkit" ]] || continue
    [[ "$required" != "$self" ]] || continue
    echo "${required##*/}"
  done
}

describe_checkout() {
  local dir="$1" rev dirty
  rev="$(git -C "$dir" rev-parse --short HEAD 2>/dev/null)" || {
    echo "no-git"
    return
  }
  dirty=""
  [[ -z "$(git -C "$dir" status --porcelain 2>/dev/null)" ]] || dirty="-dirty"
  echo "$(git -C "$dir" rev-parse --abbrev-ref HEAD) $rev$dirty"
}

# A workspace build treats every `use` member as a main module, so the go
# command may add missing hashes to their go.sum. The gate must leave every
# checkout it overlays byte-identical.
snapshot_manifest() {
  local dir="$1" work="$2" file
  mkdir -p "$work/orig/$(basename "$dir")"
  for file in go.mod go.sum; do
    [[ -f "$dir/$file" ]] && cp "$dir/$file" "$work/orig/$(basename "$dir")/$file"
  done
  return 0
}

restore_manifest() {
  local dir="$1" work="$2" name file
  name="$(basename "$dir")"
  for file in go.mod go.sum; do
    [[ -f "$work/orig/$name/$file" ]] || continue
    cmp -s "$work/orig/$name/$file" "$dir/$file" && continue
    cp "$work/orig/$name/$file" "$dir/$file"
    echo "  note: $name/$file was rewritten by the build; restored"
  done
}

check_refs

if [[ -n "$ref_query" ]]; then
  declared "$ref_query" 2
  exit 0
fi

if [[ -n "$cone_query" ]]; then
  cone_dir="$(checkout_of "github.com/$owner/$cone_query")" ||
    fail "no checkout of github.com/$owner/$cone_query under $src_dir"
  cone_of "$cone_dir"
  exit 0
fi

# The trailing slash macOS leaves on TMPDIR would put a `//` in every generated
# go.work path, and go.work `use` resolution mis-resolves that into a relative
# "../../.." it cannot open.
tmp_base="${TMPDIR:-/tmp}"
tmp="$(mktemp -d "${tmp_base%/}/fleet-build.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

failed_repos=""
dk_rev="$(describe_checkout "$root")"

echo "fleet-build: daemonkit $root ($dk_rev)"
echo "fleet-build: consumer checkouts under $src_dir"
echo

for repo in $repos; do
  module="github.com/$owner/$repo"
  if ! source_dir="$(checkout_of "$module")"; then
    printf '  %-6s %-15s %s\n' "FAIL" "$repo" "no checkout of $module under $src_dir"
    echo "no checkout of $module under $src_dir: $src_dir/$repo does not declare it" >"$tmp/$repo.log"
    failed_repos="$failed_repos $repo"
    continue
  fi

  cone=""
  for peer in $(cone_of "$source_dir"); do
    peer_dir="$(checkout_of "github.com/$owner/$peer")" || {
      echo "  note: no checkout of $peer under $src_dir; $repo builds its published pin"
      continue
    }
    cone="$cone $peer_dir"
  done

  work="$tmp/work/$repo"
  mkdir -p "$work"
  {
    printf 'go %s\n\nuse (\n' "$(highest_go "$root" "$source_dir" $cone)"
    for dir in "$root" "$source_dir" $cone; do printf '\t%s\n' "$dir"; done
    printf ')\n'
  } >"$work/go.work"

  for dir in "$root" "$source_dir" $cone; do snapshot_manifest "$dir" "$work"; done

  goos_set="$(declared "$repo" 3)"
  goos_set="${goos_set:-$default_goos}"
  verdict="PASS"
  : >"$tmp/$repo.log"
  for goos in ${goos_set//,/ }; do
    echo "---- $repo GOOS=$goos ----" >>"$tmp/$repo.log"
    if ! (cd "$source_dir" && GOWORK="$work/go.work" CGO_ENABLED=0 GOOS="$goos" go build ./...) \
      >>"$tmp/$repo.log" 2>&1; then
      verdict="FAIL"
    fi
  done
  [[ "$verdict" == PASS ]] || failed_repos="$failed_repos $repo"

  for dir in "$root" "$source_dir" $cone; do restore_manifest "$dir" "$work"; done

  printf '  %-6s %-15s %-13s ref %-14s pin %-9s %s (%s)\n' \
    "$verdict" "$repo" "$goos_set" "$(declared "$repo" 2)" \
    "$(pinned_daemonkit "$source_dir")" "$source_dir" "$(describe_checkout "$source_dir")"
  for dir in $cone; do
    printf '         cone %s (%s)\n' "$(basename "$dir")" "$(describe_checkout "$dir")"
  done
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

echo
echo "fleet-build: $((total - failed))/$total built, $failed failed"

[[ -z "$failed_repos" ]] || exit 1
