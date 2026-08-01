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
# The cone is the consumer's fleet requires intersected with the fleet set
# itself, so every overlaid peer is one ci/fleet-refs.txt pins a ref for. A peer
# outside that set keeps the published version the consumer already resolved,
# which is what its leg is meant to measure; overlaying it would join the
# workspace at whatever its default branch holds — the one tree nobody chose.
#
# A checkout counts only when its own go.mod declares the module the leg is for,
# so a directory that merely carries the name cannot stand in for it; a consumer
# with no such checkout fails its leg instead of being cloned from remote main,
# which during a migration is the unmigrated tree and a false green.
#
# ci/fleet-refs.txt declares each leg's ref and GOOS set; `--ref REPO` prints
# that ref and `--cone REPO` the substrate it needs, so the workflow checks out
# exactly the trees this script expects to build. A checkout that is not the
# tree its declared ref names is marked `*` rather than refused, because a local
# run legitimately builds a dirty or repointed peer: the verdict is real, it is
# just not a verdict about the ref the file names.
#
# Not covered, by construction. A leg compiles one configuration — the pure
# CGO_ENABLED=0 default-tag build, cross-compiled from a linux runner:
#   cc-pool's cmd/cc-pool-runtime-archive (//go:build darwin && cgo) and
#   fusekit's `-tags fuse` tree both need CGO_ENABLED=1 plus native
#   libfuse/fuse-t headers, which no cross-compile supplies. Their own CI builds
#   them on native runners; a daemonkit break confined to those files reaches
#   this gate as a green leg.
#
#   fusekit's generators write "github.com/yasyf/daemonkit/wire" into the Go
#   they emit and read the wire model to emit Swift, and a leg compiles the
#   checked-in output rather than regenerating it. A daemonkit change that only
#   moves what those generators emit breaks fusekit's codegen drift check, not
#   any compile run here.
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
default_repos="synckit fusekit reposync captain-hook cc-notes cc-patch cc-orchestrate cc-interact binrun cc-present cc-review cc-pool cookiesync"
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
      awk 'NR > 1 && /^#/ { print substr($0, 3); next } NR > 1 { exit }' "${BASH_SOURCE[0]}"
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
    [[ ! "$ref" =~ ^[0-9a-fA-F]{7,40}$ ]] ||
      fail "ci/fleet-refs.txt gives $repo the commit $ref; a ref must name a branch or tag, because a leg that needs $repo in its cone clones it with git clone --branch"
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
# A `replace`/`exclude` block's body names modules the consumer does not require,
# so the whole block is skipped, not just the line opening it.
fleet_requires() {
  awk -v prefix="github.com/$owner/" '
    $1 == "replace" || $1 == "exclude" { directive = ($NF == "("); next }
    directive { directive = ($1 != ")"); next }
    {
      for (i = 1; i < NF; i++) {
        if (index($i, prefix) == 1 && substr($(i + 1), 1, 1) == "v") { print $i }
      }
    }
  ' "$1/go.mod" | sort -u
}

# The fleet substrate the consumer checked out at $1 sits on, named by repo:
# every module it requires under the owner's prefix but itself and daemonkit,
# and only those the fleet set carries a declared ref for.
cone_of() {
  local self required peer
  self="$(module_of "$1")"
  for required in $(fleet_requires "$1"); do
    [[ "$required" != "github.com/$owner/daemonkit" ]] || continue
    [[ "$required" != "$self" ]] || continue
    peer="${required##*/}"
    case " $default_repos " in
      *" $peer "*) echo "$peer" ;;
    esac
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

# Empty when the checkout at $1 is the tree repo $2's declared ref names, `*`
# otherwise — another commit, local modifications, or no git at all. A worktree
# detached at exactly that ref's commit is that tree and goes unmarked, which is
# why this compares commits rather than branch names.
ref_mark() {
  local head declared
  head="$(git -C "$1" rev-parse HEAD 2>/dev/null || true)"
  declared="$(git -C "$1" rev-parse --verify --quiet "$(declared "$2" 2)^{commit}" 2>/dev/null || true)"
  [[ -n "$head" && "$head" == "$declared" && -z "$(git -C "$1" status --porcelain 2>/dev/null)" ]] || echo "*"
}

# Adds $1 to the roster the trailer explains `*` for. A repo reaches this both as
# its own leg and as another leg's cone peer, so the roster is a set.
record_off_ref() {
  case " $off_ref_repos " in
    *" $1 "*) ;;
    *) off_ref_repos="$off_ref_repos $1" ;;
  esac
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
off_ref_repos=""
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

  mark="$(ref_mark "$source_dir" "$repo")"
  [[ -z "$mark" ]] || record_off_ref "$repo"
  printf '  %-6s %-15s %-13s ref %-14s pin %-9s %s (%s)\n' \
    "$verdict$mark" "$repo" "$goos_set" "$(declared "$repo" 2)" \
    "$(pinned_daemonkit "$source_dir")" "$source_dir" "$(describe_checkout "$source_dir")"
  for dir in $cone; do
    peer="$(basename "$dir")"
    peer_mark="$(ref_mark "$dir" "$peer")"
    [[ -z "$peer_mark" ]] || record_off_ref "$peer"
    printf '         %-5s %s (%s)\n' "cone$peer_mark" "$peer" "$(describe_checkout "$dir")"
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
[[ -z "$off_ref_repos" ]] ||
  echo "fleet-build: * marks a checkout that is not the tree ci/fleet-refs.txt's ref names — another commit, or local modifications — so its verdict is not one about that ref. Off-ref checkouts:$off_ref_repos"

[[ -z "$failed_repos" ]] || exit 1
