#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

fail() {
  echo "release-check: $*" >&2
  exit 1
}

require_source_tree() {
  [[ "$(sed -n 's/^module //p' go.mod)" == "github.com/yasyf/daemonkit" ]] ||
    fail "go.mod must declare github.com/yasyf/daemonkit"
  if grep -Eq '^(replace|exclude)[[:space:]]' go.mod; then
    fail "release go.mod cannot contain replace or exclude directives"
  fi
  grep -Fq 'name: "daemonkit"' Package.swift || fail "Package.swift must declare package daemonkit"
  grep -Fq '.library(name: "DaemonKit"' Package.swift || fail "Package.swift must export DaemonKit"
  [[ "$(grep -Ec '^[[:space:]]*\.library\(' Package.swift)" == "1" ]] ||
    fail "Package.swift must export exactly one library product"
  if grep -Eq '\.executable(Target)?\(' Package.swift; then
    fail "daemonkit must remain a library-only Swift package"
  fi

  local residue
  local -a forbidden=(cmd .github/cask Casks Formula .goreleaser.yml .goreleaser.yaml)
  for residue in "${forbidden[@]}"; do
    if [[ -n "$(git ls-files -- "$residue" "$residue/**")" ]]; then
      fail "binary release residue remains tracked at $residue"
    fi
  done
  if grep -Eq 'release-app|sign-notarize|render-formula|MACOS_(SIGN|NOTARY)|HOMEBREW_TAP_TOKEN' \
    .github/workflows/release.yml; then
    fail "daemonkit itself is source-only; app signing remains a consumer-template concern"
  fi
}

require_release_tag() {
  local tag="$1"
  [[ "$tag" =~ ^v0\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    fail "tag must be a stable v0 semantic version"
  local version="${tag#v}"
  local latest
  latest="$(awk '/^## \[[0-9]+\.[0-9]+\.[0-9]+\] - / {sub(/^## \[/, ""); sub(/\].*$/, ""); print; exit}' CHANGELOG.md)"
  [[ "$latest" == "$version" ]] || fail "tag $tag does not match latest changelog release $latest"
  [[ "$(grep -Ec "^## \\[$version\\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" CHANGELOG.md)" == "1" ]] ||
    fail "CHANGELOG.md must contain one dated $version release heading"
  awk '
    /^## \[Unreleased\]$/ {unreleased = 1; next}
    unreleased && /^## \[/ {exit}
    unreleased && NF {exit 1}
  ' CHANGELOG.md || fail "the Unreleased section must be empty when cutting $tag"
  grep -Fqx "[Unreleased]: https://github.com/yasyf/daemonkit/compare/$tag...HEAD" CHANGELOG.md ||
    fail "Unreleased comparison link must start at $tag"
  grep -Eq "^\[$version\]: https://github.com/yasyf/daemonkit/(compare/[^ ]+\.\.\.$tag|releases/tag/$tag)$" CHANGELOG.md ||
    fail "CHANGELOG.md is missing the exact $tag release link"
}

write_notes() {
  local tag="$1"
  local output="$2"
  local version="${tag#v}"
  awk -v heading="## [$version] - " '
    index($0, heading) == 1 {inside = 1; next}
    inside && /^## \[/ {exit}
    inside {print}
  ' CHANGELOG.md >"$output"
  [[ -s "$output" ]] || fail "release notes for $tag are empty"
}

require_source_tree

case "${1:-}" in
  --tree)
    [[ "$#" == 1 ]] || fail "usage: release-check.sh --tree"
    ;;
  --notes)
    [[ "$#" == 3 ]] || fail "usage: release-check.sh --notes OUTPUT v0.X.Y"
    require_release_tag "$3"
    write_notes "$3" "$2"
    ;;
  "")
    fail "usage: release-check.sh --tree | [--notes OUTPUT] v0.X.Y"
    ;;
  *)
    [[ "$#" == 1 ]] || fail "usage: release-check.sh v0.X.Y"
    require_release_tag "$1"
    ;;
esac
