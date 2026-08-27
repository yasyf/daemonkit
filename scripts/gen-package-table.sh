#!/usr/bin/env bash
# Render the package table into the docs from the tree itself.
#
#   scripts/gen-package-table.sh            # rewrite every target in place
#   scripts/gen-package-table.sh --check    # exits non-zero on any drift
#
# AGENTS.md is a cc-guides render of .claude/fragments/AGENTS.md/ and is not a
# target: cc-guides fails any pull request that hand-edits a rendered artifact.
# Rendering the fragment is what puts the table there.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# The docs job runs on ubuntu, and a darwin-tagged package drops out of
# `go list ./...` entirely under any other GOOS — launchd would silently
# vanish from the rendered table.
export GOOS=darwin

targets=(
  "README.md"
  ".claude/fragments/AGENTS.md/daemonkit-development-guide.fragment.md"
)

begin="<!-- BEGIN GENERATED: package table (scripts/gen-package-table.sh) -->"
end="<!-- END GENERATED: package table -->"

table() {
  local module path name dir doc rel files lines summary
  module="$(go list -m)"
  echo "| Package | Owns | Files | Lines |"
  echo "|---|---|---|---|"
  go list -e -f '{{.ImportPath}}	{{.Name}}	{{.Dir}}	{{.Doc}}' ./... |
    while IFS=$'\t' read -r path name dir doc; do
      rel="${path#"$module"}"
      rel="${rel#/}"
      [[ -n "$rel" ]] || continue
      [[ "$name" != "main" ]] || continue
      case "$rel" in internal/* | ci/*) continue ;; esac
      files="$(find "$dir" -maxdepth 1 -name '*.go' | wc -l | tr -d ' ')"
      lines="$(cat "$dir"/*.go | wc -l | tr -d ' ')"
      summary="${doc#Package "$name" }"
      echo "| \`$rel\` | ${summary:-—} | $files | $lines |"
    done
}

render() {
  local file="$1" body="$2"
  grep -qF -- "$begin" "$file" || {
    echo "gen-package-table: $file has no $begin marker" >&2
    exit 2
  }
  grep -qF -- "$end" "$file" || {
    echo "gen-package-table: $file has no $end marker" >&2
    exit 2
  }
  awk -v body="$body" -v begin="$begin" -v end="$end" '
    index($0, begin) == 1 {
      print
      while ((getline line < body) > 0) print line
      close(body)
      skip = 1
      next
    }
    index($0, end) == 1 { skip = 0 }
    !skip { print }
  ' "$file"
}

body="$(mktemp)"
trap 'rm -f "$body"' EXIT
table >"$body"
status=0

for file in "${targets[@]}"; do
  rendered="$(render "$file" "$body")"
  case "${1:-}" in
    --check)
      if ! diff -u --label "$file" --label "$file (generated)" \
        "$file" <(printf '%s\n' "$rendered"); then
        status=1
      fi
      ;;
    "")
      printf '%s\n' "$rendered" >"$file"
      ;;
    *)
      echo "usage: gen-package-table.sh [--check]" >&2
      exit 2
      ;;
  esac
done

exit "$status"
