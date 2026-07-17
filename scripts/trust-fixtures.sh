#!/usr/bin/env bash
# Build + sign the trust E2E peer fixtures (two Developer ID identifiers, one
# ad-hoc) into <outdir>. Needs DAEMONKIT_SIGN_IDENTITY; darwin-only.
set -euo pipefail

[ "$(uname)" = Darwin ] || { echo "trust-fixtures: darwin-only" >&2; exit 2; }
: "${DAEMONKIT_SIGN_IDENTITY:?set DAEMONKIT_SIGN_IDENTITY to a Developer ID Application identity (see: security find-identity -v -p codesigning)}"
outdir="${1:?usage: trust-fixtures.sh <outdir>}"
mkdir -p "$outdir"
root="$(cd "$(dirname "$0")/.." && pwd)"

base="$outdir/.fixture-base"
(cd "$root" && CGO_ENABLED=0 go build -o "$base" ./internal/trustfixture)
for f in fixture-devid-a fixture-devid-b fixture-adhoc; do
  cp "$base" "$outdir/$f"
done
rm -f "$base"

codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-a --options runtime --timestamp \
  "$outdir/fixture-devid-a"
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-b --options runtime --timestamp \
  "$outdir/fixture-devid-b"
codesign --force --sign - \
  --identifier com.yasyf.daemonkit.fixture-adhoc \
  "$outdir/fixture-adhoc"

for f in fixture-devid-a fixture-devid-b fixture-adhoc; do
  codesign --verify --strict "$outdir/$f"
  echo "$f:"
  codesign --display --verbose=2 "$outdir/$f" 2>&1 |
    sed -nE 's/^(Identifier=|TeamIdentifier=|Authority=)/  \1/p'
done
echo "trust-fixtures: wrote 3 verified fixtures to $outdir"
