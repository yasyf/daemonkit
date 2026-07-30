#!/usr/bin/env bash
# Build + sign the trust E2E peer fixtures (Developer ID identity/App Group
# variants, an unhardened peer, one peer per injection entitlement, an
# entitlement-free peer, the allow-jit escape hatch, and one ad-hoc peer) into
# <outdir>. Needs
# DAEMONKIT_SIGN_IDENTITY; darwin-only.
set -euo pipefail

[ "$(uname)" = Darwin ] || { echo "trust-fixtures: darwin-only" >&2; exit 2; }
: "${DAEMONKIT_SIGN_IDENTITY:?set DAEMONKIT_SIGN_IDENTITY to a Developer ID Application identity (see: security find-identity -v -p codesigning)}"
outdir="${1:?usage: trust-fixtures.sh <outdir>}"
mkdir -p "$outdir"
root="$(cd "$(dirname "$0")/.." && pwd)"

fixtures=(
  fixture-devid-a fixture-devid-b fixture-devid-wronggroup fixture-devid-unhardened
  fixture-devid-nolv fixture-devid-gta fixture-devid-jit fixture-devid-dyldenv
  fixture-devid-unsignedmem fixture-devid-nopageprot fixture-devid-noents
  fixture-devid-relaxed fixture-adhoc
)
fixture_group="group.com.yasyf.daemonkit.fixture"

base="$outdir/.fixture-base"
(cd "$root" && CGO_ENABLED=0 go build -o "$base" ./internal/trustfixture)
for f in "${fixtures[@]}"; do
  cp "$base" "$outdir/$f"
done
rm -f "$base"

ent() {
  local path="$1" group="$2" extra="${3:-}"
  cat > "$path" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>com.apple.security.application-groups</key><array><string>$group</string></array>
EOF
  if [ -n "$extra" ]; then
    printf '<key>%s</key><true/>\n' "$extra" >> "$path"
  fi
  printf '</dict></plist>\n' >> "$path"
}

ent "$outdir/.ent-group.plist" "$fixture_group"
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-a --options runtime --timestamp --entitlements "$outdir/.ent-group.plist" \
  "$outdir/fixture-devid-a"
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-b --options runtime --timestamp --entitlements "$outdir/.ent-group.plist" \
  "$outdir/fixture-devid-b"
ent "$outdir/.ent-wronggroup.plist" "group.com.yasyf.daemonkit.other"
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-wronggroup --options runtime --timestamp --entitlements "$outdir/.ent-wronggroup.plist" \
  "$outdir/fixture-devid-wronggroup"
# Developer ID but NO hardened runtime — exercises the CS_RUNTIME rejection.
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-unhardened --timestamp --entitlements "$outdir/.ent-group.plist" \
  "$outdir/fixture-devid-unhardened"
# Hardened but injection-permissive — exercises the LV/injection rejections.
ent "$outdir/.ent-nolv.plist" "$fixture_group" com.apple.security.cs.disable-library-validation
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-nolv --options runtime --timestamp \
  --entitlements "$outdir/.ent-nolv.plist" \
  "$outdir/fixture-devid-nolv"
ent "$outdir/.ent-gta.plist" "$fixture_group" com.apple.security.get-task-allow
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-gta --options runtime --timestamp \
  --entitlements "$outdir/.ent-gta.plist" \
  "$outdir/fixture-devid-gta"
# One fixture per injection entitlement. allow-jit and
# allow-dyld-environment-variables are the two the status word cannot see
# (measured 0x22011311, bit-identical to a clean hardened binary), so the
# entitlement blob is their only oracle; allow-unsigned-executable-memory and
# disable-executable-page-protection clear CS_HARD and CS_ENFORCEMENT
# (measured 0x22010211) and are already denied by the posture floor.
for injection in \
  jit:com.apple.security.cs.allow-jit \
  dyldenv:com.apple.security.cs.allow-dyld-environment-variables \
  unsignedmem:com.apple.security.cs.allow-unsigned-executable-memory \
  nopageprot:com.apple.security.cs.disable-executable-page-protection; do
  name="${injection%%:*}"
  entitlement="${injection#*:}"
  ent "$outdir/.ent-$name.plist" "$fixture_group" "$entitlement"
  codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
    --identifier "com.yasyf.daemonkit.fixture-$name" --options runtime --timestamp \
    --entitlements "$outdir/.ent-$name.plist" \
    "$outdir/fixture-devid-$name"
  rm -f "$outdir/.ent-$name.plist"
done
# The escape hatch: allow-jit plus the app group, the exact peer AllowJIT
# admits and a strict requirement refuses.
ent "$outdir/.ent-relaxed.plist" "$fixture_group" com.apple.security.cs.allow-jit
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-relaxed --options runtime --timestamp \
  --entitlements "$outdir/.ent-relaxed.plist" \
  "$outdir/fixture-devid-relaxed"
# No --entitlements at all: op 16 answers rc=0 with an all-zero header, which
# vacuously passes the six rejections and hard-fails every required entitlement.
codesign --force --sign "$DAEMONKIT_SIGN_IDENTITY" \
  --identifier com.yasyf.daemonkit.fixture-noents --options runtime --timestamp \
  "$outdir/fixture-devid-noents"
rm -f "$outdir/.ent-group.plist" "$outdir/.ent-wronggroup.plist" "$outdir/.ent-nolv.plist" \
  "$outdir/.ent-gta.plist" "$outdir/.ent-relaxed.plist"
codesign --force --sign - \
  --identifier com.yasyf.daemonkit.fixture-adhoc \
  "$outdir/fixture-adhoc"

for f in "${fixtures[@]}"; do
  codesign --verify --strict "$outdir/$f"
  echo "$f:"
  codesign --display --verbose=2 "$outdir/$f" 2>&1 |
    sed -nE 's/^(Identifier=|TeamIdentifier=|Authority=)/  \1/p'
done
echo "trust-fixtures: wrote ${#fixtures[@]} verified fixtures to $outdir"
