#!/usr/bin/env bash
# daemonkit's real-machine signed-peer trust run: it mints .trust-fixtures with a
# real Developer ID Application identity and drives _e2e/trust against them. A
# human with that identity invokes this; no CI job and no scripts/test.sh path
# ever does.
#
# It is out of the suite because the identity does not exist here, not because
# the coverage is optional. `security find-identity -v -p codesigning` reports 0
# valid identities on this box, docs/BUILD-ORDER.md:43 records the same, and
# deploy/deploy_test.go:119 notes no runner can mint one. Ad-hoc signing yields
# TeamIdentifier=not set and flags 0x2(adhoc), while verifyToken
# (internal/trust/verify.go:40) requires a matching teamID and
# checkValidationCategory requires validation category 6 — so an ad-hoc stand-in
# would fail every accept case and pass every reject case on category rather than
# on the property it names.
#
# What only this run covers (the suite's trust tests reach the same verifyToken
# through a csops double, never through the kernel's own answer about a live
# process):
#   * PeerCredentials resolving a real audit token to the peer's real PID
#   * the accept path against a genuine Developer ID chain, hardened runtime,
#     and app-group entitlement
#   * the eleven rejections — wrong identifier, wrong team, ad-hoc, wrong app
#     group, unhardened, and each of the six injection entitlements — decided by
#     what the kernel reports rather than by a fabricated CodeDirectory
#   * AllowJIT admitting exactly the peer a strict requirement refuses
set -euo pipefail

[ "$(uname)" = Darwin ] || { echo "e2e-trust: darwin-only" >&2; exit 2; }
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# _e2e/trust/trust_test.go's fixtureTeam: the fixtures must carry this team or
# the accept cases cannot pass and the reject cases pass for the wrong reason.
required_team=SXKCTF23Q2

identities="$(security find-identity -v -p codesigning 2>&1 || true)"
identity="${DAEMONKIT_SIGN_IDENTITY:-}"
if [ -z "$identity" ]; then
  identity="$(sed -nE "s/^[[:space:]]*[0-9]+\) [0-9A-Fa-f]+ \"(Developer ID Application: .*\($required_team\))\".*/\1/p" <<<"$identities" | sed -n 1p)"
fi
if [ -z "$identity" ]; then
  {
    echo "e2e-trust: no Developer ID Application identity for team $required_team in the login keychain."
    echo "  security find-identity -v -p codesigning says:"
    sed 's/^/    /' <<<"$identities"
    echo "  The signed-peer tests need one and cannot be faked past: verifyToken matches the"
    echo "  teamID and checkValidationCategory requires category 6, both of which ad-hoc"
    echo "  signing (TeamIdentifier=not set, flags 0x2) fails."
    echo "  Mint one with the repo-bootstrap:apple-certs skill, or set DAEMONKIT_SIGN_IDENTITY"
    echo "  to a Developer ID Application identity for team $required_team."
  } >&2
  exit 2
fi

fixtures=(
  fixture-devid-a fixture-devid-b fixture-devid-wronggroup fixture-devid-unhardened
  fixture-devid-nolv fixture-devid-gta fixture-devid-jit fixture-devid-dyldenv
  fixture-devid-unsignedmem fixture-devid-nopageprot fixture-devid-noents
  fixture-devid-relaxed fixture-adhoc
)
fixture_group="group.com.yasyf.daemonkit.fixture"

outdir="$root/.trust-fixtures"
mkdir -p "$outdir"

base="$outdir/.fixture-base"
CGO_ENABLED=0 go build -o "$base" ./internal/trustfixture
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
codesign --force --sign "$identity" \
  --identifier com.yasyf.daemonkit.fixture-a --options runtime --timestamp --entitlements "$outdir/.ent-group.plist" \
  "$outdir/fixture-devid-a"
codesign --force --sign "$identity" \
  --identifier com.yasyf.daemonkit.fixture-b --options runtime --timestamp --entitlements "$outdir/.ent-group.plist" \
  "$outdir/fixture-devid-b"
ent "$outdir/.ent-wronggroup.plist" "group.com.yasyf.daemonkit.other"
codesign --force --sign "$identity" \
  --identifier com.yasyf.daemonkit.fixture-wronggroup --options runtime --timestamp --entitlements "$outdir/.ent-wronggroup.plist" \
  "$outdir/fixture-devid-wronggroup"
# Developer ID but NO hardened runtime — exercises the CS_RUNTIME rejection.
codesign --force --sign "$identity" \
  --identifier com.yasyf.daemonkit.fixture-unhardened --timestamp --entitlements "$outdir/.ent-group.plist" \
  "$outdir/fixture-devid-unhardened"
# Hardened but injection-permissive — exercises the LV/injection rejections.
ent "$outdir/.ent-nolv.plist" "$fixture_group" com.apple.security.cs.disable-library-validation
codesign --force --sign "$identity" \
  --identifier com.yasyf.daemonkit.fixture-nolv --options runtime --timestamp \
  --entitlements "$outdir/.ent-nolv.plist" \
  "$outdir/fixture-devid-nolv"
ent "$outdir/.ent-gta.plist" "$fixture_group" com.apple.security.get-task-allow
codesign --force --sign "$identity" \
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
  codesign --force --sign "$identity" \
    --identifier "com.yasyf.daemonkit.fixture-$name" --options runtime --timestamp \
    --entitlements "$outdir/.ent-$name.plist" \
    "$outdir/fixture-devid-$name"
  rm -f "$outdir/.ent-$name.plist"
done
# The escape hatch: allow-jit plus the app group, the exact peer AllowJIT
# admits and a strict requirement refuses.
ent "$outdir/.ent-relaxed.plist" "$fixture_group" com.apple.security.cs.allow-jit
codesign --force --sign "$identity" \
  --identifier com.yasyf.daemonkit.fixture-relaxed --options runtime --timestamp \
  --entitlements "$outdir/.ent-relaxed.plist" \
  "$outdir/fixture-devid-relaxed"
# No --entitlements at all: op 16 answers rc=0 with an all-zero header, which
# vacuously passes the six rejections and hard-fails every required entitlement.
codesign --force --sign "$identity" \
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
echo "e2e-trust: wrote ${#fixtures[@]} verified fixtures to $outdir"

signed_team="$(codesign --display --verbose=2 "$outdir/fixture-devid-a" 2>&1 | sed -nE 's/^TeamIdentifier=//p')"
[ "$signed_team" = "$required_team" ] ||
  { echo "e2e-trust: fixtures carry team $signed_team, _e2e/trust requires $required_team — every accept case would fail and every reject case would pass on the team mismatch alone" >&2; exit 2; }

echo "e2e-trust: $identity, $(sw_vers -productVersion), $(go version | cut -d' ' -f3)" >&2
scripts/test.sh -count=1 -timeout 300s -v ./_e2e/trust
echo "e2e-trust: green" >&2
