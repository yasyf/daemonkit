#!/bin/sh
# daemonkit's real-machine launchd end-to-end run. A human invokes this; no CI
# job and no scripts/test.sh path ever does.
#
# It is out of the suite because it is irreversible, not because it is
# optional. launchd.loadOnce runs `launchctl enable` on every install and
# Remove never disables, so each label leaves a permanent row in the root-owned
# /var/db/com.apple.xpc.launchd/disabled.<uid>.plist that no launchctl verb
# deletes and uid <uid> cannot write. The labels are therefore a fixed set —
# the residue is O(labels), not O(runs).
#
# What only this run covers (nothing in the suite observes real launchd):
#   * ExitTimeOut firing on a daemon parked over half-done work
#   * a parked daemon that nothing but SIGKILL unparks
#   * a successor recovering after a kill mid-drain
#   * Remove leaving nothing behind
#   * the deploy verb chain — Install, Activate, Supersede, Uninstall, Reset —
#     against launchd, a launchd-spawned daemon, and the real process table
#
# Measured expectations (cc-notes 40764b0, 2026-08-01); a run far outside these
# is drift worth chasing, and the tests' own budgets are set against them:
#   ExitTimeOut 6.005-6.028s against a 6s setting; Ensure cold start 1.11-2.95s;
#   converged no-op 15-91ms; upgrade 8.18-9.04s (ThrottleInterval=10 bound);
#   Control.Drain 255-256ms; launchd.Remove 37-55ms; SIGKILL to gone 26ms;
#   post-kill recovery 9.66-9.69s; deploy Install 261ms, Activate 1.51-2.93s,
#   Supersede 0.95-1.97s, Uninstall 0.76-0.92s, Reset 0.59-0.66s.
set -eu

[ "$(uname)" = Darwin ] || { echo "e2e-launchd: darwin-only" >&2; exit 2; }
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
uid="$(id -u)"

# The one precondition, asserted rather than skipped past: without a
# bootstrappable GUI domain there is no LaunchAgent to install and the run is a
# failure, not a no-op.
/bin/launchctl print "gui/$uid" >/dev/null 2>&1 ||
  { echo "e2e-launchd: no bootstrappable gui/$uid domain — run this from a real login session" >&2; exit 2; }

labels="com.yasyf.daemonkit.e2e.ladder com.yasyf.daemonkit.e2e.wedgekill
com.yasyf.daemonkit.e2e.wedgepark com.yasyf.daemonkit.e2e.wedgeclose
com.yasyf.daemonkit.e2e.recover com.yasyf.daemonkit.e2e.deployagent
com.yasyf.daemonkit.e2e.deploy"

sweep() {
  for label in $labels; do
    /bin/launchctl bootout "gui/$uid/$label" >/dev/null 2>&1 || true
  done
}
trap sweep EXIT INT TERM
sweep

# Both halves go through scripts/test.sh for its RLIMIT_NPROC cap. _e2e/ is
# invisible to ./... because the go tool skips an underscore-prefixed
# directory; deploy's half reads the package-private deployment.requirement, so
# it cannot leave package deploy and carries the dke2e tag instead.
echo "e2e-launchd: gui/$uid launchd, $(sw_vers -productVersion), $(go version | cut -d' ' -f3)" >&2

status=0
scripts/test.sh -count=1 -timeout 900s -v ./_e2e || status=$?
scripts/test.sh -count=1 -timeout 900s -v -tags dke2e -run 'TestE2EDeploy' ./deploy || status=$?

if [ "$status" -ne 0 ]; then
  echo "e2e-launchd: FAILED (exit $status)" >&2
  exit "$status"
fi
echo "e2e-launchd: both halves green" >&2
