#!/usr/bin/env bash
# Builds the Go wire test server, then runs the Swift suite against it. The
# client suites dial this real internal/wire server over a unix socket, so the
# binary must exist and its path must reach the tests via the environment.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

CGO_ENABLED=0 go build -o .build/wire-test-server ./internal/wire/testserver
export DAEMONKIT_WIRE_TEST_SERVER="$root/.build/wire-test-server"

exec swift test "$@"
