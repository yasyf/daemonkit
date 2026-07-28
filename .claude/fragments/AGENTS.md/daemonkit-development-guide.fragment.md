# daemonkit Development Guide

Daemons that spawn detached, trust by codesign, and drain on upgrade.

## Repository Structure

```
daemonkit/
├── doc.go            # module godoc; the Go packages land beside it at the root
├── Package.swift     # SPM manifest — stays at the repo root (SPM requires it)
├── Sources/DaemonKit/       # the Swift half: socket serving, peer trust,
│                            #   launchd registration, snapshot watching
├── Sources/STYLEGUIDE.md    # Swift style rules (root STYLEGUIDE.md is Go)
├── Tests/DaemonKitTests/    # Swift Testing suite
├── scripts/test.sh   # the ONLY way to run Go tests — see Testing below
├── .github/workflows/ci.yml # Go vet/test/-race via the harness + pure build
│                            #   + the macos-26 Swift job
├── AGENTS.md         # This file — shared conventions
└── README.md         # Project overview
```

The Go packages sit beside `doc.go` at the module root. Read them out of the
tree, never out of a list someone typed — `scripts/gen-package-table.sh` writes
the table below and `--check` gates it in CI, so an edit made here by hand is
reverted rather than trusted.

<!-- BEGIN GENERATED: package table (scripts/gen-package-table.sh) -->
| Package | Owns | Files | Lines |
|---|---|---|---|
| `artifact` | resolves a version-exact executable from a declarative descriptor, for the cc-family's one central "give me the binary that matches my version" primitive. | 16 | 2230 |
| `bundle` | reads a macOS .app's Info.plist and resolves the stable bundle paths a daemon installs to. | 5 | 200 |
| `codeidentity` | defines daemon-safe signed-code identity and opaque policy proofs. | 7 | 671 |
| `daemon` | is the consumer-agnostic process runtime for a detached daemon: exclusive listener ownership, readiness, ordered shutdown, skew observation, idle exit, and embedded-process coordination. | 19 | 4691 |
| `deployment` | owns sealed installation, activation, upgrade, and removal of one fixed signed application. | 18 | 4170 |
| `ghrelease` | queries GitHub for a repository's latest published release. | 2 | 170 |
| `paths` | owns the canonical state-directory layout under the user's home directory, resolved through the passwd database — never the caller's HOME or CLAUDE_CONFIG_DIR — so a sandboxed environment cannot relocate state. | 2 | 137 |
| `peer` | defines the OS-authenticated identity shared by transport and trust. | 4 | 175 |
| `proc` | holds exact durable process identity, ownership, and reaping. | 64 | 12340 |
| `service` | converges an exact durable set of macOS user LaunchAgents. | 28 | 10596 |
| `templates` | — | 2 | 218 |
| `trust` | verifies the code-signing identity of a connected unix-socket peer: a same-UID floor on every platform plus, on signed darwin builds, a designated requirement checked against the peer's audit token. | 12 | 2341 |
| `version` | classifies and compares release and development builds for launcher-owned runtime settlement and release ordering. | 2 | 302 |
| `wire` | is daemonkit's persistent multiplexed unix-socket transport. | 39 | 10278 |
| `wire/wiretest` | is the in-process harness for wire's transport and peer tests: short-path socket dirs, a real client/server pair, an injectable peer, and a manually-advanced clock mirroring proc's seam. | 2 | 232 |
| `worker` | runs bounded disposable commands under daemonkit process ownership. | 2 | 1934 |
<!-- END GENERATED: package table -->

Everything under `internal/` is module-private machinery no consumer may import,
and is left out of the table on purpose.

daemonkit is **extracted from the fleet** — fusekit (`proc/`, `service/`),
cc-interact (`version/`, `paths/`), cc-orchestrate (`worker/`),
claude-pool (module `github.com/yasyf/cc-pool`), synckit, captain-hook, and
authkit are the donors and the consumers.
When porting code in, `cp` the file then edit in place — never recreate from
scratch — so lifecycle bytes and reviewable diffs stay identical. daemonkit
imports nothing from the fleet; dev wiring across repos uses an untracked
`go.work`, never a committed `replace`.

## Testing — always via `scripts/test.sh`

Run Go tests with `scripts/test.sh ./...` (a `ulimit -u` wrapper around
`go test`). **Never run bare `go test` on a real machine.** The trust path
spawns a disposable child of `os.Executable()` — `daemon.Runtime` captures that
path at construction and expects `trust.RunVerifierChild` to intercept the child
verb at the top of `main`. A *test* binary has no such intercept: Go's flag
parser stops at the non-flag subcommand, `testing.Main` re-runs the whole suite,
and the suite re-enters the spawn — an exponential fork bomb that exhausts the
process table and freezes the machine.
The harness caps the per-UID process count so a runaway fails fast with
`EAGAIN`. CI runs through the harness too. (See the 2026-06-24 mount-holder
fork-storm incident, recorded in claude-pool's cc-notes: `ccn doc show ef281ea`.)
