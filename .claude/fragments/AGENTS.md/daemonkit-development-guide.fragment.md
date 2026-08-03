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
├── docs/DESIGN.md    # the lifecycle-core design; §8 is the migration contract
├── docs/BUILD-ORDER.md      # how DESIGN.md gets built and shipped, phase by phase
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
| `artifact` | resolves a version-exact executable from a declarative descriptor, for the cc-family's one central "give me the binary that matches my version" primitive. | 16 | 2227 |
| `bundle` | reads a macOS .app's Info.plist and resolves the stable bundle paths a daemon installs to. | 5 | 198 |
| `deploy` | owns sealed installation, activation, supersession, and removal of one fixed signed application. | 16 | 5895 |
| `durable` | makes filesystem state survive crashes: atomic, fsynced publication of files and directory mutations, a strict validated JSON codec, and one bounded cross-process lock. | 9 | 1029 |
| `ghrelease` | queries GitHub for a repository's latest published release. | 2 | 170 |
| `launchd` | is the value-type model for one exact macOS user LaunchAgent and the stateless primitives that apply it. | 12 | 2521 |
| `paths` | owns the canonical state-directory layout under the user's home directory, resolved through the passwd database — never the caller's HOME or CLAUDE_CONFIG_DIR — so a sandboxed environment cannot relocate state. | 4 | 209 |
| `templates` | — | 2 | 218 |
| `version` | classifies and compares release and development builds for launcher-owned runtime settlement and release ordering. | 2 | 302 |
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
`go test`). **Never run bare `go test` on a real machine.** Several suites spawn
a child of `os.Executable()` and branch on an environment variable at the top of
`TestMain`. A branch that does not fire — a dropped env var, a renamed variable,
a helper that reaches `m.Run()` — makes the child re-run the whole suite and
re-enter the spawn: an exponential fork bomb that exhausts the process table and
freezes the machine. The harness caps the per-UID process count so a runaway
fails fast with `EAGAIN`. CI runs through the harness too. (See the 2026-06-24
mount-holder fork-storm incident, recorded in claude-pool's cc-notes:
`ccn doc show ef281ea`.)
