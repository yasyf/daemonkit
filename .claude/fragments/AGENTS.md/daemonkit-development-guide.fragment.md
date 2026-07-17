# daemonkit Development Guide

Daemons that spawn detached, trust by codesign, and drain on upgrade.

## Repository Structure

```
daemonkit/
├── doc.go            # module godoc; the Go packages land beside it at the root
│                     #   (proc/, service/, version/, paths/, appgroup/, bundle/,
│                     #   wire/, trust/, daemon/, drain/, supervise/)
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

daemonkit is **extracted from the fleet** — fusekit (`proc/`, `service/`,
`appgroup/`), cc-interact (`version/`, `paths/`), cc-orchestrate (`supervise/`),
cc-pool, synckit, captain-hook, and authkit are the donors and the consumers.
When porting code in, `cp` the file then edit in place — never recreate from
scratch — so lifecycle bytes and reviewable diffs stay identical. daemonkit
imports nothing from the fleet; dev wiring across repos uses an untracked
`go.work`, never a committed `replace`.

## Testing — always via `scripts/test.sh`

Run Go tests with `scripts/test.sh ./...` (a `ulimit -u` wrapper around
`go test`). **Never run bare `go test` on a real machine.** The spawn path
(`proc.Spawn`) materializes and execs `os.Executable()`; if that executable is a
*test* binary, Go's flag parser stops at the non-flag subcommand and
`testing.Main` re-runs the whole suite, which re-enters the spawn — an
exponential fork bomb that exhausts the process table and freezes the machine.
The harness caps the per-UID process count so a runaway fails fast with
`EAGAIN`. CI runs through the harness too. (See the 2026-06-24 mount-holder
fork-storm incident, recorded in cc-pool's cc-notes: `ccn doc show ef281ea`.)
