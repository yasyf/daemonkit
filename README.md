# ![daemonkit](docs/assets/readme-banner.webp)

**Daemons that spawn detached, trust by codesign, and drain on upgrade.** daemonkit is the daemon + signed-app pattern extracted from fusekit, cc-pool, cc-interact, and synckit — one Go module and one Swift package.

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/daemonkit/ci.yml?branch=main&label=ci)](https://github.com/yasyf/daemonkit/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](https://github.com/yasyf/daemonkit/blob/main/LICENSE)

## Get started

```bash
go get github.com/yasyf/daemonkit@latest
```

```text
go: added github.com/yasyf/daemonkit v<version>
```

<details>
<summary>Swift (SPM)</summary>

Add the package to your dependencies and link the `DaemonKit` library product into your app or helper target:

```swift
.package(url: "https://github.com/yasyf/daemonkit", branch: "main"),
```

</details>

Driving with an agent? Paste this:

```text
Add github.com/yasyf/daemonkit to my Go module (go get github.com/yasyf/daemonkit@latest),
check the package table in its README for what has landed, and replace this repo's
hand-rolled daemon lifecycle (spawn, singleton socket, version takeover) with
daemonkit's primitives.
```

---

## Use cases

### Hold a resource through `kill -9`

A daemon that owns kernel state — a FUSE mount, a keychain session — can't ride its parent's lifetime. Spawn it detached (new session, closed fds, a flock held for the listener's life) and the resource survives the CLI, the terminal, and the login session that started it. fusekit's mount holder has run this way in production since day one; `proc` is that machinery, extracted.

### Retire the old daemon when a new version ships

Two versions of the same daemon meet on one socket after every upgrade. The kit's answer: probe health and version first, never evict a tie, hand off when the peer advertises it, and treat "I couldn't tell" as "do nothing" — a busy daemon holding real resources is never killed for being older. Draining hands the old daemon's work to the new one instead of dropping it.

### Trust the process on the other end of the socket

A unix socket's permission bits say which UID connected, not which binary. On macOS, daemonkit's trust check resolves the peer's audit token to its code signature and pins team + signing identifier — same-team-but-different-tool is rejected, and a configured requirement with no verifier fails closed.

## The packages

One row per package; the Status column is the extraction's live state.

| Surface | Owns | Status |
|---|---|---|
| `proc` | Detached spawn, single-entrant sockets, process caps, orphan reaping | Porting from fusekit |
| `service` | LaunchAgent + keepalive-app reconciliation | Porting from fusekit |
| `version` | Release/dev version taxonomy, newest-wins skew | Porting from cc-interact |
| `paths` | Per-app dotdir conventions | Porting from cc-interact |
| `appgroup` | App Group container resolution, cgo-free | Porting from fusekit |
| `bundle` | Info.plist reads, stable `.app` path conventions | Designed |
| `wire` | LF-JSON framing, bounded serve loop, peer credentials | Designed |
| `trust` | Codesign peer verification (audit-token designated requirements) | Designed |
| `daemon` | Takeover ladder, skew watch, idle exit | Designed |
| `drain` | Drain-on-upgrade: journals, fences, dead-peer adoption | Designed |
| `supervise` | In-process child supervision with corroborated death | Porting from cc-orchestrate |
| `Sources/DaemonKit` | Swift: socket server, XPC trust pinning, login items, snapshot watching | Skeleton |

Status: v0.x, mid-extraction — the API lands package-by-package and stabilizes at v1.0.0 when the last donor repo flips.

Licensed under [PolyForm-Noncommercial-1.0.0](LICENSE).
