# ![daemonkit](docs/assets/readme-banner.webp)

**Daemons that spawn detached, trust by codesign, and drain on upgrade.** daemonkit is the daemon + signed-app pattern extracted from fusekit, claude-pool, cc-interact, and synckit, shipped as one Go module and one Swift package.

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
.package(url: "https://github.com/yasyf/daemonkit", exact: "0.15.0"),
```

</details>

Driving with an agent? Paste this:

```text
Add github.com/yasyf/daemonkit to my Go module (go get github.com/yasyf/daemonkit@latest),
check the package table in its README for what has landed, and replace this repo's
hand-rolled daemon process ownership (spawn, singleton socket, drain, stop control) with
daemonkit's primitives.
```

---

## Use cases

### Hold a resource through `kill -9`

A daemon that owns kernel state — a FUSE mount, a keychain session — can't ride its parent's lifetime. Spawn it detached (new session, closed fds, a flock held for the listener's life) and the resource survives the CLI, the terminal, and the login session that started it. fusekit's mount holder has run this way in production since day one; `proc` is that machinery, extracted.

### Retire the old daemon when a new version ships

The service controller observes the product's typed runtime health, starts an
exact hidden role from the fixed executable, and durably authorizes that one
process to call `daemon.control.stop`. The runtime validates and consumes the
one-shot authority before draining. The controller waits for both the endpoint
and exact process identity to settle before it starts the replacement.

### Trust the process on the other end of the socket

A unix socket's permission bits say which UID connected, not which binary. On macOS, daemonkit's trust check resolves the peer's audit token to its code signature and pins team + signing identifier — same-team-but-different-tool is rejected, and a configured requirement with no verifier fails closed.

### Own a typed Swift service generation

`StaticSessionServiceRuntime<Request, Response>` binds one exact Unix socket,
checks the peer's effective UID and role before admission, publishes Ready only
after the typed route exists, and drains every accepted request before unlinking
the listener. Products provide their codec, operation, tenant, handler, and
resource limits; daemonkit owns lifecycle controls, framing, backpressure,
cancellation, settlement, and authenticated successor following.

## The packages

One row per package; the Status column is each surface's live state.

| Surface | Owns | Status |
|---|---|---|
| `proc` | Detached spawn, single-entrant sockets, process caps, child reaping, exact epoch-1 durable process ledger | Landed |
| `service` | Exact desired/applied/loaded LaunchAgent state with typed restart policy, durable convergence, explicit runtime stop budgets, and signed-app stop ownership | Landed |
| `version` | Release/dev version taxonomy, newest-wins skew | Landed |
| `paths` | The `~/<app>` state layout: daemon socket, HTTP handshake file, per-subject artifacts, start lock, sqlite database, daemon log, turn-snapshot scratch dirs | Landed |
| `bundle` | Info.plist reads, stable `.app` path conventions | Landed |
| `deployment` | Exact local staging, installation, activation, upgrade, deactivation, rollback, and uninstall of a fixed signed app | Landed |
| `wire` | Exact-v1 persistent business transport, generation-aware service clients, typed product observations, receipt-authenticated stop control, and the sole composed daemon runtime constructor | Landed |
| `trust` | Codesign peer verification (audit-token designated requirements) | Landed |
| `daemon` | Opaque process runtime, readiness, ordered shutdown, skew observation, embedded processes, and idle exit | Landed |
| `drain` | Drain-on-upgrade: journals, fences, dead-peer adoption | Landed |
| `supervise` | Bounded disposable workers and managed long-lived process handles with pre-exec durable identity, readiness gating, cancellation settlement, and cross-generation orphan recovery | Landed |
| `Sources/DaemonKit` | Swift: typed static service runtimes, generation-aware service clients, signed-process App Group resolution, peer trust (same-UID floor + designated-requirement pinning), `SMAppService` login items, snapshot watching | Landed |

The LaunchAgents `service` writes use no socket activation — the daemon binds and flocks its own socket (`proc`); launchd only keeps the process alive. Every `Agent` and `AppKeepAlive` selects `RestartAlways`, `RestartOnFailure`, or `NoRestart`; the policy is rendered directly into the launchd plist. On the Swift side, `DaemonKit` reconciles `SMAppService` login items (opening the Login Items settings pane when the item needs approval), watches snapshot directories, and rides the signed `.app` bundle for a stable bundle + TCC identity.

`BrokerSocketBridge` requires a lifecycle `RuntimeClientConfiguration` and a
distinct, nonempty `handoffRole`. The lifecycle session is limited to receipt
and readiness preflight. A separate persistent handoff session sends only
`daemon.broker-handoff.v1`, pinned to the exact ready-runtime receipt. There is
no single-role or compatibility initializer.

`deployment.Controller` never downloads an application. Packaging supplies one
exact local signed `.app` resource. `ApplyInstalledCandidate` copies it into a
private controller-owned stage, verifies its bundle digest, version, signature,
identity, and file generation, then owns first install or atomic upgrade through
activation and rollback. Its opaque `CandidatePlan` is validated against the
packaged app, persisted with relative program paths, and rebound and revalidated
only after the exact candidate occupies the canonical path.
`DeactivateCurrentInstalled` derives prior build,
policy, plan, and generation only from sealed state. `UninstallCurrentInstalled`
owns quiescence and crash-recoverable namespace removal. Consumers never write a
candidate path, swap the installed app, inspect private JSON, or remove the
canonical app. Exact v1 receipts, service state, and locks live beside the app
under `.daemonkit-deployment/<Product>`.

Status: v0.17.2 is the hard-cut release line. Protocol and durable-state epochs
begin at 1 with exact equality; the API stabilizes at v1.0.0.

Licensed under [PolyForm-Noncommercial-1.0.0](LICENSE).
