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
.package(url: "https://github.com/yasyf/daemonkit", exact: "0.20.10"),
```

</details>

Driving with an agent? Paste this:

```text
Add github.com/yasyf/daemonkit to my Go module (go get github.com/yasyf/daemonkit@latest),
check the package table in its README for what has landed, and replace this repo's
hand-rolled daemon process ownership (spawn, exclusive socket, drain, stop control) with
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

Each row below is read out of the tree by `scripts/gen-package-table.sh` — the
package's own godoc summary and its file and line counts. CI regenerates the
table and fails the build when it drifts from disk, so a package that ships or
disappears cannot leave a stale row behind.

<!-- BEGIN GENERATED: package table (scripts/gen-package-table.sh) -->
| Package | Owns | Files | Lines |
|---|---|---|---|
| `artifact` | resolves a version-exact executable from a declarative descriptor, for the cc-family's one central "give me the binary that matches my version" primitive. | 16 | 2230 |
| `bundle` | reads a macOS .app's Info.plist and resolves the stable bundle paths a daemon installs to. | 5 | 200 |
| `codeidentity` | defines daemon-safe signed-code identity and opaque policy proofs. | 7 | 617 |
| `daemon` | is the consumer-agnostic process runtime for a detached daemon: exclusive listener ownership, readiness, ordered shutdown, skew observation, idle exit, and embedded-process coordination. | 19 | 4691 |
| `deployment` | owns sealed installation, activation, upgrade, and removal of one fixed signed application. | 18 | 4170 |
| `ghrelease` | queries GitHub for a repository's latest published release. | 2 | 170 |
| `paths` | owns the canonical state-directory layout under the user's home directory, resolved through the passwd database — never the caller's HOME or CLAUDE_CONFIG_DIR — so a sandboxed environment cannot relocate state. | 2 | 137 |
| `peer` | defines the OS-authenticated identity shared by transport and trust. | 4 | 175 |
| `proc` | holds exact durable process identity, ownership, and reaping. | 64 | 12716 |
| `service` | converges an exact durable set of macOS user LaunchAgents. | 28 | 10991 |
| `templates` | — | 2 | 218 |
| `trust` | verifies the code-signing identity of a connected unix-socket peer: a same-UID floor on every platform plus, on signed darwin builds, a designated requirement checked against the peer's audit token. | 12 | 2356 |
| `version` | classifies and compares release and development builds for launcher-owned runtime settlement and release ordering. | 2 | 302 |
| `wire` | is daemonkit's persistent multiplexed unix-socket transport. | 40 | 10407 |
| `wire/wiretest` | is the in-process harness for wire's transport and peer tests: short-path socket dirs, a real client/server pair, an injectable peer, and a manually-advanced clock mirroring proc's seam. | 2 | 232 |
| `worker` | runs bounded disposable commands under daemonkit process ownership. | 2 | 1934 |
<!-- END GENERATED: package table -->

Packages under `internal/` are module-private machinery and carry no compatibility
promise; they are left out of the table on purpose.

The Swift half lives in `Sources/DaemonKit`: typed static service runtimes,
generation-aware service clients, signed-process App Group resolution, peer trust
(a same-UID floor plus designated-requirement pinning), `SMAppService` login
items, and snapshot watching.

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

## The consumer trust contract

Peer verification runs in a disposable child of the daemon's own executable.
The one obligation a product keeps is dispatching that child verb at the top of
`main`, before argument parsing or any other output:

```go
func main() {
	if handled, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); handled {
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	// ordinary argument parsing and startup
}
```

daemonkit owns the other half of the exchange: the verifier worker lane is
sized from daemonkit's constants, so a product's worker pool budgets cannot
truncate a verdict, and `daemon.Runtime.Begin` proves one verifier exchange end
to end against the daemon's own identity before serving. A daemon whose
executable skips the dispatch refuses to start with
`daemon.ErrTrustVerifierProbe` instead of silently rejecting every peer as
untrusted.

Status: the module is pre-1.0 and hard-cut — no release carries a compatibility
shim for the one before it. Protocol and durable-state epochs begin at 1 with
exact equality; the API stabilizes at v1.0.0.

Licensed under [PolyForm-Noncommercial-1.0.0](LICENSE).
