# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.12.0] - 2026-07-23

### Added

- The `artifact` package resolves a version-exact executable from a declarative
  descriptor (schema 1, dotslash dialect). `Store.Resolve` materializes a
  release binary into a content-addressed cache (hash-while-streaming,
  verify-before-rename), a Python tool into a version-addressed `uv tool` store,
  or a signed app through `deployment.Controller` (attest-only, with a
  `brew upgrade --cask` handoff, for TCC-bound installs). A dynamic version is
  refused for a release binary, which has no independent integrity gate.
  Resolution pins the exact descriptor version and never consults a latest
  release. `Store.CacheEntries` and `Store.RemoveCacheEntry` enumerate and prune
  the content cache for a garbage collector, surfacing even entries whose
  meta.json is damaged.
- `ghrelease.Latest` queries a repository's latest published release for
  self-update flows; artifact resolution never consults it.
- `version.Equal` reports exact-release equality, treating the TAG and BARE
  spellings of one release as equal but nothing looser.
- `proc.FileStamp` is a cross-process throttle: at most one `Claim` succeeds per
  window, resolving racing processes to a single winner.
- `proc.FileStore.UnsupportedSchema` opts a keyed store into archiving a wedged
  store aside and continuing fresh instead of failing closed;
  `proc.ArchiveUnsupportedStore` exposes the rename-aside for reuse.
  `service.ControllerConfig.UnsupportedSchema` threads the policy to the
  worker/process-record store.

### Changed

- Also carries the wire and runtime work landed on `main` since the 0.11.0
  preparation: the authenticated broker socket handoff, explicit peer-role
  session binding, typed runtime recovery and durable stop replay, sealed
  spawned sessions, and the lifecycle/workers/trust hard cut.

### Fixed

- The rendered application cask guards its stop hook on the installed binary
  being executable and removes a binary-less husk left by an aborted upgrade,
  so `brew upgrade` no longer aborts with exit 127 when Homebrew has already
  moved the app aside.

## [0.11.0] - 2026-07-23

### Added

- Go `wire.ServiceClient` and Swift `ServiceSocketClient` keep one lazy,
  exact-build session across service startup and replace it across drain,
  listener turnover, and takeover. Typed `runtime_starting` and
  `server_draining` response codes distinguish the only safe retry states.
- `service.StopBudget` and `StandardStopBudget` expose the exact identity,
  durable-tracking, child-settlement, parent-margin, and deferred-untrack
  phases that bound `Controller.StopRuntime`.

### Changed

- The application release template pins the shared staging and publication
  actions to their atomic-publication implementations.

### Fixed

- Graceful wire shutdown waits for an interrupted whole-frame write to settle,
  so admission cannot close ahead of a completed response during GoAway.
- LaunchAgent convergence enables the exact loaded job before bootstrap or
  kickstart and retries the complete bootout/bootstrap/enable sequence after a
  transient load failure. Disabled jobs are repaired instead of being accepted
  as converged.

## [0.10.0] - 2026-07-23

### Added

- `deployment.Controller` is the sole public signed-application publication
  workflow. `Deploy`, `Deactivate`, `Recover`, and `Status` operate on exact
  `Config` inputs, generation proofs, immutable service plans, and durable v1
  receipts and transactions under `.daemonkit-deployment`.

### Changed

- Service replacement is fenced by an exact operation, consumer-policy
  binding, and canonical plan. Completion and deployment acknowledgement are
  persisted independently, ordinary convergence is rejected while a fence is
  active, executable paths must be exact, and prior plan history survives when
  its executable is no longer resident.
- The application release template consumes an artifact-only reusable
  workflow, stages and publishes one caller-owned draft by exact release ID,
  and publishes a stable cask only after local and public-asset verification.

### Removed

- The public `fetch` package and its one-step installation API. This is a hard
  cut with no compatibility aliases, legacy readers, or fallback state paths.

## [0.9.0] - 2026-07-23

### Changed

- Durable daemon state now uses exact v1 identities and schema fingerprints.
  Drain journals, generation owners, strike accounting, process-reaping
  ledgers, service-controller state, fetch receipts, and fetch transactions
  reject missing, legacy, foreign, incomplete, or extended representations.
- `daemon.ExactStateFile` requires a caller-owned codec, identity, and
  fingerprint. Missing-state initialization is explicit; daemonkit no longer
  preserves unknown JSON while mutating state it owns.
- Swift `SnapshotWatcher` requires a caller-owned `SnapshotSchema` and
  `SnapshotCodec`, and reports exact identity, v1, and fingerprint skew before
  invoking the caller's payload decoder.

### Removed

- Permissive `daemon.StateFile`, its untyped mutation callback, and the Swift
  watcher's version-only schema check.
- Readers for pre-v1 or structurally incomplete daemonkit-owned state. Runtime
  state is rebuilt or migrated manually at the fleet hard cut.

## [0.8.1] - 2026-07-23

### Fixed

- Per-frame read and write deadlines are cleared under their serialized I/O
  ownership, so quiet duplex sessions survive beyond the frame timeout without
  losing explicit cleanup failures or completed-write state.
- Managed-process completion now publishes its exact exit result before
  readiness cancellation or worker-slot release, so an observable natural exit
  deterministically outranks concurrent readiness and shutdown signals.
- Session shutdown accepts a child that exits successfully when daemonkit closes
  its owned duplex connection instead of reporting that clean EOF as a failure.
- Stop-control children are durably pending before arming, and are released only
  when the committed authority still retains its complete fixed consumption
  window; exhausted commit reserve is durably revoked and reaped.

## [0.8.0] - 2026-07-23

### Changed

- `wire.NewRuntime` is the sole public daemon runtime composer. It atomically
  binds protected capacity, typed product observations, readiness, and the
  receipt-authenticated `daemon.control.stop` route, then returns only
  `*daemon.Runtime`.
- `service.Controller.StopRuntime` launches one exact hidden role, records its
  post-exec process identity and one-shot stop authority before release, and
  returns only after the child and target runtime settle or a bounded cleanup
  reaps the child.
- Ordinary clients carry only the exact business-suite build. Product readiness
  uses each product's typed runtime-health observation; launch ownership uses
  `service.Controller.Status` desired/applied/loaded/exact state.

### Removed

- Public `wire.LifecyclePeer`, `Server.RegisterLifecycle`,
  `ClientConfig.LifecycleBuild`, `daemon.Peer`, `daemon.EnsureCurrent`, and the
  public takeover runner/configuration.
- Go `wire/lifeproto`, the private lifecycle schema, and Swift `LifecycleWire`;
  there is no lifecycle control channel or ordinary-session fallback.

## [0.7.1] - 2026-07-23

### Changed

- `fetch.Release` requires the exact signed bundle marketing version, asset
  URL, and embedded SHA-256. The mutable checksum-side lookup and DR-only reuse
  contract are removed.
- `bundle.ShortVersion` reads both XML and binary property lists.

### Fixed

- Signed app installs serialize through a never-unlinked per-app lock, stage
  durably on the target filesystem, and publish real canonical `.app`
  directories with exclusive rename or atomic exchange.
- Strict v1 prepared/final receipts bind release and codesign policy to the
  canonical directory identity. Generation-fenced recovery completes an exact
  prepared transaction without an absence window and never reuses conflicting,
  corrupt, symlinked, or unattributed state.

## [0.7.0] - 2026-07-23

### Changed

- Swift socket client and server lifecycle operations are fully asynchronous;
  request cancellation and shutdown now expose exact settlement barriers.
- Session transport moves blocking descriptor work off cooperative executors
  and bounds admitted writes with explicit backpressure.

### Fixed

- Cancellation, handshake, writer, response acknowledgement, server start and
  stop, request deadline, and descriptor ownership races settle exactly once
  without leaking file descriptors or poisoning unrelated multiplexed calls.

## [0.6.1] - 2026-07-23

### Added

- New `fetch` package: downloads a signed macOS `.app` bundle from a GitHub
  release, verifies its SHA-256 against the release checksums and the unpacked
  bundle against a pinned codesign designated requirement (`codesign --verify
  -R`), and installs it into a caller-managed directory. It preserves the
  asset's build-time signature and never re-signs. Idempotent: an installed
  bundle that still satisfies the requirement is reused without re-downloading.

## [0.5.0] - 2026-07-22

### Added

- `service.Agent` gains `WatchPaths []string` (start the job when a listed path
  changes) and `StartCalendarInterval []CalendarInterval` (calendar-scheduled
  launch; launchd ORs the set), each rendered into the plist with the same
  exact-absolute-path and range validation as the existing keys. A
  `service.Daily(hour, minute)` helper covers the common once-a-day case.

## [0.4.2] - 2026-07-22

### Fixed

- Process-store and launchd-controller opens return an exact deadline error
  when a computed deadline is already expired, even before a custom context
  publishes cancellation.
- Disposable, managed-session, and terminal children cross a pool-owned durable
  tracking barrier before caller cancellation can settle them; pool shutdown
  remains able to interrupt tracking.
- Managed processes settle every surviving member of their dedicated session
  before completion or durable untracking, including when the leader exits
  before a backgrounded descendant.
- `supervise.ErrProcessExitedBeforeReadiness` identifies only an actual early
  managed-child exit while retaining its typed exit status when available.
- Swift client/server sessions use nonblocking descriptors with poll-backed
  whole-frame deadlines, so strict cooperative executors wait for readiness
  without spinning or surfacing transient `EAGAIN`.
- Untracked post-spawn cleanup is bounded and wrapper gate EOF exits directly,
  preventing signal failures from trapping startup cleanup indefinitely.

## [0.4.1] - 2026-07-21

### Added

- `supervise.Pool.StartSession` owns durable duplex child processes with exact
  readiness, bounded framed I/O, cancellation, process-group termination, and
  synchronous reaping.

### Fixed

- `supervise.SessionProcess.Wait` closes the child connection before returning
  the process result, so no caller can observe an exited session with a live
  transport.
- Swift session and shutdown-pipe writes suppress `SIGPIPE`, including during
  concurrent peer teardown.

## [0.3.4] - 2026-07-21

### Fixed

- `wire.AcceptedSession.Disconnected` now publishes only after transport intake
  ends and the session is canceled, across graceful GoAway, server stop, write
  failure, and context cancellation. Existing duplex sessions close on context
  cancellation, eliminating the handshake-to-registration shutdown gap.

## [0.3.3] - 2026-07-21

### Added

- `wire.AcceptedSession.Disconnected` closes as soon as transport intake ends,
  before admitted request settlement. Resource owners can publish peer loss
  immediately without weakening `Done` as the exact final-settlement barrier.

## [0.3.2] - 2026-07-21

### Fixed

- `service.CanonicalExecutable` resolves the current process to one exact regular executable without PATH lookup. Callers assign that resolved path explicitly; `service.Agent.Program` requires a nonempty exact path and retains strict no-symlink validation.

## [0.3.1] - 2026-07-21

### Fixed

- `daemon.EmbeddedProcess` now rejects nil and typed-nil factory runtimes before settlement, preserving any factory error without calling runtime methods through a nil value.

## [0.3.0] - 2026-07-21

### Removed

- Removed the Go `appgroup` package. This breaking change leaves App Group container resolution only in Swift `AppGroupContainer` inside the signed application topology.

## [0.2.0] - 2026-07-20

### Added

- `service.RestartPolicy` is required by `Agent` and `AppKeepAlive`, with direct launchd plist rendering for `RestartAlways`, `RestartOnFailure`, and `NoRestart`.
- `daemon.Runtime`: a config-validated lifecycle host composing admission, the session server, workers, and resources behind one `Run`, with `Health`/`Shutdown`/`Handoff`/`Close` and a 30s default shutdown timeout.
- `wire` v1 session transport: a length-prefixed binary frame codec (`DKS1`, protocol version 1, 4 MiB default frame cap) multiplexing request/response/cancel/event/stream exchanges per connection with explicit per-stream window credits and session-bound terminal acknowledgements; `Server.RegisterLifecycle` serves `daemon.Peer` lifecycle ops over it, and `LifecyclePeer` (with `UnixDialer`) is the client side.
- Swift `SessionTransport`: the exact-v1 counterpart to the Go codec, sharing the protocol version, frame cap, bounded delivery, per-stream flow control, and terminal acknowledgement contract.
- `wire.Server.ServeSession` and `wire.NewDuplexConn`: the exact v1 engine can own one daemonkit-authenticated spawned-process session over independent streams without a synthetic listener; spawned-parent identities remain ordinary and cannot authorize lifecycle traffic.
- `service.Controller`: durable, generation-fenced convergence for launchd agents and signed login apps, including typed bundle associations, verify-before-effect recovery, and exact stop acknowledgement.
- `supervise.Terminal`: durable resumable PTY sessions with bounded output, authenticated reconnects, terminal-intent settlement, process-group recovery, and exact owner handoff.
- `codeidentity` and `daemonrole`: typed executable identities and stable signed-app/daemon role classification for fail-closed launch and recovery decisions.
- Swift `AppGroupContainer`: entitlement-checked protected-container resolution with validated socket leaves; unsigned Go processes do not need to traverse App Group containers.

### Changed

- The v0.2 hard-cut runtime begins at epoch 1 across the `DKS1` session wire, lifecycle payloads, durable process ledger, and launchd controller state. Every surface requires exact equality; fresh state is initialized directly at epoch 1 with no compatibility reader or negotiation path.
- Replaced `proc.Flock`, `proc.TryLock`, and `proc.FlockHandle.Release` with the sole typed `proc.FileLockSpec` contract. Shared/exclusive mode and a positive acquisition deadline are mandatory, and the idempotent `FileLockHandle.Close` is the only release path.
- Replaced ticker-based `supervise.Supervisor` with a bounded process `Pool`. Disposable workers are durably identified before payload dispatch; long-lived `Process` handles cannot exec or report readiness before their process-group record is durable. Both paths synchronously reap through a fixed TERM/revalidate/KILL ladder, and startup recovery settles records from prior daemon generations.
- Accepted Go `wire.Peer` values now include the kernel PID/start identity captured at accept and can be matched directly against a managed process record; executable-name changes across `exec` no longer invalidate the same kernel process instance.
- `proc.Reaper` now tracks, revalidates, untracks, and reaps process-group records so worker recovery enumerates session members after a leader exits, while unresolved membership retains the forensic record and fails recovery.
- Process recovery now uses a boot-fenced keyed receipt ledger with monotonic delivery outcomes; ownership can move only through recorded, exact-generation handoff rather than mutable PID files or unproved liveness.
- Replaced Swift `PeerTrust`'s raw/optional requirement and unhardened bypass with one typed signed-peer policy: exact Developer ID Team + signing identifiers, mandatory Hardened Runtime and injection rejection, and closed consumer-owned entitlement predicates. Go `trust.Requirement` enforces the equivalent contract; consumers that share an App Group opt into its exact membership explicitly.
- `SocketServer` now requires an explicit `PeerTrust`; there is no production UID-only default. `LOCAL_PEERTOKEN` remains documented as query-time identity, so substitution by another process satisfying the same policy before admission is a residual macOS limitation.
- The one-JSON-per-line `wire.Framing` is replaced by the exact-v1 frame codec; `wire.Server` admits sessions over it and rejects legacy LF clients and oversized frames.

## [0.1.0] - 2026-07-18

Initial release: the fleet's detached-daemon + signed-app pattern as one Go module and one Swift SPM package.

### Added

- `proc`: detached spawning with launch-site strike gating (`Spawn.Gate`), single-entrant locks, flocks, backoff, a durable strike store with a parking ladder, reaper, nice, launch strategies, and boot-session process identity.
- `service`: LaunchAgent management, including `AppKeepAlive` with `AssociatedBundleIdentifiers`.
- `version`: version parsing and comparison with a dev-string taxonomy and stat-once binary versioning.
- `paths`, `bundle`, and `appgroup` (App Group containers via purego).
- `wire`: one-JSON-per-line framing with `MaxLine`, a concurrent socket `Server` with slots-based admission, shutdown drain, and a per-connection EUID floor; `Peer`, the timeout `Ladder`, and `wiretest`.
- `wire/lifeproto`: the lifecycle wire protocol generated from a single declarative schema that emits both the Go bindings and the Swift `LifecycleWire`, with one shared cross-language golden fixture and a CI check that regeneration is a no-op.
- `trust`: peer trust policy — the same-EUID floor always applies, a configured Developer ID requirement augments it and fails closed — with a darwin audit-token verifier that requires Hardened Runtime and rejects injection entitlements.
- `daemon`: takeover with socket-release and PID-exit wait modes, skew watch, idle exit, peer health, and durable state files.
- `drain`: crash-safe daemon handoff — durable canonical and per-generation journals serialized by one never-unlinked root lock, incarnation-bound generation handles, scoped truncation, an ownership-revalidating sweep, dead-generation adoption with identity re-proof, and strike accounting at the launch site. Hardened over five adversarial review rounds; the consumer contracts (idempotent yield, exclusive fence, gated spawn) are load-bearing godoc.
- `supervise`: process supervision.
- Swift `DaemonKit`: `SocketServer` with `PeerTrust` (audit-token codesign check over the same EUID-floor posture as Go `trust`), `SnapshotWatcher`, `LoginItem`, `RealHome`, `ReloadCoalescer`, and the generated `LifecycleWire`.
- `templates/release.yml.tmpl`: the caller workflow consumers use to release signed, notarized apps through the shared tap pipeline.

[Unreleased]: https://github.com/yasyf/daemonkit/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/yasyf/daemonkit/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/yasyf/daemonkit/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/yasyf/daemonkit/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/yasyf/daemonkit/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/yasyf/daemonkit/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/yasyf/daemonkit/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/yasyf/daemonkit/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/yasyf/daemonkit/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/yasyf/daemonkit/compare/v0.5.0...v0.6.1
[0.5.0]: https://github.com/yasyf/daemonkit/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/yasyf/daemonkit/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/yasyf/daemonkit/compare/v0.3.4...v0.4.1
[0.3.4]: https://github.com/yasyf/daemonkit/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/yasyf/daemonkit/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/yasyf/daemonkit/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/yasyf/daemonkit/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/yasyf/daemonkit/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/yasyf/daemonkit/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yasyf/daemonkit/releases/tag/v0.1.0
