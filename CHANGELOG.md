# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `service.RestartPolicy` is required by `Agent` and `AppKeepAlive`, with direct launchd plist rendering for `RestartAlways`, `RestartOnFailure`, and `NoRestart`.
- `daemon.Runtime`: a config-validated lifecycle host composing admission, the session server, workers, and resources behind one `Run`, with `Health`/`Shutdown`/`Handoff`/`Close` and a 30s default shutdown timeout.
- `wire` v3 session transport: a length-prefixed binary frame codec (`DKS3`, protocol version 3, 4 MiB default frame cap) multiplexing request/response/cancel/event/stream exchanges per connection with explicit per-stream window credits; `Server.RegisterLifecycle` serves `daemon.Peer` lifecycle ops over it, and `LifecyclePeer` (with `UnixDialer`) is the client side.
- Swift `SessionTransport`: the exact-v3 counterpart to the Go codec, sharing the protocol version, frame cap, bounded delivery, and per-stream flow control.

### Changed

- Replaced `proc.Flock`, `proc.TryLock`, and `proc.FlockHandle.Release` with the sole typed `proc.FileLockSpec` contract. Shared/exclusive mode and a positive acquisition deadline are mandatory, and the idempotent `FileLockHandle.Close` is the only release path.
- Replaced ticker-based `supervise.Supervisor` with a bounded disposable worker-process `Pool`. Workers are placed in dedicated sessions and process groups, durably identified before payload dispatch, synchronously reaped, and canceled through a fixed TERM/revalidate/KILL ladder; `Close`, `Cancel`, and safety-settled `Wait` define pool shutdown.
- `proc.Reaper` now tracks, revalidates, untracks, and reaps process-group records so worker recovery enumerates session members after a leader exits, while unresolved membership retains the forensic record and fails recovery.
- Replaced Swift `PeerTrust`'s raw/optional requirement and unhardened bypass with one typed signed-peer policy: exact Developer ID Team + signing identifiers, mandatory Hardened Runtime and injection rejection, and closed consumer-owned entitlement predicates. Go `trust.Requirement` enforces the equivalent contract; consumers that share an App Group opt into its exact membership explicitly.
- `SocketServer` now requires an explicit `PeerTrust`; there is no production UID-only default. `LOCAL_PEERTOKEN` remains documented as query-time identity, so substitution by another process satisfying the same policy before admission is a residual macOS limitation.
- The one-JSON-per-line `wire.Framing` is replaced by the v2 frame codec; `wire.Server` admits sessions over it and rejects legacy LF clients and oversized frames.

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

[Unreleased]: https://github.com/yasyf/daemonkit/compare/v0.1.0...main
[0.1.0]: https://github.com/yasyf/daemonkit/releases/tag/v0.1.0
