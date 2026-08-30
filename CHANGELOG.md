# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed

- The legacy bbolt sweep in `internal/proc`, and the `go.etcd.io/bbolt`
  dependency with it. `Store.Recover` no longer takes a `legacy []string`
  parameter: both call sites already passed `nil`, so every machine the fleet
  runs settled its prior generation from the JSON record alone. The dependency
  cone loses bbolt entirely, which is what DESIGN §8 said the cut was for.
- The pre-rename deployment-metadata migration in `deploy`. A `.daemonkit-deploy`
  tree is the only shape any install has, so `archiveLegacy` had no
  `.daemonkit-deployment` tree left to move aside and `layout` no longer carries
  a path to one.
- `Client.Stop`'s markerless-plist fallback. Every plist daemonkit writes has
  carried the ownership marker since v0.21, so `removeAgent` is
  `launchd.Remove` and nothing else — a plist that fails its ownership proof is
  now refused rather than deleted through the escape.
  `launchd.RemoveUnmarked` and `launchd.ErrMarked` stay exported: captain-hook
  calls the verb directly to sweep its own pre-v0.21 labels.
- `docs/MIGRATING-0.21.md`, the v0.20.x repin guide. The migration it describes
  closed with v0.21, and nothing referenced the file.

## [0.23.0] - 2026-08-26

### Added

- `AgentPaths` — the Swift half of `paths.Agent`. The Swift package shipped no
  label-to-socket derivation at all, so captain-hook's helper hand-wrote the
  formula in its own `HelperPaths.hostSocket` and pinned an older daemonkit to
  keep that copy agreeing with the Go half. Under that arrangement a path move
  on the Go side breaks the Swift side silently, at runtime, on a socket
  nothing can dial. `AgentPaths(label:).socket()` derives it once and refuses a
  path past `sun_path` the way `paths.Socket` does, and
  `paths/testdata/agent-layout.json` is one golden both halves assert against,
  so the two cannot drift without a test saying so.

### Changed

- **Breaking.** Daemon state moved again, from `~/.daemonkit/agents/<Label>` to
  `~/.daemonkit/a/<Label>`. v0.22.0 spent 18 bytes of the 103 darwin's
  `sun_path` leaves a socket path, and cc-orchestrate's pty daemons landed on
  104 — one byte over, `paths.Socket` refusing every spawn under a long account
  name. Their label already carries 64 bits of a nonce hash and nothing else,
  precisely so the socket clears the limit, so the byte had to come out of the
  root. The directory is one letter because every byte it spends comes out of
  the label budget every consumer shares. As in v0.22.0 there is no migration
  and no fallback read: old state is abandoned where it lies and a daemon comes
  up fresh, so delete `~/.daemonkit/agents` by hand once every consumer is on
  this release.

## [0.22.0] - 2026-08-17

### Added

- `Ctx.ServeChannel` serves the fd-3 channel a `ChannelHandoff` child inherits,
  over two verbs. `mint` hands the child a fresh pre-connected session end;
  `adopt` takes a connection the child accepted and admits it through the same
  full-trust path an accepted connection already takes. A child that takes its
  sessions this way never learns or dials a socket path, so there is no path a
  same-UID process can bind first and no constant for a consumer to drift from
  — the failure mode that a socket path in a child's argv invites. A minted
  session is business-lane only, carries a per-mint nonce compared in constant
  time, and refuses the drain preamble outright, because a spawned peer must
  never inherit authority to drain the daemon that spawned it.

- `Spawn` pins its child's code identity while the child is still suspended,
  before `Verify` releases it, and `Child` carries that audit token. A
  suspended child cannot exit, be reaped, or surrender its PID, so the pin
  names the execution that was spawned rather than whoever holds that PID by
  the time anyone asks — the same guarantee `Control` assembles at attach, in
  the one place where a spawn makes it free.

- `RemoteError` carries the daemon's own terminal failure to the caller, so a
  failure the daemon reported can be recovered with `errors.As` instead of
  matched by its text.

### Changed

- A business session's terminal failure is a typed error rather than a
  flattened string. `Response.Err` crossed the wire as text and was rendered
  with `%s`, so a caller could not tell a deadline the daemon hit from its own
  expired context — the same words for two different facts, and nothing but
  string matching to separate them. The serving side now stamps a response
  code that the client resolves through the one classification table
  `RejectionError` already used, so `errors.Is(err, context.DeadlineExceeded)`
  answers for the daemon's own deadline. `Client.Health` takes the same
  treatment.

- **Breaking.** Every daemon's private state moved from `~/<Label>` to
  `~/.daemonkit/agents/<Label>`. A daemon used to cut a non-hidden directory
  straight into the user's home for its socket, owner record, and start lock —
  one per label, thirteen of them on a fleet machine — beside the hidden
  `~/.daemonkit` daemonkit already owns for `bin/`, `cache/`, `tools/`, and
  `locks/`. `paths.Agent(label)` is the new constructor for that layout and
  `paths.Socket` routes through it; `paths.Paths{App: …}` keeps its `~/<App>`
  meaning for an application that owns its home directory outright. There is no
  migration and no fallback read: old state is abandoned where it lies and a
  daemon comes up fresh, so delete `~/<Label>` by hand once every consumer is
  on this release. The socket path grows 18 bytes, which darwin's 104-byte
  `sun_path` still fits — `paths.Socket` returns its `*SocketPathError` for the
  combinations of long home and long label that no longer do.

## [0.21.4] - 2026-08-03

### Added

- `deploy.BundleDigest` — the bundle-tree digest `Candidate.Digest` requires,
  now callable. Nothing may be installed without that field, and the only
  implementation of it lived unexported inside the package, so every consumer
  packaging its own application hand-copied the walk, the field order, and the
  length-prefixed encoding out of daemonkit's source: captain-hook, fusekit's
  holder, and cc-notes each carried one. Three transcriptions of a hash are
  three chances to diverge from the value `Install` and `Supersede` re-derive
  from the bytes themselves, and a divergence surfaces only as an
  `ErrConflict` refusal at install time. The digest is byte-identical to what
  the package has always computed — a candidate digested by a hand-copy and
  one digested by the export are the same value, so records already stored
  against a copy stay valid — and one fixture tree's digest is now pinned to a
  constant, so the encoding cannot drift without a test saying so.

## [0.21.3] - 2026-08-03

### Added

- `Session.Disconnected` — closes when the session's transport ends, before
  in-flight handlers necessarily return; `Done` keeps its settle-after-handlers
  meaning, and Disconnected never closes after Done. The internal wire layer
  already told the two edges apart, but the root `Session` exposed only `Done`,
  which fires after every in-flight handler returns — so fusekit's
  native-session supervision had no way to present a FUSE mount as unavailable
  the moment its backing process died mid-handler; the loss only surfaced after
  blocked reads drained. A spawned session closes Disconnected the same way,
  when its handoff channel ends.

## [0.21.2] - 2026-08-03

### Fixed

- `Client.Stop` no longer requires a `Program`. Stop renders no LaunchAgent
  and places nothing, so a launcher's Daemon declaring only its Label — one
  that must answer "not installed" without ever constructing a Program — stops
  with the same call. v0.21.1's Stop reached the agent derivation through a
  nil placement and panicked on exactly that Daemon. Observation now splits
  the runtime half (socket and owner record) from launchd's applied state,
  and without a Program the no-record rung has no inventory to consult:
  absence rests on the socket, the record, and the removal's own bootout,
  which takes down anything launchd still runs under the label.

## [0.21.1] - 2026-08-03

Both additions close the same v0.20-to-v0.21 transition wall, which three
consumers hit independently: cc-patch and synckit each open-coded legacy plist
removal, and cc-interact's reconciliation review found Stop/Ensure races and
unremovable v0.20 plists (its findings D2/D3).

### Added

- `Client.Stop` — drain the incumbent and remove its LaunchAgent in one verb,
  under the same state-directory start lock `Ensure` serializes on. Nothing
  exported let a consumer's stop serialize with `Ensure` before, so a
  hand-rolled stop raced it into false-success outcomes: a concurrent `Ensure`
  could re-apply the agent the stop had just removed, or the stop could remove
  the replacement `Ensure` had just started. Stop reuses Ensure's ladder aimed
  at absence: a serving incumbent is drained through the control lane, pinned
  to the build and generation just observed; one already draining, or a husk
  with a dead listener, settles out of the process table from the durable
  owner record, wedge repair included; no record at all leaves absence to the
  executable-scoped inventory; and stopping a stopped daemon is success. The
  LaunchAgent is removed last, after departure is proven, so launchd cannot
  relaunch what was just drained. A markerless (pre-v0.21) plist at the label
  is removed through `launchd.RemoveUnmarked` — the Client's own
  `Daemon.Label` is the assertion that verb requires — so one `Stop` call is a
  whole uninstall on a machine upgraded from v0.20.

- `launchd.RemoveUnmarked` and `launchd.ErrMarked` — the named escape for the
  pre-marker era. `Remove` refuses any plist without the
  `DAEMONKIT_AGENT_OWNER` marker (`ErrNotOwned`), which is correct
  steady-state but strands every pre-v0.21 install: v0.20 never wrote markers,
  so uninstall paths hit `ErrNotOwned` on plists those consumers themselves
  wrote. `RemoveUnmarked` boots out and deletes the label's agent only when
  its plist is markerless — naming the label is the caller's ownership
  assertion, the same prefix-guard pattern synckit's migration used — and a
  plist carrying the marker refuses untouched with `ErrMarked`, so the two
  verbs partition plist shapes exactly and neither bypasses the other.

## [0.21.0] - 2026-08-02

### Added

- `durable` — a public leaf package of durable filesystem verbs.
  `WriteFile`, `Create`, and `Writer`
  publish bytes and streams atomically; `SyncDir`, `Rename`, `Mkdir`, `Remove`,
  and `RemoveTree` make a directory mutation survive a power loss; `Marshal`,
  `Unmarshal`, and `ReadFile` strictly decode a `Validating` payload, refusing
  unknown fields, trailing values, and duplicate object keys at any depth; and
  `AcquireLock` bounds one exclusive `Lock` by a context deadline it requires.
  That lock excludes goroutines as well as processes, and by one mechanism
  rather than two — `flock(2)` binds ownership to the open file description,
  and every acquisition opens its own, so a caller needs no in-process mutex
  beside it — and contention is `durable.ErrLockBusy`, the one identity the
  fleet aliases and matches with `errors.Is`.

  The package has one tier, and its name is the contract: a no-fsync write
  inside `durable` would be a lie in the import path. Every lock is
  caller-held, so the lost-update class that grows an `UpdateUnlocked` twin
  cannot arise here. A temp name derives from its target's basename
  (`.<base>.<random>`), which makes a crashed writer's stump attributable in a
  scanned directory without a caller-supplied prefix; a publication sweeps
  stale stumps for the same target, bounded to temps at least an hour old,
  because two writers racing one path is legal and an unbounded sweep would
  unlink the slower one's in-flight temp.

  `internal/durablefile` is deleted. Its durable write and directory fsync move
  into `durable` unchanged in behavior; `ExactStateFile` and `ExactStateCodec`
  go with no replacement. Nothing ever used them, and every piece — the
  envelope, the fingerprint, the fallible `New`, the read that defaults an
  absent file, `UpdateUnlocked` — is something daemonkit's own record ladder in
  `deploy/record.go` declined when it was written with a free hand.

- Public subprocess supervision: `Cmd` and its `Run`/`Spawn`/`Adopt` verbs,
  exposed twice from one implementation — as methods on `Ctx`, bound to a
  serving daemon's record store, and on the `*Owned` scope `OwnProcesses` opens
  for a CLI with no daemon, no socket, and no wire. `Run` returns a bounded
  `RunResult` beside its error, never discarding output on failure: a nonzero
  exit lands in `*ExitError`, an overflowed stream in `ErrTruncated` with the
  capped bytes still in hand, and a spent budget in an error matching
  `context.DeadlineExceeded`. `Spawn` returns a `*Child` whose channel is
  established and whose stderr is already draining before the child is
  released, so it can never block on an unwired pipe or race its own recording;
  the channel is a closed enum — `ChannelNone`, `ChannelHandoff` (a socketpair
  end at fd 3), `ChannelStdio` (stdin and stdout joined into one
  deadline-aware `net.Conn`) — and `Child.Conn` hands the parent end out
  exactly once. `Adopt` records a process whose fork the caller had to own and
  returns a `*Tracked` that signals and observes but never waits: two waiters
  on one pid is a lost wakeup, so `Stop` returns a `Reap` and `Release` retires
  a record. `Owned.Close` and `Serve`'s `StageChildren` answer over one held
  set, and a verb enters that set before it can start anything: admission and
  registration are one step under one lock, so the whole spawn — `posix_spawn`,
  the fsynced record, the exec verify, the release — runs inside a claim the
  settle already sees. A settle waits an admitted verb out rather than
  snapshotting past it, and a verb that has not finished starting when the
  deadline runs out is an `ErrUnsettled` fault naming the verb, never a `nil`
  answered over the process it already started. Within that set a `Run` holds
  its child for the run's duration exactly as `Spawn` does, so an in-flight
  `Run` is terminated and proven gone before `err == nil` or an empty
  `Drained.Abandoned` is published, and the settled run returns the bytes
  it collected beside an `*ExitError` naming the fatal signal. Proven gone means
  proven: a settlement ladder that times out with nothing proved publishes
  `ReapUndetermined`, and that child stays registered so `Close` faults with
  `ErrUnsettled` instead of answering `nil` over a process still in the table.
  Nor does settling a run pin its caller — the post-terminal EOF drain and the
  stdin wait are bounded by the settlement deadline the demand carried, so a
  descendant that inherited a pipe cannot hold the run open for the rest of a
  budget the scope already stopped spending. A child abandoned mid-`Spawn`
  settles on the settlement grace, not on whatever deadline the caller happened
  to hand down. A verb reaching a settled scope is refused in daemonkit's own
  register, never the record store's, and so is a verb handed a context with no
  budget left — before a child exists to abort, and never as the lock's
  wording, which would name a contention that never happened. A dedicated
  session whose survivors outlast their own settlement publishes undetermined
  too: the leader's exit is not the
  group's, so a leader proven gone over members that were not is no proof.
  Descendants are covered by the child's session and only by it. `Run` always
  spawns into a dedicated one, so a run settles whatever its command forked and
  not merely the command; a `Spawn` without `Cmd.Session` settles the child it
  started and nothing below it; and a descendant that `setsid()`s out of its
  session leaves the only scope the kernel offers, so it is neither signalled
  nor counted. `NewGate` is the record-before-first-instruction guarantee for that
  caller — a wrapper that signals readiness on fd 3, parks on fd 4, and execs
  the target only on `Release`. `NewCapture` bounds a child's stderr without
  ever blocking it, `ClaimHandoff` and `CloseInheritedFDs` are the child half,
  and `Owned.Ctx` mints a `Ctx` so product code written against a daemon runs
  unchanged under a CLI-owned scope.

- `Cmd.Exec` — the trust posture the executable behind `Cmd.Path` must prove
  before its child runs one instruction, required on every `Run` and `Spawn`.
  `posix_spawn(START_SUSPENDED)` stops the child at its entry point *after* the
  kernel has established the CodeDirectory, so the full shipping verifier reads
  the exact image that will run, in place, against a process that has not
  executed an instruction; the token comes from `task_name_for_pid` +
  `task_info(TASK_AUDIT_TOKEN)`, and a failed verify returns into the existing
  abort path — the child is killed and reaped and `ErrUntrusted` comes back,
  never a live-looking `*Child` that instantly reports 137. `Serving` is a
  two-constructor sum, so the dangerous posture is not the zero value:
  `ServingSigned(r)` pins a bundle, and `ServingSameUser()` is the named waiver
  a Python interpreter, a platform binary, or a homebrew tool takes. Spawn
  never copies, stages, or rewrites `Path` — App Group entitlement grants
  attach to the deployed path, so the recorded image, the verified image, and
  the running image are one file. The signed-acceptance half of this needs the
  `.trust-fixtures` signed binaries, which CI does not build; what CI covers is
  the denial, the abort, and the token read against a suspended child.

### Changed

- **Breaking.** The wire's `Tenant` header is reserved and must be empty. Its
  two length bytes and its position stay in the frozen frame layout — the
  golden fixtures are byte-identical and every era still decodes every other's
  frames — but no live session may populate it: `Call`, `Open`, and their
  spawned-session and Swift mirrors drop the parameter, `wire.Request` drops
  the field, and the Go codec's per-kind contract refuses a request or stream
  frame carrying one, in both directions. Nothing in the fleet ever set it, and
  a routing key the transport neither reads nor authorizes is a field that
  looks load-bearing while carrying an attacker's string into a consumer's
  handler. The Swift codec keeps the clause on every kind but `.request`, and
  `SessionFrame.tenant` is still public and settable, so a Swift caller that
  populates it by hand encodes a frame its own codec admits and a Go daemon
  refuses as structurally invalid.

- **Breaking.** `Serve` admits the frozen two-byte drain preamble as an inbound
  request, below the protocol gate and above the trust gate — what
  `docs/DESIGN.md` invariant I14, Device 4, and step 7 of the serve ladder have
  specified since the cut, and what the tree did not do. A connection whose
  first bytes are the preamble is dispatched to the trust-gated drain path
  without a frame, a version, or a schema ever being read, so a new-era
  successor can gracefully drain an old-era incumbent across a protocol bump
  instead of falling back to SIGTERM; `Trust.Control` still authorizes it, so
  an untrusted peer's preamble leaves the incumbent serving. The mixed-era
  matrix's `drain-preamble` and `drain-preamble-trust-gate` rows leave their
  declared cut-era absence and are redeemed against real daemons on the
  process table: the open one the OS reaped, and the strict one still holding
  its socket after an untrusted peer's preamble reached it. The SIGTERM path
  is unchanged and remains the mechanism of record:
  measured on a real machine, launchd sends exactly one, the forwarder consumes
  it, and the process dies at `ExitTimeOut` — six seconds of a held flock the
  preamble now avoids.

- **Breaking.** `Run` always gives its child a dedicated session, and a `Cmd`
  naming `Cmd.Session` on `Run` is refused at the boundary — the same shape as
  `Cmd.MaxOutput` being refused on `Spawn`. A run is bounded, disposable, and
  run to completion, so a command that outlives itself through a fork is a
  leak, not a posture worth a field; `Spawn` keeps `Cmd.Session` as a real
  choice, since a long-lived co-process may legitimately want the caller's
  session or its own. This is what makes `Owned.Close`'s "every live child
  terminated and proven gone" true of a `Run` child: before it, terminating a
  run's child signalled the child alone — it had inherited the *caller's*
  process group — and its own fork survived a `Close` that answered `nil`.

  Three consequences a `Run` caller sees. The run no longer settles until its
  whole session has, so the abnormal teardown path pays the session's own
  settlement grace after the child's ladder: a scope holding a SIGTERM-ignoring
  run child needs roughly 15s of `Close` budget where 12s sufficed, and a
  `Close` sized only for the child ladder now returns `ErrUnsettled` on a shape
  that used to answer `nil`. The command is no longer in the caller's process
  group, so it receives neither the terminal's SIGINT nor a controlling tty. And
  a descendant the command forked is terminated with it — including one that was
  holding the run's stdout pipe, which used to keep the drain open to the
  deadline and hand back `ErrTruncated`. The one shape still outside the scope
  is a descendant that `setsid()`s out of the session; macOS offers nothing that
  survives that, and it is documented rather than claimed.
- A `Run` whose hold refuses carries the teardown's verdict out. The refusal
  admitted before the spawn and held after it, so an `Owned.Close` completing in
  that window tore the child down on the settlement grace and discarded the
  exit — an undetermined terminal there reached nobody, though it is the only
  settlement that child would ever get. The hold's error is now joined with the
  undetermined verdict, and the terminal rides out in the result rather than a
  zero value.
- **Breaking.** The daemon's singleton lock moves from `socket + ".lock"` to
  `recordPath + ".lock"`, and is taken inside the record store's open rather
  than beside it. `Daemon.RecordPath` is exported and the store open took no
  lock at all, so `OwnProcesses(ctx, d.RecordPath())` against a serving daemon
  was constructible and its reclaim would have killed that daemon's live
  children. The lock now lives where the invariant lives: `Serve` and
  `OwnProcesses` take the identical lock on the identical path, and a second
  owner is refused before it can read a record. Contention on the
  `OwnProcesses` side is `durable.ErrLockBusy`, never `ErrBusy` — that one
  means a live incumbent owns the socket, which is a different fact — and
  `Serve` keeps its socket probe for exactly that detection.
- **Breaking.** `Ctx.Reclaimed` is `[]Reclaimed`, the root package's own type;
  it was `[]proc.Reclaimed`, an `internal/` type in an exported signature that
  no consumer could name. `Exit` gains `Signal`, and `Code` is now `-1` on a
  signal death rather than `128 + signal`, so a status and a signal can never
  be confused for one another.
- **Breaking.** The interim `daemonkit.Run(ctx, *proc.Store, proc.Cmd)` is
  deleted. Its own doc admitted no external module could call it — both its
  parameter and its result were `internal/` types; `Owned.Run` and `Ctx.Run`
  are the surface it stood in for.
- A `Run` stream that is not the whole stream is an error, not data. Overflow
  used to set a `Truncated` flag beside a nil error, which a caller parsing the
  stream had to know to check; it is now `ErrTruncated`, with the retained bytes
  still in the `RunResult`, so nothing is discarded and a forgotten cap is loud.
  The sentinel covers both shortfalls it can carry — a stream past its cap and a
  drain severed at the settlement deadline — since a caller parsing either gets
  a prefix, not the output. The
  default cap rises from 1 MiB to 4 MiB, and one `Cmd.MaxOutput` replaces the
  separate stdout and stderr limits. The cap bounds what a run retains, not
  what it allocates to retain it: retention grows geometrically with every
  growth clamped to the cap, so a silent command no longer pays the whole cap
  up front and a command that fills it no longer pays several times over on
  `append`'s doubling.
- Duplicate `Cmd.Env` keys are a boundary refusal. `posix_spawn` passes envp
  verbatim, and both macOS `__findenv` and Go's `syscall.copyenv` return the
  *first* occurrence — so the "copy the environment and append an override"
  pattern silently ran children on the original value. Deduplicate before the
  boundary. The `DAEMONKIT_SPAWNED_*` namespace is daemonkit's own: a
  caller-supplied key there is refused, an inherited one is stripped, and a
  `ChannelHandoff` spawn appends exactly the attach nonce and the conveyed
  `Cmd.Limits` after the caller's environment. The nonce is not a secret — any
  same-UID process reads a peer's environment through `KERN_PROCARGS2` — and
  nothing leans on it as one: its role is fd-mixup defence, proof that the
  attaching peer inherited fd 3 from this exec. The child unsets both variables
  as it claims the descriptor.
- Durable writes create no directories. `durablefile.WriteFileDurable` ran an
  implicit `mkdir -p` at a hardcoded 0700, a mode that silently disagreed with
  call sites owning their directory at another one; `durable.WriteFile` returns
  the plain os error instead, and a call site that owns a directory creates it
  with `durable.Mkdir` at its own mode. The `launchd` applier gains exactly
  that: it writes the agent plist into `~/Library/LaunchAgents`, which a fresh
  macOS account does not have.
- **Breaking.** Peer code-identity verification is kernel-only and in-process.
  The packages `trust`, `peer`, and `codeidentity` are deleted whole and replaced
  by module-private `internal/trust`; with them go `trust.RunVerifierChild` and
  the disposable verifier child every consumer had to dispatch at the top of
  `main`, both purego Security.framework bindings, the role and authority
  registry (`trust.PeerRole`, `TrustPolicy`, `NewTrustPolicy`, and every
  `Allows*` accessor), and `codeidentity`'s three sentinels. Verification is now
  seven `csops_audittoken` reads against the accepted socket's audit token: no
  file is opened, no daemon is contacted, no CoreFoundation object is built, and
  the path cannot block, so it needs no worker lane, no timeout, and no
  `context.Context`.

  Two clauses are new denials rather than relocations. `CS_VALID` is now
  required explicitly: xnu serves the identity ops for `CS_VALID | CS_DEBUGGED`,
  so a successful read never implied a valid signature, and a process whose
  signature the kernel invalidated at runtime was admitted before. And the six
  injection entitlements are rejected unconditionally — the old verifier skipped
  `disable-library-validation` whenever the status word proved library
  validation, which every peer reaching that clause did, so the clause was dead
  code. `Requirement.AllowJIT` is the one relaxation, and it is documented as
  the marker of a peer this mechanism cannot authenticate rather than as a
  bounded exception.

  A peer's signing leaf is now resolved through the CMS SignerInfo and asserted
  to carry the CodeDirectory's team as its Organizational Unit. Apple's own
  `SecStaticCode::verifySignature()` silently skips that comparison when the
  leaf has no Organizational Unit, and the old designated-requirement string
  failed closed on such a leaf where a kernel-only check would not.
- The daemon-facing binary-absence invariant is narrowed. A daemon-facing binary
  still carries no consumer-supplied policy value — no app group, no required
  entitlement key or value — but the six injection entitlement identifiers and
  `com.apple.security.application-groups` are unconditional constants in the one
  verifier now, so every daemonkit binary contains them. The information given
  up is a list of Apple-published entitlement names in an open-source library.
- **Breaking.** The wire handshake gates on `ProtocolVersion` alone. `WireBuild`
  is still required and still exchanged, but it is now a diagnostic rather than a
  gate, and the four post-handshake re-checks and the readiness-path build gate
  are gone with it. A protocol mismatch returns a typed
  `wire.ProtocolMismatchError{Theirs, Ours}` unwrapping to `ErrProtocolVersion`.
  `ErrBuildMismatch` and `ResponseCodeBuildMismatch` stay exported, since a new
  client still meets servers that reject on those terms. **A consumer needing
  schema equality must now enforce it itself, on `Request.WireBuild`.**

  Gating a transport on a schema identity was self-wedging. `WireBuild` is a
  digest of the consumer's own RPC schema, generated into the consumer's repo, so
  a mismatch means one consumer's long-lived daemon has outlived a client upgrade.
  The client then could not complete a handshake to tell that stale daemon to
  drain and exit, which is the one action that repairs it.
- **Breaking.** The advisory file lock moved out of `proc` into module-private
  `internal/flock`, dropping twelve exported symbols: `proc.FileLockSpec` (and its
  `Path`/`Mode`/`Deadline`/`Acquire`/`AcquireExisting`/`TryAcquire` members),
  `proc.FileLockHandle` (`Close`), `proc.FileLockMode`, `proc.FileLockShared`, and
  `proc.FileLockExclusive`. The lock is not part of `proc`'s process-ownership
  surface, and no fleet consumer needs it as public API. The three contention
  sentinels stay exported from `proc` as aliases of their `internal/flock`
  definitions, so `errors.Is(err, proc.ErrLockBusy)` keeps matching. Their
  messages move to daemonkit's own register — `daemonkit: invalid file lock`
  and `daemonkit: unsafe lock file` replace a `proc:` prefix that named a
  package no consumer can import; the values themselves are untouched, so
  every `errors.Is` match is unaffected.
- `Controller.verify` issues no launchctl mutation. It previously ran
  `launchctl enable` as a side effect of a read, which also wrote a permanent
  root-owned override-database entry per label that uninstall never removes. A
  loaded-but-disabled agent is therefore no longer silently re-enabled.
- **Breaking.** A deployment's durable state moved from
  `.daemonkit-deployment/<Product>` to `.daemonkit-deploy/<Product>`, and the
  first open of a deployment renames its own tree at the old path to a
  `<Product>.bak` sibling. The rename is not cosmetic: every record `deploy`
  reads now decodes with `DisallowUnknownFields` against types this cut
  renamed, so a v0.20.10-shaped tree fails to decode on 100% of installed
  machines. Archiving it turns that hard failure into a clean re-install and
  keeps the old receipts as evidence rather than deleting them. The archive is
  one rename, so concurrent openers produce exactly one `.bak`, and it covers
  only the product doing it — a sibling installed beside it that an older
  binary still manages keeps the lock path that binary opens by name. An
  occupied name rotates to `<Product>.bak.2`, `.bak.3`, and on: a downgrade to a
  pre-cut release recreates the tree at the old path, and nothing on either side
  of that downgrade removes an archive, so the re-upgrade meets its own. Failing
  there would fail every operation the deployment is ever asked to do; no era's
  evidence is overwritten by the era that follows it.
- The `launchd` package carries `//go:build darwin`. Its files compiled
  anywhere and failed only at runtime, exec'ing `/bin/launchctl`; they now fail
  at build time, which is the guarantee the rest of the module already gives.
  The exported packages that survive off darwin — `artifact`, `bundle`,
  `ghrelease`, `paths`, `version` — are recorded per platform in
  `ci/exported.txt`. `ci/portable.txt` declares the whole linux-portable
  partition, module-private packages included, and
  `scripts/portable-gate.sh` gates it in both directions. The directions do not
  share a remedy: `--write` records an undeclared gain and refuses outright
  while any declared package is regressed, since regenerating over a regression
  would drop the package from the manifest and launder the broken boundary into
  an approved one in one command. A regression prints the linux build and vet
  output that explains it, and the command to reproduce it.
- `Stable`'s program placement writes through `durable.WriteFile` rather than
  the second durable write it carried. That copy existed because the shared
  write named its temps `.durable-*`, which attributes a crash stump to nobody
  on a program root every daemonkit consumer shares. `durable`'s temp now
  derives from the target's basename, which for a program path is
  `.<Label>.<random>` — byte for byte the name the private copy built — and its
  sweep carries the same rule that stops one label from taking a dot-extension
  sibling's temps. It adds a bound the copy leaned on the start lock for: a temp
  under an hour old is spared, so a concurrent launcher's in-flight write
  survives. The one behavior that changes is when the sweep runs — on a
  placement that writes, not at the top of every `Ensure` — and the hour-old
  bound that spares a live temp spares a fresh stump too. A crash stump
  outlives every no-op convergence, and the first placement that writes an hour
  or more after the crash collects it; a launcher that crash-loops inside that
  hour leaves one stump per attempt where the unbounded pre-write sweep left at
  most one.

### Fixed

- Every admitted wire request runs. `Server.dispatch` raced the request context
  against the queue send after admission, so an admitted job could be dropped
  without ever running — and the terminal the peer received was byte-identical to
  an executed request's, so a client could not tell whether its mutation had been
  applied. Measured at 191 of 400 dispatches with the queue empty: Go picks
  uniformly among ready `select` cases, so a done context was a coin flip against
  a healthy buffered send.
- launchctl outcomes are classified in exactly one place, as loaded, not loaded,
  refused with launchd's own reason, in flux, or unknown, and only the in-flux
  case retries. `AppKeepAlive.Uninstall` now removes the plist when the agent is
  already unloaded, and `Stop` no longer fails whenever the agent is not loaded.
  The classifier meant to cover those cases asserted an interface the producer
  never satisfied, so it could never fire. Exit 5 is an aggregate batch code and
  is never read as a specific condition.
- A spawned child parked at the readiness gate dies on `SIGTERM`. The wrapper
  masked the signal for the whole gate wait, unbounded and untested.
- `FileStore` serializes in-process operations on one store path behind a FIFO
  gate and validates an established store under a read transaction rather than an
  empty write commit. bbolt guards the file with a queueless whole-file flock
  retried on a 50ms poll, so uncoordinated openers paid seconds under contention;
  sixteen concurrent untracks now stay well inside worker's settlement reserve,
  and a concurrent independent `Load` drops to single-digit milliseconds.
- A process-table probe that races a child's exit fails with `ESRCH` rather than
  `ENOENT`, and only `ENOENT` settled it — so a child the kernel had already
  reaped surfaced as `settlement incomplete`. Either now settles it, mirroring the
  tri-state `Liveness` rule in the other direction: Undetermined never reads as
  dead, and provably gone never reads as incomplete.
- A LaunchAgent declaring `Agent.LimitLoadToSessionType` loads. launchd refuses
  any job whose session type names a domain other than the bootstrap domain's own
  (error 134, "Service cannot load in requested session") and daemonkit bootstraps
  only into `gui/<uid>`, so every value but `SessionTypeAqua` rendered a plist
  launchd permanently refused. The key is no longer emitted.
- A controller state store whose schema, identity, or fingerprint is not the exact
  current one archives aside — naming the backup path and every abandoned applied
  LaunchAgent in the log — and the controller opens onto a fresh store, instead of
  wedging every open of an upgraded daemon.

### Security

- The Go client judges the process accepting for it before it writes a byte.
  `ClientConfig.Authorize` runs between the dial and the hello, and is
  required — a constructor whose connection needs no judging passes a named
  waiver rather than leaving the field nil. `Control` passes `authorizeServer`,
  so `Trust.Serving` is now enforced ahead of the handshake instead of after
  it. Previously the whole handshake completed first, which let a same-UID
  process that unlinked and rebound the socket forge the two-byte drain
  preamble and pin a consumer in `WaitReady`'s retry loop, forge a build
  mismatch and drive it into a redeploy, or harvest the schema digest — none of
  which required surviving verification, because verification ran after the
  damage. The captured-connection closure `Control` used to reach the dialed
  socket is gone with it: per-connection state is established inside the
  authorization, so nothing but immutable config survives to a replacement
  connection. The Swift client carries no counterpart: it still connects,
  adopts the codec, and completes the handshake without judging the accepting
  process, so every forgery listed above remains open against a Swift
  consumer.

- `trust`'s three denials stay disjoint through the root sentinel. Only a
  policy mismatch is `ErrUntrusted`; a peer that exited before verification
  completed and a configured requirement with no verifier keep their own
  identities. They had been flattened together, so a caller branching on
  `ErrUntrusted` read an absence race and a build defect as a trust verdict.

- The spawned business lane no longer verifies itself. `NewSpawnedClient` read
  peer credentials from the parent's own end of a socketpair and checked them
  against a floor-only requirement — credentials on a self-created pair name
  the creator, so the gate authenticated nothing while reading as protection.
  The lane's real property is directional confinement: the pair is a channel no
  other process has a path to dial. The nonce is documented for what it is —
  fd-mixup defence, not a secret, since any same-UID process reads a peer's
  environment via `KERN_PROCARGS2`.

- Swift daemons apply the same-UID floor unconditionally. It was guarded behind an
  optional session policy that defaults to nil, so `getpeereid` ran and its result
  was discarded: a default-configured Swift daemon had no floor at all, while Go
  treats it as unconditional on every platform. The authoritative uid is now a
  non-optional property, so a caller may relocate the floor but no value removes it.
- `codeidentity` requires `CS_ENFORCEMENT` and `CS_HARD`, rejecting a peer whose
  code-signature enforcement has been switched off by the
  `allow-unsigned-executable-memory` or `disable-executable-page-protection`
  entitlements.
- `trust` and `codeidentity` share one code-status check, so a clause added to one
  verifier cannot be missing from the other. They had diverged: `codeidentity`
  checked six clauses and `trust` three. Unifying adds the two enforcement clauses
  to `trust`, which already denied the responsible entitlements by name — the
  status check is a backstop for a peer that exposes no entitlement dictionary.

### Deprecated

- `service.SessionType`, its five constants, `service.ParseSessionType`, and
  `service.Agent.LimitLoadToSessionType` are accepted and ignored: the field is
  neither rendered into the plist nor stored with the agent, and setting anything
  but `SessionTypeAqua` logs a warning naming the value. They are removed in a
  future breaking release; drop the field.

## [0.20.10] - 2026-07-27

### Added

- `trust.ErrPeerGone` and `codeidentity.ErrPeerGone` classify a peer that
  exited before code-identity verification completed. OSStatus 100003
  (kPOSIXErrorBase + ESRCH) and errSecCSNoSuchCode (-67065, "host has no
  guest with the requested attributes") now wrap this sentinel — OSStatus
  preserved in the message — instead of `ErrNoVerifier`, at the
  `SecCodeCopyGuestWithAttributes` (100003 only),
  `SecCodeCheckValidityWithErrors`, and `SecCodeCopySigningInformation`
  failure sites in both darwin verify packages and across the verifier-child
  protocol (a new `peer_gone` result). Every other non-zero OSStatus keeps
  its fail-closed classification.

### Fixed

- The wire server no longer logs the departed-peer verification race at
  Error under its infrastructure-must-be-loud rule, which flooded consumer
  logs with "peer verification infrastructure failure" lines under load.
  `trust.ErrPeerGone` now takes the same quiet path as a policy denial: one
  debug line per rejected connection. Genuine verifier-infrastructure
  failures stay loud at Error.
- The Swift session transport no longer mislabels a poll deadline expiry as
  `systemCall(operation:, errno: EAGAIN)`: `waitUntilReady` surfaces expiry
  as `ETIMEDOUT`, the transport's existing deadline convention, so a read
  that runs out of time reports a timeout instead of errno 35. A `poll(2)`
  call that itself fails with `EAGAIN` — a documented internal-allocation
  transient — retries like `EINTR`, bounded by the same deadline, instead of
  surfacing a permanent-looking system-call failure. Deadline timing is
  unchanged.
- `ServiceSocketClient` no longer counts a deadline expiry as
  session-teardown proof: `EAGAIN` and `ETIMEDOUT` are out of the peer-end
  errno set, and an expiry during session establishment is no longer
  retained as a permanent lifetime failure. Under load, one expired
  handshake bricked the broker bridge's lifecycle client — every later probe
  reported "bridge failed: disconnected" until the process restarted.
- `ServiceSocketClient` treats a transport-internal handshake or per-frame
  write timeout that fires while the caller's deadline is still open as a
  session transition, not a call failure: the current generation retires and
  the call retries on a successor session (replaying an in-flight request
  only under the `.idempotent` policy). `deadlineExceeded` now surfaces only
  when the call's own deadline has passed, and a no-progress expiry surfaces
  as `ReadinessNoProgressError` with its last lifecycle snapshot.
- `BrokerSocketBridge` duplicates its connected-socket handoff failure line
  to standard error beside the os_log call, so daemon plists that capture
  stderr surface bridge failures in the file log instead of only in os_log.

## [0.20.9] - 2026-07-26

### Changed

- Every user-home-derived durable path — the launchd plist directory, the
  stable program root, the `paths` defaults, the artifact store, and the
  runtime `PATH` extension — now resolves the invoking user's home through
  the passwd database instead of `$HOME`, so a sandboxed caller environment
  (Homebrew postinstall's temp HOME) can no longer redirect durable machine
  state into a throwaway directory. The `DAEMONKIT_HOME` override remains as
  a test seam and is honored in production with a once-per-process warning.
- `launchctl` exit 5 is no longer classified as a transient in-flux state to
  wait out: bootstrap retries 3 times instead of 6, and the giving-up
  diagnostic names the bootstrapped plist path and points at launchd's own
  log, since exit 5 covers permanent launchd denials.

### Fixed

- launchd bootstrap no longer fails with EPERM/exit 5 under Homebrew
  postinstall, which was rendering plists and staging binaries under the
  sandboxed temp HOME that launchd refuses to bootstrap from; real-home
  resolution keeps those paths stable regardless of the caller's
  environment.
- Controller startup recovery reconciles in a recovery mode: a persisted
  desired agent whose install fails, or a stale agent whose removal fails,
  is logged and left as drift for a later `Converge` to retry instead of
  failing recovery closed — so a persisted failed desired state cannot wedge
  the very install that would fix it. Caller-requested convergence stays
  strict, and durable-store write failures remain fatal in both modes.

## [0.20.8] - 2026-07-26

### Fixed

- Release re-cut of 0.20.7, whose tag predates the Unreleased-link retarget
  and cannot satisfy the release gate; no code changes. The v0.20.7 tag was
  never published as a release.

## [0.20.7] - 2026-07-26

### Added

- `service.StableProgramFrom` stages a foreign binary (one that is not the
  calling process) into the stable root under a caller-chosen name, keyed by
  content digest instead of build version: replacement happens exactly when
  the source bytes differ from the staged copy, and a damaged sidecar over
  matching bytes is repaired in place. Source bytes are read once, so the
  digest that drives the decision and the bytes that land on disk are the
  same snapshot.

### Changed

- Stable-program sidecars carry an explicit staging policy (schema 2), and
  the two entry points refuse to overwrite each other's stages: build-keyed
  and digest-keyed staging under one name is an error in both directions,
  never a silent replacement. A sidecar that exists but cannot be read is
  treated as foreign by the digest-keyed path (every schema-1 sidecar is
  build-keyed by construction) and refused; the build-keyed path still
  converges its own legacy sidecars on first touch.

### Fixed

- A rolled-back deployment apply no longer blocks a new candidate: a
  differing-fingerprint receipt in the rolled-back phase is retired and
  restaged like a stale active apply, instead of failing with
  `ErrInstallConflict` on every retry. Machines wedged by a failed agent
  bootstrap converge on the next upgrade.

## [0.20.6] - 2026-07-25

### Fixed

- Durable untrack (`Store.Remove`) no longer runs inside the child settlement
  reserve: settlement proves the exact reap alone, and record removal happens
  in a deferred background task with retry, so flock'd-store latency can never
  terminalize a worker claim. A removal that exhausts its retries leaks one
  stale record that the next generation's `Recover` reaps.
- `Manager.Shutdown` drains deferred untracks inside a bounded 500ms window
  instead of spending the caller's settlement budget on a stalled store,
  abandoning stragglers to the next generation's `Recover`.
- Recovery tolerates a record removed between `Load` and `BeginReap` (a prior
  generation's deferred untrack racing a successor) instead of aborting the
  whole recovery pass.

### Changed

- `PreparedChild.Done`/`Stop` no longer imply the durable record is gone;
  durability is guaranteed at `Manager.Shutdown`, which drains the deferred
  untracks. Consumers that inspect the store immediately after a settled
  child must poll or wait for shutdown.

## [0.20.5] - 2026-07-25

### Fixed

- `launchctl print` exit 113 ("Could not find service", current macOS) is now
  recognized as not-loaded alongside exit 3, wherever launchctl outcomes are
  classified (verify, status inspect, uninstall/reload bootout, replacement
  quiesce inspect, keepalive). Previously a stored agent whose service was
  booted out hard-failed controller recovery and `Status` instead of reading
  as drift — wedging deployment applies whose prior host was stopped, the
  exact cold-upgrade path a build-mismatched host requires.

## [0.20.4] - 2026-07-25

### Fixed

- Release re-cut of 0.20.3, whose tag pointed at a Guides-bot re-render
  commit that can never satisfy the CI release gate (Actions-token pushes
  trigger no workflows); no code changes. The v0.20.3 tag predates the
  re-cut and was never published as a release.

## [0.20.3] - 2026-07-25

### Changed

- Worker children inherit the daemon's `PATH` instead of a hardcoded
  hermetic `/usr/bin:/bin:/usr/sbin:/sbin` (which remains the fallback when
  the parent has none), and `daemon.Runtime` extends the process `PATH` once
  at construction with the standard user bin directories launchd omits
  (`/usr/local/bin`, `/opt/homebrew/bin`, `~/.local/bin`, `~/.bun/bin`) —
  so daemon-context subprocesses resolve user-installed CLIs (`claude`,
  `codex`, `gh`) by plain inheritance. Spawn requests still cannot override
  `PATH`.

## [0.20.2] - 2026-07-25

### Fixed

- Release re-cut of 0.20.1 with a tagger identity matching the trusted
  release key; no code changes. The v0.20.1 tag predates the re-cut and was
  never published as a release.

## [0.20.1] - 2026-07-24

### Fixed

- Release re-cut of 0.20.0 with the trusted release signature; no code
  changes. The v0.20.0 tag predates the re-cut and was never published as a
  release.

## [0.20.0] - 2026-07-24

### Added

- `service.StableProgram(name, build)` maintains `~/.daemonkit/bin/<name>` as
  a version-stable launchd program path: an atomically-replaced byte copy of
  the invoking executable with a digest/size/mtime sidecar, a newer-or-repair
  replace predicate under an exclusive lock, a stat-only lockless fast path,
  and a canonical return that never follows a symlink at the final component.
  `service.RemoveStableProgram` clears the pair.

### Changed

- `service.Controller` no longer re-validates stored agents' program liveness
  when loading persisted state: `Desired` and `Applied` decode structurally,
  reconcile skips (and logs) a desired agent whose program path is missing,
  and `verify` reports a missing program as drift — so an upgrade that
  deletes an old versioned binary path (a Homebrew Caskroom or content-cache
  directory) heals on the next converge instead of permanently wedging the
  controller before it can reconcile. Only the missing-path error class is
  treated as heal-able; permission failures, symlinked ancestry, and
  non-regular or non-executable programs still fail closed at reconcile,
  verify, and every write/effect site.

## [0.19.1] - 2026-07-24

### Added

- `worker.RuntimeClaim.Terminalized` exposes the claim's sticky terminal state
  as a level-triggered channel, with `Terminal` returning the terminal error.

### Changed

- A worker claim that terminalizes after activation now tears the runtime
  down: `daemon.Runtime.Wait` returns the terminal error instead of the daemon
  serving forever with a closed pool — a state in which every peer was
  rejected as untrusted and exit-based supervisors never intervened because
  the process stayed alive. Ordered shutdown (`worker.ErrClosed`) is exempt.

### Fixed

- The reaper's termination-grace wait is now an early-settle poll instead of
  an unconditional full-grace sleep, returning unused grace to the settlement
  budget and making timeout-driven claim terminalization under load
  correspondingly less likely.

## [0.18.0] - 2026-07-24

### Added

- `trust.VerifierWorkerBudgets` fixes the verifier worker lane's time and byte
  bounds as daemonkit-owned constants, so the verifier lane no longer inherits
  the product pool's budgets and a product configuration can never truncate a
  verifier verdict. Caller deadlines beyond the lane's time budget clamp to it.
- `trust.ProcessVerifier.Probe` runs one complete verifier child exchange and
  reports transport health only; any well-formed verdict passes.

### Changed

- **Breaking:** `worker.Pool.ClaimRuntime` now takes `worker.VerifierBudgets`
  (pass `trust.VerifierWorkerBudgets()`); direct callers must update. Products
  that only construct pools are unaffected.
- `daemon.Runtime.Begin` self-probes the trust verifier after worker activation
  and before serving. A daemon whose executable does not dispatch
  `trust.RunVerifierChild`, or whose verifier lane cannot complete an exchange,
  now refuses to start with `daemon.ErrTrustVerifierProbe` instead of silently
  rejecting every peer as untrusted.
- The wire server logs peer-verification infrastructure failures — worker
  kills, decode failures, child exec errors, and fail-closed verifier absence
  (`trust.ErrNoVerifier`) — at Error level; policy denial verdicts keep their
  Debug logging. The peer-facing response stays `PeerUntrusted` in both cases.

## [0.17.4] - 2026-07-24

### Fixed

- Keep accepted wire sessions alive after runtime intake closes, then settle
  every written terminal response through its session-bound acknowledgement
  before canceling transport during shutdown.

## [0.17.3] - 2026-07-24

### Fixed

- Fence new client calls when graceful close begins, settle already-admitted
  calls through their terminal acknowledgements, and reject late control frames
  before sending GoAway so peer closure cannot poison an otherwise clean drain.

## [0.17.2] - 2026-07-24

### Added

- `daemon.PublicationSlot.Value` resolves the exact resource graph carried by
  an already-admitted request while its admission lease remains live, without
  opening a nested admission or consulting the runtime's current publication.

### Fixed

- Re-prove exact dedicated-session absence when Darwin reports `EPERM` for a
  signal raced by natural process-group exit, while retaining ownership if any
  member remains.
- Make the Swift broker-handoff deadline test prove delivery to a still-blocked
  peer without assuming a wall-clock scheduler-latency bound.

## [0.17.1] - 2026-07-23

### Fixed

- Correct the release comparison metadata required by the source-release
  contract. The v0.17.0 tag contains the complete deployment and publication
  hard cut, but its automated GitHub release stopped before publication.

## [0.17.0] - 2026-07-23

### Added

- `daemon.PublicationSlot.Acquire` atomically admits a ready publication,
  pins its resource graph for the caller's operation, and makes release part of
  runtime drain settlement. Unpinned publication loads are removed.
- `deployment.NewCandidatePlan` binds exact service policy to an existing local
  packaged app and persists only relative program paths. `ApplyInstalledCandidate`
  owns exact local candidate copying,
  attestation, first install, atomic upgrade, activation, rollback, and durable
  replay without exposing a staging path or downloading an artifact.
- `deployment.DeactivateCurrentInstalled` derives prior generation, build,
  policy, and plan from controller-sealed state and recovers any pending apply
  transaction before deactivation.
- `deployment.UninstallCurrentInstalled` owns exact quiescence, deactivation,
  atomic private removal, deletion, and lost-response recovery. Consumers no
  longer remove a canonical installed app themselves.

## [0.16.0] - 2026-07-23

### Fixed

- `proc.ClaimSpawnedSessionIdentity` now duplicates the inherited session
  descriptor before inspection and leaves the original descriptor and flags
  untouched unless the exact ownership proof succeeds.
- Spawned-session ownership is bound to the direct manager parent through
  kernel AF_UNIX peer credentials, independently captured process identity,
  and the exact v1 bootstrap, receipt, acknowledgement, and nonce exchange.
  Reparented sessions, foreign peers, malformed acknowledgements, and partial
  proofs fail without transferring or damaging descriptor ownership.

## [0.15.0] - 2026-07-23

### Added

- `deployment.ActivateInstalled` activates only a caller-packaged app at one
  canonical full path. Its schema-v1 receipt seals daemonkit's fresh 64-hex
  operation ID, exact build and policy, bundle and entitlement digests, Team
  ID, signing identifier, designated requirement, CDHash, inode, service plan,
  and operation-bound readiness proof before reporting active.
- `StatusInstalled` distinguishes an exactly verified but unactivated app from
  prepared and active receipts. `AttestInstalled` returns the read-only signed
  bundle, entitlement, tree, and file-identity facts consumed by activation.
  `DeactivateInstalled` requires exact receipt
  ownership, quiesces through a request-scoped runtime stopper, removes only
  receipt and service state, and leaves the packaged app untouched.
- Swift `StaticSessionServiceRuntime<Request, Response>` owns one typed,
  same-EUID service generation from listener acquisition through Ready,
  draining, request settlement, unlink, and retained terminal result.
- `SessionServiceHandler`, `SessionServiceCodec`, and
  `SessionServiceConfiguration` make the product route and every transport
  bound explicit while keeping raw socket requests and responses internal.
- The Swift service runtime owns receipt and readiness control operations;
  `ServiceSocketClient` follows an authenticated successor without product
  code implementing lifecycle framing or retry policy.

### Changed

- Protected runtime controls are exact unary calls with an empty tenant and
  are rejected before product dispatch when their framing is incomplete.
- A session owns at most one readiness subscription. Duplicate registration is
  rejected with `SocketResponseCode.readinessSubscriptionExists`; the service
  client retires that session and reconnects instead of replacing an active or
  terminal-settlement owner.
- Trust is evaluated against the peer's effective UID before a session can
  reserve capacity or send application bytes.
- Shutdown has one deadline-independent settlement task. A caller deadline can
  expire, but a later shutdown joins the same drain and reaping work.

### Removed

- `Deploy`, `Recover`, staged replacement, artifact-driven signed-app
  publication, and `WithSignedAppDeploy` are removed. Packaging owns app bytes;
  daemonkit owns activation only, with no compatibility or adoption path.
- Swift consumers no longer construct public raw `SocketServer`,
  `SocketRequest`, or `SocketResponse` service loops. There is no compatibility
  wrapper for the deleted public server surface.

## [0.14.0] - 2026-07-23

### Changed

- Swift `BrokerSocketBridge` now requires a lifecycle
  `RuntimeClientConfiguration` and a distinct, nonempty `handoffRole`. The
  lifecycle session performs only receipt and readiness preflight; a separate
  persistent handoff session sends only `daemon.broker-handoff.v1`, pinned to
  the exact ready-runtime receipt.
- `deployment.RuntimeStopControlStore` returns the exact `*proc.FileStore`
  consumed by holders, without an interface assertion.
- Trust policies allow any exact, disjoint lifecycle role topology that fits
  the server's configured session capacity; the lifecycle-specific two-role
  ceiling is removed.

### Removed

- The single-role `BrokerSocketBridge` initializer and lifecycle-session
  handoff path are removed. There is no compatibility API.

## [0.13.0] - 2026-07-23

### Fixed

- Swift service-client readiness fixtures use a private test operation instead
  of impersonating protected `daemon.*` authority. Production Swift servers
  continue to reject every `daemon.*` operation.
- Schema-archive tests use the typed `RecoveryTaskID` introduced by the runtime
  recovery hard cut, restoring the Go vet and lint gates.

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
- Go `wire.ServiceClient` and Swift `ServiceSocketClient` keep one lazy,
  exact-build session across service startup and replace it across drain,
  listener turnover, and takeover. Typed `runtime_starting` and
  `server_draining` response codes distinguish the only safe retry states.
- `service.StopBudget` and `StandardStopBudget` expose the exact identity,
  durable-tracking, child-settlement, parent-margin, and deferred-untrack
  phases that bound `Controller.StopRuntime`.

### Changed

- The runtime stack hard-cuts to authenticated broker socket handoff, explicit
  peer-role session binding, typed runtime recovery and durable stop replay,
  sealed spawned sessions, and composed lifecycle/workers/trust ownership.
- The application release template pins the shared staging and publication
  actions to their atomic-publication implementations.

### Fixed

- The rendered application cask guards its stop hook on the installed binary
  being executable and removes a binary-less husk left by an aborted upgrade,
  so `brew upgrade` no longer aborts with exit 127 when Homebrew has already
  moved the app aside.
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

[Unreleased]: https://github.com/yasyf/daemonkit/compare/v0.23.0...HEAD
[0.23.0]: https://github.com/yasyf/daemonkit/compare/v0.22.0...v0.23.0
[0.22.0]: https://github.com/yasyf/daemonkit/compare/v0.21.4...v0.22.0
[0.21.4]: https://github.com/yasyf/daemonkit/compare/v0.21.3...v0.21.4
[0.21.3]: https://github.com/yasyf/daemonkit/compare/v0.21.2...v0.21.3
[0.21.2]: https://github.com/yasyf/daemonkit/compare/v0.21.1...v0.21.2
[0.21.1]: https://github.com/yasyf/daemonkit/compare/v0.21.0...v0.21.1
[0.21.0]: https://github.com/yasyf/daemonkit/compare/v0.20.10...v0.21.0
[0.20.10]: https://github.com/yasyf/daemonkit/compare/v0.20.9...v0.20.10
[0.20.9]: https://github.com/yasyf/daemonkit/compare/v0.20.8...v0.20.9
[0.20.8]: https://github.com/yasyf/daemonkit/compare/v0.20.7...v0.20.8
[0.20.7]: https://github.com/yasyf/daemonkit/compare/v0.20.6...v0.20.7
[0.20.6]: https://github.com/yasyf/daemonkit/compare/v0.20.5...v0.20.6
[0.20.5]: https://github.com/yasyf/daemonkit/compare/v0.20.4...v0.20.5
[0.20.4]: https://github.com/yasyf/daemonkit/compare/v0.20.3...v0.20.4
[0.20.3]: https://github.com/yasyf/daemonkit/compare/v0.20.2...v0.20.3
[0.20.2]: https://github.com/yasyf/daemonkit/compare/v0.20.1...v0.20.2
[0.20.1]: https://github.com/yasyf/daemonkit/compare/v0.20.0...v0.20.1
[0.20.0]: https://github.com/yasyf/daemonkit/compare/v0.19.1...v0.20.0
[0.19.1]: https://github.com/yasyf/daemonkit/compare/v0.18.0...v0.19.1
[0.18.0]: https://github.com/yasyf/daemonkit/compare/v0.17.4...v0.18.0
[0.17.4]: https://github.com/yasyf/daemonkit/compare/v0.17.3...v0.17.4
[0.17.3]: https://github.com/yasyf/daemonkit/compare/v0.17.2...v0.17.3
[0.17.2]: https://github.com/yasyf/daemonkit/compare/v0.17.1...v0.17.2
[0.17.1]: https://github.com/yasyf/daemonkit/compare/v0.17.0...v0.17.1
[0.17.0]: https://github.com/yasyf/daemonkit/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/yasyf/daemonkit/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/yasyf/daemonkit/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/yasyf/daemonkit/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/yasyf/daemonkit/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/yasyf/daemonkit/compare/v0.10.0...v0.12.0
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
