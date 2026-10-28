# daemonkit — the lifecycle core, final design

**Migration decision (yasyf, 2026-07-29): the module path does not change.** The packages are
rewritten in place and cut as one breaking release; every consumer repins together. §8 is
written to that decision — the earlier `/v2` module path, its sentinel alias layer, and the
open-ended overlap window are not built. Elsewhere "v1" means the pre-cut release line and
"the cut" means this design's release.

**Trust removal (yasyf, 2026-10-27, cc-notes answer `0cfb108`).** The owner ruled that the
per-connection security checks go and that processes are trusted implicitly. Any process that can
open the socket in the user's 0700 state directory is trusted. The ruling removed the same-EUID floor, the kernel-only code-signature verifier (`internal/trust`,
`internal/csposture`), `Requirement`/`Requirements`/`PolicyDigest`/`Trust`/`Serving`,
`Cmd.Exec`, the separate control lane, the SHA-256 build digest, and `Ensure`'s `launchctl print`
on every call. This document describes the system after it. The one signature check left is
`deploy`'s install-time `codesign` verification of the app bundle.

Synthesized from four independent clean-slate designs (invariant-first, consumer-first,
failure-first, ownership-first) and three judgements (structural, failure-replay, adoptability).
Base: **consumer-first's surface** (winner on two lenses, second on the third), rebuilt on
**invariant-first's lifecycle body** (winner on failure-replay), with the grafts every judge
independently named: failure-first's observed Converge, unexported comparable identity, and
evidence-gated retry; ownership-first's protocol-blind drain admission and
observe-free/affect-gated readiness split.

Baseline read at `0e60747`. Every cite to current code is a migration anchor, never a shape to
preserve. Facts re-verified for this synthesis: wire defaults `wire/server.go:47-55`;
`DefaultShutdownTimeout`/`trustProbeTimeout` `daemon/runtime.go:27,29`; the 8 stop-control
`Record` fields `proc/reaper.go:91-100`; the claimed-record silent skip `proc/filestore.go:793-795`;
the Swift floor **unconditional** at `Sources/DaemonKit/SocketServer.swift:558` (non-optional
`uid_t` — consumer-first's "nil at three call sites" premise was about `sessionPolicy`, not the
floor, and is dropped).

---

## 1. The shape in one page

Every defect in the wreckage is one defect wearing four faces: a duty reachable from more than
one owner, a bound restated instead of inherited, an operation reporting what it did not do, and
a repair channel gated on the thing it repairs. The design is one noun, five verbs, and four
structural devices — one per face.

```
Daemon      one value. Both halves of a consumer read it; no fact is stated twice.
Serve       host it.   ctx + Daemon + Start → Drained. The ladder is one function body.
Open        reach it.  Daemon → *Client.
  .Ensure   make the running daemon be this exact build and ready → Ensured.
  .Business the product lane, a distinct type → Call → Reply.
  .Health   ask what is there — below dispatch, answerable during drain.
  .WaitReady  observe readiness — free, a subscription, no authority needed.
  .Drain    evict the incumbent pinned on the client's session → Stopped.
```

**Device 1 — `Budget` replaces every duration.** A `Budget` is a deadline being spent. Its
fields are unexported in the root package, so not even daemonkit's own `internal/` packages can
mint one from a duration — unexported fields cross package lines where a doc comment does not.
Durations enter the module at exactly two `Daemon` fields (`Shutdown`, `Handshake` — bounding
*disjoint* operations, so neither must nest inside the other) and at caller-context deadlines on
client entry points. Every inner bound is `b.Share(name, fraction)` (never later than its
parent) or `b.Reserve(name, fraction)` (a guaranteed *remainder* at the same final deadline — the
ack write and child settlement live here, so they can neither be starved nor outlive the deadline
they belong to; both budgets are live at once, and the property is non-starvation rather than
mutual exclusion). The handler bound is not a field at all: `Product.Handle`'s ctx
carries the **client's** deadline, inherited over the wire, so the Handler-vs-Shutdown sibling
pair that afflicted two of the four designs has no field to exist in. Today's six restated
handshake literals, `Reaper.Grace: 500ms`, `PeerVerificationTimeout: 10s`, and the readiness
triple 30/30/65 all stop being expressible.

**Device 2 — settlement authority is a closure local.** `Ctx.Spawn` starts one driver goroutine
that closes over the record, the store handle, and the reap authority. `*Child` exports
`PID()`, `Terminate()`, `Done() <-chan Exit` — no record field, no store field, no manager
field, and **no public kill or probe API anywhere in the module**, so kill authority never
leaves the library. All three adversarially-proven settlement evasions fail to compile; the
`go/ast` guard has nothing left to guard. `Done` is never pinned by an fsync: the durable retire
is a bounded-share handoff to the store's single-writer loop, and its non-confirmation is the
`RecordAbandoned` **value** inside `Exit` — named debt the next open reclaims — never a control
edge.

**Device 3 — outcomes are observed, not claimed.** The four operations that can partially
succeed (`Serve`, `Ensure`, `Client.Drain`, child exit) return closed outcome values whose zero
is never returned. `RecordRemoved` is constructible only from a post-write re-read of the store;
launchd convergence derives every `Action` from its own before/after observations of the world,
so an adapter's return value can only become a `Reason`; retry is constructible only from
launchd's own in-progress exits (36/37) or `Evidence` read out of launchd's log. `FileStore.Remove`
returning nil while skipping a claimed record (`proc/filestore.go:793-795`) has no analogue.

**Device 4 — the repair channel sits below every gate it might repair.** There is one session
kind (wire protocol 3), and transport admission reads `Protocol` alone. The hello is
`{protocol, schema, nonce}`, but the schema gates product dispatch, never admission. A product
request on a session whose schema is outside `Schemas` is answered `build_mismatch`, checked by
**set membership** (a build spanning an upgrade lists both eras). synckit's pre-dispatch
rejection contract survives *with* an upgrade path, and a successor of another schema era still
reads health and drains. The always-works drain is SIGTERM — a POSIX signal with no
compatibility axis — and the wire Drain verb is the fast path, admitted unconditionally below the
protocol gate (a frozen two-byte preamble, like the frame magic), so a new-era successor reaches
an old-era incumbent. A daemon speaking another protocol answers `ErrProtocolMismatch`, and
`Ensure` and `Stop` settle it by SIGTERM at its owner-record identity. The 18,999-handshake wedge
has no site to occur at, twice over.

The verifier child dies, and so does every per-connection check after it. No peer is judged:
no code signature, team, entitlement, or UID is read from a connecting process or a spawned
child, and nothing runs on the handshake path but the hello. That takes the fork-bomb primitive
out of every consumer binary, and all three A1 blockers with it. bbolt dies
(one fsynced JSON record file with a **frozen identity core** any future era can extract — so
archive-aside repairs without amnesia). The health route dies four times (a native transport
verb; the payload is a daemonkit envelope plus product bytes, so no consumer's frozen JSON
breaks).

**Size:** 3 new public packages (~80 exported symbols, ~55 of them in root) plus the 5 retained
unchanged (`artifact`, `bundle`, `ghrelease`, `version`, `paths`), ~7.2k core lines — from
16 / 1,272 / ~30k.

---

## 2. Core types

```go
// Package daemonkit hosts and reaches one detached daemon.
// Module github.com/yasyf/daemonkit — path unchanged across the cut.
package daemonkit

// ── identity ────────────────────────────────────────────────────────────────

// Daemon is one daemon's whole identity. The process that serves it and the
// process that launches it read this same value, so no fact is declared twice
// and none can disagree with another. Socket, lock, state dir, record file,
// and launchd job all derive from Label through paths.
type Daemon struct {
	Label       Label
	Program     Program  // the executable launchd runs
	Args        []string
	Schemas     []Schema // [0] is what this build speaks; the rest are prior eras still accepted
	Restart     Restart
	Shutdown    Grace  // the whole drain budget AND the plist's ExitTimeOut; 30s when zero
	Handshake   Grace  // the whole admission budget; 10s when zero (ptyhost: 500ms)
	Log         string // launchd's stderr sink
	MaxFrame    Bytes  // 4 MiB when zero; synckit 16 MiB, cc-interact/captain-hook 64 MiB
	Concurrency int    // in-flight requests; every queue depth derives; 8 when zero, captain-hook 64
}

type (
	Label  string
	Schema string
	Bytes  int64
)

// Grace is a whole operation budget and the only duration this API accepts.
// Shutdown and Handshake bound disjoint operations; every bound inside either
// is a Share or Reserve of it, so a sibling literal has no parameter to take.
type Grace time.Duration

// Program has two constructors — its two policies — and no fields to set.
// No constructor writes anything: placement is Ensure's, under the start lock.
type Program struct{ policy placement }

// Stable runs the daemon from a copy of the invoking executable, kept at
// ~/.daemonkit/bin/<Label>: outside any versioned install directory, so a
// package upgrade cannot delete the running program, and fixed across
// upgrades, so the plist's Program key stays put and path-keyed TCC grants
// survive. build is the version string the executable already reports, and
// it is the whole build identity: Ensure makes the copy and records the build
// at ~/.daemonkit/builds/<Label>, and copies again only when that differs.
func Stable(build string) (Program, error)

// InBundle names an executable inside a signed .app and never copies it:
// macOS keys TCC and notification grants to identifier, team, and path. Its
// build is the bundle's CFBundleShortVersionString.
func InBundle(app, rel string) (Program, error)

type Restart uint8

const (
	RestartNever Restart = iota
	RestartOnFailure
	RestartAlways
)

// ── budget ──────────────────────────────────────────────────────────────────

// Budget is a deadline being spent. Its fields are unexported, so not even
// internal packages can mint one from a duration: it enters from Shutdown or
// Handshake inside Serve, or from a caller ctx deadline at a client entry.
type Budget struct {
	deadline time.Time
	path     string // "shutdown/requests" — diagnostics only
}

// Share carves a named fraction of what remains; its deadline is never later
// than its parent's. An early-finishing phase's surplus flows to the next.
func (b Budget) Share(name string, of float64) Budget

// Reserve carves the tail out of b as a guaranteed remainder: work ends
// reserved before b's deadline and the tail (ack write, child settlement) runs
// to the deadline itself, so however long the work runs the tail still has its
// slice, and the tail cannot outlive the deadline it was carved from. Both are
// live at once — the property is non-starvation, not mutual exclusion.
func (b Budget) Reserve(name string, of float64) (work, tail Budget)

// ValidateForServe rejects a Daemon no derivation may see: the Label must be
// one launchd itself would accept, since every path, lock, socket, and job
// name is joined from it, and each Grace must fall in (0, 24h]. Serve and
// deploy.Open run it once, at the config boundary, so no arithmetic below it
// can overflow or invert.
func (d Daemon) ValidateForServe() error

func (b Budget) Left() time.Duration
func (b Budget) Context(parent context.Context) (context.Context, context.CancelFunc)

// ── serving ─────────────────────────────────────────────────────────────────

// Serve owns the daemon's whole life in program order: arm signals → flock →
// recover prior children → bind → start → ready → serve → drain → release.
// The steps are not callable, so they cannot be reordered, skipped, or
// claimed twice, and recovery cannot precede singleton ownership.
func Serve(ctx context.Context, d Daemon, start Start) (Drained, error)

// Start builds the product once ownership is proven. Its return IS readiness.
type Start func(Ctx) (Product, error)

// Ctx is what a starting product holds. It carries no knobs.
type Ctx struct {
	Context   context.Context // cancelled when the drain begins
	Reclaimed []Reclaimed     // the prior generation's children, already settled
	Report    func(detail []byte) // the product's half of Health.Detail, verbatim bytes
	Stop      func(error)         // product-initiated drain (ptyhost's child-exit arm)
}

// Spawn starts an owned child. Its record is durable before its first
// instruction runs: spawned suspended, continued only after the fsync.
func (Ctx) Spawn(Cmd) (*Child, error)

// Run executes one bounded disposable command, reap included. ctx must carry
// a deadline — the run's whole budget derives from it (synckit's 12-minute
// Touch ID run is context.WithTimeout at synckit's own boundary).
func (Ctx) Run(ctx context.Context, c Cmd) (Result, error)

// Child is a running owned process. It has no record, store, or reaper field:
// settlement's only executor is the driver goroutine Spawn started, which
// closes over all three. Nothing else compiles.
type Child struct{ /* unexported */ }

func (*Child) PID() int
func (*Child) Terminate()        // a demand: non-blocking, idempotent, unordered with Done
func (*Child) Done() <-chan Exit // the terminal, published exactly once, never pinned by fsync

// Product is the consumer's daemon. Handle owns dispatch — no route table, no
// registry — and its ctx carries the client's own deadline, inherited over
// the wire: there is no server-side handler knob to misfit against Shutdown.
type Product interface {
	Handle(ctx context.Context, req Request) (Reply, error)
	Drain(Budget) error
	Close(Budget) error
}

// Reloader marks a product for which SIGHUP means reload. Any other product
// drains gracefully on SIGHUP (ptyhost's terminator, without the hang).
type Reloader interface{ Reload(Budget) error }

type Request struct {
	Op   string
	Body []byte
	// Caller is the immediate socket peer's kernel identity as data — behind a
	// byte proxy it is the proxy, not the originator.
	Caller Caller
	// Session names the accepted session this request arrived on.
	Session Session
}

type Reply struct{ Body []byte }

type Caller struct {
	UID uint32
	PID int
}

// Session is one accepted client session: a comparable token products key
// per-session state by, with the close signal that releases it.
type Session struct{ /* unexported: id + done channel */ }

func (s Session) ID() uint64
func (s Session) Done() <-chan struct{}

// ── reaching ────────────────────────────────────────────────────────────────

// Open prepares a client and performs no I/O beyond validation: a malformed
// Daemon fails here, not at first Call. Every call refuses a context without a
// deadline: all stall bounds derive from it. Health, Drain, WaitReady, and
// Ensure's observation ride the client's one session; the first Business lane
// takes it over, so a command that converges and then calls opens one
// connection.
func Open(d Daemon) (*Client, error)
func (Daemon) ValidateForClient() error

func (*Client) Business() *Business // no I/O; the session is acquired on first Call
func (*Client) Health(ctx context.Context) (Health, error) // below dispatch and the schema gate; answers during drain
func (*Client) WaitReady(ctx context.Context) (Health, error) // a subscription, never a poll; no authority
func (*Client) Ensure(ctx context.Context) (Ensured, error)
func (*Client) Drain(ctx context.Context, e Expect) (Stopped, error) // refuses an incumbent e does not name

func (*Business) Call(ctx context.Context, op string, body []byte) (Reply, error)

// ── outcomes: nothing reports success without saying what it did ────────────

// Ensured is what Ensure did. A caller learns nothing by discarding it.
type Ensured struct {
	Before Health // zero when nothing was running
	Did    Action
	After  Health
}

type Action uint8

const (
	actionInvalid Action = iota // zero: never returned
	ActionNothing
	ActionStarted
	ActionUpgraded
	ActionRestarted
)

// Stopped proves what became of the daemon. Only Reap asserts the process is
// gone, and it is reached by observing the process table — never by a
// delivered signal.
type Stopped struct {
	Before Health
	Reap   Reap
}

type Reap uint8

const (
	ReapUndetermined Reap = iota // never returned: undetermined keeps the record
	ReapAbsent
	ReapCrossBoot
	ReapReused
	ReapTerminated
)

// Exit is a child's terminal value. Record is observed by re-reading the
// store after the write; a fate the store merely claimed cannot be published.
type Exit struct {
	Code   int
	Reap   Reap
	Record RecordFate
}

type RecordFate uint8

const (
	recordInvalid   RecordFate = iota
	RecordRemoved              // the post-write re-read proved the record gone
	RecordAbandoned            // not confirmed within its share; the next open reclaims it
)

type Reclaimed struct {
	PID  int
	Exit Exit
}

// Drained is what a shutdown achieved. A non-empty Abandoned deliberately
// retains the flock; launchd's ExitTimeOut — the same Shutdown field — is
// the backstop that SIGKILLs and releases it with the process.
type Drained struct {
	Settled   []Stage
	Abandoned []Stage
	Children  []Exit
	Archived  string // non-empty: where an unreadable record file was moved
}

type Stage uint8

const (
	StageIntake Stage = iota
	StageRequests
	StageProductDrain
	StageProductClose
	StageChildren
)

// Outcome is what the transport proved about one request.
type Outcome uint8

const (
	outcomeInvalid Outcome = iota
	NotSent        // proven non-dispatch; the only replayable outcome
	Delivered      // the peer acked a terminal; never replay, even an error reply
	Unproven       // sent, unacked; only the peer knows
	RejectedByPeer // typed refusal: NotReady, Draining, Busy, schema
)

// Health is the envelope daemonkit owns plus the product bytes it does not.
// Build is the version string the serving Program reports; Ensure's decide
// step compares it, and no wire gate does. Detail carries the product's
// Report bytes verbatim — every consumer's frozen health JSON lives there.
type Health struct {
	Phase      Phase
	Protocol   uint16
	Generation uint64
	PID        int
	Build      string
	Detail     []byte
}

type Phase uint8

const (
	phaseInvalid Phase = iota
	PhaseStarting
	PhaseReady
	PhaseDraining
)

type Cmd struct {
	Path, Dir            string
	Args, Env            []string
	Stdin                []byte
	MaxStdout, MaxStderr Bytes
	Session              bool // dedicated session+group; its id is durable kill identity
}

type Result struct {
	Exit           Exit
	Stdout, Stderr []byte
	Truncated      bool
}
```

Sentinels (`ErrBusy`, `ErrAbsent`, `ErrNotReady`, `ErrDraining`, `ErrFrameTooLarge`,
`ErrProtocolMismatch`, `ErrSchemaRejected`) are the whole error vocabulary. The launchd verb is
its own public package because three consumers converge agents for processes daemonkit does not
serve (cc-patch, cc-present, cc-orchestrate):

```go
package launchd

// Converge applies the caller's complete fresh desired set and reports what
// it observed and did. Actions derive from Converge's own before/after reads
// of launchd's world; an adapter's return can only become a Reason.
func Converge(ctx context.Context, agents []Agent) Convergence

type Agent struct {
	Label    daemonkit.Label
	Program  string
	Args     []string
	Log      string
	Schedule Schedule // NoRestart | KeepAlive(Restart) | Calendar(...)
}

func Daily(hour, min int) Schedule
func CalendarInterval(...) Schedule
func (Agent) PlistPath() string

type Convergence struct {
	Applied []daemonkit.Label
	Drifted []Drift   // reportable, never an error
	Refused []Refusal
}

type Refusal struct {
	Label  daemonkit.Label
	Kind   RefusalKind
	Detail string // launchd's own words, never daemonkit's guess
}

type RefusalKind uint8

const (
	refusalInvalid    RefusalKind = iota
	RefusalNotKnown               // exits 3 and 113
	RefusalInProgress             // exits 36 and 37: the only pair retryable from the code alone
	RefusalAggregate              // exit 5: resolves through launchd's log or not at all
)
```

`daemonkit/deploy` (sealed `Install` / `Activate` / `Supersede` / `Uninstall` / `Reset` of one
signed app over `artifact`, each generation checked by `codesign --verify` against a
`deploy.Requirement{TeamID, SigningIdentifier}` before it lands, one swap receipt, CDHash as
generation identity) completes the public surface; its shape follows failure-first's vocabulary and closes P2 `c1997af` with
`Reset`.

---

## 3. The invariant table

Each row: the invariant, and the structural reason it cannot be violated. "A test checks it" and
"reads as deliberate at review" appear nowhere in the right column — the honest non-structural
residue is quarantined below the table, not dressed as invariants.

| # | Invariant | Why it is unrepresentable |
|---|---|---|
| I1 | Every bound is inherited and subdivided; zero sibling literals | `Budget`'s fields are unexported in the root package, so no package — `internal/` included — can construct one from a duration. Durations exist at exactly two `Daemon` fields bounding disjoint operations, plus caller-ctx deadlines at client entries. No internal function takes a `time.Duration`; a restated bound has no parameter to be passed as. |
| I2 | An ack cannot extend the deadline it acknowledges within | The ack allowance is `Reserve`'s **guaranteed remainder** of the same handshake `Budget`: the work share ends `reserved` before the parent's deadline, so however long the work runs the tail still has its slice, and the tail's deadline is the parent's, so it cannot outlive it. There is no `now.Add` to write because there is no duration to add. (Earlier wording said the two are "disjoint", which a reviewer reasonably read as the two contexts never being live at once — they are; the property is non-starvation, not exclusivity.) |
| I3 | The handler bound cannot misfit its siblings | There is no server-side handler field. `Handle`'s ctx carries the client's own deadline, conveyed on the wire; a bound that is inherited cannot be misfit against `Shutdown`. |
| I4 | Settlement has exactly one executor per child | The driver's record, store handle, and reap authority are closure locals of the goroutine `Spawn` starts. Go exposes no path to another function's locals; `Child` has no record/store/manager field, so both prior evasions (`c.driver.…`, `go c.manager.reaper.Untrack(ctx, c.record)`) have no selector to name. The AST guard deletes. |
| I5 | `Done` closes unconditionally and is never pinned by an fsync | The driver's settle path contains no fsync call: durable retire is a handoff to the store's single-writer loop, waited only for a bounded `Reserve` tail; non-confirmation is the `RecordAbandoned` value inside `Exit`, never a control edge between the exit proof and the channel close. |
| I6 | A record fate cannot be claimed, only observed | The internal store's mutation API returns the post-write re-read; `RecordRemoved` is constructible only from an observed absence. The claimed-record silent skip (`proc/filestore.go:793-795`) yields `RecordAbandoned`, because the re-read still sees the record. |
| I7 | No running child without a durable record | The child is spawned `POSIX_SPAWN_START_SUSPENDED`; the record's fsync returns before SIGCONT, both inside `Spawn`'s body. (Non-darwin: named residue below.) |
| I8 | Nothing signals a stale identity; identity is compared at one site | There is no public kill, probe, or ID type: kill authority never leaves the library. Internally, identity is an unexported comparable struct, revalidated inside the single signal function immediately before delivery; init and self are refused at the same site. Widening the tuple is one declaration. |
| I9 | Undetermined is never Dead | `Reap`'s zero value is `ReapUndetermined` and no publish site emits it: a failed probe keeps the record and publishes nothing. There is no boolean for the tri-state to collapse into. |
| I10 | One live daemon per socket; recovery cannot race a healthy incumbent | Program order inside `Serve`: flock (inode-identified, never unlinked) is acquired *before* the store opens and recovery runs, and the listener binds after. The listener is a local of `Serve`'s frame; no API returns it and no second bind site exists. |
| I11 | Ready is published exactly once, exactly when the product exists | Ready is `Start`'s return inside `Serve`. There is no publish call to invoke early, twice, or never. |
| I12 | The listener and lock release only after the product's close is proven or named | Program order; `Product.Close`'s result is recorded in `Drained` before release, and a non-empty `Abandoned` retains the flock. The plist's `ExitTimeOut` renders from the same `Shutdown` field `Serve` spends: one fact, two readers. |
| I13 | An application schema never gates the transport or the repair | Admission reads `Protocol` alone. The hello carries the client's schema, but `Schemas` is checked only at product dispatch, by set membership, answering `build_mismatch`; `Health`, `WaitReady`, and `Drain` never see it, so a successor of another schema era still reads health and drains. |
| I14 | The repair channel has no compatibility axis | SIGTERM is the drain mechanism of record — a POSIX signal has no protocol version. The wire Drain verb is the fast path, admitted unconditionally on a frozen two-byte preamble *below* the protocol gate: protocol-blind, and granting nothing SIGTERM does not already grant the process that can open the socket. |
| I16 | Broken-era durable state never blocks the release that fixes it — and cannot orphan children | `Open` has no failed-schema path. The record file's envelope — the schema int and each record's `{pid, start, boot, generation}` identity core — is frozen forever, additive-only beyond it. An unknown-schema file yields its identity cores for the reap sweep, then archives aside; `Drained.Archived` / `Ctx.Reclaimed` name what happened. Repair without amnesia. |
| I17 | The business lane cannot spell a lifecycle request | `Business` carries only `Call` and `Close`; `Health` and `Drain` are `Client` methods. Server-side, lifecycle verbs are frame kinds dispatched before the op mux, and handlers receive `Caller` — identity as data, no methods. |
| I18 | `Delivered` never replays | `Outcome` is closed; the client's replay predicate admits exactly `NotSent`. captain-hook's "rejected before dispatch" mislabel of `Unproven` has no site: the outcome is returned, not interpreted. |
| I19 | A test binary cannot fork-bomb the machine | There is no self-exec verb to intercept: nothing in the module execs `os.Executable()`, and no argv-dispatched child mode exists for a test binary to re-enter. (`os.Executable()` survives only as a read: `Stable` resolves the path `Ensure` copies from, and `version.Running` stats it for a dev build's mtime — nothing execs it, and the copy `Ensure` places is written, never run, by this module. The earlier wording claimed the symbol appears nowhere, which is false and was caught by the phase-0 build.) `scripts/test.sh` stays as CI hygiene — the rule outlives the hazard. |
| I20 | A permanent refusal cannot be retried; exit 5 is never specific | One internal launchctl boundary returns a closed outcome; no `error` crosses it, so there is no second classifier and no fixture for a type production never emits. Retryable is constructible only from exits 36/37 or from `Evidence` read out of launchd's own log. |
| I21 | Convergence cannot report an effect it did not have | `Ensure` derives its `Action` from its own observations of the socket and the owner record (`internal/converge.Observe`), before and after; nothing it stored is read back as fact. launchd is read only by the step that applies the LaunchAgent, which runs only when `Ensure` installs or restarts. |
| I22 | Eviction is an explicit caller act | `Serve` refuses a live incumbent with `ErrBusy` — there is no takeover option on the serving side. The only evictors are `Ensure` (whose pure decide step chose upgrade/restart) and `Client.Drain` — both deliberate calls by the consumer. |

**Named non-structural residue** — the honest limits, quarantined here rather than dressed as
invariants: (1) `context.WithTimeout` exists in the language; a determined in-module contributor
can thread a literal around `Budget`. The structure makes it signature-less and grep-visible,
not impossible. (2) `RecordFate` is forgeable *within* `internal/proc`; the re-read discipline
is one package's single implementation. (3) Non-darwin `Spawn` has a microsecond
record-before-run window (CI-only platform; darwin is structural). (4) A record file corrupt
beyond its frozen envelope loses identities; the loss is named in `Drained.Archived`, not
silent. (5) `internal/state`'s `File[T]` has a legally constructible zero value, and no Go
device removes it: a method set attaches to a type rather than to the values its constructor
produced, and every nameable type has a zero — unexporting `File` would take the zero value and
the consumer's field declaration with it. So the unbound file is refused, not unrepresentable:
`Load` and `Store` open by minting the unexported `bound` path that every filesystem operation
in the package takes as its receiver, at the one site that returns `ErrUnbound` for an empty
one. No operation can reach the disk on a path that site did not resolve, and in particular a
read of a file that was never named answers a refusal instead of fresh state — but the barrier
is a returned error, one deliberate in-package conversion away, exactly as in (1).

---

## 4. Package layout

| Package | Owns | ≈ lines |
|---|---|---|
| `daemonkit` (root) | `Daemon`, `Budget`, `Serve`, `Open`/`Client`/`Business`, `Product`, `Ctx`, `Child`, `Run`, every outcome type, sentinels. The entire lifecycle API. | 800 |
| `daemonkit/launchd` | `Agent`, `Converge`, `Convergence`, `Refusal`, plist render, `Daily`/`CalendarInterval`/`PlistPath`/`NoRestart`. Public: cc-patch, cc-present, cc-orchestrate converge agents for processes daemonkit does not serve. | 700 |
| `daemonkit/deploy` | sealed `Install`/`Activate`/`Supersede`/`Uninstall`/`Reset` of one signed app over `artifact`; install-time `codesign --verify` against `deploy.Requirement`; one swap receipt; CDHash as generation identity. | 900 |
| `daemonkit/internal/wire` | frame v1 byte-identical (K27), one session kind (protocol 3), hello `{protocol, schema, nonce}`/ack (ack carries `Phase`), the frozen Drain preamble, FrameAck (K32), SCM_RIGHTS + single-use nonce (K30/K31), phase stream, server. | 2,000 |
| `daemonkit/internal/proc` | unexported comparable identity, suspended spawn, the driver, the record store (frozen identity core, single-writer loop), flock, the reap ladder, the one comparison site. | 1,400 |
| `daemonkit/internal/converge` | `World` and `Observe`: one observation of the socket and the owner record, re-derived at read time. launchd is not part of it. | 200 |
| `daemonkit/internal/state` | write-temp → fsync → rename → fsync-dir; frozen-envelope extraction; archive-aside. | 250 |
| `artifact` `bundle` `ghrelease` `version` `paths` | unchanged; `paths` gains `Socket(name)` with a typed error on overlong socket paths (K34/K51). | — |
| `Sources/DaemonKit` | the client mirror and the fd bridge only (§5.4). | 2,500 |

Deleted as packages: `daemon`, `wire` (public), `proc` (public), `worker`, `trust` (public),
`service`, `codeidentity`, `peer`, `templates`, `wire/wiretest`, and, by the 2026-10-27 ruling,
`internal/trust` and `internal/csposture`. `internal/` is the point:
"the packages ARE the deliberate public API" is root cause RC5 stated as a principle, and
inverting it is the single largest reduction available — a consumer cannot rebuild a serve
ladder from parts that are not exported.

---

## 5. The lifecycle

### 5.1 `Serve` — ordered, and the order is not a choice

| # | Step | Notes |
|---|---|---|
| 1 | **arm** — SIGINT/SIGTERM/SIGHUP, before anything can block. SIGHUP → `Reload` if the product implements `Reloader`, else a graceful drain. | captain-hook gains signal handling it has nowhere today (`command.go:75-83`); ptyhost's SIGHUP-terminates becomes SIGHUP-drains-in-5s. |
| 2 | **own** — flock by inode (polled, ctx-observing, never unlinked). `ErrBusy` on a live incumbent — no takeover exists here. | Recovery cannot race a healthy incumbent: ownership precedes it. |
| 3 | **recover** — open the record file; extract identity cores (frozen envelope, any era); for each prior-generation record: probe → TERM → grace (a `Share`) → probe → KILL → retire on observed absence. Undetermined keeps the record. Collect `[]Reclaimed`; archive an unknown-schema remainder aside. | |
| 4 | **bind** — unlink the stale socket file under the flock, listen. Health and phase answer immediately (`PhaseStarting`). | |
| 5 | **start** — the consumer's `Start(Ctx)`. An error tears down through steps 7–9 with business never opened. | |
| 6 | **ready** — `PhaseReady`; business admission opens; every handshake ack carries the phase; the phase stream wakes every `WaitReady`. `Health.Build` is `Daemon.Program`'s build, so `Serve` needs a `Program`. | |
| 7 | **serve** — accept → first frame: the frozen Drain preamble dispatches to the drain path unconditionally; anything else is the hello → protocol gate → session ack, under `Handshake`'s budget with the ack as `Reserve`'s tail. No peer is judged. Dispatch: a product request on a session whose schema is outside `Schemas` is answered `build_mismatch`; otherwise, slot → `Product.Handle` under the client's conveyed deadline; terminal → FrameAck. | |
| 8 | **drain** — triggered by ctx, signal, `Ctx.Stop`, or the wire Drain verb `Client.Drain` sends: one path, four triggers. | |
| 9 | **release** — listener closed, flock released **iff** `Drained.Abandoned` is empty; otherwise deliberately retained until launchd's `ExitTimeOut` (same field) SIGKILLs. | |

The drain budget is minted from `Shutdown` once:
`work, children := b.Reserve("children", 0.15)` — child settlement is the guaranteed tail a
wedged product cannot starve — then shares of `work`: `intake` 0.05 (stop accepting; Health
still answers), `requests` 0.40 (join the dispatch group; every admitted request reaches a
terminal and its ack), `drain` 0.30 (`Product.Drain`), `close` 0.25 (`Product.Close`). Each
expiry names its `Stage` in `Abandoned`.

### 5.2 `Ensure` — the seven-step ladder, shipped once

lock (two `Ensure`s serialize) → **place** — `Stable` copies the executable only when the build
recorded at `~/.daemonkit/builds/<Label>` differs → observe `Health` on the client's session and
the owner record → **decide** — a pure function `(Health, build) Action`, cc-interact's
`runtimeAction` (`launcher.go:212-240`) promoted verbatim: newer-build refusal,
incomplete-identity refusal, upgrade, drain-observe, failed→restart, healthy→noop; I/O-free and
table-testable. A daemon already serving the wanted build is `ActionNothing`, and launchd is not
asked → otherwise `Client.Drain` pinned to the observed build and generation, or SIGTERM at the
owner-record identity for a daemon speaking another protocol; observe the process leave
(`Stopped.Reap`) → apply the LaunchAgent (rewritten and reloaded when drifted, kickstarted always) → `WaitReady` for the
exact build — a subscription that deletes all four fleet poll loops (25ms cc-interact, 10ms
ptyhost, 100ms captain-hook, 100ms cc-notes) → re-observe → `Ensured{Before, Did, After}`. A
plist that drifted under a daemon already at the wanted build stays until that daemon is next
replaced.

### 5.3 `Call` — the four hand-rolled clients, unified

One live session per `Client`, shared by `Health`, `Drain`, `WaitReady`, and `Ensure`'s
observation until the first `Business` lane takes it over, so a command that converges and then
calls opens one connection. Behind it: a mutex, single-flight dial, generation-tagged retire, waiter-refcounted
teardown, graceful `Close`. Two rules the consumers each got half of: **retire only on transport
failure** (a `NotReady`/`Draining` rejection proves the session healthy; retry in-session on a
share of the caller's deadline — ptyhost's L1 preserved without the redial-per-poll); **replay
only `NotSent`**. `Probe` replaces cc-interact's eager-dial construction probe (L2); the
implicit default deadline stays at cc-interact's own boundary (L3) — a library that invents a
deadline is choosing a product policy.

### 5.4 What Swift mirrors

The client half and the fd bridge, never the server half: the v1 frame byte-exact (one generated
schema, one shared golden, a CI no-op gate — restoring what `7ec51bc` deleted), the
protocol-3 hello `{protocol, schema, nonce}` and the phase-carrying ack,
the frozen Drain preamble, `Outcome` semantics (`Delivered` never replays), FrameAck, and the
SCM_RIGHTS handoff with its single-use nonce, plus the `~/.daemonkit/a/<label>` state layout a
client needs to find the socket it dials (`AgentPaths`, pinned to `paths.Agent` by one shared
golden). Swift does not mirror `Serve`, `launchd`, child
ownership, or server-side budgets — it has no daemon to host.

---

## 6. The deletion list

Default answer: nothing survives. Deleted, with why nothing needs it:

| Mechanism | Why nothing needs it |
|---|---|
| Reap-receipt ledger (sequences, floors, digests, `ReapReceipt`, `RecoverReapReceipts`, `VerifyReapReceipt`) | The record's absence asserts what a receipt asserted; recovery re-derives from store × process table. Its sole fleet consumer verifies then unconditionally acks (`ptyhost/host.go:226-232`); `service/` acks its own with no-op callbacks. |
| `RecoveryCapability` / `.Consume()` / recovery receipts | Reclamation is a value — `Ctx.Reclaimed` — inside `Serve`, before `Start`; no consumer-visible recovery step remains to authorize. fusekit's six passes and ptyhost's 47-line reconcile become a slice read. |
| Three capability-token registries (`fenceauth`, `runtimeauth`, `spawnedsession`) | Lanes are types; capabilities are closure locals and unexported fields. A registry exists to pass authority through code that shouldn't hold it; no such code remains. |
| Three stacked lifecycle machines + four persisted phase machines (`Activation`, `Stage`, `CommitReady`, `ClaimProductSettlement`, both enums) | One in-memory `Phase`; ready is `Start`'s return; order is program order. Durable state records observations, never phases. |
| Stop authorities, prepared-stop ceremony, `internal/stopcontrol`, the 8 stop-control `Record` fields (`proc/reaper.go:91-100`) | Stop is the wire Drain verb or a POSIX signal, and neither needs an authority: any process that can open the socket may send either (K13 honored by vacancy). |
| The `go/ast` settlement guard + `settledUntrack*` + `//nolint:contextcheck` | The authority is a closure local; nothing remains for a walker to forbid. |
| The trust verifier child (`RunVerifierChild`, self-probe, `trustProbeTimeout` `daemon/runtime.go:29`, `VerifierWorkerBudgets`, the `/bin/sh` spawn gate + `trap ':' TERM`) | No peer is verified. Deletes the fork-bomb primitive from every consumer binary, the 15 `main` dispatch sites, and all three A1 blockers; the 1.78–2.69s "verifier cost" was host fork/exec latency (A1). The VFS-hang and CF-crash properties the child bought are **deleted** with the verification they protected. |
| `SecCodeCopyGuestWithAttributes` / `SecRequirementCreateWithString` / `SecCodeCheckValidityWithErrors` / `SecCodeCopySigningInformation` and the whole purego Security.framework binding (`loadSecurity`, ~19 symbols) | Nothing replaces them: no peer is verified. |
| `codeidentity` as a second verifier | No verifier remains to be second to; the fork-divergence generator (P0 `7059185`) has no implementation to diverge from. `deploy`'s install check takes its own `deploy.Requirement`. |
| The same-EUID floor, the kernel-only `csops_audittoken` verifier, `internal/trust`, `internal/csposture`, and the `daemonkit_unsigned` build tag (2026-10-27) | The socket sits in the user's 0700 state directory, and any process that can open it is trusted. No code signature, team, entitlement, designated requirement, or UID is checked on a connecting process or a spawned child. |
| `Requirement`, `Requirements`, `EntitlementMatch`/`EntitlementRequirement`, `PolicyDigest`, `Serving`/`ServingSigned`/`ServingSameUser`, `Trust` and `Daemon.Trust`, `Cmd.Exec`, `ErrUntrusted`, `ErrPeerGone`, `ErrNoVerifier` (2026-10-27) | Nothing is pinned, so nothing names a pin or reports one failing. |
| The separate control connection: `Control`, `Client.Control`, the lane field in the hello (2026-10-27) | One session kind (wire protocol 3). `Client.Health` and `Client.Drain(ctx, Expect)` ride the client's one session, and the first `Business` lane takes it over. |
| The SHA-256 build digest of the executable (2026-10-27) | The build is the version string the tool already reports: `Stable(build)`, `InBundle`'s `CFBundleShortVersionString`, published by `Serve` from `Program`. Placement compares the record at `~/.daemonkit/builds/<Label>`. |
| `Ensure`'s `launchctl print` on every call, `converge.World.Applied` (2026-10-27) | A daemon already serving the wanted build is `ActionNothing` without asking launchd; the apply step reads launchd when it installs or restarts. |
| `worker` as a package (pool claims, `ClaimRuntime`→`Recover`→`Activate` dance, 15 nil-receiver guards) | `Ctx.Run` is one bounded command; capacity is the caller's semaphore (synckit already keeps one). Two consumers build 1-byte degenerate pools purely to satisfy `RuntimeConfig.Workers`; the field is gone. |
| Credit/ack flow control, observation slots, per-frame build re-checks, `writerStateMu`/`beginWriterDrain`/the writer-identity panic | Backpressure is the `Concurrency` group plus the socket buffer; the writer owns the fd; observation is a native verb, not a slotted lane. FrameAck survives as terminal-reached-peer proof (K32) — a different mechanism. |
| Seven version axes → two; five schema fingerprints; nine build gates | The wire `Protocol` and the store schema int. `Schemas` is set membership at product dispatch — synckit's pre-dispatch contract kept, with an upgrade path. `Build` gates nothing on the wire; only `Ensure`'s decide step compares it. |
| bbolt + open-per-operation flock | One fsynced JSON record file behind one owner with a frozen identity envelope; the measured 16-writer 5.003s contention was the open-per-op pattern. |
| `PeerVerificationTimeout`, `HandshakeTimeout`-as-wire-field, `NoProgressTimeout`, `ListenerWait`, `MaxSessions`, `Backlog`, `InboundQueue`, `OutboundQueue`, `StreamQueue`, reaper `Grace`/`Settlement` | Derived from `Concurrency` or subdivided from `Shutdown`/`Handshake`/caller deadlines. captain-hook's 10s clamp and its apology comment die with the field. |
| The route registry (`HandlerSpec`, `Register`, `ObservationRoute`, `MaxResponseBytes`, ladder rungs) + four independently-written health routes | `Product.Handle` owns dispatch; health is a native verb answered pre-dispatch, drain included. |
| `reconcileMode`, `NewController`'s open-time replay and its four wedge doors (`002e2e9a`) | Converge reads the world; there is no stored intent to replay, and partial success has a return path (`Convergence`). |
| Two disagreeing launchctl classifiers + `launchctlEIOExit` retry + `bootstrapAttempts = 3` + `LimitLoadToSessionType`/`SessionType` | One boundary returning a closed outcome (I20); the field that re-acquires exit-5 EIO does not exist. |
| `ServiceClient` spanning two lanes + the four hand-rolled clients + the 10ms/25ms/100ms polls | One `Client` session, taken over by the first `Business` lane, + the phase-stream subscription. |
| `daemon.State` as a daemonkit-owned wire value; `IdleExit`, `ActivityLease`, `Strikes`/`Backoff`/`Ladder`, `FileStamp`, `Nice`, `MonotonicUptime`, `EmbeddedProcess`, `Takeover`, `EnsureCurrent` (as exported machinery) | Health strings are product data in `Detail`; the rest are zero-consumer orphans (census DROPPED). The respawn-gate question is re-filed in §12, not silently lost. |
| The husk-removal band-aid in `cask.rb.tmpl` + four embedded homebrew-tap SHAs in `release.yml.tmpl` | An uninstall runs `Client.Drain` and asserts `Stopped.Reap`; a husk is prevented, not deleted after the fact. |

**Survivors, named because the default is none:** the v1 frame layout (K27); FrameAck (K32);
the SCM_RIGHTS handoff and its single-use nonce (K30/K31 — captain-hook's signed `.app` depends
on it; the *ceremony* around the nonce dies); flock-by-inode (K6/K7); `proc.ErrLockBusy`'s
identity (as `ErrBusy`, aliased from v1 — §8); write-temp→fsync→rename→fsync-dir (K15);
archive-aside, generalized to the only schema behavior and repaired of its amnesia (I16);
`artifact`'s security trio byte-identical; `open -g -W` app-agent semantics and goldens (K40);
`scripts/test.sh`'s ulimit cap (the rule outlives the hazard).

---

## 7. Consumer impact

Every consumer deletes; none loses a capability. Rows corrected against the adoptability
judge's source checks (cc-patch, cc-present, cc-review, binrun import daemonkit directly).

| Repo | Adopts | Deletes | Keeps / gains |
|---|---|---|---|
| **cc-interact** | `Serve` (MaxFrame 64MiB), `Client`/`Business`, `Ensure` (its `runtimeAction` promoted verbatim) | the Serve ladder (`server.go:260-306`), `client.go` (345), most of `launcher.go` (333), the degenerate 1-slot pool, the health route, the 25ms poll, ladder-rung arithmetic | `operationContext` and `handleTimeout` at its own boundary; `CallError` from `Outcome`+`Reply`. Net ≈ −700. |
| **cc-present** | one `daemonkit.Daemon` value + `launchd` | `daemonRoles`, the `trust.NewTrustPolicy` role-map builder (`app.go:73-82`), five constants, launcher plumbing — a source change, not a version bump: it imports `paths`×6, `trust`×4, `service`, `wire`, `daemon` directly | its own network token for LAN and tailnet HTTP clients; daemonkit pins nothing on its socket. Net ≈ −90. |
| **cc-review** | same | `internal/runtimeconfig/runtime.go` (52); direct `trust`/`version`/`service`/`proc`/`paths` imports | ≈ 12 lines of `Daemon`. Net ≈ −40. |
| **cc-orchestrate (ptyhost)** | `Serve` (Shutdown 5s, Handshake **500ms** — both real needs, both expressible) | `recoverPTYChild` (47), the 3-way select, signal arming, `Reaper.Grace: 500ms` (hand-computed share), the 10ms NotReady spin, `MaxSessions: 8` | `OnChildExit` → `Ctx.Stop(nil)`; SIGHUP drains gracefully in 5s; in-session NotReady retry preserved (B4 L1). Net ≈ −280. |
| **synckit** | `Serve` (MaxFrame 16MiB); `Schemas` set membership **preserves** the WireBuild pre-dispatch contract with an upgrade path; 12-min Touch ID run = `context.WithTimeout` at its boundary + its own semaphore | the serve ladder + two dropped close errors (`serve.go:167-168`), `helperruntime` (146), `withOwner` (~80), `rpc/client.go` session mgmt | **gains product settlement — a fixed latent bug** (no `ClaimProductSettlement` today, listener released without proof); SIGHUP reloads via `Reloader`; `TransportError` from `Outcome`. Net ≈ −350. |
| **reposync** | through synckit; `Daemon.Shutdown` | helperruntime types, its 30s restatement | Net ≈ −40. |
| **captain-hook** | `Serve` (Concurrency 64, MaxFrame 64MiB) | the Stage-first ladder, inline health route, `PeerVerificationTimeout: 10s` + comment, `Backlog: 192`/`InboundQueue: 256` arithmetic, the 204-line client, the errno mapping, `runDeploymentTool` dance, the 100ms poll | **gains signal handling it has nowhere today**; install hint via `ErrAbsent` (errno preserved under `errors.Join`); the OQ4 mislabel has no site; broker fd handoff untouched. Net ≈ −420. |
| **fusekit** | `Serve`; six recovery passes → `Ctx.Reclaimed`; `Signals` seam → `Serve` arms; readiness triple 30/30/65 → one ctx deadline | the 2-way select, `mountservice.RuntimeHealthObservation`, `defaultWorkerLimit` | the closest fit of the six; loses nothing. |
| **cc-notes** | `Ensure` + `Client.Drain`; `deployment` proofs (~41 usages of `RuntimeProof`/`ReadinessProof`/`RuntimeStopper`) migrate to `Client.Drain` + `deploy` | the 100ms poll, `RuntimeShutdownTimeout`, the proof plumbing, its policy digest | **Named cost:** its source-text contract test greps daemonkit literals (`release_contract_test.go`) and is rewritten in the same change. Net ≈ −250. |
| **cc-patch** | `launchd` as a leaf | — | `Agent`, `Daily`/`CalendarInterval`, `PlistPath()`, `NoRestart` all survive by name — it imports daemonkit directly (go.mod, no cc-interact). |
| **claude-pool / cc-pool** | repin; broker path bit-identical | leaked-process cleanup workarounds | — |
| **cookiesync** | repin | — | health JSON decodes from `Health.Detail` verbatim. |
| **binrun** | — | — | `artifact`/`ghrelease` untouched, imported directly; dependency cone shrinks (bbolt gone). |
| **authkit** | **unread in every lane** — named open question (§12), not silently assumed. | | |

### The five divergence axes, decided

Settlement claimed? — **bug**; `Serve` always joins `Product.Close`. `Shutdown`+`Wait` vs
`Close` — **neither exposed**; `Serve` returns `(Drained, error)`. Pre-settlement recovery —
**always**, step 3, after ownership. Shutdown budget — **one field**, mirrored into
`ExitTimeOut`. Signal arming — **`Serve`, first**; SIGHUP forks on `Reloader`.

### The knob audit (required artifact)

Surviving fields, each citing its consumer: `MaxFrame` (cc-interact + captain-hook 64MiB,
synckit 16MiB), `Concurrency` (captain-hook 64), `Shutdown` (ptyhost 5s), `Handshake` (ptyhost
500ms), `Restart`, `Schemas` (synckit), `Label`/`Program`/`Args`/`Log` (all).

Died as restated defaults: `ShutdownTimeout: 30s`×3, `MaxSessions: 64`, `Reaper.Settlement: 2s`,
children capacity 64×3. As hand-derived arithmetic: `Reaper.Grace: 500ms`, `Backlog: 192`,
`InboundQueue: 256`, `MaxTotalRun = ShutdownTimeout`, ladder rungs, `handleTimeout + 1s`. As
preference: `MaxSessions: 8`, children capacity 1. With their mechanism: `PeerVerificationTimeout`,
`Workers`, the whole `worker.Config`, the readiness triple, and on 2026-10-27 `Trust.Control`,
`Trust.Business`, and `Trust.Serving`.

---

## 8. Migration — one flag day, in place

**The module path does not change.** `github.com/yasyf/daemonkit` is rewritten in place and cut
as one breaking release; every consumer repins together. The `/v2` path, the sentinel alias
layer, and the open-ended overlap window were this section's earlier answer to the MVS diamond
(cc-orchestrate sits under cc-interact + cc-notes + reposync + synckit + daemonkit at once);
the diamond is instead dissolved by moving every repo in one event, so **none of that is built**.

What the single-major cut buys, stated plainly: a deleted symbol becomes a **compile error** in
each consumer rather than a silent `errors.Is` miss, because only one major is ever linked. The
"sentinel identity is load-bearing with no compile-time signal" hazard is specifically a
two-majors-linked hazard — with one path, `proc.ErrLockBusy`'s disappearance surfaces in
synckit's `hostregistry` and reposync's `state` at build time, which is the migration signal
rather than a risk to mitigate. What it costs is an 11-repo lockstep event: the exact shape that
produced three incompatible schema epochs in two days. Hence the gates below, none waivable.

1. **`main` was deliberately unbuildable-for-consumers during phases 1–3.** Phase intermediates
   may be broken; only the final state must cohere. As first written, this gate made fleet-build
   a **release gate**, not a per-commit gate, until phase 3 closed — and no tag until it was
   green — because its redness carried no information while the surface moved. Phase 3's close
   inverted the release-gate half (yasyf, 2026-08-01, `d691ed8`): the cut tags `v0.21.0`
   **before any consumer migrates**, so at tag time every fleet-build leg is red by construction
   — consumers still pin `v0.20.*` and import packages the cut deleted — and a tag gated on
   fleet-build is not late but unreachable. The tag's gate is `verify-main-gates`
   (`.github/workflows/release.yml`): CI, Guides, Export census, Docs drift, and Mixed era, all
   green on the tagged commit; fleet-build is deliberately absent from that list and gates the
   **close of the migration** instead — its redness now carries information, one red leg per
   unmigrated consumer, and it joins the release gate once the last leg turns green. An API
   mistake found mid-migration is fixed forward as `v0.21.1`, never by re-cutting the tag. The
   in-repo suite and `task build`/`task lint` stay green per commit, as they did throughout.
2. **The legacy record sweep survived the decision** — it answered durable state crossing an
   upgrade, not a module path. For one release cycle the recover step also read a legacy bbolt
   file if present, reaped its recorded children, and archived it, so a machine crashing
   mid-upgrade still settled its previous generation. The crash window failure-first refused and
   consumer-first hand-waved, answered head-on. That cycle is over and the sweep is deleted.
3. **The rename ledger is a build artifact, not a memory.** Before the cut, `scripts/export-census.sh`
   emits every deleted and renamed exported symbol; each consumer PR is checked against it. A
   symbol that moved without appearing there is a bug in the ledger, found before the tag rather
   than by a consumer at repin.
4. **The mixed-era gate is absolute**: `ci/mixedera` green in both directions — new drains old
   (via SIGTERM and via the frozen preamble), old client meets new daemon and gets a crisp
   `ErrProtocolMismatch`, never a hang — on every release, non-waivable. It reproduces the
   18,999-failure wedge in 2.4s and is the only check that would have caught it.
5. **Merge order is dependency order, not a schedule.** Every consumer PR is authored against the
   tagged release and adopts-and-deletes in the same diff; they merge in one event, ordered only
   where one repo consumes another's surface: cc-interact before cc-present/cc-review; synckit
   before reposync; cc-patch (a `launchd` leaf) and the pool/cookiesync repins last. **No repo is
   left behind** — though under the tag-first order (gate 1) a stalled repo is no longer a red
   release but an open migration: its fleet-build leg stays red, and fleet-build joins the
   release gate only when every leg is green.
6. **Bundle identity is untouched** — labels, teams, signing identifiers, install paths
   unchanged; no TCC or notification grant resets. `Stable`'s Label-keyed program path —
   `~/.daemonkit/bin/<Label>`, fixed across upgrades — is a plist change per consumer and needs
   one real-machine re-bootstrap test before the cut (§12).

---

## 9. Weakest point — answered honestly

**The weakest point is trust, by decision.** Since 2026-10-27 the socket's only gate is the
user's 0700 state directory. Any process running as that user can open it, call every product
operation, and drain the daemon, and the owner record beside it is just as writable. The ruling
takes that trade. A process running as the user could already signal the daemon, rewrite its
plist, or replace the copy at `~/.daemonkit/bin/<Label>`, so a per-connection check bought little
against its cost. A consumer that serves clients beyond the user's own processes keeps its own
credential at that boundary, as cc-present does for LAN and tailnet HTTP.

Second: **the drain's `requests` share is the only thing between a slow product and a retained
socket.** `Handle`'s ctx is the client's deadline, so a context-honoring handler cannot outlive
its caller — but a handler that ignores context lands `StageRequests` in `Abandoned`, and
`Serve` deliberately retains the flock until launchd's `ExitTimeOut` SIGKILLs. Correct, and a
worse operator experience than per-op timeouts. A reviewer arguing for a route table with per-op
budgets is arguing for the six restated bounds to come back; the trade is taken knowingly.

Third: **the `deploy` boundary is the least-verified line drawn.** The claim that cc-notes' and
captain-hook's runtime-proof/readiness-proof/stop machinery (~41 usages) migrates cleanly to
`Client.Drain` rests on reading call sites, not the semantics of `RuntimeProof`'s role binding.
If those proofs carry a property beyond "this exact incumbent left the process table", that is
understated scope — carried forward verbatim from consumer-first's admission because no lane verified it
since.

Fourth: **ready = `Start` returned is opinionated.** A product wanting pre-ready business
traffic or degraded-ready cannot express it. Every consumer's boot fits inside `Start` today; if
one surfaces that genuinely cannot, the shape bends here first.

---

## 10. Judge reconciliation

Every fatal flaw raised by any judge, and its disposition here. (S = structural,
F = failure-replay, A = adoptability.)

| # | Judge → design | Fatal | Disposition |
|---|---|---|---|
| 1 | S → consumer | server hello read has no bound; hidden literal | **Fixed**: `Daemon.Handshake` is the second `Grace` field — a named config boundary bounding a disjoint operation, ptyhost's 500ms home; inner shares/ack derive from it. |
| 2 | S → consumer | false Swift-floor premise | **Moot**: no floor exists in either language since 2026-10-27; no peer is judged. |
| 3 | S → consumer | I13 "reads as deliberate at review" is discipline | **Fixed**: no such invariant row; non-structural residue is quarantined below the table, named as residue. |
| 4 | S → consumer | `Child.Wait` undefined; Health redundant-axes bools | **Fixed**: types coherent (`Done() <-chan Exit` only); `Health` is envelope + `Detail` bytes — the redundant bools live in consumer-owned JSON, not the API. |
| 5 | S → invariant | `budget.Enter(d)` exported; `Reserve(tail duration)` | **Does not apply**: `Budget` has unexported fields in root, no duration constructor anywhere; `Reserve` takes a fraction. |
| 6 | S → invariant | Handler 35s vs Shutdown 30s sibling pair | **Fixed structurally**: no handler field exists; the bound is the client's conveyed deadline. |
| 7 | S → invariant | `proc.ID` fields exported | **Fixed**: identity is unexported and comparable in `internal/proc`; no public ID or kill API at all — stronger than any of the four. |
| 8 | S → invariant | public `wire` server core reopens the ladder vector | **Fixed**: everything mechanical is `internal/`; ~80 public symbols total. |
| 9 | S → ownership | `Child.rec` field + comment; in-package reachability | **Does not apply**: closure-local settlement; no record-typed field exists. |
| 10 | S → ownership | one client type spans lanes | **Does not apply**: `Business` carries only `Call`/`Close`; lifecycle verbs are `Client` methods. |
| 11 | S → ownership | `Peer{lifecycle bool}` mode flag | **Does not apply**: `Business` and `Client` are distinct types; server lifecycle verbs are frame kinds pre-mux; handlers get `Caller` (data, no methods). |
| 12 | S → ownership | config sibling pair + SIGHUP two owners | **Fixed**: no handler knob; `Serve` arms all signals; SIGHUP forks on `Reloader`. |
| 13 | S/A → failure | consumer re-owns signals/drain sequencing (captain-hook's bug survives) | **Does not apply**: `Serve` owns both; the ladder is one function body. |
| 14 | S/F → failure | holdable `Live` breaks revalidate-before-signal | **Does not apply**: no public probe/signal; revalidation is inside the one internal signal site, immediately before delivery. |
| 15 | S/A → failure | `Business` embeds `Control`: unsigned same-UID drain, K48 collision | **Accepted by ruling** (2026-10-27): any process that can open the socket may drain, which grants nothing SIGTERM does not; repair = SIGTERM + the protocol-blind Drain verb. |
| 16 | S → failure | "zero duration constants" with no admission bound | **Fixed** as #1. |
| 17 | S → failure | stale premises at 0e60747 | **Fixed**: every carried cite re-verified this pass or marked; stale claims dropped. |
| 18 | F → invariant | archive-aside = supersession-by-amnesia (orphans children) | **Fixed**: the record file's identity core (`{pid, start, boot, gen}` + schema int) is frozen forever; an unknown-schema open extracts cores, reaps, then archives the remainder. Corrupt-beyond-envelope is named residue, reported in `Drained.Archived`. |
| 19 | F → invariant | no typed record-debt handoff; fsync can pin completion | **Fixed**: driver settle path contains no fsync (I5); retire is a bounded-share handoff to the store loop; `RecordAbandoned` is the typed debt the next open reclaims. |
| 20 | F → invariant | ID externally decomposable; widening misses sites | **Fixed** as #7: unexported comparable struct, one comparison site. |
| 21 | F → consumer | recovery runs before singleton ownership (kills a healthy incumbent's children) | **Fixed**: `Serve` order is flock → recover → bind (I10). |
| 22 | F → consumer | `Done()` couples exit to a durable verdict fsync can block | **Fixed** as #19. |
| 23 | F → consumer | archive-aside contradiction; unknown records never repaired | **Fixed** as #18, plus the one-release legacy bbolt sweep (§8). |
| 24 | F → ownership | `Stop(Budget) Outcome` conflates waiter timeout with terminal verdict | **Does not apply**: `Terminate()` is a demand; `Done()` publishes the terminal once; a caller's wait is its own `select` under its own ctx and never contaminates settlement. |
| 25 | F → ownership | `OpenRecords` error return vs "broken state never blocks repair" | **Fixed**: open has no failed-schema path (I16); environmental I/O failure (EACCES) remains an error — that is not broken-era state. |
| 26 | F → ownership | `RecordRetired` minted from process-death proof, not store re-read | **Fixed**: `RecordRemoved` derives only from the post-write re-read (I6). |
| 27 | F → failure | fsync claim false (JSON still fsyncs) | **Accepted and stated**: this design claims decoupling (I5/I19), never that fsync became interruptible. |
| 28 | F → failure | the new store cannot read pre-cut records; children stranded | **Fixed**: the legacy sweep (§8.2), one release, scheduled to die. Survives the flag-day decision — it answers durable state, not module paths. |
| 29 | F → failure | `Outcome.Kind` exported ⇒ forgeable retryable | **Does not apply**: retry decisions live inside the single internal boundary; public `RefusalKind` is output-only data. |
| 30 | F → failure | no child/settlement type exists | **Does not apply**: `Child`/`Exit` are core types. |
| 31 | A → consumer | ptyhost's 500ms handshake has no home | **Fixed** as #1. |
| 32 | A → consumer | cc-patch absent; `CalendarInterval`/`Daily`/`PlistPath`/`NoRestart` homeless | **Fixed**: named survivors in public `launchd`; cc-patch row corrected. |
| 33 | A → consumer | crash-window reap asserted, not designed | **Fixed** as #28. |
| 34 | A → consumer | `Cmd{Session:true}` understates synckit's spawned kind | **Accepted as open question** (§12) with the one-function replacement named; gated on synckit confirming live users. |
| 35 | A → invariant | `deployment` missing entirely (~60 cc-notes call sites) | **Fixed**: `deploy` is a public package, budgeted (~900 lines), verbs named; the proof-migration residual carried in §9. |
| 36 | A → invariant/ownership/failure | cc-present/cc-review recorded as version bumps | **Fixed**: both rows are source changes with the exact imports named. |
| 37 | A → invariant | `codeidentity` silently dropped (`PolicyDigest` users) | **Moot**: `Requirement` and `PolicyDigest` were deleted on 2026-10-27 with every peer check; cc-notes drops its digest, named in its row. |
| 38 | A → ownership | no `Ensure`/upgrade verb ⇒ least deletion | **Does not apply**: `Ensure` ships with the pure decide step promoted verbatim. |
| 39 | A → ownership | forever `compat.go` aliases = the forbidden compat layer | **Does not apply, and more strongly than the synthesis claimed**: the flag-day cut keeps one module path, so no alias layer exists at all — a deleted sentinel is a compile error at each consumer, not a silent `errors.Is` miss. |
| 40 | A → ownership | below-protocol Drain widens same-EUID on unprotected daemons | **Accepted**: every daemon is unprotected since 2026-10-27, and the verb grants what SIGTERM already grants the same user — convenience, not capability. Named in §11. |
| 41 | A → failure | four B1 real-need knobs have no field | **Fixed**: `MaxFrame`, `Concurrency`, `Handshake` are fields; handler deadline is inherited by design. |
| 42 | A → failure | `Start` arms nothing | **Does not apply**: `Serve` arms first. |
| 43 | A → failure | four consumer rows factually wrong | **Fixed**: table rebuilt from the verified import lists. |
| 44 | F/S → all four | outcome enums assertable without proof | **Fixed** as #26/I21: re-read-derived fates and observed convergence throughout. |

---

## 11. What this design refuses to do

Deliberately absent, so the next agent does not re-add the cruft:

- **No per-op route table, per-op budgets, or `MaxResponseBytes`.** That is the six restated
  bounds asking to come back. The product owns dispatch; the client owns the deadline.
- **No server-side takeover/eviction option.** `Serve` refuses a live incumbent, always.
  Eviction is `Ensure`'s decide step or an explicit `Client.Drain` — a caller's deliberate act
  (K48). Do not add `AcquireOptions.Takeover`.
- **No per-connection checks.** No code-signature, team, entitlement, designated-requirement,
  or UID verification of a connecting process or a spawned child (2026-10-27). The 0700 state
  directory is the boundary. Do not re-add a verifier, a cache, or a policy digest to the
  handshake path; a consumer that needs a credential for remote clients owns it at its own
  network boundary.
- **No persisted phases, receipts, claims, or capability registries.** Durable state is
  evidence — live child records and the converged agent set. If you feel the need for a receipt,
  the record's absence already asserts it.
- **No third version axis.** `Protocol` (wire, exact) and the store schema int. Build strings
  never gate the wire; gating admission on one is the 18,999-failure bug. Only `Ensure`'s decide
  step compares a build, to choose whether to replace the incumbent. Schema is set membership at
  product dispatch and nowhere else.
- **No health enum owned by daemonkit.** `healthy`/`degraded`/`failed` are consumer JSON inside
  `Detail`. Owning the enum re-freezes every consumer's protocol to daemonkit's release cadence.
- **No readiness authority.** Observation is free (phase in every ack, stream on every session).
  Adding a subscription capability recreates B4's four-consumer blocker.
- **No retry without OS evidence.** Exits 36/37, or a launchd log line. "Retry because the code
  was 5" must stay unwritable.
- **No idle-exit, activity leases, strike ladders, or nice levels** — zero-consumer orphans; the
  respawn-gate question, if it returns, returns as launchd `KeepAlive` config in `launchd`, not
  process machinery.
- **No compat shims, no alias layer, no deprecation window.** The only bridge code in the tree is
  the one named legacy record sweep, its deletion release written into its own TODO. A single
  module path makes a deleted symbol a compile error, and that *is* the migration mechanism —
  re-adding aliases to soften it re-creates the silent-`errors.Is` hazard the cut removes. A
  shim without a deletion date is a second mechanism.
- **No protocol gate on the Drain verb.** It is admitted unconditionally below the protocol
  gate and grants nothing SIGTERM does not; gating it on protocol re-creates RC3's self-wedge.
- **No degraded-ready or pre-ready business traffic.** Ready is `Start`'s return. If a consumer
  genuinely needs it, bend the shape here deliberately — do not add a phase.

---

## 12. Open questions

1. **synckit's `spawned` service kind**: live users? If yes, `Cmd` gains a handoff option
   carrying K16's dup-then-prove fd rule — one function, not a subsystem. If no, it stays dead.
2. **authkit and cc-pool ladders are unaudited** — either could carry a sixth divergence axis.
3. **`Stable`'s plist `Program` change** is a live launchd re-bootstrap per consumer — needs
   one real-machine test before wave 1.
4. **The observe payload's final field set** — `Health` as specced; confirm cookiesync/reposync
   decode `Detail` without a wrapper change.
5. **Task `72313e5`** (pre-main `_dyld_start` provenance hang) stays open — it is pre-main, so not
   a property of verifier placement, though deleting the Security.framework binding may resolve it
   incidentally.

## 13. Phase-0 findings that bind later phases

Established while building phase 0, and recorded here because a later phase would otherwise have
to re-derive or silently contradict them.

- **The cut bumped `ProtocolVersion` 1 → 2, and the single-session change bumped it to 3**
  (2026-10-27). §8.4 requires an old client meeting a new daemon to get a crisp typed
  `ErrProtocolMismatch`; with both eras at one protocol the hellos differ in shape only and there
  is nothing to reject on.
- **The frozen drain preamble is `0x4452` ("DR").** Chosen so it can never open a frame: a frame
  begins with a big-endian `uint32` body length, so `0x4452….` implies a body over 1 GiB — above
  every `MaxFrame` ceiling (4 / 16 / 64 MiB).
- **P1 against the shipped tree, independent of this design.** `wire/server.go:405-418` calls
  `startConnection` inline in the accept loop, and `:445-449` block for up to
  `defaultHandshakeTimeout` (10s) in `readClientHello`. A process that connects and sends nothing
  stops every other accept for 10 seconds; a loop of them stops the daemon accepting anyone. The
  cut fixed it: handshakes run off the accept goroutine (`go s.startConnection(...)`), and pending
  connections draw from a bounded pool and are dropped without a write past it.
