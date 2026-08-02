# Migrate a consumer to daemonkit v0.21.0

Repin a fleet repo from daemonkit v0.20.x. Ten packages are gone and three capabilities came back
in new shapes, so this is a refactor, not a version bump. Plan a branch, not an afternoon.

Two facts set the size of the job. The exported surface went from 1,272 symbols to 510, and
daemonkit no longer compiles off macOS: there are no `!darwin` stubs and no runtime
"unsupported platform" errors, because a missing verifier should be a build failure instead of an
error value a caller can ignore. A consumer that ships Linux binaries gates its daemon subsystem
behind `//go:build darwin`.

## 1. Repin and let the compiler write your worklist

```console
$ go get github.com/yasyf/daemonkit@v0.21.0
$ go build ./...
```

Every failure is `no required module provides package github.com/yasyf/daemonkit/<pkg>`. That list
is your scope, and the next section maps it.

## 2. Map the imports

| v0.20.x import | v0.21.0 home |
|---|---|
| `daemonkit/service` | `daemonkit` — `Daemon`, `Client.Ensure` — plus `daemonkit/launchd` |
| `daemonkit/daemon` | `daemonkit` — `Serve`, `Ctx`, `Product` — plus `daemonkit/durable` |
| `daemonkit/deployment` | `daemonkit/deploy` |
| `daemonkit/proc` | `daemonkit` — `Owned`, `Ctx`, `Cmd`, `Child` |
| `daemonkit/worker` | `daemonkit` — `Owned.Run` — or `os/exec` |
| `daemonkit/wire` | `daemonkit` — `Client.Control`, `Client.Business` |
| `daemonkit/trust`, `daemonkit/codeidentity`, `daemonkit/peer` | `daemonkit` — `Requirement`, `Serving`, `Trust`, `Caller` |

## 3. Declare the daemon once

`service.ControllerConfig`, `daemon.RuntimeConfig`, and the plist knobs collapse into one `Daemon`
value that both halves read. Build it in code both the launcher and the daemon import.

```go
package myd

import (
	"time"

	"github.com/yasyf/daemonkit"
)

// Spec is the one declaration the launcher and the daemon share.
func Spec(program daemonkit.Program) daemonkit.Daemon {
	return daemonkit.Daemon{
		Label:    "com.example.myd",
		Program:  program,
		Schemas:  []daemonkit.Schema{"myd.v1"},
		Shutdown: daemonkit.Grace(30 * time.Second),
		Trust: daemonkit.Trust{
			Serving: daemonkit.ServingSameUser(),
		},
	}
}
```

`Program` replaces `service.StableProgram` and `StableProgramFrom`. Pick the constructor that
matches how you ship:

- `daemonkit.Stable()` runs the daemon from a copy of the invoking executable at
  `~/.daemonkit/bin/<Label>`. That path survives package upgrades, so the TCC grants a bare Mach-O
  carries on its absolute path survive one too.
- `daemonkit.InBundle(app, rel)` runs it in place inside a signed `.app`. Nothing is copied,
  because an executable moved out of its bundle loses the entitlements and App Group membership it
  inherits from it.

Neither constructor writes anything. `Client.Ensure` places the executable, under the lock that
already serializes every other transition of the live daemon.

## 4. Serve on one side, Open on the other

The daemon half calls `Serve` with a `Start` that returns your `Product`:

```go
func main() {
	program, err := daemonkit.Stable()
	if err != nil {
		log.Fatal(err)
	}
	start := func(c daemonkit.Ctx) (daemonkit.Product, error) {
		return newProduct(c), nil
	}
	if _, err := daemonkit.Serve(context.Background(), Spec(program), start); err != nil {
		log.Fatal(err)
	}
}
```

`Product` is three methods, replacing `daemon.Runtime` and its lifecycle interfaces:
`Handle(ctx, Request) (Reply, error)`, `Drain(Budget) error`, and `Close(Budget) error`.

The launcher half calls `Open`, which is now fallible: it runs `ValidateForClient`, so a malformed
`Daemon` or an unstated trust posture fails here with a config error naming the field, rather than
on the first call inside a retry loop.

```go
client, err := daemonkit.Open(Spec(program))
if err != nil {
	return fmt.Errorf("open myd: %w", err)
}
if _, err := client.Ensure(ctx); err != nil {
	return fmt.Errorf("ensure myd: %w", err)
}
```

`Ensure` replaces `service.NewController` plus its plist bookkeeping. Reach for `daemonkit/launchd`
(`RestorePlan`, `Apply`, `Remove`, `Verify`) only when you manage a job daemonkit does not own.

## 5. Replace durable writes

`daemon.WriteFileDurable` and `daemon.SyncDir` become `durable.WriteFile` and `durable.SyncDir`,
same shapes, from `github.com/yasyf/daemonkit/durable`:

```go
if err := durable.WriteFile(path, data, 0o600); err != nil {
	return fmt.Errorf("publish %s: %w", path, err)
}
```

`durable` also carries `Create`, `Writer`, `Rename`, `Mkdir`, `Remove`, `RemoveTree`, `Marshal`,
`Unmarshal`, `ReadFile`, `Validating`, and one bounded cross-process `Lock`.

`daemon.ExactStateFile` and `ExactStateCodec` have no successor. Every consumer that used them had
already hand-rolled two thirds of a different policy around them; rebuild yours from
`durable.Marshal`/`Unmarshal` plus `durable.Lock`, keeping your own absence policy and envelope
visible at the call site.

One behavior change worth checking: each call site owns its directory now. `launchd/apply.go`
gained an explicit `durable.Mkdir` because `~/Library/LaunchAgents` does not exist on a fresh
account. Audit your own writes for the same assumption.

## 6. Replace subprocess supervision

`proc.Manager`, `PreparedChild`, `SpawnRequest`, and `worker.Pool` become one owner value.

A daemon gets its scope from `Ctx`, which `Start` already hands you. A CLI opens its own:

```go
owned, err := daemonkit.OwnProcesses(ctx, recordPath)
if err != nil {
	return fmt.Errorf("own processes: %w", err)
}
defer func() { _ = owned.Close(ctx) }()

result, err := owned.Run(ctx, daemonkit.Cmd{Path: "/usr/bin/git", Args: []string{"status"}})
```

`Run` is the one-shot; `Spawn` returns a `*Child` for a long-lived one; `Adopt` takes over a pid.
Both `Owned` and `Ctx` carry all three.

**The pool is gone with no successor.** Scheduling was never daemonkit's business — the one real
scheduler in the fleet is domain-keyed and in-house. A one-shot `worker.CommandRequest` becomes
`Run`; anything that needs queueing or capacity keeps that logic itself. There is no `ErrCapacity`,
no `ErrTimedOut`, and no pool-level byte cap. Limits are per-call (`Cmd.MaxOutput`, ctx deadlines)
or per-session (`Cmd.Limits`).

## 7. Replace raw wire dispatch

Consumers that dialed `daemonkit/wire` directly now pick a lane, and each names its own
authentication:

```go
business := client.Business()
reply, err := business.Call(ctx, "myd.sync", body)
```

`Client.Business` verifies the accepting process's kernel-held code identity against
`Trust.Serving` on every acquisition, before a single byte — the wire handshake included — is
written. This closes a hole every raw-`wire` consumer carried: nothing judged the process accepting
on the socket, so a same-UID squatter that rebound it harvested business payloads.

The other two constructors are waivers, and they are greppable on purpose:

- `Child.Business(ctx, contract)` proves directional confinement of a spawned socketpair, not the
  identity of what holds it.
- `BusinessOverConn(ctx, conn, contract)` proves nothing; the caller authenticated the transport.

Lifecycle verbs live on `Client.Control`. `Undispatched(err)` reports whether a failed call
provably never reached dispatch and may be resent — true guarantees a safe resend, false means
unknown, never "dispatched".

Child processes that serve a lane call `ServeSpawned(ctx, contract, handler)`, which claims fd 3,
adopts the limits the parent conveyed, and serves exactly one session. A non-Go child uses fd 3
directly; a Go child that only wants the descriptor calls `ClaimHandoff`.

## 8. State a trust posture — there is no default

`Trust.Serving` is a `Serving` value rather than a nilable pointer, and the zero value is refused
at `Open`. Every consumer picks one explicitly:

- `ServingSigned(requirement)` — verify the peer's code-signing identity. Use this wherever you
  gate an irreversible action on the daemon being who you think it is.
- `ServingSameUser()` — the named waiver. It proves nothing beyond the same-EUID floor, and it is
  the only posture available to a Python interpreter, a platform binary, or a homebrew tool.

`Trust.Business` is a set now: any element admits. An app plus its File Provider extension is an
ordinary macOS shape and their entitlements differ, so one requirement could not describe both.
A stated-but-empty set is a config error rather than a silent floor. `Trust.Control` stays a single
requirement, because the control lane admits one session server-wide.

## What has no successor

| Withdrawn | Do instead |
|---|---|
| `worker.Pool` and its error vocabulary | Keep your own scheduling; use `Owned.Run` per command |
| `daemon.ExactStateFile`, `ExactStateCodec` | `durable.Marshal`/`Unmarshal` + `durable.Lock` |
| Request/response streaming, server-pushed events | Poll unary snapshots |
| `Tenant` as an API parameter | Nothing — the header field is reserved and must be empty |
| Peer fencing, `SignatureDigest`, `ProcessReceipt` | `Cmd.Exec` plus the socketpair |
| Two-phase `Prepare`/`Start` | Wiring happens before the gated release by construction |
| `Child.Release()` — children outliving their owner | A process that must outlive a CLI is a LaunchAgent |

## Watch for

- **`MaxFrame` is not your payload ceiling.** A terminal is base64'd and carries a 4 KiB envelope
  reserve, so the largest body a session moves is `(MaxFrame - 4096) * 3/4` — about 75% of the
  number you set. In v0.20 the wire carried the body raw and `MaxFrame` was the payload ceiling,
  so any consumer that sized `MaxFrame` for a specific payload loses a quarter of it on the way
  across. Size it from the payload instead, and pin it with `daemonkit.MaxDetail(MaxFrame)`, which
  reports the real ceiling:

  ```go
  const maxPayload = 64 << 20
  // The smallest frame whose MaxDetail is at least maxPayload.
  const maxFrame = (maxPayload*4+2)/3 + 4<<10
  ```

  This bites hardest where an oversize payload is handled by falling back rather than failing:
  cc-interact's guard-edit gate silently stopped seeing whole-Write payloads between 48 and 64 MiB
  and allowed the edits through. Assert the ceiling in a test — `MaxDetail(spec.MaxFrame) >=
  maxPayload` — so a future change to the reserve or the encoding fails loudly.

- **State directory rename.** Deployment state moved to `.daemonkit-deploy/<name>/`. The old tree
  is archived on first open rather than decode-failed.
- **Signed policy digests change.** Consumers that bake a requirement digest — captain-hook's
  `RequireExactStopReceipt`, cc-notes' `RequireDaemonkitStopReceipt` and its golden hash — rev it.
- **Source-text contracts.** cc-notes' `internal/helperapp/release_contract_test.go` asserts on
  source text, so no compiler will find it for you.
- **Substrate order.** Tag a substrate before its dependents pin it. MVS selects the max across the
  graph, so a stale substrate compiles against the new daemonkit and fails.

## See also

- `docs/DESIGN.md` — the lifecycle core, and §8 for the migration contract
- `ci/exported.txt` — the full exported surface, gated in CI
- `CHANGELOG.md` — the per-change record with rationale
