# daemonkit — build order

How DESIGN.md gets built and shipped. **The module path does not change** (yasyf, 2026-07-29):
`github.com/yasyf/daemonkit` is rewritten in place and cut as one breaking release, every
consumer repinning together. Phase intermediates may be broken; only the final state must
cohere — no interphase adapters, no dual-mode params, no alias layer.

Two consequences of building in place, both deliberate:

- **`main` is unbuildable-for-consumers during phases 1–3.** fleet-build is a release gate, not
  a per-commit gate, until phase 3 closes; its redness carries no information while the surface
  is still moving. The in-repo suite, `task build`, `task lint`, and `swift build && swift test`
  stay green **per commit** — those are the phase gates.
- **The old packages are deleted as their replacements land**, not after. A phase that leaves
  both the old and new mechanism alive has built the compat layer the design refuses.

Everything runs through `scripts/test.sh`. Never bare `go test` — the harness's `RLIMIT_NPROC`
cap stays even after the self-exec verifier is deleted (the rule outlives the hazard, I19).

## Phase 0 — foundations and the gate (nothing depends on anything)

**Lands:** `Budget` (unexported fields, `Share`/`Reserve`/`Left`/`Context`) with its property
tests (containment, tail disjointness); `internal/state` (write-temp → fsync → rename →
fsync-dir, archive-aside, frozen-envelope extraction); the `Daemon` value and `paths.Socket(name)`
with its typed overlong-path error; **the mixed-era CI gate rebuilt as a release gate** — pre-cut
daemon ↔ new client and new daemon ↔ pre-cut client, both directions, asserting new-drains-old
(SIGTERM and the frozen preamble) and crisp `ErrProtocolMismatch`, never a hang.

Phase 0 is purely additive — nothing is deleted yet, so `main` still builds for consumers
throughout it.

**Also in phase 0, concurrently:** **G1, the AMFI team-identifier binding measurement**
(DESIGN §9/§12.1). With a real Developer ID identity for team A, sign a binary whose CodeDirectory
declares a foreign `teamID`/`identifier`, run it, and read `CS_OPS_VALIDATION_CATEGORY` /
`CS_OPS_TEAMID`. Category ≠ 6, or the team reported as A, confirms the binding and `internal/trust`
ships as specified; category 6 with the foreign team reported adds the `cdhash`-memoized
off-handshake-path chain check before phase 2 starts, and changes nothing else. **No host
saturation is required, and none is permitted** — the kernel path performs no I/O and contacts no
daemon, so there is nothing for load to affect. This box reports `0 valid identities`; the fleet
ships signed apps and `repo-bootstrap:apple-certs` mints them, so the gate is runnable (~30 min).

**Also in phase 0:** **G4** — give the `DAEMONKIT_TRUST_E2E` suite and `scripts/trust-fixtures.sh`
a CI or release-gate home. They appear in no workflow today, so **the signed-peer path is verified
by nobody**, and G2 in phase 1 is meaningless without it. Requires a signing identity in CI: a
release-infrastructure prerequisite, not a test chore.

**Gates:** budget property tests; mixed-era harness runs (against a stub daemon until phase 3);
fleet-build still green (nothing has been deleted).

## Phase 1 — proc and trust (parallel; both depend only on phase 0)

**Lands (proc):** `internal/proc` — unexported comparable identity, the one comparison site,
suspended spawn (record fsynced before SIGCONT), the driver goroutine with closure-local
authority, the single-writer record store with the frozen identity core, flock-by-inode, the reap
ladder, the one-release legacy bbolt sweep (reads a pre-cut file, reaps, archives — its deletion
release named in the code's TODO). **Deletes:** public `proc`, `worker`.

**Lands (trust):** `internal/trust` — the unconditional floor, plus one kernel-only verifier:
five `csops_audittoken` reads against the audit token (status → `csposture.Check(…,
RequireLibraryValidation)`; validation category == 6; team identifier; signing identifier; DER
entitlements parsed with stdlib `encoding/asn1`, the six injection rejections unconditional, plus
`RequiredEntitlements`/`RequiredAppGroup`). No Security.framework binding, no cache, no budget
share. Non-darwin and `daemonkit_unsigned` stubs as today. **Deletes:** public `trust`,
`codeidentity`, `peer`, `RunVerifierChild` and every `main` dispatch site for it, and the entire
purego Security.framework binding (`loadSecurity`, ~19 symbols).

**Lands (wire, same phase):** `go s.startConnection(...)` — off the accept loop, fixing a live
unauthenticated same-UID DoS in the shipped tree — plus the pre-verification connection cap, its
own named counter bounding unverified handshakes in flight, derived from `Concurrency`. Session
capacity acquisition **stays after** `verifyPeer`: the per-role capacity-1 reservation is what
keeps the trust-gated Drain verb reachable under flood, and `identity.Role` is peer-supplied, so
anything acquired before verification is a denial primitive an unverified peer can hold.

**Gates:** the A2 adversarial settlement scenarios replayed as **compile-fail** tests (the two
evasions must not compile) plus the concurrent-stop and fsync-pin scenarios as runtime tests; the
export census records every symbol these deletions remove, seeding the rename ledger (§8.3).
**G2** — every consumer's real signed fixture admits on the minimum supported macOS and on each
supported release, including one under App Translocation and one notarized-and-stapled, with DER
entitlements present and the magics and header lengths as measured; this is the per-release ABI
pin and where a broken fleet gets caught. **G3** — the union-policy audit
(`codesign -d --entitlements -` per consumer against the six), recorded, with the escape-hatch
scope from DESIGN §11 written into `Requirement`; adopting the injection rejections onto the
launchd fixed-app path (`service/keepalive.go:264 → 418-422`) is the dangerous direction, since an
Electron or CEF fixed app needs `allow-jit` and `allow-unsigned-executable-memory` and would be
rejected outright. A DER-parser fuzz target. A test that a silent connection no longer blocks
other accepts. A test that an unverified peer cannot consume a protected role's slot.

## Phase 2 — wire and the Swift mirror (depends on phase 1's trust)

**Lands:** `internal/wire` — frame v1 byte-identical, `{Protocol, Lane}` hello, phase-carrying
ack, the frozen Drain preamble (below the protocol gate, above the trust gate), business attach
with `Schemas` set membership, FrameAck, SCM_RIGHTS + nonce, the phase stream, the server;
`Sources/DaemonKit` regenerated from the one shared schema with the golden fixture and the CI
no-op drift gate (restoring what `7ec51bc` deleted). **Deletes:** public `wire`, `wire/wiretest`.

The Swift trapping-conversion fix rides here rather than landing on the old tree — the dropped
A3 patch's three sites (`ServiceSocketClient.swift:842`, `RuntimeReadiness.swift:298`,
`SessionIOQueue.swift:7`) all route through `SessionFrameCodec.durationNanoseconds`. `min()` does
not guard a trapping conversion; both arguments evaluate before the call.

**Gates:** the shared golden byte-for-byte in both languages; macos-26 Swift CI; the mixed-era
harness now runs against the real wire.

## Phase 3 — the assembly (depends on phases 1–2)

**Lands:** root `Serve` (the one function body: arm → flock → recover → bind → start → ready →
serve → drain → release), `Open`/`Client`/`Control`, `Ensure` with the promoted pure decide step
(table tests ported from cc-interact's `runtimeAction` cases), `launchd` on `internal/converge`
(observed actions, one launchctl boundary, 36/37-or-Evidence retry), `deploy`
(Install/Activate/Supersede/Uninstall/Reset over one swap receipt). **Deletes:** `daemon`,
`service`, `deployment`, `templates`.

**Gates:** mixed-era both directions against a real pre-cut consumer build (the 18,999
reproduction must stay red-proof); the B1 five-axes matrix as integration tests (settlement
unconditional, recovery-after-flock, signals armed first); **fleet-build turns from expected-red
to required-green here** — it is the phase-3 exit criterion, and the export census emits the
final rename ledger.

## Phase 4 — the flag day

Every consumer PR is authored against the tagged release, adopts-and-deletes in the same diff,
and is **green before the tag**, not after it. Ordering is dependency-only; there are no waves,
because there is no overlap window to stage them across.

| Order | Repos | Constraint |
|---|---|---|
| 1 (parallel) | synckit, captain-hook, cc-orchestrate, fusekit | each owns both ends of its socket — no cross-repo wire compatibility. One real-machine `Staged()` re-bootstrap test before the tag (DESIGN §12.4) |
| 2 (lockstep ×3) | cc-interact + cc-present + cc-review | cc-present/cc-review import daemonkit directly *and* consume cc-interact's launcher. Signed-app end-to-end: `Ensure` against a real signed DR |
| 3 | cc-notes, reposync, cc-patch, claude-pool/cc-pool, cookiesync | consume order 1–2's surfaces. cc-notes rewrites its source-text contract test (`release_contract_test.go`) in the same change; cc-patch is a `launchd`-leaf bump |

**Release gate:** every PR green · fleet-build green · mixed-era green both directions · export
census reconciles against the rename ledger · full suite on CI or a quiet machine ·
`task build`/`task lint` · `swift build && swift test` · the real-machine E2E from the plan's
Verification section. A stalled repo is a **red release, not a slow one** — this is the flag
day's whole cost, and paying it is the decision.

**After the cut:** the legacy bbolt sweep deletes at its named release. The mixed-era gate
narrows to protocol-bump coverage and stays forever — it is the one check that reproduces the
class that motivated the redesign.
