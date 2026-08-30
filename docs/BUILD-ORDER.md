# daemonkit — build order

How DESIGN.md gets built and shipped. **The module path does not change** (yasyf, 2026-07-29):
`github.com/yasyf/daemonkit` is rewritten in place and cut as one breaking release, every
consumer repinning together. Phase intermediates may be broken; only the final state must
cohere — no interphase adapters, no dual-mode params, no alias layer.

Two consequences of building in place, both deliberate:

- **`main` is unbuildable-for-consumers during phases 1–3.** While the surface moves,
  fleet-build's redness carries no information, so it gates nothing per commit. The rule as
  first written also made it the release gate — no tag until green — but phase 3's close
  inverted that: the cut tags `v0.21.0` before any consumer migrates, so fleet-build is red by
  construction at tag time and gates the close of the migration instead (DESIGN §8.1). The
  in-repo suite, `task build`, `task lint`, and `swift build && swift test` stay green **per
  commit** — those are the phase gates.
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

**Also in phase 0:** **G4 — closed by measurement (cc-notes `bbdbd8c`): the signed-peer suite has
no CI home.** It needs a Developer ID identity, and this box reports `0 valid identities` (line 43
above) as does every runner; ad-hoc signing reports `TeamIdentifier=not set` and validation
category ≠ 6, which `verifyToken` refuses. So the suite left `internal/trust` for `_e2e/trust/`,
where the go tool cannot reach it, and `scripts/e2e-trust.sh` — which mints the fixtures and exits
2 naming the missing identity — is its home; CI vets the package so it cannot rot silently.
**The signed-peer path is therefore verified by whoever holds the identity and by nobody in CI**,
and G2 in phase 1 rests on that hand run. Putting a signing identity in CI remains a
release-infrastructure prerequisite, not a test chore.

**Gates:** budget property tests; mixed-era harness runs (against a stub daemon until phase 3);
fleet-build still green (nothing has been deleted).

## Phase 1 — proc and trust (parallel; both depend only on phase 0)

**Lands (proc):** `internal/proc` — unexported comparable identity, the one comparison site,
suspended spawn (record fsynced before SIGCONT), the driver goroutine with closure-local
authority, the single-writer record store with the frozen identity core, flock-by-inode, the reap
ladder, and the one-release legacy bbolt sweep (since deleted). **Deletes:** public `proc`,
`worker`.

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
unconditional, recovery-after-flock, signals armed first); **fleet-build's redness starts
carrying information here** — from phase 3's close each red leg names exactly one unmigrated
consumer (DESIGN §8.1) — and the export census emits the final rename ledger.

## Phase 4 — the flag day

The cut tags `v0.21.0` first; consumers migrate after it (DESIGN §8.1). Every consumer PR is
authored against the tagged release and adopts-and-deletes in the same diff. Ordering is
dependency-only; there are no waves, because there is no overlap window to stage them across.

| Order | Repos | Constraint |
|---|---|---|
| 1 (parallel) | synckit, captain-hook, cc-orchestrate, fusekit | each owns both ends of its socket — no cross-repo wire compatibility. One real-machine `Stable()` re-bootstrap test before the tag (DESIGN §12.4) |
| 2 (lockstep ×3) | cc-interact + cc-present + cc-review | cc-present/cc-review import daemonkit directly *and* consume cc-interact's launcher. Signed-app end-to-end: `Ensure` against a real signed DR |
| 3 | cc-notes, reposync, cc-patch, claude-pool/cc-pool, cookiesync | consume order 1–2's surfaces. cc-notes rewrites its source-text contract test (`release_contract_test.go`) in the same change; cc-patch is a `launchd`-leaf bump |

**Tag gate** (`verify-main-gates` in `.github/workflows/release.yml`): CI · Guides · Export
census · Docs drift · Mixed era, all green on the tagged commit — fleet-build is deliberately
absent, since every leg is red by construction until consumers migrate (DESIGN §8.1).
**Migration-close gate:** every consumer PR green · fleet-build green · export census
reconciles against the rename ledger · the real-machine E2E from the plan's Verification
section. A stalled repo is an **open migration, not a slow one** — fleet-build joins the
release gate only when its last leg turns green, and `v0.21.1` is the fix path for an API
mistake the migration surfaces.

**After the cut:** the mixed-era gate narrows to protocol-bump coverage and stays forever — it is the one check that reproduces the
class that motivated the redesign.
