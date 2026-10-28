package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
)

// Expect pins which incumbent Drain may stop or Settle may settle. Zero
// fields are unchecked; the zero Expect acts unconditionally. Non-zero
// fields compare against the Health served on the pinned session (Drain)
// or the durable owner record (Settle) immediately before acting, so a
// mismatch proves the incumbent is not the runtime the caller observed and
// nothing is stopped.
type Expect struct {
	Build      string
	Generation uint64
}

func (e Expect) mismatch(build string, generation uint64) bool {
	return (e.Build != "" && e.Build != build) ||
		(e.Generation != 0 && e.Generation != generation)
}

// Stopped proves what became of the incumbent. Before is the identity echo a
// caller cross-checks against its own earlier observation: the Health served
// on the session that delivered the drain, or the owner-record synthesis on
// the session-less path. Reap is reached only by observing the process
// table, never by a delivered signal or an acked verb.
//
// The proof reaches exactly one identity, and only as far as whatever named
// it: Drain pins whatever process answers on the socket, and Settle reads its
// identity out of a writable owner record. Neither says anything about
// orphaned children, a second instance, or a separate app executable. Gating
// an irreversible action — uninstall, deactivation — on Stopped alone is
// therefore unsound: require an executable-scoped inventory of the real
// process table to be empty besides.
type Stopped struct {
	Before Health
	Reap   Reap
}

// Reap is what observation of the process table proved about one pinned
// identity, never about every process running the daemon's executables. It is
// shared with child settlement; values mirror internal/proc one-to-one.
type Reap uint8

const (
	// ReapUndetermined is never returned by Drain or Settle; child settlement
	// publishes it only when a demanded settlement timed out unproven.
	ReapUndetermined Reap = iota
	// ReapAbsent means the pinned identity was observed gone.
	ReapAbsent
	// ReapCrossBoot means the boot session changed; the process cannot exist.
	ReapCrossBoot
	// ReapReused means the PID now names a different process instance.
	ReapReused
	// ReapTerminated is reached only by a ladder that delivered signals and then
	// observed the exact instance leave — owned-child settlement and Terminate.
	// Drain and Settle never signal and never return it.
	ReapTerminated
)

// Health reports the daemon serving on the socket, drain included: the verb is
// answered below product dispatch and below the schema gate, so a draining
// daemon and one of another schema era still answer. It rides the client's own
// session, the one Ensure observes on and the first business lane takes over,
// so asking costs no connection of its own once one is open.
func (c *Client) Health(ctx context.Context) (Health, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Health{}, errors.New("daemonkit: Health requires a context deadline")
	}
	report, err := c.link.health(ctx)
	if err != nil {
		return Health{}, err
	}
	return healthFromReport(report), nil
}

// Drain stops the incumbent serving on the socket and proves it gone.
//
// Order: pin the process answering on the client's session — its socket PID,
// probed for {start, boot}, and the same-session health self-attestation that
// proves the probed identity is the session's peer and not a reused PID;
// refuse with ErrWrongIncumbent when a non-zero Expect field disagrees with
// the Health just read (nothing dispatched); send the drain verb, which the
// server acknowledges once its runtime observably reached PhaseDraining; then
// observe the pinned {pid, start, boot} until it leaves the process table. The
// ack is not the proof and its loss is not a failure: a verb whose terminal
// was lost to session teardown proceeds to observation anyway — only a typed
// refusal or a pre-send failure returns without observing.
//
// The verb is idempotent. Success returns Stopped with Reap one of ReapAbsent,
// ReapReused, or ReapCrossBoot — each an anti-PID-reuse absence proof reached
// only by observing the process table. ErrAbsent on a proven no-listener and
// ErrDraining on an incumbent already leaving leave settlement to Settle.
// ErrUnsettled (joined with ctx.Err()) reports a delivered drain whose exit
// was not observed within ctx; the incumbent keeps draining on its own
// Shutdown budget and Settle re-observes without a session.
//
// The proof reaches exactly one identity: the process answering on the
// socket, which any process able to open the socket can be. It says nothing
// about orphaned children, a second instance, or a separate app executable,
// so gating an irreversible action — uninstall, deactivation — on Stopped
// alone is unsound: require an executable-scoped inventory of the real
// process table to be empty besides.
func (c *Client) Drain(ctx context.Context, expect Expect) (Stopped, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Stopped{}, errors.New("daemonkit: Drain requires a context deadline")
	}
	session, pinned, report, err := c.link.pinned(ctx)
	if err != nil {
		return Stopped{}, err
	}
	before := healthFromReport(report)
	if expect.mismatch(before.Build, before.Generation) {
		return Stopped{}, ErrWrongIncumbent
	}
	refusal := dispatchDrain(ctx, session)
	c.link.retire(session)
	if refusal != nil {
		return Stopped{}, refusal
	}
	reap, err := observeGone(ctx, proc.Observe, pinned)
	if err != nil {
		return Stopped{}, err
	}
	return Stopped{Before: before, Reap: reap}, nil
}

// dispatchDrain sends the drain verb. Only a typed refusal or a pre-send
// failure returns non-nil; a delivered ack, a lost terminal, and an error
// terminal all proceed to observation — the verb already landed.
func dispatchDrain(ctx context.Context, session *wire.Client) error {
	result, err := session.Drain(ctx)
	if rejection := result.Rejection(); rejection != nil {
		return fmt.Errorf("daemonkit: drain refused: %w", rejection)
	}
	if err != nil && result.Outcome == wire.PreSendFailure {
		return fmt.Errorf("daemonkit: send drain: %w", err)
	}
	return nil
}

// assemblePin runs the attach pin in the documented order: self-pin refusal,
// probe for {start, boot}, then the same-session health self-attestation that
// proves the probed identity belongs to the session's peer.
func assemblePin(ctx context.Context, self, peerPID int, session *wire.Client) (proc.Identity, wire.HealthReport, error) {
	if peerPID == self {
		return proc.Identity{}, wire.HealthReport{}, fmt.Errorf("daemonkit: refusing to pin own process %d", self)
	}
	pinned, err := proc.ProbeIdentity(peerPID)
	if err != nil {
		return proc.Identity{}, wire.HealthReport{}, fmt.Errorf("daemonkit: probe peer %d: %w", peerPID, err)
	}
	report, err := session.Health(ctx)
	if err != nil {
		return proc.Identity{}, wire.HealthReport{}, fmt.Errorf("daemonkit: health self-attestation: %w", err)
	}
	if report.PID != peerPID {
		return proc.Identity{}, wire.HealthReport{}, fmt.Errorf(
			"daemonkit: peer self-attests pid %d, socket observed %d", report.PID, peerPID,
		)
	}
	return pinned, report, nil
}

// classifyWire turns one wire-side failure into the root sentinel a caller
// branches on: an attach refusal or a dispatch rejection. One function reads
// them all, so a rejection and an attach naming the same condition cannot
// answer with two different sentinels.
func classifyWire(err error) error {
	switch {
	case errors.Is(err, wire.ErrDraining):
		return ErrDraining
	case errors.Is(err, wire.ErrNotReady):
		return ErrNotReady
	case errors.Is(err, ErrSessionCapacity), errors.Is(err, ErrProtocolMismatch):
		return err
	case errors.Is(err, syscall.ENOENT), errors.Is(err, os.ErrNotExist), errors.Is(err, syscall.ECONNREFUSED):
		return fmt.Errorf("%w: %w", ErrAbsent, err)
	default:
		return err
	}
}
