package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/paths"
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
// it: Control judges the process answering for it against Trust.Serving —
// floor alone when nil, which any same-UID socket squatter clears — and
// Settle reads its identity out of a same-UID-writable owner record. Neither
// says anything about orphaned children, a second instance, or a separate app
// executable. Gating an irreversible action — uninstall, deactivation — on
// Stopped alone is therefore unsound: pin Trust.Serving to a code-signing
// requirement and require an executable-scoped inventory of the real process
// table to be empty besides.
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
	// ReapTerminated is reached only by owned-child settlement ladders that
	// delivered signals; Drain and Settle never signal and never return it.
	ReapTerminated
)

// Control is the trust-gated lifecycle lane of one connected daemon, pinned
// to exactly one kernel process instance. The pin is assembled at attach, in
// an order that closes the PID-reuse window without a server nonce:
//
//  1. the peer PID is read from the connected socket (LOCAL_PEERPID on
//     darwin, SO_PEERCRED on linux);
//  2. that PID is probed through the process table for {start, boot};
//  3. the native health verb is asked on the same session and must answer
//     PID == the socket-observed PID — a self-attestation only a peer still
//     alive after the probe can serve, which is what proves the probed
//     {start, boot} belong to the session's peer and not a reused PID;
//  4. Health.Generation is captured as the instance pin.
//
// Attach refuses on any mismatch, and refuses a pinned PID equal to
// os.Getpid(). Every subsequent verb re-reads Health on the pinned session
// and refuses when PID or Generation moved — a replacement on the same
// socket is a dead session, never a silently retargeted one.
type Control struct {
	session    *wire.Client
	pinned     proc.Identity
	generation uint64
	observe    func(proc.Identity) (proc.Reap, bool, error)
}

// Control attaches the control lane. It exists only past Trust.Control (the
// same-EUID floor runs regardless and cannot be turned off). ErrAbsent on a
// proven no-listener; ErrDraining when the incumbent answers with the frozen
// drain preamble; ErrUntrusted on a trust refusal. Both ErrAbsent and
// ErrDraining leave settlement to Settle, which needs no session.
//
// Authorization runs in both directions. The daemon judges the attaching
// client against Trust.Control; the client judges the process accepting for
// it against Trust.Serving, on the same kernel-held code-signing state and
// over the same unconditional same-EUID floor. Without it a same-UID process
// that rebinds the socket answers the pin with its own honest identity and
// hands back an absence proof for a daemon that is still running.
func (c *Client) Control(ctx context.Context) (*Control, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("daemonkit: Control requires a context deadline")
	}
	socket, err := paths.Socket(string(c.daemon.Label))
	if err != nil {
		return nil, fmt.Errorf("daemonkit: derive socket path: %w", err)
	}
	var conn *net.UnixConn
	base := wire.UnixDialer(socket)
	dial := func(ctx context.Context) (net.Conn, error) {
		netConn, err := base(ctx)
		if err != nil {
			return nil, err
		}
		conn = netConn.(*net.UnixConn)
		return netConn, nil
	}
	session, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: dial, Lane: wire.LaneControl, MaxFrame: int(c.daemon.MaxFrame),
	})
	if err != nil {
		return nil, classifyAttach(err)
	}
	peer, err := proc.PeerCredentials(conn)
	if err != nil {
		_ = session.Abort(err)
		return nil, fmt.Errorf("daemonkit: read socket peer: %w", err)
	}
	if err := authorizeServer(conn, peer.UID, c.daemon.Trust.Serving); err != nil {
		_ = session.Abort(err)
		return nil, err
	}
	pinned, report, err := assemblePin(os.Getpid(), peer.PID, c.probe, func() (wire.HealthReport, error) {
		return session.Health(ctx)
	})
	if err != nil {
		_ = session.Abort(err)
		return nil, err
	}
	return &Control{
		session:    session,
		pinned:     pinned,
		generation: report.Generation,
		observe:    c.observe,
	}, nil
}

// authorizeServer judges the process accepting for this client: the
// unconditional same-EUID floor, then requirement against the peer's audit
// token. A nil requirement is explicit UID-only trust, exactly as on the
// serving side.
func authorizeServer(conn *net.UnixConn, uid int, requirement *Requirement) error {
	if err := trust.Floor(uid); err != nil {
		return fmt.Errorf("%w: serving peer: %w", ErrUntrusted, err)
	}
	req := wireRequirement(requirement)
	if req == nil {
		return nil
	}
	peer, err := trust.PeerCredentials(conn)
	if err != nil {
		return fmt.Errorf("%w: read serving peer identity: %w", ErrUntrusted, err)
	}
	if err := trust.Verify(peer, req); err != nil {
		return fmt.Errorf("%w: serving peer: %w", ErrUntrusted, err)
	}
	return nil
}

// assemblePin runs the attach pin in the documented order: self-pin refusal,
// probe for {start, boot}, then the same-session health self-attestation that
// proves the probed identity belongs to the session's peer.
func assemblePin(
	self, peerPID int,
	probe func(int) (proc.Identity, error),
	health func() (wire.HealthReport, error),
) (proc.Identity, wire.HealthReport, error) {
	if peerPID == self {
		return proc.Identity{}, wire.HealthReport{}, fmt.Errorf("daemonkit: refusing to pin own process %d", self)
	}
	pinned, err := probe(peerPID)
	if err != nil {
		return proc.Identity{}, wire.HealthReport{}, fmt.Errorf("daemonkit: probe peer %d: %w", peerPID, err)
	}
	report, err := health()
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

func classifyAttach(err error) error {
	switch {
	case errors.Is(err, wire.ErrDraining):
		return ErrDraining
	case errors.Is(err, syscall.ENOENT), errors.Is(err, os.ErrNotExist), errors.Is(err, syscall.ECONNREFUSED):
		return fmt.Errorf("%w: %w", ErrAbsent, err)
	case errors.Is(err, wire.ErrUntrustedPeer):
		return fmt.Errorf("%w: %w", ErrUntrusted, err)
	default:
		return err
	}
}

// Health reports the pinned incumbent, drain included: the verb is answered
// below product dispatch, so a draining daemon still answers on this session.
// A report naming another PID or generation is refused, never returned: a
// caller derives its Expect and its proofs from this read.
func (c *Control) Health(ctx context.Context) (Health, error) {
	report, err := c.session.Health(ctx)
	if err != nil {
		return Health{}, err
	}
	if err := c.pinnedBy(report); err != nil {
		return Health{}, err
	}
	return healthFromReport(report), nil
}

func (c *Control) pinnedBy(report wire.HealthReport) error {
	if report.PID != c.pinned.PID || report.Generation != c.generation {
		return fmt.Errorf(
			"%w: health names pid %d generation %d, pinned pid %d generation %d",
			errPinMoved, report.PID, report.Generation, c.pinned.PID, c.generation,
		)
	}
	return nil
}

// Drain stops the pinned incumbent and proves it gone.
//
// Order: read Health on the pinned session; refuse with ErrWrongIncumbent
// when a non-zero Expect field disagrees (nothing dispatched); send the
// drain verb, which the server acknowledges once its runtime observably
// reached PhaseDraining; then observe the pinned {pid, start, boot} until it
// leaves the process table. The ack is not the proof and its loss is not a
// failure: a verb whose terminal was lost to session teardown proceeds to
// observation anyway — only a typed refusal or a pre-send failure returns
// without observing.
//
// The verb is idempotent; draining an already-draining incumbent through an
// established session settles normally. Success returns Stopped with Reap
// one of ReapAbsent, ReapReused, or ReapCrossBoot — each an anti-PID-reuse
// absence proof reached only by observing the process table. ErrUnsettled
// (joined with ctx.Err()) reports a delivered drain whose exit was not
// observed within ctx; the incumbent keeps draining on its own Shutdown
// budget and Settle re-observes without a session.
func (c *Control) Drain(ctx context.Context, expect Expect) (Stopped, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Stopped{}, errors.New("daemonkit: Drain requires a context deadline")
	}
	report, err := c.session.Health(ctx)
	if err != nil {
		return Stopped{}, fmt.Errorf("daemonkit: pre-drain health: %w", err)
	}
	if err := c.pinnedBy(report); err != nil {
		return Stopped{}, err
	}
	before := healthFromReport(report)
	if expect.mismatch(before.Build, before.Generation) {
		return Stopped{}, ErrWrongIncumbent
	}
	if refusal := c.dispatchDrain(ctx); refusal != nil {
		return Stopped{}, refusal
	}
	reap, err := observeGone(ctx, c.observe, c.pinned)
	if err != nil {
		return Stopped{}, err
	}
	return Stopped{Before: before, Reap: reap}, nil
}

// dispatchDrain sends the drain verb. Only a typed refusal or a pre-send
// failure returns non-nil; a delivered ack, a lost terminal, and an error
// terminal all proceed to observation — the verb already landed.
func (c *Control) dispatchDrain(ctx context.Context) error {
	result, err := c.session.Drain(ctx)
	if rejection := result.Rejection(); rejection != nil {
		if errors.Is(rejection, wire.ErrPermissionDenied) {
			return fmt.Errorf("%w: %w", ErrUntrusted, rejection)
		}
		return fmt.Errorf("daemonkit: drain refused: %w", rejection)
	}
	if err != nil && result.Outcome == wire.PreSendFailure {
		return fmt.Errorf("daemonkit: send drain: %w", err)
	}
	return nil
}

// Close releases the control session without affecting the daemon. Like every
// other verb it refuses a context without a deadline, and it honors the one it
// is given: a pending call that will not settle inside ctx tears the session
// down instead of holding Close for the transport's own timeouts.
func (c *Control) Close(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("daemonkit: Close requires a context deadline")
	}
	return c.session.Close(ctx)
}
