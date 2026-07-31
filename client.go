package daemonkit

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
)

// Open prepares a client for d's daemon and performs no I/O. Every verb
// refuses a context without a deadline: all stall bounds derive from it.
func Open(d Daemon) *Client {
	return &Client{
		daemon:     d,
		recordPath: d.recordPath(),
		probe:      proc.ProbeIdentity,
		observe:    proc.Observe,
		readOwner:  proc.ReadOwner,
	}
}

// Client reaches one daemon named by its Daemon identity.
type Client struct {
	daemon     Daemon
	recordPath string
	probe      func(int) (proc.Identity, error)
	observe    func(proc.Identity) (proc.Reap, bool, error)
	readOwner  func(string) (proc.Owner, bool, error)
}

// Settle proves the recorded incumbent gone without a live session. It reads
// the durable owner record — the {pid, start, boot, generation, build} core
// Serve persists into the record file behind the flock before it binds — and
// observes that exact identity out of the process table under ctx. It dials
// nothing, sends nothing, and never signals: it works mid-drain (where
// Control returns ErrDraining with no session to pin), against a husk (live
// flock-holder, dead listener, ErrAbsent), and after a caller crash.
//
// A non-zero Expect field disagreeing with the record refuses with
// ErrWrongIncumbent. No owner record at all is ErrUnrecorded. Success returns
// Stopped whose Before is synthesized from the record — PID, Generation, and
// Build set; Phase, Protocol, and Detail zero — and whose Reap is the same
// anti-PID-reuse proof Drain returns. ErrUnsettled (joined with ctx.Err())
// when the recorded identity was still present at ctx end.
//
// The record file is same-UID writable, so the proof is no stronger than it.
// A record missing any field an observation keys on is refused outright, but
// a same-UID writer that copies the live incumbent's Build and Generation and
// repoints {pid, start, boot} at a dead instance is answered with a true
// ReapAbsent for a daemon still running. Expect cannot backstop that — it
// compares against the same record. A caller gating an irreversible action —
// uninstall, deactivation — must also require an executable-scoped inventory
// of the real process table to be empty, which no record file can forge.
func (c *Client) Settle(ctx context.Context, expect Expect) (Stopped, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Stopped{}, errors.New("daemonkit: Settle requires a context deadline")
	}
	owner, ok, err := c.readOwner(c.recordPath)
	if err != nil {
		return Stopped{}, err
	}
	if !ok {
		return Stopped{}, ErrUnrecorded
	}
	if expect.mismatch(owner.Build, owner.Generation) {
		return Stopped{}, ErrWrongIncumbent
	}
	reap, err := observeGone(ctx, c.observe, owner.Identity())
	if err != nil {
		return Stopped{}, err
	}
	return Stopped{
		Before: Health{Generation: owner.Generation, PID: owner.PID, Build: owner.Build},
		Reap:   reap,
	}, nil
}

const (
	// observationsPerBudget derives the observation cadence from the caller's
	// own deadline: no timer constant of this package's choosing.
	observationsPerBudget = 64
	// maxObservationCadence caps that derivation, so a generous give-up bound
	// stays a bound and never becomes the resolution at which an exit is seen.
	maxObservationCadence = 250 * time.Millisecond
)

func observeGone(
	ctx context.Context,
	observe func(proc.Identity) (proc.Reap, bool, error),
	id proc.Identity,
) (Reap, error) {
	deadline, _ := ctx.Deadline()
	cadence := min(time.Until(deadline)/observationsPerBudget, maxObservationCadence)
	timer := time.NewTimer(cadence)
	defer timer.Stop()
	var lastProbe error
	for {
		reap, settled, err := observe(id)
		if err == nil && settled {
			return Reap(reap), nil
		}
		lastProbe = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ReapUndetermined, errors.Join(ErrUnsettled, ctxErr, lastProbe)
		}
		timer.Reset(cadence)
		select {
		case <-ctx.Done():
			return ReapUndetermined, errors.Join(ErrUnsettled, ctx.Err(), lastProbe)
		case <-timer.C:
		}
	}
}
