package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
)

// WaitReady blocks until the pinned incumbent publishes PhaseReady and returns
// the Health it then serves. It is a subscription on the session's own phase
// stream — the daemon's publication wakes it — never a poll, and it costs the
// daemon nothing while it waits.
//
// ErrDraining when the pinned incumbent leaves instead of becoming ready; the
// runtime's terminal failure is returned verbatim, since only the product's
// own Start error explains it.
func (c *Control) WaitReady(ctx context.Context) (Health, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Health{}, errors.New("daemonkit: WaitReady requires a context deadline")
	}
	if err := c.session.WaitReady(ctx); err != nil {
		if errors.Is(err, wire.ErrDraining) {
			return Health{}, fmt.Errorf("%w: %w", ErrDraining, err)
		}
		return Health{}, err
	}
	return c.Health(ctx)
}

// WaitReady attaches and waits for a daemon to publish PhaseReady, returning
// the Health it serves. A daemon launchd has only just been asked to start has
// no socket to subscribe to yet, and one still leaving answers the frozen drain
// preamble — so the attach retries on those two proven-transient refusals, on a
// cadence derived from the caller's own deadline, and the readiness itself is
// still the daemon's own publication rather than a poll.
//
// Every other refusal is returned as it came: an untrusted server, an
// incomplete pin, and a runtime that failed to start are all answers, not
// states to wait out. An attach that only ran out of budget is none of them —
// it is the caller's deadline, reported as the deadline joined with the timeout
// net raised in its place.
func (c *Client) WaitReady(ctx context.Context) (Health, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Health{}, errors.New("daemonkit: WaitReady requires a context deadline")
	}
	timer := time.NewTimer(attachCadence(ctx))
	defer timer.Stop()
	for {
		health, err := c.waitReadyOnce(ctx)
		if err == nil {
			return health, nil
		}
		if spent(ctx, err) {
			return Health{}, errors.Join(err, context.DeadlineExceeded)
		}
		if !errors.Is(err, ErrAbsent) && !errors.Is(err, ErrDraining) {
			return Health{}, err
		}
		timer.Reset(attachCadence(ctx))
		select {
		case <-ctx.Done():
			return Health{}, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}

func (c *Client) waitReadyOnce(ctx context.Context) (Health, error) {
	control, err := c.Control(ctx)
	if err != nil {
		return Health{}, err
	}
	defer func() { _ = control.Close(ctx) }()
	return control.WaitReady(ctx)
}

// attachCadence derives the retry interval from the caller's own deadline, on
// the same terms observeGone derives its observation interval: no timer
// constant of this package's choosing, and a generous bound never becomes the
// resolution at which a socket's appearance is noticed.
func attachCadence(ctx context.Context) time.Duration {
	deadline, _ := ctx.Deadline()
	return max(min(time.Until(deadline)/observationsPerBudget, maxObservationCadence), time.Millisecond)
}
