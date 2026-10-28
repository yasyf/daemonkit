package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
)

// WaitReady attaches and waits for a daemon to publish PhaseReady, returning
// the Health it serves. A daemon launchd has only just been asked to start has
// no socket to subscribe to yet, and one still leaving answers the frozen drain
// preamble — so the attach retries on those two proven-transient refusals, on a
// cadence derived from the caller's own deadline, and the readiness itself is
// still the daemon's own publication rather than a poll.
//
// Every other refusal is returned as it came: a foreign protocol and a
// runtime that failed to start are answers, not states to wait out. An attach
// that only ran out of budget is none of them — it is the caller's deadline,
// reported as the deadline joined with the timeout net raised in its place.
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

// waitReadyOnce rides the phase stream of the client's session — the daemon's
// publication wakes it — and costs the daemon nothing while it waits, then
// reads Health on that same session, so the report is the ready daemon's. A
// session whose peer leaves instead of becoming ready, or dies under it, is
// retired and reported as the drain it was, for the loop to attach again.
func (c *Client) waitReadyOnce(ctx context.Context) (Health, error) {
	session, _, err := c.link.acquire(ctx)
	if err != nil {
		return Health{}, err
	}
	err = session.WaitReady(ctx)
	var report wire.HealthReport
	if err == nil {
		report, err = session.Health(ctx)
	}
	if err == nil {
		return healthFromReport(report), nil
	}
	broken := session.Failure() != nil
	c.link.retire(session)
	if errors.Is(err, wire.ErrDraining) || broken {
		return Health{}, fmt.Errorf("%w: %w", ErrDraining, err)
	}
	return Health{}, err
}

// attachCadence derives the retry interval from the caller's own deadline, on
// the same terms observeGone derives its observation interval: no timer
// constant of this package's choosing, and a generous bound never becomes the
// resolution at which a socket's appearance is noticed.
func attachCadence(ctx context.Context) time.Duration {
	deadline, _ := ctx.Deadline()
	return max(min(time.Until(deadline)/observationsPerBudget, maxObservationCadence), time.Millisecond)
}
