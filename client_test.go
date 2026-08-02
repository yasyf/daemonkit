package daemonkit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
)

// openClient states the same-user posture a test daemon takes and fails the
// test on a Daemon Open refuses.
func openClient(t *testing.T, d Daemon) *Client {
	t.Helper()
	d.Trust.Serving = ServingSameUser()
	client, err := Open(d)
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	return client
}

// TestObserveGoneCadenceIsClamped keeps a caller's give-up bound from becoming
// the resolution at which an exit is observed: a 24h Settle deadline once
// derived a 22m30s cadence, so a daemon gone a second after the first probe
// went unobserved for another 22 minutes.
func TestObserveGoneCadenceIsClamped(t *testing.T) {
	var probes atomic.Int64
	observe := func(proc.Identity) (proc.Reap, bool, error) {
		if probes.Add(1) == 1 {
			return 0, false, nil
		}
		return proc.ReapAbsent, true, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	start := time.Now()
	reap, err := observeGone(ctx, observe, proc.Identity{PID: 4242, Start: 1, Boot: 2})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("observeGone() = %v", err)
	}
	if reap != ReapAbsent {
		t.Fatalf("Reap = %d, want ReapAbsent", reap)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("observeGone() took %v to see a departure the second probe proved", elapsed)
	}
}

func TestObserveGoneStopsAtAnExpiredContext(t *testing.T) {
	var probes atomic.Int64
	observe := func(proc.Identity) (proc.Reap, bool, error) {
		probes.Add(1)
		return 0, false, nil
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := observeGone(ctx, observe, proc.Identity{PID: 4242, Start: 1, Boot: 2})
	if !errors.Is(err, ErrUnsettled) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("observeGone() = %v, want ErrUnsettled joined with the ctx error", err)
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("probes = %d, want one observation before the expired deadline stops the loop", got)
	}
}
