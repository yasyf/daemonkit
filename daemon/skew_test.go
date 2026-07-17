package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// neverClock's After never fires, so a Run loop's only ready select case is a
// cancelled ctx.
type neverClock struct{}

func (neverClock) Now() time.Time                       { return time.Time{} }
func (neverClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

// TestSkewNeedsConsecutiveConfirmations: OnSkew fires only after Confirmations
// consecutive skew observations, and any non-skew tick resets the count.
func TestSkewNeedsConsecutiveConfirmations(t *testing.T) {
	installed := "2.0.0"
	fires := 0
	w := NewSkewWatch(SkewConfig{
		Running:       func() string { return "1.0.0" },
		Installed:     func() (string, error) { return installed, nil },
		OnSkew:        func(context.Context) error { fires++; return nil },
		Confirmations: 3,
	})
	ctx := context.Background()
	now := time.Now()
	tick := func() {
		if err := w.tick(ctx, now); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}

	tick() // c=1
	tick() // c=2
	if fires != 0 {
		t.Fatalf("fires = %d after 2 confirmations, want 0", fires)
	}
	installed = "1.0.0" // artifact matches running: not a skew
	tick()              // c reset to 0
	installed = "2.0.0"
	tick() // c=1
	tick() // c=2
	if fires != 0 {
		t.Fatalf("fires = %d, want 0 (reset must force a fresh streak)", fires)
	}
	tick() // c=3 -> fire
	if fires != 1 {
		t.Fatalf("fires = %d after 3 consecutive confirmations, want 1", fires)
	}
}

// TestSkewStormBudget: the proc.Strikes budget caps firing within its window.
func TestSkewStormBudget(t *testing.T) {
	fires := 0
	w := NewSkewWatch(SkewConfig{
		Running:   func() string { return "1.0.0" },
		Installed: func() (string, error) { return "2.0.0", nil },
		OnSkew:    func(context.Context) error { fires++; return nil },
		Strikes:   &proc.Strikes{Limit: 2, Window: time.Hour},
	})
	ctx := context.Background()
	base := time.Now()

	for i := range 5 {
		if err := w.tick(ctx, base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	if fires != 2 {
		t.Fatalf("fires = %d over 5 skew ticks, want 2 (strike budget)", fires)
	}
}

// TestSkewInstalledReadErrorIsNotSkew: a failing artifact read is not a skew and
// resets the streak rather than firing.
func TestSkewInstalledReadErrorIsNotSkew(t *testing.T) {
	fires := 0
	w := NewSkewWatch(SkewConfig{
		Running:   func() string { return "1.0.0" },
		Installed: func() (string, error) { return "", context.DeadlineExceeded },
		OnSkew:    func(context.Context) error { fires++; return nil },
	})
	if err := w.tick(context.Background(), time.Now()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if fires != 0 {
		t.Fatalf("fires = %d on an unreadable artifact, want 0", fires)
	}
}

// TestSkewOlderArtifactIsNotSkew: an installed artifact OLDER than the running
// version never fires — skew is a NEWER artifact, so a drain never downgrades.
func TestSkewOlderArtifactIsNotSkew(t *testing.T) {
	fires := 0
	w := NewSkewWatch(SkewConfig{
		Running:   func() string { return "2.0.0" },
		Installed: func() (string, error) { return "1.0.0", nil },
		OnSkew:    func(context.Context) error { fires++; return nil },
	})
	if err := w.tick(context.Background(), time.Now()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if fires != 0 {
		t.Fatalf("fires = %d on an older artifact, want 0", fires)
	}
}

// TestSkewRunHonorsContext: Run returns the ctx error when cancelled.
func TestSkewRunHonorsContext(t *testing.T) {
	w := NewSkewWatch(SkewConfig{
		Running:   func() string { return "1.0.0" },
		Installed: func() (string, error) { return "1.0.0", nil },
		OnSkew:    func(context.Context) error { return nil },
		clock:     neverClock{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Run(ctx); err != context.Canceled {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
}
