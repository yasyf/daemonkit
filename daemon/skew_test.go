package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

type neverClock struct{}

func (neverClock) Now() time.Time                       { return time.Time{} }
func (neverClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

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

	tick()
	tick()
	if fires != 0 {
		t.Fatalf("fires = %d after 2 confirmations, want 0", fires)
	}
	installed = "1.0.0"
	tick()
	installed = "2.0.0"
	tick()
	tick()
	if fires != 0 {
		t.Fatalf("fires = %d, want 0 (reset must force a fresh streak)", fires)
	}
	tick()
	if fires != 1 {
		t.Fatalf("fires = %d after 3 consecutive confirmations, want 1", fires)
	}
}

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
