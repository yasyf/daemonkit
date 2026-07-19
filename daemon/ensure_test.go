package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestEnsureCurrentPollsPastOldVersion(t *testing.T) {
	old := Health{Build: "1.0.0", PID: 100}
	target := Health{Build: "2.0.0", PID: 200}
	peer := &fakePeer{health: []healthResult{
		{h: old}, {h: old}, {h: old}, {h: target},
	}}
	cfg := EnsureConfig{
		Peer:     peer,
		LockPath: filepath.Join(t.TempDir(), "ensure.lock"),
		clock:    newAutoClock(),
	}

	if err := EnsureCurrent(context.Background(), cfg, "2.0.0"); err != nil {
		t.Fatalf("EnsureCurrent: %v", err)
	}
	if got := peer.healthCalls(); got < 4 {
		t.Errorf("health calls = %d, want >=4 (must poll past the old answers)", got)
	}
}

func TestEnsureCurrentTimeout(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{h: Health{Build: "1.0.0", PID: 100}}}}
	cfg := EnsureConfig{
		Peer:     peer,
		LockPath: filepath.Join(t.TempDir(), "ensure.lock"),
		Interval: 100 * time.Millisecond,
		Timeout:  350 * time.Millisecond,
		clock:    newAutoClock(),
	}

	err := EnsureCurrent(context.Background(), cfg, "2.0.0")
	if !errors.Is(err, ErrEnsureTimeout) {
		t.Fatalf("EnsureCurrent err = %v, want ErrEnsureTimeout", err)
	}
}

func TestEnsureCurrentEnsureError(t *testing.T) {
	boom := errors.New("spawn failed")
	peer := &fakePeer{health: []healthResult{{h: Health{Build: "1.0.0"}}}}
	cfg := EnsureConfig{
		Peer:     peer,
		LockPath: filepath.Join(t.TempDir(), "ensure.lock"),
		Ensure:   func(context.Context) error { return boom },
		clock:    newAutoClock(),
	}

	if err := EnsureCurrent(context.Background(), cfg, "2.0.0"); !errors.Is(err, boom) {
		t.Fatalf("EnsureCurrent err = %v, want the Ensure error", err)
	}
}

func TestEnsureCurrentRequiresExactProtocol(t *testing.T) {
	peer := &fakePeer{health: []healthResult{
		{h: Health{Build: "2.0.0", Protocol: 1, PID: 100}},
		{h: Health{Build: "2.0.0", Protocol: 2, PID: 100}},
	}}
	cfg := EnsureConfig{
		Peer:     peer,
		Protocol: 2,
		LockPath: filepath.Join(t.TempDir(), "ensure.lock"),
		clock:    newAutoClock(),
	}

	if err := EnsureCurrent(context.Background(), cfg, "2.0.0"); err != nil {
		t.Fatalf("EnsureCurrent: %v", err)
	}
	if got := peer.healthCalls(); got < 2 {
		t.Fatalf("health calls = %d, want protocol mismatch to keep polling", got)
	}
}
