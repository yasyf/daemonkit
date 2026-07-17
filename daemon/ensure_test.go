package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestEnsureCurrentPollsPastOldVersion: while the retiring older daemon still
// answers Health, EnsureCurrent keeps polling and returns only once the peer
// reports EXACTLY the target version.
func TestEnsureCurrentPollsPastOldVersion(t *testing.T) {
	old := Health{Version: "1.0.0", PID: 100}
	target := Health{Version: "2.0.0", PID: 200}
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

// TestEnsureCurrentTimeout: a peer stuck on the old version fails with
// ErrEnsureTimeout once the deadline elapses.
func TestEnsureCurrentTimeout(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{h: Health{Version: "1.0.0", PID: 100}}}}
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

// TestEnsureCurrentEnsureError: an Ensure hook failure aborts the wait.
func TestEnsureCurrentEnsureError(t *testing.T) {
	boom := errors.New("spawn failed")
	peer := &fakePeer{health: []healthResult{{h: Health{Version: "1.0.0"}}}}
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
