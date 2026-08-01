package durable

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
)

func TestAcquireLockRequiresADeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.lock")
	if _, err := AcquireLock(context.Background(), path); err == nil {
		t.Fatal("AcquireLock accepted a deadline-free context")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a refused acquisition minted the lock file: %v", statErr)
	}
}

// TestAcquireLockExcludesGoroutines pins the property that deletes cc-patch's
// sync.Mutex+flock pair: flock(2) binds ownership to the open file description
// and every acquisition opens its own, so goroutines contending in one process
// exclude each other exactly as separate processes do. The barrier releases
// all contenders at once, so a lock that stopped excluding shows a peak above
// one rather than an interleaving that happened not to overlap.
func TestAcquireLockExcludesGoroutines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.lock")
	var inside, peak atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			<-start
			lock, err := AcquireLock(ctx, path)
			if err != nil {
				t.Errorf("AcquireLock: %v", err)
				return
			}
			held := inside.Add(1)
			for {
				high := peak.Load()
				if held <= high || peak.CompareAndSwap(high, held) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inside.Add(-1)
			if err := lock.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := peak.Load(); got != 1 {
		t.Fatalf("peak concurrent lock holders = %d, want 1", got)
	}
}

func TestAcquireLockReportsContentionAsErrLockBusy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.lock")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	held, err := AcquireLock(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	busyCtx, busyCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer busyCancel()
	if _, err := AcquireLock(busyCtx, path); !errors.Is(err, ErrLockBusy) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended AcquireLock = %v, want ErrLockBusy joined with the ctx error", err)
	}
}

// TestErrLockBusyKeepsOneIdentity pins the cross-module contract: consumers
// alias this value and match it with errors.Is from other repos, so it is
// aliased here, never re-declared.
func TestErrLockBusyKeepsOneIdentity(t *testing.T) {
	if ErrLockBusy.Error() != "durable: lock held by another owner" {
		t.Fatalf("ErrLockBusy = %q, want the durable register", ErrLockBusy)
	}
	spec := flock.Spec{Path: filepath.Join(t.TempDir(), "state.lock"), Mode: flock.Exclusive, Deadline: time.Second}
	held, err := spec.TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if _, err := spec.TryAcquire(); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("a flock contention error = %v, does not match durable.ErrLockBusy", err)
	}
}

// TestAcquireLockDoesNotReportAConfigErrorAsContention pins the one condition
// §3.1 names: ErrLockBusy means the deadline expired with the lock still held.
// A caller retries on contention, so masking an unusable lock path as
// contention turns a permanent refusal into an infinite retry.
func TestAcquireLockDoesNotReportAConfigErrorAsContention(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := AcquireLock(ctx, "relative/state.lock")
	if errors.Is(err, ErrLockBusy) {
		t.Fatalf("AcquireLock over an unusable path = %v, want the path refusal, not contention", err)
	}
	if !errors.Is(err, flock.ErrInvalidFileLock) {
		t.Fatalf("AcquireLock over an unusable path = %v, want ErrInvalidFileLock", err)
	}
}

func TestLockCloseIsIdempotentAndRetainsTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.lock")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lock, err := AcquireLock(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file unlinked on release: %v", err)
	}
	reacquired, err := AcquireLock(ctx, path)
	if err != nil {
		t.Fatalf("re-acquire after Close: %v", err)
	}
	if err := reacquired.Close(); err != nil {
		t.Fatal(err)
	}
}
