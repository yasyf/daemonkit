package durable

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
)

// ErrLockBusy means an acquisition attempt found the lock held by another
// owner. It is the one identity the fleet aliases and matches with errors.Is,
// declared once (STYLEGUIDE.md § Sentinel identity is load-bearing).
var ErrLockBusy = flock.ErrLockBusy

// AcquireLock takes exclusive ownership of the lock file at path, bounded by
// ctx, which must carry a deadline. flock(2) binds ownership to the open file
// description and every acquisition opens its own, so goroutines in one
// process exclude each other exactly as processes do, with no mutex beside it.
//
// An ended budget returns ctx.Err(), joined with ErrLockBusy only when an
// attempt within it found the lock held; a context spent before the first
// attempt is the context error alone, never contention.
//
// The lock is not reentrant. A scope that mutates several files under one
// lock holds one Lock over all of them and orders nested locks itself —
// acquisition order is the caller's invariant, and this package never takes a
// lock the caller cannot see.
func AcquireLock(ctx context.Context, path string) (*Lock, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, fmt.Errorf("durable: lock %s requires a context deadline", path)
	}
	handle, err := (flock.Spec{
		Path:     path,
		Mode:     flock.Exclusive,
		Deadline: max(time.Until(deadline), time.Nanosecond),
	}).Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &Lock{handle: handle}, nil
}

// Lock is exclusive ownership of one lock file until Close. Reads that feed a
// write must run while it is held; a lock-free ReadFile is consistent only
// because rename is atomic, and writing back what it returned is a lost
// update.
type Lock struct {
	handle *flock.Handle
	once   sync.Once
	err    error
}

// Close releases the lock. Idempotent. The lock file is retained: unlinking a
// held lock mints a second inode another process can own concurrently.
func (l *Lock) Close() error {
	l.once.Do(func() { l.err = l.handle.Close() })
	return l.err
}
