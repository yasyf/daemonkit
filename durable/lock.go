package durable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
)

// ErrLockBusy means the lock was held by another owner for the caller's whole
// deadline. It is the one identity the fleet aliases and matches with
// errors.Is; it is declared exactly once (aliased here from daemonkit's
// internal flock, per STYLEGUIDE.md § Sentinel identity is load-bearing).
var ErrLockBusy = flock.ErrLockBusy

// AcquireLock takes exclusive ownership of the lock file at path, bounded by
// ctx, which must carry a deadline. Exclusion covers goroutines as well as
// processes, and by one mechanism rather than two: flock(2) binds ownership to
// the open file description, and every acquisition opens its own, so two
// goroutines contending in one process exclude each other exactly as two
// processes do. A caller needs no in-process mutex beside this lock. A
// deadline that expires with the lock still held returns ErrLockBusy joined
// with ctx.Err().
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
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			return nil, errors.Join(ErrLockBusy, ctxErr)
		}
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
