package proc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

const defaultEvictPollTimeout = 30 * time.Second

// SingleEntrant makes the stale-check/remove/bind of a unix socket
// single-entrant across processes via an exclusive flock on Socket+".lock",
// held for the listener's lifetime. The lock file is never removed: unlinking
// a held lock would let a third process flock a fresh inode.
type SingleEntrant struct {
	Socket string
	// Evict decides what to do about a live peer, consulted at most once per Listen. Required.
	Evict func() (evicted bool, err error)
	// Timeout bounds the post-evict lock poll; zero means a sensible default.
	Timeout time.Duration

	clock clock
}

// Listen binds the socket (0600), consulting Evict on contention, and returns
// the listener and the held lock. A post-evict lock poll honors ctx.
func (se SingleEntrant) Listen(ctx context.Context) (net.Listener, *os.File, error) {
	if err := os.MkdirAll(filepath.Dir(se.Socket), 0o700); err != nil {
		return nil, nil, fmt.Errorf("ensure socket dir: %w", err)
	}
	lock, err := openFileLock(se.Socket + ".lock")
	if err != nil {
		return nil, nil, fmt.Errorf("open socket lock: %w", err)
	}
	lockErr := tryFileLock(lock, FileLockExclusive)
	contended := errors.Is(lockErr, unix.EWOULDBLOCK)
	if lockErr != nil && !contended {
		lock.Close()
		return nil, nil, fmt.Errorf("lock socket %s: %w", se.Socket, lockErr)
	}
	// Evict runs even on a free lock: a live peer may predate the lock discipline.
	evicted, eerr := se.Evict()
	switch {
	case eerr != nil:
		lock.Close()
		return nil, nil, eerr
	case evicted && contended:
		if err := se.pollLock(ctx, lock); err != nil {
			lock.Close()
			return nil, nil, err
		}
	case !evicted && contended:
		lock.Close()
		return nil, nil, ErrPeerStarting
	}
	_ = os.Remove(se.Socket) // stale socket: the lock is ours and any live peer was evicted
	ln, err := net.Listen("unix", se.Socket)
	if err != nil {
		lock.Close()
		return nil, nil, fmt.Errorf("listen on %s: %w", se.Socket, err)
	}
	if err := os.Chmod(se.Socket, 0o600); err != nil {
		ln.Close()
		lock.Close()
		return nil, nil, fmt.Errorf("chmod %s: %w", se.Socket, err)
	}
	return ln, lock, nil
}

// Polls: the evictee's flock releases only at its process exit, which can lag socket death by seconds.
func (se SingleEntrant) pollLock(ctx context.Context, lock *os.File) error {
	timeout := se.Timeout
	if timeout <= 0 {
		timeout = defaultEvictPollTimeout
	}
	clk := clockOrReal(se.clock)
	deadline := clk.Now().Add(timeout)
	for {
		err := tryFileLock(lock, FileLockExclusive)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return fmt.Errorf("poll socket lock %s: %w", se.Socket, err)
		}
		if clk.Now().After(deadline) {
			return fmt.Errorf("%w within %s: %w", ErrLockStillHeld, timeout, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrLockStillHeld, ctx.Err())
		case <-clk.After(100 * time.Millisecond):
		}
	}
}
