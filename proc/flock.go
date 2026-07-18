package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const flockPollInterval = 25 * time.Millisecond

// FlockHandle owns an acquired advisory lock.
type FlockHandle struct {
	mu       sync.Mutex
	f        *os.File
	released bool
}

// Release idempotently drops the lock and closes the handle, returning the
// first call's unlock or close failure. The lock file is left on disk on
// purpose: unlinking under flock races other processes that have it open.
func (h *FlockHandle) Release() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return nil
	}
	unlockErr := unix.Flock(int(h.f.Fd()), unix.LOCK_UN)
	closeErr := h.f.Close()
	h.released = true
	if unlockErr != nil {
		return fmt.Errorf("unlock %s: %w", h.f.Name(), unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close lock %s: %w", h.f.Name(), closeErr)
	}
	return nil
}

// TryLock takes an exclusive advisory lock on path without blocking, returning
// ErrLockBusy when another owner already holds it. The caller Releases the
// returned handle; the lock file is left on disk (see Release).
func TryLock(path string) (*FlockHandle, error) {
	if err := mkdirAllDurable(filepath.Dir(path), 0o700, fsyncDir); err != nil { //nolint:gosec // G703: callers pass lock paths they own, not user-tainted input
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: callers pass lock paths they own, not user input
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", path, err)
	}
	err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err == nil {
		return &FlockHandle{f: f}, nil
	}
	_ = f.Close()
	if errors.Is(err, unix.EWOULDBLOCK) {
		return nil, ErrLockBusy
	}
	return nil, fmt.Errorf("flock %s: %w", path, err)
}

// Flock takes an exclusive cross-process advisory lock on path. It polls
// rather than blocking in the syscall so ctx cancellation is observed and no
// goroutine leaks on a stuck holder.
func Flock(ctx context.Context, path string) (*FlockHandle, error) {
	//nolint:gosec // G703: callers pass lock paths they own, not user-tainted input
	if err := mkdirAllDurable(filepath.Dir(path), 0o700, fsyncDir); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: callers pass lock paths they own, not user input
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", path, err)
	}
	return flockPoll(ctx, f, path)
}

func flockPoll(ctx context.Context, f *os.File, path string) (*FlockHandle, error) {
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &FlockHandle{f: f}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			_ = f.Close()
			return nil, fmt.Errorf("flock %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, fmt.Errorf("flock %s: %w", path, ctx.Err())
		case <-time.After(flockPollInterval):
		}
	}
}

func mkdirAllDurable(path string, perm os.FileMode, syncDir func(string) error) error {
	if _, err := os.Stat(path); err == nil { //nolint:gosec // G703: callers pass validated state-dir paths, not user input
		if err := syncDir(filepath.Dir(path)); err != nil {
			return fmt.Errorf("fsync parent of %s: %w", path, err)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if err := mkdirAllDurable(parent, perm, syncDir); err != nil {
		return err
	}
	if err := os.Mkdir(path, perm); err != nil && !errors.Is(err, os.ErrExist) { //nolint:gosec // G703: callers pass validated state-dir paths, not user input
		return err
	}
	if err := syncDir(parent); err != nil {
		return fmt.Errorf("fsync parent of %s: %w", path, err)
	}
	return nil
}

func fsyncDir(path string) error {
	dir, err := os.Open(path) //nolint:gosec // G304: callers pass lock paths they own, not user input
	if err != nil {
		return fmt.Errorf("open dir %s: %w", path, err)
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return fmt.Errorf("fsync dir %s: %w", path, err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close dir %s: %w", path, err)
	}
	return nil
}
