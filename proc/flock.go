package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const flockPollInterval = 25 * time.Millisecond

// FileLockMode selects shared or exclusive advisory ownership.
type FileLockMode uint8

const (
	// FileLockShared admits other shared owners and excludes exclusive owners.
	FileLockShared FileLockMode = iota + 1
	// FileLockExclusive excludes every other owner.
	FileLockExclusive
)

// FileLockSpec identifies one bounded advisory-lock acquisition.
type FileLockSpec struct {
	Path string
	Mode FileLockMode
	// Deadline bounds acquisition. It must be positive even for TryAcquire so a
	// spec cannot silently become unbounded when its acquisition mode changes.
	Deadline time.Duration
}

// FlockHandle owns an acquired advisory lock.
type FlockHandle struct {
	mu       sync.Mutex
	f        *os.File
	closed   bool
	closeErr error
}

// Close idempotently drops the lock and closes the handle. The lock file is
// deliberately retained because unlinking a held lock creates a second inode
// that another process can own concurrently.
func (h *FlockHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return h.closeErr
	}
	unlockErr := unix.Flock(int(h.f.Fd()), unix.LOCK_UN)
	closeErr := h.f.Close()
	h.closed = true
	if unlockErr != nil {
		h.closeErr = fmt.Errorf("unlock %s: %w", h.f.Name(), unlockErr)
		return h.closeErr
	}
	if closeErr != nil {
		h.closeErr = fmt.Errorf("close lock %s: %w", h.f.Name(), closeErr)
		return h.closeErr
	}
	return nil
}

// Release drops ownership and closes the handle.
func (h *FlockHandle) Release() error { return h.Close() }

// FileLockHandle owns a FileLockSpec acquisition.
type FileLockHandle = FlockHandle

// Acquire waits for ownership until the earlier of ctx cancellation and the
// spec's explicit Deadline.
func (s FileLockSpec) Acquire(ctx context.Context) (*FileLockHandle, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidFileLock)
	}
	ctx, cancel := context.WithTimeout(ctx, s.Deadline)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("flock %s: %w", s.Path, err)
	}
	f, err := openFileLock(s.Path)
	if err != nil {
		return nil, err
	}
	return fileLockPoll(ctx, f, s.Path, s.Mode)
}

// TryAcquire attempts ownership once and returns ErrLockBusy on contention.
func (s FileLockSpec) TryAcquire() (*FileLockHandle, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	f, err := openFileLock(s.Path)
	if err != nil {
		return nil, err
	}
	err = tryFileLock(f, s.Mode)
	if err == nil {
		return &FileLockHandle{f: f}, nil
	}
	if errors.Is(err, unix.EWOULDBLOCK) {
		_ = f.Close()
		return nil, ErrLockBusy
	}
	_ = f.Close()
	return nil, fmt.Errorf("flock %s: %w", s.Path, err)
}

func (s FileLockSpec) validate() error {
	if s.Path == "" {
		return fmt.Errorf("%w: empty path", ErrInvalidFileLock)
	}
	if !filepath.IsAbs(s.Path) || filepath.Clean(s.Path) != s.Path || s.Path == string(filepath.Separator) {
		return fmt.Errorf("%w: path %q must be absolute, clean, and non-root", ErrInvalidFileLock, s.Path)
	}
	if s.Mode != FileLockShared && s.Mode != FileLockExclusive {
		return fmt.Errorf("%w: mode %d", ErrInvalidFileLock, s.Mode)
	}
	if s.Deadline <= 0 {
		return fmt.Errorf("%w: deadline must be positive", ErrInvalidFileLock)
	}
	return nil
}

// TryLock takes an exclusive advisory lock on path without blocking, returning
// ErrLockBusy when another owner already holds it.
func TryLock(path string) (*FlockHandle, error) {
	f, err := openFileLock(path)
	if err != nil {
		return nil, err
	}
	err = tryFileLock(f, FileLockExclusive)
	if err == nil {
		return &FlockHandle{f: f}, nil
	}
	_ = f.Close()
	if errors.Is(err, unix.EWOULDBLOCK) {
		return nil, ErrLockBusy
	}
	return nil, fmt.Errorf("flock %s: %w", path, err)
}

// Flock takes an exclusive cross-process advisory lock on path, polling so ctx
// cancellation is observed without a goroutine leak on a stuck holder.
func Flock(ctx context.Context, path string) (*FlockHandle, error) {
	f, err := openFileLock(path)
	if err != nil {
		return nil, err
	}
	return fileLockPoll(ctx, f, path, FileLockExclusive)
}

func fileLockPoll(ctx context.Context, f *os.File, path string, mode FileLockMode) (*FileLockHandle, error) {
	for {
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("flock %s: %w", path, err)
		}
		err := tryFileLock(f, mode)
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

func tryFileLock(f *os.File, mode FileLockMode) error {
	operation := unix.LOCK_SH
	if mode == FileLockExclusive {
		operation = unix.LOCK_EX
	}
	return unix.Flock(int(f.Fd()), operation|unix.LOCK_NB)
}

func openFileLock(path string) (*os.File, error) {
	if err := validateFileLockPath(path); err != nil {
		return nil, err
	}
	if err := mkdirAllDurable(filepath.Dir(path), 0o700, fsyncDir); err != nil { //nolint:gosec // G703: validated absolute lock path
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600) //nolint:gosec // G304,G703: validated absolute lock path
	if errors.Is(err, unix.EEXIST) {
		fd, err = openExistingFileLock(path)
	}
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", path, err)
	}
	f := os.NewFile(uintptr(fd), path)
	if err := secureFileLock(f, path); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func openExistingFileLock(path string) (int, error) {
	flags := unix.O_CLOEXEC | unix.O_NOFOLLOW
	var lastErr error
	for _, access := range []int{unix.O_RDWR, unix.O_RDONLY, unix.O_WRONLY} {
		fd, err := unix.Open(path, flags|access, 0) //nolint:gosec // G304: validated absolute lock path
		if err == nil {
			return fd, nil
		}
		if errors.Is(err, unix.ELOOP) {
			return -1, fmt.Errorf("%w: lock path is a symlink: %w", ErrUnsafeLockFile, err)
		}
		lastErr = err
	}
	return -1, fmt.Errorf("no usable access mode: %w", lastErr)
}

func validateFileLockPath(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return fmt.Errorf("%w: path %q must be absolute, clean, and non-root", ErrInvalidFileLock, path)
	}
	return nil
}

func secureFileLock(f *os.File, path string) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat lock %s: %w", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%w: lock %s has unsupported metadata", ErrUnsafeLockFile, path)
	}
	if !info.Mode().IsRegular() || stat.Nlink != 1 {
		return fmt.Errorf("%w: lock %s must be a regular single-link file", ErrUnsafeLockFile, path)
	}
	if int64(stat.Uid) != int64(os.Geteuid()) {
		return fmt.Errorf("%w: lock %s is owned by uid %d", ErrUnsafeLockFile, path, stat.Uid)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%w: lock %s is group/world writable (%#o)", ErrUnsafeLockFile, path, info.Mode().Perm())
	}
	pathInfo, err := os.Lstat(path) //nolint:gosec // G703: validated absolute lock path
	if err != nil {
		return fmt.Errorf("lstat lock %s: %w", path, err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, pathInfo) {
		return fmt.Errorf("%w: lock %s changed during open", ErrUnsafeLockFile, path)
	}
	if info.Mode().Perm() != 0o600 {
		if err := f.Chmod(0o600); err != nil {
			return fmt.Errorf("chmod lock %s: %w", path, err)
		}
	}
	return nil
}

func mkdirAllDurable(path string, perm os.FileMode, syncDir func(string) error) error {
	if _, err := os.Stat(path); err == nil { //nolint:gosec // G703: validated state-dir paths
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
	if err := os.Mkdir(path, perm); err != nil && !errors.Is(err, os.ErrExist) { //nolint:gosec // G703: validated state-dir paths
		return err
	}
	if err := syncDir(parent); err != nil {
		return fmt.Errorf("fsync parent of %s: %w", path, err)
	}
	return nil
}

func fsyncDir(path string) error {
	dir, err := os.Open(path) //nolint:gosec // G304: caller-owned lock paths
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
