package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ErrInvalidFileStamp means a file-stamp specification is incomplete or unsafe.
var ErrInvalidFileStamp = errors.New("proc: invalid file stamp")

const (
	fileStampLockDeadline = 5 * time.Second
	// fileStampFutureSlack bounds clock skew: a stamp whose mtime is further than
	// this into the future is a crashed writer plus a corrected clock, not a
	// recent claim, so it is reclaimed rather than blocking every caller.
	fileStampFutureSlack = 5 * time.Second
)

// FileStamp is a cross-process throttle: at most one Claim succeeds per Window at
// Path. It answers "has enough time passed since the last claim" with the stamp
// file's modification time, so independent processes racing to run a periodic
// job resolve to exactly one winner per window.
type FileStamp struct {
	Path   string
	Window time.Duration
	now    func() time.Time
}

func (s FileStamp) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Claim atomically claims the stamp; it reports true when this caller won the
// current Window and may proceed. A sidecar file lock serializes the whole
// check-and-refresh, so racing callers resolve to one winner: an absent stamp, a
// stamp older than Window, or a stamp whose mtime is implausibly far in the
// future is claimed and refreshed to now; any other stamp loses.
func (s FileStamp) Claim() (bool, error) {
	if !filepath.IsAbs(s.Path) || filepath.Clean(s.Path) != s.Path {
		return false, fmt.Errorf("%w: path %q is not exact and absolute", ErrInvalidFileStamp, s.Path)
	}
	if s.Window <= 0 {
		return false, fmt.Errorf("%w: window must be positive", ErrInvalidFileStamp)
	}
	directory := filepath.Dir(s.Path)
	if err := mkdirAllDurable(directory, 0o700, fsyncDir); err != nil {
		return false, fmt.Errorf("proc: create file stamp directory: %w", err)
	}
	lock, err := (FileLockSpec{Path: s.Path + ".lock", Mode: FileLockExclusive, Deadline: fileStampLockDeadline}).Acquire(context.Background())
	if err != nil {
		return false, fmt.Errorf("proc: lock file stamp: %w", err)
	}
	defer func() { _ = lock.Close() }()
	return s.claimLocked(directory)
}

func (s FileStamp) claimLocked(directory string) (bool, error) {
	info, err := os.Stat(s.Path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s.refresh(directory)
	case err != nil:
		return false, fmt.Errorf("proc: inspect file stamp: %w", err)
	}
	// A stamp older than the window is stale; one implausibly far in the future
	// is a crashed writer, also stale. A minor future skew stays a recent claim
	// (an early extra fire on a forward clock jump is acceptable for a throttle).
	if age := s.clock().Sub(info.ModTime()); age >= s.Window || age < -fileStampFutureSlack {
		return s.refresh(directory)
	}
	return false, nil
}

func (s FileStamp) refresh(directory string) (bool, error) {
	now := s.clock()
	file, err := os.OpenFile(s.Path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return false, fmt.Errorf("proc: write file stamp: %w", err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("proc: write file stamp: %w", err)
	}
	if err := os.Chtimes(s.Path, now, now); err != nil {
		return false, fmt.Errorf("proc: stamp file time: %w", err)
	}
	if err := fsyncDir(directory); err != nil {
		return false, fmt.Errorf("proc: persist file stamp: %w", err)
	}
	return true, nil
}
