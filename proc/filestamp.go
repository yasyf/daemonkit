package proc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ErrInvalidFileStamp means a file-stamp specification is incomplete or unsafe.
var ErrInvalidFileStamp = errors.New("proc: invalid file stamp")

// FileStamp is a cross-process throttle: at most one Claim succeeds per Window at
// Path. It answers "has enough time passed since the last claim" with the stamp
// file's existence and modification time, so independent processes racing to run
// a periodic job resolve to exactly one winner per window.
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
// current Window and may proceed. An O_EXCL create decides the absent-stamp race,
// a stamp younger than Window loses, and a stamp older than Window is unlinked
// and reclaimed — so a caller racing a stale peer's entry resolves to one winner.
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
	for range 2 {
		file, err := os.OpenFile(s.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			if err := errors.Join(file.Close(), fsyncDir(directory)); err != nil {
				return false, fmt.Errorf("proc: persist file stamp: %w", err)
			}
			return true, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return false, fmt.Errorf("proc: claim file stamp: %w", err)
		}
		info, statErr := os.Stat(s.Path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return false, fmt.Errorf("proc: inspect file stamp: %w", statErr)
		}
		if s.clock().Sub(info.ModTime()) < s.Window {
			return false, nil
		}
		if err := os.Remove(s.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("proc: reclaim file stamp: %w", err)
		}
	}
	return false, nil
}
