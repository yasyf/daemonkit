package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SyncDir fsyncs a directory so entry creations, renames, and removals in it
// survive a power loss. Prefer the paired verbs below: knowing which
// directories to sync, and in what order, is where the silent-corruption bugs
// live.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("durable: open dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("durable: fsync dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("durable: close dir %s: %w", dir, err)
	}
	return nil
}

// Rename renames from to to and fsyncs both parent directories when they
// differ. Syncing only the destination leaves the source's entry removal
// undurable: a crash can resurface the tree at both paths at once.
func Rename(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}
	source, target := filepath.Dir(from), filepath.Dir(to)
	if err := SyncDir(target); err != nil {
		return err
	}
	if source == target {
		return nil
	}
	return SyncDir(source)
}

// Mkdir creates dir and fsyncs its parent. An already-present directory is
// success: creation, like removal, is a state, not an event, so resumable
// ladders re-run it.
func Mkdir(dir string, perm os.FileMode) error {
	if err := os.Mkdir(dir, perm); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return SyncDir(filepath.Dir(dir))
}

// Remove unlinks path and fsyncs its parent directory. An already-absent path
// is success: removal is a state, not an event, so resumable ladders re-run it.
func Remove(path string) error {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return SyncDir(filepath.Dir(path))
}

// RemoveTree removes the tree at path and fsyncs its parent directory.
// An already-absent path is success.
func RemoveTree(path string) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(path))
}
