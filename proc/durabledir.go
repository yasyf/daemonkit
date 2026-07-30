package proc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

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
