package state

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

type bound string

func (p bound) read() ([]byte, error) {
	raw, err := os.ReadFile(string(p))
	if err != nil {
		return nil, fmt.Errorf("state: read %s: %w", p, err)
	}
	return raw, nil
}

func (p bound) write(raw []byte) error {
	dir := filepath.Dir(string(p))
	if err := mkdirDurable(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return fmt.Errorf("state: create temp beside %s: %w", p, err)
	}
	if err := writeSynced(tmp, raw); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("state: write temp for %s: %w", p, err)
	}
	if err := os.Rename(tmp.Name(), string(p)); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("state: publish %s: %w", p, err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("state: persist %s: %w", p, err)
	}
	return nil
}

func (p bound) archive() (string, error) {
	dir := filepath.Dir(string(p))
	reserved, err := os.CreateTemp(dir, filepath.Base(string(p))+".*.bak")
	if err != nil {
		return "", fmt.Errorf("state: reserve archive beside %s: %w", p, err)
	}
	aside := reserved.Name()
	reserved.Close()
	if err := os.Rename(string(p), aside); err != nil {
		_ = os.Remove(aside)
		return "", fmt.Errorf("state: archive %s: %w", p, err)
	}
	if err := syncDir(dir); err != nil {
		return "", fmt.Errorf("state: persist archive of %s: %w", p, err)
	}
	slog.Warn("state: archived unusable state file", "path", string(p), "archived", aside)
	return aside, nil
}

func writeSynced(f *os.File, raw []byte) (err error) {
	defer func() {
		if closed := f.Close(); err == nil {
			err = closed
		}
	}()
	if _, err := f.Write(raw); err != nil {
		return err
	}
	return f.Sync()
}

func mkdirDurable(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("state: inspect %s: %w", dir, err)
	}
	parent := filepath.Dir(dir)
	if err := mkdirDurable(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("state: create %s: %w", dir, err)
	}
	if err := syncDir(parent); err != nil {
		return fmt.Errorf("state: persist %s: %w", dir, err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
