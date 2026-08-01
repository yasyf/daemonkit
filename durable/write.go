package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const stumpStaleAfter = time.Hour

// WriteFile publishes data at path atomically and durably: temp file in path's
// directory, write, chmod, fsync, rename, fsync directory. A power loss leaves
// the previous contents or data, never a splice. Empty and nil data are legal —
// a zero-byte file is a valid ordering barrier. The directory must already
// exist; its absence returns the os error unwrapped. The temp name derives
// from path's basename (".<base>.<random>"), and stale temps for the same
// target are swept on success — bounded to temps whose mtime is at least an
// hour old, so a concurrent writer's in-flight temp is never unlinked while a
// crash's stump neither survives forever nor masquerades as foreign state to
// a directory scanner.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	w, err := Create(path, perm)
	if err != nil {
		return err
	}
	defer w.Close()
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("durable: write temp for %s: %w", path, err)
	}
	return w.Commit()
}

// Create opens a pending durable write for payloads that are streamed rather
// than buffered. Nothing is visible at path until Commit.
func Create(path string, perm os.FileMode) (*Writer, error) {
	dir, base := filepath.Dir(path), filepath.Base(path)
	file, err := os.CreateTemp(dir, "."+base+".*")
	if err != nil {
		return nil, err
	}
	return &Writer{file: file, path: path, dir: dir, base: base, perm: perm}, nil
}

// Writer is one pending durable publication.
type Writer struct {
	file    *os.File
	path    string
	dir     string
	base    string
	perm    os.FileMode
	settled bool
}

func (w *Writer) Write(p []byte) (int, error) {
	return w.file.Write(p)
}

// Commit fsyncs, closes, renames into place, and fsyncs the directory.
func (w *Writer) Commit() error {
	tmp := w.file.Name()
	w.settled = true
	if err := w.publish(tmp); err != nil {
		w.file.Close()
		_ = os.Remove(tmp)
		return err
	}
	w.sweep()
	return nil
}

// Close discards an uncommitted write; after Commit it is a no-op.
func (w *Writer) Close() error {
	if w.settled {
		return nil
	}
	w.settled = true
	tmp := w.file.Name()
	w.file.Close()
	if err := os.Remove(tmp); err != nil {
		return fmt.Errorf("durable: discard temp for %s: %w", w.path, err)
	}
	return nil
}

func (w *Writer) publish(tmp string) error {
	if err := w.file.Chmod(w.perm); err != nil {
		return fmt.Errorf("durable: chmod temp for %s: %w", w.path, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("durable: fsync temp for %s: %w", w.path, err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("durable: close temp for %s: %w", w.path, err)
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return fmt.Errorf("durable: publish %s: %w", w.path, err)
	}
	return SyncDir(w.dir)
}

func (w *Writer) sweep() {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return
	}
	prefix := "." + w.base + "."
	cutoff := time.Now().Add(-stumpStaleAfter)
	for _, entry := range entries {
		random, ours := strings.CutPrefix(entry.Name(), prefix)
		if !ours || strings.Contains(random, ".") {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.ModTime().After(cutoff) {
			continue
		}
		_ = os.Remove(filepath.Join(w.dir, entry.Name()))
	}
}
