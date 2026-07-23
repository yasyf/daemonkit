package deployment

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// unzip extracts src into dest, rejecting any entry — regular file or symlink —
// whose resolved path escapes dest (zip-slip), and preserves each entry's mode
// so an .app's inner Mach-O stays executable.
func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("deployment: open zip: %w", err)
	}
	defer r.Close()
	for _, entry := range r.File {
		if err := extractEntry(entry, dest); err != nil {
			return err
		}
	}
	return nil
}

func extractEntry(entry *zip.File, dest string) error {
	target := filepath.Join(dest, entry.Name) //nolint:gosec // G305: within() rejects entries escaping dest
	if !within(dest, target) {
		return fmt.Errorf("%w: %q", ErrUnsafeArchive, entry.Name)
	}
	if entry.FileInfo().IsDir() {
		return mkdirAllNoSymlink(dest, target, 0o750)
	}
	if err := mkdirAllNoSymlink(dest, filepath.Dir(target), 0o750); err != nil {
		return fmt.Errorf("deployment: mkdir %q: %w", filepath.Dir(target), err)
	}
	if entry.Mode()&os.ModeSymlink != 0 {
		return extractSymlink(entry, target, dest)
	}
	return extractFile(entry, target)
}

func extractFile(entry *zip.File, target string) error {
	rc, err := entry.Open()
	if err != nil {
		return fmt.Errorf("deployment: open entry %q: %w", entry.Name, err)
	}
	defer rc.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, entry.Mode().Perm())
	if err != nil {
		return fmt.Errorf("deployment: create %q: %w", target, err)
	}
	_, copyErr := io.Copy(out, rc) //nolint:gosec // G110: asset SHA-256 is verified against the pinned release checksum before unzip
	closeErr := out.Close()
	if copyErr != nil {
		return fmt.Errorf("deployment: write %q: %w", target, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("deployment: close %q: %w", target, closeErr)
	}
	return nil
}

func extractSymlink(entry *zip.File, target, dest string) error {
	rc, err := entry.Open()
	if err != nil {
		return fmt.Errorf("deployment: open entry %q: %w", entry.Name, err)
	}
	link, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("deployment: read link %q: %w", entry.Name, err)
	}
	dst := string(link)
	if filepath.IsAbs(dst) {
		return fmt.Errorf("%w: absolute symlink %q -> %q", ErrUnsafeArchive, entry.Name, dst)
	}
	resolved := filepath.Join(filepath.Dir(target), dst)
	if !within(dest, resolved) {
		return fmt.Errorf("%w: symlink %q -> %q", ErrUnsafeArchive, entry.Name, dst)
	}
	if err := os.Symlink(dst, target); err != nil {
		return fmt.Errorf("deployment: symlink %q: %w", target, err)
	}
	return nil
}

func within(dir, target string) bool {
	rel, err := filepath.Rel(dir, target)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func mkdirAllNoSymlink(root, target string, perm os.FileMode) error {
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: directory %q escapes archive root", ErrUnsafeArchive, target)
	}
	current := root
	if rel == "." {
		return nil
	}
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, perm); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: archive directory %q is not a real directory", ErrUnsafeArchive, current)
		}
	}
	return nil
}
