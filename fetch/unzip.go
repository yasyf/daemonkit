package fetch

import (
	"archive/zip"
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
		return fmt.Errorf("fetch: open zip: %w", err)
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
		return os.MkdirAll(target, 0o750)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return fmt.Errorf("fetch: mkdir %q: %w", filepath.Dir(target), err)
	}
	if entry.Mode()&os.ModeSymlink != 0 {
		return extractSymlink(entry, target, dest)
	}
	return extractFile(entry, target)
}

func extractFile(entry *zip.File, target string) error {
	rc, err := entry.Open()
	if err != nil {
		return fmt.Errorf("fetch: open entry %q: %w", entry.Name, err)
	}
	defer rc.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, entry.Mode().Perm())
	if err != nil {
		return fmt.Errorf("fetch: create %q: %w", target, err)
	}
	_, copyErr := io.Copy(out, rc) //nolint:gosec // G110: asset SHA-256 is verified against the pinned release checksum before unzip
	closeErr := out.Close()
	if copyErr != nil {
		return fmt.Errorf("fetch: write %q: %w", target, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("fetch: close %q: %w", target, closeErr)
	}
	return nil
}

func extractSymlink(entry *zip.File, target, dest string) error {
	rc, err := entry.Open()
	if err != nil {
		return fmt.Errorf("fetch: open entry %q: %w", entry.Name, err)
	}
	link, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("fetch: read link %q: %w", entry.Name, err)
	}
	dst := string(link)
	resolved := dst
	if !filepath.IsAbs(dst) {
		resolved = filepath.Join(filepath.Dir(target), dst)
	}
	if !within(dest, resolved) {
		return fmt.Errorf("%w: symlink %q -> %q", ErrUnsafeArchive, entry.Name, dst)
	}
	if err := os.Symlink(dst, target); err != nil {
		return fmt.Errorf("fetch: symlink %q: %w", target, err)
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
