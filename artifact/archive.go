package artifact

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// maxArchiveEntries caps how many members an extraction processes. A release
// binary is one executable plus a few sidecars; this bounds the fsync-per-file
// wall time and inode count a crafted archive can force under the per-artifact
// lock, regardless of how few bytes each member writes.
const maxArchiveEntries = 1024

func extract(ctx context.Context, format Format, src, dest string, maxBytes int64) error {
	ctx, cancel := context.WithTimeout(ctx, materializeLockDeadline)
	defer cancel()
	switch format {
	case TarGz:
		return extractTarGz(ctx, src, dest, maxBytes)
	case Zip:
		return extractZip(ctx, src, dest, maxBytes)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedFormat, format)
	}
}

func extractTarGz(ctx context.Context, src, dest string, maxBytes int64) error {
	file, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("artifact: open archive: %w", err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("artifact: open gzip: %w", err)
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	budget := archiveBudget{remaining: maxBytes, maxBytes: maxBytes}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("artifact: read tar: %w", err)
		}
		if err := budget.admit(header.Size); err != nil {
			return err
		}
		target, err := safeJoin(dest, header.Name)
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o750); err != nil {
				return fmt.Errorf("artifact: create archive directory: %w", err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
				return fmt.Errorf("artifact: create archive directory: %w", err)
			}
			if err := writeArchiveFile(target, reader, &budget.remaining); err != nil {
				return err
			}
		case tar.TypeSymlink, tar.TypeLink:
			return fmt.Errorf("%w: link %q", ErrUnsafeArchive, header.Name)
		}
	}
}

func extractZip(ctx context.Context, src, dest string, maxBytes int64) error {
	reader, err := zip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("artifact: open zip: %w", err)
	}
	defer reader.Close()
	budget := archiveBudget{remaining: maxBytes, maxBytes: maxBytes}
	for _, entry := range reader.File {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.UncompressedSize64 > math.MaxInt64 {
			return fmt.Errorf("%w: declared size overflows int64", ErrUnsafeArchive)
		}
		if err := budget.admit(int64(entry.UncompressedSize64)); err != nil {
			return err
		}
		target, err := safeJoin(dest, entry.Name)
		if err != nil {
			return err
		}
		if entry.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o750); err != nil {
				return fmt.Errorf("artifact: create archive directory: %w", err)
			}
			continue
		}
		if entry.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink %q", ErrUnsafeArchive, entry.Name)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return fmt.Errorf("artifact: create archive directory: %w", err)
		}
		rc, err := entry.Open()
		if err != nil {
			return fmt.Errorf("artifact: open zip entry: %w", err)
		}
		err = writeArchiveFile(target, rc, &budget.remaining)
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// archiveBudget bounds an extraction by entry count, declared uncompressed size,
// and — via writeArchiveFile decrementing remaining — bytes actually written, so
// neither a header that lies small nor a swarm of tiny entries can run unbounded.
type archiveBudget struct {
	remaining int64
	declared  int64
	entries   int
	maxBytes  int64
}

func (b *archiveBudget) admit(declaredSize int64) error {
	b.entries++
	if b.entries > maxArchiveEntries {
		return fmt.Errorf("%w: archive has more than %d entries", ErrUnsafeArchive, maxArchiveEntries)
	}
	if declaredSize < 0 {
		return fmt.Errorf("%w: negative declared size", ErrUnsafeArchive)
	}
	if b.declared += declaredSize; b.declared > b.maxBytes {
		return fmt.Errorf("%w: declared size exceeds decompression budget", ErrUnsafeArchive)
	}
	return nil
}

// safeJoin joins name under dest and refuses any name that escapes dest. It
// contains descriptor-supplied paths (a platform Path, a tool dist/version, an
// app exec) and archive member names alike. The check is lexical: it does not
// resolve symlinks. That is airtight for the archive path (symlink and hardlink
// members are rejected outright) and for cache and tool paths this package
// itself lays down; the one residual is a signed-app attest whose installed
// bundle already contains an attacker-planted symlink, which is outside this
// package's trust boundary.
func safeJoin(dest, name string) (string, error) {
	target := filepath.Join(dest, name)
	rel, err := filepath.Rel(dest, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: %q escapes its containment root", ErrUnsafeArchive, name)
	}
	return target, nil
}

func writeArchiveFile(target string, r io.Reader, remaining *int64) error {
	file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("artifact: create archive file: %w", err)
	}
	written, err := io.Copy(file, io.LimitReader(r, *remaining+1))
	if err != nil {
		file.Close()
		return fmt.Errorf("artifact: write archive file: %w", err)
	}
	if written > *remaining {
		file.Close()
		return fmt.Errorf("%w: exceeds decompression budget", ErrUnsafeArchive)
	}
	*remaining -= written
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("artifact: sync archive file: %w", err)
	}
	return file.Close()
}
