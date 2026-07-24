package artifact

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func extract(format Format, src, dest string, maxBytes int64) error {
	switch format {
	case TarGz:
		return extractTarGz(src, dest, maxBytes)
	case Zip:
		return extractZip(src, dest, maxBytes)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedFormat, format)
	}
}

func extractTarGz(src, dest string, maxBytes int64) error {
	remaining := maxBytes
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
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("artifact: read tar: %w", err)
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
			if err := writeArchiveFile(target, reader, &remaining); err != nil {
				return err
			}
		case tar.TypeSymlink, tar.TypeLink:
			return fmt.Errorf("%w: link %q", ErrUnsafeArchive, header.Name)
		}
	}
}

func extractZip(src, dest string, maxBytes int64) error {
	reader, err := zip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("artifact: open zip: %w", err)
	}
	defer reader.Close()
	remaining := maxBytes
	for _, entry := range reader.File {
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
		err = writeArchiveFile(target, rc, &remaining)
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// safeJoin joins name under dest and refuses any name that escapes dest. It
// contains descriptor-supplied paths (a platform Path, a tool dist/version, an
// app exec) and archive member names alike.
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
