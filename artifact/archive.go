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

func extract(format Format, src, dest string) error {
	switch format {
	case TarGz:
		return extractTarGz(src, dest)
	case Zip:
		return extractZip(src, dest)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedFormat, format)
	}
}

func extractTarGz(src, dest string) error {
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
			if err := writeArchiveFile(target, reader); err != nil {
				return err
			}
		case tar.TypeSymlink, tar.TypeLink:
			return fmt.Errorf("%w: link %q", ErrUnsafeArchive, header.Name)
		}
	}
}

func extractZip(src, dest string) error {
	reader, err := zip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("artifact: open zip: %w", err)
	}
	defer reader.Close()
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
		err = writeArchiveFile(target, rc)
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func safeJoin(dest, name string) (string, error) {
	target := filepath.Join(dest, name)
	rel, err := filepath.Rel(dest, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: %q escapes archive root", ErrUnsafeArchive, name)
	}
	return target, nil
}

func writeArchiveFile(target string, r io.Reader) error {
	file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("artifact: create archive file: %w", err)
	}
	if _, err := io.Copy(file, r); err != nil {
		file.Close()
		return fmt.Errorf("artifact: write archive file: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("artifact: sync archive file: %w", err)
	}
	return file.Close()
}
