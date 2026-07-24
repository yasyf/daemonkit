package artifact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/daemon"
)

func (s Store) resolveReleaseBinary(ctx context.Context, desc *Descriptor, o options) (string, error) {
	platform, err := CurrentPlatform()
	if err != nil {
		return "", err
	}
	entry, ok := desc.Platforms[platform]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedPlatform, platform)
	}
	target := s.cachePath(entry.Digest, entry.Path)
	if regular(target) {
		return target, nil
	}
	if err := s.withLock(ctx, "release:"+entry.Digest, func() error {
		if regular(target) {
			return nil
		}
		return s.materializeReleaseBinary(ctx, desc, entry, o)
	}); err != nil {
		return "", err
	}
	return target, nil
}

func (s Store) cachePath(digest, path string) string {
	return filepath.Join(s.CacheDir(), digest[:2], digest, path)
}

func (s Store) materializeReleaseBinary(ctx context.Context, desc *Descriptor, entry PlatformEntry, o options) error {
	url, err := entry.Providers[0].URL()
	if err != nil {
		return err
	}
	digestDir := filepath.Join(s.CacheDir(), entry.Digest[:2], entry.Digest)
	if err := os.MkdirAll(digestDir, 0o700); err != nil {
		return fmt.Errorf("artifact: create cache directory: %w", err)
	}
	tmp, err := os.CreateTemp(digestDir, ".download-")
	if err != nil {
		return fmt.Errorf("artifact: create download temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	digest, size, err := download(ctx, o.http(), url, o.githubToken, tmp)
	if err != nil {
		return err
	}
	if size != entry.Size {
		return fmt.Errorf("%w: got %d want %d", ErrSizeMismatch, size, entry.Size)
	}
	if digest != strings.ToLower(entry.Digest) {
		return fmt.Errorf("%w: got %s want %s", ErrChecksumMismatch, digest, entry.Digest)
	}

	if err := place(entry.Format, tmpPath, digestDir, entry.Path); err != nil {
		return err
	}
	entrypoint := filepath.Join(digestDir, entry.Path)
	info, err := os.Stat(entrypoint)
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: entrypoint %q missing after materialization", ErrInvalidDescriptor, entry.Path)
	}
	// A resolved binary must be executable; extracted archive members are 0600.
	if err := os.Chmod(entrypoint, 0o755); err != nil {
		return fmt.Errorf("artifact: mark entrypoint executable: %w", err)
	}
	if err := writeCacheMeta(digestDir, desc.Name, entry); err != nil {
		return err
	}
	return daemon.SyncDir(digestDir)
}

func place(format Format, src, digestDir, entryPath string) error {
	if format != Raw {
		return extract(format, src, digestDir)
	}
	dst := filepath.Join(digestDir, entryPath)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return fmt.Errorf("artifact: create entry directory: %w", err)
	}
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("artifact: place artifact: %w", err)
	}
	return nil
}

func regular(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
