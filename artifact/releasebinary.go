package artifact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/daemon"
)

const (
	decompressionRatio     = 200
	minDecompressionBudget = 64 << 20 // 64 MiB
	maxDecompressionBudget = 4 << 30  // 4 GiB
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
	digestDir := s.digestDir(entry.Digest)
	target, err := safeJoin(digestDir, entry.Path)
	if err != nil {
		return "", err
	}
	if cacheHit(digestDir, target) {
		return target, nil
	}
	if err := s.withLock(ctx, "release:"+entry.Digest, func() error {
		if cacheHit(digestDir, target) {
			return nil
		}
		return s.materializeReleaseBinary(ctx, desc, entry, digestDir, o)
	}); err != nil {
		return "", err
	}
	return target, nil
}

func (s Store) digestDir(digest string) string {
	return filepath.Join(s.CacheDir(), digest[:2], digest)
}

// cacheHit requires both the entrypoint and its meta.json, so a partially
// materialized entry (a pre-atomic crash) is never mistaken for a verified one.
func cacheHit(digestDir, target string) bool {
	return regular(target) && regular(filepath.Join(digestDir, "meta.json"))
}

func (s Store) materializeReleaseBinary(ctx context.Context, desc *Descriptor, entry PlatformEntry, digestDir string, o options) error {
	url, err := entry.Providers[0].URL()
	if err != nil {
		return err
	}
	shardDir := filepath.Dir(digestDir)
	if err := os.MkdirAll(shardDir, 0o750); err != nil {
		return fmt.Errorf("artifact: create cache shard: %w", err)
	}
	stage, err := os.MkdirTemp(shardDir, ".stage-")
	if err != nil {
		return fmt.Errorf("artifact: create stage: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(stage)
		}
	}()
	tmp, err := os.CreateTemp(shardDir, ".download-")
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
	if digest != entry.Digest {
		return fmt.Errorf("%w: got %s want %s", ErrChecksumMismatch, digest, entry.Digest)
	}

	stageEntry, err := safeJoin(stage, entry.Path)
	if err != nil {
		return err
	}
	if err := place(entry.Format, tmpPath, stage, stageEntry, decompressionBudget(entry.Size)); err != nil {
		return err
	}
	if !regular(stageEntry) {
		return fmt.Errorf("%w: entrypoint %q missing after materialization", ErrInvalidDescriptor, entry.Path)
	}
	// A resolved binary must be executable; extracted archive members are 0600.
	if err := os.Chmod(stageEntry, 0o755); err != nil {
		return fmt.Errorf("artifact: mark entrypoint executable: %w", err)
	}
	if err := writeCacheMeta(stage, desc.Name, entry); err != nil {
		return err
	}
	if err := daemon.SyncDir(stage); err != nil {
		return err
	}

	// Publish the verified staging tree atomically; a prior partial (a
	// pre-atomic crash) is cleared first under the same per-digest lock.
	if err := os.RemoveAll(digestDir); err != nil {
		return fmt.Errorf("artifact: clear prior cache entry: %w", err)
	}
	if err := os.Rename(stage, digestDir); err != nil {
		return fmt.Errorf("artifact: publish cache entry: %w", err)
	}
	keep = true
	return daemon.SyncDir(shardDir)
}

func place(format Format, src, stageDir, dst string, maxBytes int64) error {
	if format != Raw {
		return extract(format, src, stageDir, maxBytes)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return fmt.Errorf("artifact: create entry directory: %w", err)
	}
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("artifact: place artifact: %w", err)
	}
	return nil
}

func decompressionBudget(size int64) int64 {
	switch budget := size * decompressionRatio; {
	case budget < minDecompressionBudget:
		return minDecompressionBudget
	case budget > maxDecompressionBudget:
		return maxDecompressionBudget
	default:
		return budget
	}
}

func regular(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
