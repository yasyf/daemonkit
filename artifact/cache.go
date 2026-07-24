package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/daemon"
)

// CacheEntry is one materialized release-binary in the content-addressed cache,
// as enumerated for garbage collection. Digest and Dir are always set; Name,
// Tag, and FetchedAt come from the entry's meta.json and are zero when it is
// missing or unreadable, so a damaged entry can still be pruned.
type CacheEntry struct {
	Name      string
	Tag       string
	Digest    string
	Dir       string
	FetchedAt time.Time
}

// CacheEntries walks the content cache and returns one entry per digest
// directory, reading each meta.json for provenance. A digest directory with a
// missing or corrupt meta.json still yields an entry (Digest and Dir only) plus
// one warning, so gc can prune it rather than orbit it forever.
func (s Store) CacheEntries() ([]CacheEntry, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	shards, err := os.ReadDir(s.CacheDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("artifact: read cache directory: %w", err)
	}
	var entries []CacheEntry
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		shardDir := filepath.Join(s.CacheDir(), shard.Name())
		digests, err := os.ReadDir(shardDir)
		if err != nil {
			return nil, fmt.Errorf("artifact: read cache shard %q: %w", shard.Name(), err)
		}
		for _, digest := range digests {
			if digest.IsDir() {
				entries = append(entries, readCacheEntry(filepath.Join(shardDir, digest.Name()), digest.Name()))
			}
		}
	}
	return entries, nil
}

func readCacheEntry(dir, digest string) CacheEntry {
	entry := CacheEntry{Digest: digest, Dir: dir}
	data, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		slog.Warn("artifact: cache entry has unreadable meta", "dir", dir, "error", err)
		return entry
	}
	var meta cacheMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		slog.Warn("artifact: cache entry has corrupt meta", "dir", dir, "error", err)
		return entry
	}
	entry.Name = meta.Name
	entry.Tag = meta.Tag
	entry.FetchedAt = meta.FetchedAt
	return entry
}

// RemoveCacheEntry removes a cache entry's digest directory under the same
// per-artifact lock materialization holds, so a concurrent resolve of the same
// digest completes whole before the removal begins and never observes a
// half-deleted entry.
func (s Store) RemoveCacheEntry(entry CacheEntry) error {
	if err := s.validate(); err != nil {
		return err
	}
	rel, err := filepath.Rel(s.CacheDir(), entry.Dir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("artifact: cache entry dir %q is not within the cache", entry.Dir)
	}
	return s.withLock(context.Background(), "release:"+entry.Digest, func() error {
		if err := os.RemoveAll(entry.Dir); err != nil {
			return fmt.Errorf("artifact: remove cache entry: %w", err)
		}
		if err := daemon.SyncDir(filepath.Dir(entry.Dir)); err != nil {
			return fmt.Errorf("artifact: persist cache entry removal: %w", err)
		}
		return nil
	})
}
