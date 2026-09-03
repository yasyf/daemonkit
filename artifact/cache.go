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

	"github.com/yasyf/daemonkit/durable"
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

// ToolEntry is one materialized python-tool environment in the version-addressed
// tool store, as enumerated for garbage collection. Dist, Version, and Dir are
// always set; InstalledAt comes from the environment's install marker and is
// zero when that marker is missing, so the partial env an interrupted install
// left behind sorts oldest and is reclaimed first.
type ToolEntry struct {
	Dist        string
	Version     string
	Dir         string
	InstalledAt time.Time
}

// ToolEntries walks the tool store and returns one entry per <dist>/<version>
// environment, reading each install marker for its timestamp.
func (s Store) ToolEntries() ([]ToolEntry, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	dists, err := os.ReadDir(s.ToolsDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("artifact: read tools directory: %w", err)
	}
	var entries []ToolEntry
	for _, dist := range dists {
		if !dist.IsDir() {
			continue
		}
		distDir := filepath.Join(s.ToolsDir(), dist.Name())
		versions, err := os.ReadDir(distDir)
		if err != nil {
			return nil, fmt.Errorf("artifact: read tool dist %q: %w", dist.Name(), err)
		}
		for _, version := range versions {
			if version.IsDir() {
				entries = append(entries, readToolEntry(filepath.Join(distDir, version.Name()), dist.Name(), version.Name()))
			}
		}
	}
	return entries, nil
}

func readToolEntry(dir, dist, version string) ToolEntry {
	entry := ToolEntry{Dist: dist, Version: version, Dir: dir}
	info, err := os.Stat(filepath.Join(dir, installedMarker))
	if err != nil {
		slog.Warn("artifact: tool env has no install marker", "dir", dir, "error", err)
		return entry
	}
	entry.InstalledAt = info.ModTime()
	return entry
}

// RemoveToolEntry removes a tool environment under the same per-artifact lock
// installation holds, so a concurrent install of the same version completes
// whole before the removal begins.
func (s Store) RemoveToolEntry(entry ToolEntry) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := within(s.ToolsDir(), entry.Dir); err != nil {
		return err
	}
	return s.withLock(context.Background(), toolLockKey(entry.Dist, entry.Version), func() error {
		if err := durable.RemoveTree(entry.Dir); err != nil {
			return fmt.Errorf("artifact: remove tool entry: %w", err)
		}
		return nil
	})
}

func within(root, dir string) error {
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("artifact: entry dir %q is not within %q", dir, root)
	}
	return nil
}

// RemoveCacheEntry removes a cache entry's digest directory under the same
// per-artifact lock materialization holds, so a concurrent resolve of the same
// digest completes whole before the removal begins and never observes a
// half-deleted entry.
func (s Store) RemoveCacheEntry(entry CacheEntry) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := within(s.CacheDir(), entry.Dir); err != nil {
		return err
	}
	return s.withLock(context.Background(), "release:"+entry.Digest, func() error {
		if err := durable.RemoveTree(entry.Dir); err != nil {
			return fmt.Errorf("artifact: remove cache entry: %w", err)
		}
		return nil
	})
}
