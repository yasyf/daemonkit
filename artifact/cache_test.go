package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func seedCacheEntry(t *testing.T, store Store, name, tag, digest string) CacheEntry {
	t.Helper()
	dir := filepath.Join(store.CacheDir(), digest[:2], digest)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tool"), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	fetched := time.Now().UTC().Truncate(time.Second)
	data, err := json.Marshal(cacheMeta{Name: name, Tag: tag, Digest: digest, FetchedAt: fetched})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	return CacheEntry{Name: name, Tag: tag, Digest: digest, Dir: dir, FetchedAt: fetched}
}

func sameEntry(a, b CacheEntry) bool {
	return a.Name == b.Name && a.Tag == b.Tag && a.Digest == b.Digest && a.Dir == b.Dir && a.FetchedAt.Equal(b.FetchedAt)
}

func entriesByDigest(t *testing.T, store Store) map[string]CacheEntry {
	t.Helper()
	entries, err := store.CacheEntries()
	if err != nil {
		t.Fatalf("CacheEntries() = %v", err)
	}
	byDigest := make(map[string]CacheEntry, len(entries))
	for _, e := range entries {
		byDigest[e.Digest] = e
	}
	return byDigest
}

func TestCacheEntriesWalk(t *testing.T) {
	store := Store{Root: t.TempDir()}
	a := seedCacheEntry(t, store, "tool-a", "v1", strings.Repeat("a", 64))
	b := seedCacheEntry(t, store, "tool-b", "v2", strings.Repeat("b", 64))

	byDigest := entriesByDigest(t, store)
	if len(byDigest) != 2 {
		t.Fatalf("entries = %d, want 2", len(byDigest))
	}
	if !sameEntry(byDigest[a.Digest], a) {
		t.Fatalf("entry a = %+v, want %+v", byDigest[a.Digest], a)
	}
	if !sameEntry(byDigest[b.Digest], b) {
		t.Fatalf("entry b = %+v, want %+v", byDigest[b.Digest], b)
	}
}

func TestCacheEntriesEmpty(t *testing.T) {
	entries, err := (Store{Root: t.TempDir()}).CacheEntries()
	if err != nil || entries != nil {
		t.Fatalf("CacheEntries() on an empty store = %v, %v; want nil, nil", entries, err)
	}
}

func TestCacheEntriesDamagedMetaStillPrunable(t *testing.T) {
	store := Store{Root: t.TempDir()}
	missing := strings.Repeat("c", 64)
	corrupt := strings.Repeat("d", 64)
	for _, tc := range []struct {
		digest string
		meta   []byte // nil = write no meta.json
	}{
		{missing, nil},
		{corrupt, []byte("{not json")},
	} {
		dir := filepath.Join(store.CacheDir(), tc.digest[:2], tc.digest)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		if tc.meta != nil {
			if err := os.WriteFile(filepath.Join(dir, "meta.json"), tc.meta, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	byDigest := entriesByDigest(t, store)
	for _, digest := range []string{missing, corrupt} {
		entry, ok := byDigest[digest]
		if !ok {
			t.Fatalf("damaged entry %s not returned", digest)
		}
		if entry.Name != "" || entry.Tag != "" || !entry.FetchedAt.IsZero() {
			t.Fatalf("damaged entry carries provenance: %+v", entry)
		}
		if entry.Dir == "" {
			t.Fatalf("damaged entry %s has no Dir", digest)
		}
	}
}

func TestRemoveCacheEntry(t *testing.T) {
	store := Store{Root: t.TempDir()}
	entry := seedCacheEntry(t, store, "tool", "v1", strings.Repeat("a", 64))

	if err := store.RemoveCacheEntry(entry); err != nil {
		t.Fatalf("RemoveCacheEntry() = %v", err)
	}
	if _, err := os.Stat(entry.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cache entry dir not removed: %v", err)
	}
	if err := store.RemoveCacheEntry(entry); err != nil {
		t.Fatalf("second RemoveCacheEntry() = %v (want idempotent)", err)
	}
}

func TestRemoveCacheEntryRejectsDirOutsideCache(t *testing.T) {
	store := Store{Root: t.TempDir()}
	if err := store.RemoveCacheEntry(CacheEntry{Digest: "x", Dir: t.TempDir()}); err == nil {
		t.Fatal("RemoveCacheEntry accepted a dir outside the cache")
	}
	if err := store.RemoveCacheEntry(CacheEntry{Digest: "x", Dir: store.CacheDir()}); err == nil {
		t.Fatal("RemoveCacheEntry accepted the cache root itself")
	}
}

func TestRemoveCacheEntryWaitsForConcurrentMaterialization(t *testing.T) {
	store := Store{Root: t.TempDir()}
	entry := seedCacheEntry(t, store, "tool", "v1", strings.Repeat("a", 64))

	acquired := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = store.withLock(context.Background(), "release:"+entry.Digest, func() error {
			close(acquired)
			<-release
			return nil
		})
	}()
	<-acquired

	done := make(chan error, 1)
	go func() { done <- store.RemoveCacheEntry(entry) }()
	select {
	case <-done:
		t.Fatal("RemoveCacheEntry did not wait for the held materialization lock")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("RemoveCacheEntry after release = %v", err)
	}
	if _, err := os.Stat(entry.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cache entry dir not removed after lock release: %v", err)
	}
}

func TestCacheEntriesReadsMaterializedMeta(t *testing.T) {
	content := []byte("#!/bin/sh\necho hi\n")
	store, opt, _ := serveContent(t, content)
	desc := releaseDescriptor(t, PlatformEntry{
		Size: int64(len(content)), Hash: "sha256", Digest: sha256hex(content), Format: Raw, Path: "tool",
	})
	if _, err := store.Resolve(context.Background(), desc, opt); err != nil {
		t.Fatalf("Resolve() = %v", err)
	}

	entries, err := store.CacheEntries()
	if err != nil {
		t.Fatalf("CacheEntries() = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	entry := entries[0]
	if entry.Digest != sha256hex(content) || entry.Name != "tool" || entry.Tag != "v1.0.0" || entry.FetchedAt.IsZero() {
		t.Fatalf("materialized entry = %+v", entry)
	}
	if err := store.RemoveCacheEntry(entry); err != nil {
		t.Fatalf("RemoveCacheEntry() = %v", err)
	}
	if left := entriesByDigest(t, store); len(left) != 0 {
		t.Fatalf("entries after remove = %d, want 0", len(left))
	}
}

func seedToolEntry(t *testing.T, store Store, dist, version string, installedAt time.Time) ToolEntry {
	t.Helper()
	dir := filepath.Join(store.ToolsDir(), dist, version)
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o750); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, installedMarker)
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(marker, installedAt, installedAt); err != nil {
		t.Fatal(err)
	}
	return ToolEntry{Dist: dist, Version: version, Dir: dir, InstalledAt: installedAt}
}

func toolEntriesByVersion(t *testing.T, store Store) map[string]ToolEntry {
	t.Helper()
	entries, err := store.ToolEntries()
	if err != nil {
		t.Fatalf("ToolEntries() = %v", err)
	}
	byVersion := make(map[string]ToolEntry, len(entries))
	for _, e := range entries {
		byVersion[e.Dist+"@"+e.Version] = e
	}
	return byVersion
}

func TestToolEntriesWalk(t *testing.T) {
	store := Store{Root: t.TempDir()}
	installed := time.Now().Add(-time.Hour).Truncate(time.Second)
	old := seedToolEntry(t, store, "capt-hook", "12.21.3", installed)
	current := seedToolEntry(t, store, "capt-hook", "12.22.5", installed.Add(time.Hour))
	other := seedToolEntry(t, store, "other-tool", "1.0.0", installed)

	byVersion := toolEntriesByVersion(t, store)
	if len(byVersion) != 3 {
		t.Fatalf("entries = %d, want 3", len(byVersion))
	}
	for _, want := range []ToolEntry{old, current, other} {
		got := byVersion[want.Dist+"@"+want.Version]
		if got.Dir != want.Dir || got.Dist != want.Dist || got.Version != want.Version {
			t.Fatalf("entry = %+v, want %+v", got, want)
		}
		if !got.InstalledAt.Equal(want.InstalledAt) {
			t.Fatalf("entry %s InstalledAt = %v, want %v", want.Version, got.InstalledAt, want.InstalledAt)
		}
	}
}

func TestToolEntriesEmpty(t *testing.T) {
	entries, err := (Store{Root: t.TempDir()}).ToolEntries()
	if err != nil || entries != nil {
		t.Fatalf("ToolEntries() on an empty store = %v, %v; want nil, nil", entries, err)
	}
}

func TestToolEntriesPartialInstallStillPrunable(t *testing.T) {
	store := Store{Root: t.TempDir()}
	dir := filepath.Join(store.ToolsDir(), "capt-hook", "12.22.0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	entry, ok := toolEntriesByVersion(t, store)["capt-hook@12.22.0"]
	if !ok {
		t.Fatal("env without an install marker not returned")
	}
	if !entry.InstalledAt.IsZero() {
		t.Fatalf("unmarked env carries a timestamp: %+v", entry)
	}
	if err := store.RemoveToolEntry(entry); err != nil {
		t.Fatalf("RemoveToolEntry() = %v", err)
	}
}

func TestRemoveToolEntry(t *testing.T) {
	store := Store{Root: t.TempDir()}
	entry := seedToolEntry(t, store, "capt-hook", "12.21.3", time.Now())

	if err := store.RemoveToolEntry(entry); err != nil {
		t.Fatalf("RemoveToolEntry() = %v", err)
	}
	if _, err := os.Stat(entry.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tool entry dir not removed: %v", err)
	}
	if err := store.RemoveToolEntry(entry); err != nil {
		t.Fatalf("second RemoveToolEntry() = %v (want idempotent)", err)
	}
}

func TestRemoveToolEntryRejectsDirOutsideToolStore(t *testing.T) {
	store := Store{Root: t.TempDir()}
	if err := store.RemoveToolEntry(ToolEntry{Dist: "x", Version: "1", Dir: t.TempDir()}); err == nil {
		t.Fatal("RemoveToolEntry accepted a dir outside the tool store")
	}
	if err := store.RemoveToolEntry(ToolEntry{Dist: "x", Version: "1", Dir: store.ToolsDir()}); err == nil {
		t.Fatal("RemoveToolEntry accepted the tool store root itself")
	}
	if err := store.RemoveToolEntry(ToolEntry{Dist: "x", Version: "1", Dir: store.CacheDir()}); err == nil {
		t.Fatal("RemoveToolEntry accepted the content cache")
	}
}

func TestRemoveToolEntryWaitsForConcurrentInstall(t *testing.T) {
	store := Store{Root: t.TempDir()}
	entry := seedToolEntry(t, store, "capt-hook", "12.22.5", time.Now())

	acquired := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = store.withLock(context.Background(), toolLockKey(entry.Dist, entry.Version), func() error {
			close(acquired)
			<-release
			return nil
		})
	}()
	<-acquired

	done := make(chan error, 1)
	go func() { done <- store.RemoveToolEntry(entry) }()
	select {
	case <-done:
		t.Fatal("RemoveToolEntry did not wait for the held install lock")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("RemoveToolEntry after release = %v", err)
	}
	if _, err := os.Stat(entry.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tool entry dir not removed after lock release: %v", err)
	}
}
