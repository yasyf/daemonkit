package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/durable"
	dkversion "github.com/yasyf/daemonkit/version"
)

func (s Store) resolveSignedApp(ctx context.Context, desc *Descriptor, version string, _ options) (string, error) {
	exec := desc.App.Exec
	if exec == "" {
		exec = filepath.Join("Contents", "MacOS", desc.App.AppName)
	}
	entrypoint, info, err := attestSignedApp(desc, version, exec)
	if err != nil {
		return "", err
	}
	if !desc.App.CopyExec {
		return entrypoint, nil
	}
	return s.resolveExecCopy(ctx, desc, version, entrypoint, info)
}

func attestSignedApp(desc *Descriptor, version, exec string) (string, os.FileInfo, error) {
	dir, err := expandHome(desc.App.Dir)
	if err != nil {
		return "", nil, err
	}
	appPath, err := safeJoin(dir, desc.App.AppName+".app")
	if err != nil {
		return "", nil, err
	}
	want := ""
	if !desc.Version.Dynamic() {
		want = version
	}
	if _, err := os.Stat(appPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, &ManualUpgradeError{Name: desc.Name, Cask: desc.App.Cask, Formula: desc.App.Formula, Want: want}
		}
		return "", nil, fmt.Errorf("artifact: inspect installed app: %w", err)
	}
	switch {
	case want != "":
		installed, err := installedVersion(appPath)
		if err != nil {
			return "", nil, err
		}
		if !dkversion.Equal(installed, want) {
			return "", nil, &ManualUpgradeError{Name: desc.Name, Cask: desc.App.Cask, Formula: desc.App.Formula, Want: want, Got: installed}
		}
	case desc.App.MinVersion != "":
		installed, err := installedVersion(appPath)
		if err != nil {
			return "", nil, err
		}
		if dkversion.Newer(desc.App.MinVersion, installed) {
			return "", nil, &ManualUpgradeError{Name: desc.Name, Cask: desc.App.Cask, Formula: desc.App.Formula, Want: desc.App.MinVersion, Got: installed, AtLeast: true}
		}
	}
	entrypoint, err := safeJoin(appPath, exec)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Stat(entrypoint)
	if err != nil || !info.Mode().IsRegular() {
		return "", nil, fmt.Errorf("%w: installed app entrypoint %q missing", ErrInvalidDescriptor, exec)
	}
	return entrypoint, info, nil
}

func installedVersion(appPath string) (string, error) {
	installed, err := bundle.ShortVersion(appPath)
	if err != nil {
		return "", fmt.Errorf("artifact: read installed app version: %w", err)
	}
	return installed, nil
}

type execCopyIdentity struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Source    string `json:"source"`
	Device    string `json:"device"`
	Inode     string `json:"inode"`
	Size      int64  `json:"size"`
	ModTimeNS int64  `json:"mtime_ns"`
}

func execCopyKey(desc *Descriptor, version, source string, info os.FileInfo) string {
	stat := info.Sys().(*syscall.Stat_t)
	identity, err := json.Marshal(execCopyIdentity{
		Name:      desc.Name,
		Version:   version,
		Source:    source,
		Device:    fmt.Sprint(stat.Dev),
		Inode:     fmt.Sprint(stat.Ino),
		Size:      info.Size(),
		ModTimeNS: info.ModTime().UnixNano(),
	})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(identity)
	return hex.EncodeToString(sum[:])
}

func (s Store) resolveExecCopy(ctx context.Context, desc *Descriptor, version, source string, info os.FileInfo) (string, error) {
	key := execCopyKey(desc, version, source, info)
	digestDir := s.digestDir(key)
	target := filepath.Join(digestDir, filepath.Base(source))
	if cacheHit(digestDir, target) {
		return target, nil
	}
	materialized := false
	if err := s.withLock(ctx, "release:"+key, func() error {
		if cacheHit(digestDir, target) {
			return nil
		}
		materialized = true
		return s.materializeExecCopy(desc, version, source, digestDir, target)
	}); err != nil {
		return "", err
	}
	if materialized {
		s.pruneExecCopies(ctx, source, digestDir)
	}
	return target, nil
}

func (s Store) materializeExecCopy(desc *Descriptor, version, source, digestDir, target string) error {
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
	digest, err := copyExecutable(source, filepath.Join(stage, filepath.Base(target)))
	if err != nil {
		return err
	}
	if err := writeCacheMeta(stage, cacheMeta{Name: desc.Name, Tag: version, Digest: digest, Source: source}); err != nil {
		return err
	}
	if err := durable.SyncDir(stage); err != nil {
		return err
	}
	if err := os.RemoveAll(digestDir); err != nil {
		return fmt.Errorf("artifact: clear prior cache entry: %w", err)
	}
	if err := os.Rename(stage, digestDir); err != nil {
		return fmt.Errorf("artifact: publish cache entry: %w", err)
	}
	keep = true
	return durable.SyncDir(shardDir)
}

func copyExecutable(source, target string) (string, error) {
	in, err := os.Open(source)
	if err != nil {
		return "", fmt.Errorf("artifact: open installed app entrypoint: %w", err)
	}
	defer in.Close()
	out, err := durable.Create(target, 0o755)
	if err != nil {
		return "", fmt.Errorf("artifact: create entrypoint copy: %w", err)
	}
	defer out.Close()
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(out, hash), in); err != nil {
		return "", fmt.Errorf("artifact: copy installed app entrypoint: %w", err)
	}
	if err := out.Commit(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (s Store) pruneExecCopies(ctx context.Context, source, current string) {
	entries, err := s.CacheEntries()
	if err != nil {
		slog.Warn("artifact: enumerate cache for stale signed-app copies", "error", err)
		return
	}
	for _, entry := range entries {
		if entry.Source != source || entry.Dir == current {
			continue
		}
		if err := s.removeCacheEntry(ctx, entry); err != nil {
			slog.Warn("artifact: prune stale signed-app copy", "dir", entry.Dir, "error", err)
		}
	}
}
