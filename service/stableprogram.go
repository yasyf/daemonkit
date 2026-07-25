package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/version"
)

const (
	stableProgramSchema       = 1
	stableProgramLockDeadline = 2 * time.Minute
)

// stableProgramMeta lets the next call decide without hashing: digest detects
// drifted bytes, size and mtime fingerprint the file for the fast path.
type stableProgramMeta struct {
	Schema int    `json:"schema"`
	Build  string `json:"build"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	MTime  int64  `json:"mtime"`
}

// StableProgram maintains ~/.daemonkit/bin/<name> as a byte copy of the calling
// executable and returns that path for Agent.Program — a real file launchd keeps
// exec'ing across upgrades that move the versioned original. A strictly newer
// build replaces the copy, an equal or older one keeps it, and drifted bytes are
// repaired at any build. Downgrade is manual.
//
// The daemon launchd started from the copy returns without touching the disk.
func StableProgram(name, build string) (string, error) {
	self, err := CanonicalExecutable()
	if err != nil {
		return "", err
	}
	root, err := stableRoot()
	if err != nil {
		return "", err
	}
	return stableProgram(root, name, build, self)
}

// RemoveStableProgram deletes ~/.daemonkit/bin/<name> and its sidecar for
// uninstall and zap flows. An absent copy is not an error.
func RemoveStableProgram(name string) error {
	root, err := stableRoot()
	if err != nil {
		return err
	}
	return removeStableProgram(root, name)
}

func stableRoot() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("service: resolve home directory: %w", err)
	}
	return filepath.Join(home, ".daemonkit"), nil
}

func stableProgram(root, name, build, self string) (string, error) {
	if err := validateStableName(name); err != nil {
		return "", err
	}
	if build == "" {
		return "", errors.New("service: stable program build is empty")
	}
	stablePath := stableProgramPath(root, name)
	if self == stablePath {
		return self, nil
	}
	metaPath := stableMetaPath(stablePath)
	if stableProgramCurrent(stablePath, metaPath, build) {
		return canonicalExecutablePath(stablePath)
	}
	handle, err := stableProgramLock(root, name)
	if err != nil {
		return "", err
	}
	defer handle.Close()
	if stableProgramCurrent(stablePath, metaPath, build) {
		return canonicalExecutablePath(stablePath)
	}
	replace, refresh, err := stableProgramNeedsReplace(stablePath, metaPath, build)
	if err != nil {
		return "", err
	}
	if replace {
		if err := materializeStableProgram(stablePath, metaPath, build, self); err != nil {
			return "", err
		}
	} else if refresh != nil {
		if err := writeStableMeta(metaPath, *refresh); err != nil {
			return "", err
		}
	}
	return canonicalExecutablePath(stablePath)
}

func removeStableProgram(root, name string) error {
	if err := validateStableName(name); err != nil {
		return err
	}
	stablePath := stableProgramPath(root, name)
	handle, err := stableProgramLock(root, name)
	if err != nil {
		return err
	}
	defer handle.Close()
	for _, path := range []string{stablePath, stableMetaPath(stablePath)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("service: remove stable program %q: %w", path, err)
		}
	}
	if err := dkdaemon.SyncDir(filepath.Dir(stablePath)); err != nil {
		return fmt.Errorf("service: persist stable program removal: %w", err)
	}
	return nil
}

func stableProgramPath(root, name string) string { return filepath.Join(root, "bin", name) }

func stableMetaPath(stablePath string) string { return stablePath + ".meta.json" }

func stableProgramLock(root, name string) (*proc.FileLockHandle, error) {
	locks := filepath.Join(root, "locks")
	if err := os.MkdirAll(locks, 0o700); err != nil {
		return nil, fmt.Errorf("service: create locks directory: %w", err)
	}
	spec := proc.FileLockSpec{
		Path:     filepath.Join(locks, "stable-"+name+".lock"),
		Mode:     proc.FileLockExclusive,
		Deadline: stableProgramLockDeadline,
	}
	handle, err := spec.Acquire(context.Background())
	if err != nil {
		return nil, fmt.Errorf("service: lock stable program %q: %w", name, err)
	}
	return handle, nil
}

// stableProgramCurrent is the lock-free fast path, run by every CLI invocation
// and per-turn hook, so it never reads the binary.
func stableProgramCurrent(stablePath, metaPath, build string) bool {
	meta, err := readStableMeta(metaPath)
	if err != nil {
		return false
	}
	if !version.Equal(build, meta.Build) {
		return false
	}
	info, err := os.Lstat(stablePath)
	if err != nil {
		return false
	}
	return info.Mode().IsRegular() &&
		info.Mode().Perm()&0o111 != 0 &&
		info.Size() == meta.Size &&
		info.ModTime().UnixNano() == meta.MTime
}

func stableProgramNeedsReplace(stablePath, metaPath, build string) (bool, *stableProgramMeta, error) {
	meta, err := readStableMeta(metaPath)
	if err != nil {
		return true, nil, nil
	}
	info, err := os.Lstat(stablePath)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return true, nil, nil
	}
	digest, err := fileDigest(stablePath)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	if digest != meta.Digest {
		return true, nil, nil
	}
	if version.Newer(build, meta.Build) {
		return true, nil, nil
	}
	if info.Size() != meta.Size || info.ModTime().UnixNano() != meta.MTime {
		meta.Size = info.Size()
		meta.MTime = info.ModTime().UnixNano()
		return false, &meta, nil
	}
	return false, nil, nil
}

// materializeStableProgram writes the binary before its sidecar: a crash between
// the two leaves a copy the next call repairs by digest.
func materializeStableProgram(stablePath, metaPath, build, self string) error {
	data, err := os.ReadFile(self) //nolint:gosec // canonical executable path of this process
	if err != nil {
		return fmt.Errorf("service: read executable %q: %w", self, err)
	}
	if err := os.MkdirAll(filepath.Dir(stablePath), 0o700); err != nil {
		return fmt.Errorf("service: create bin directory: %w", err)
	}
	if err := dkdaemon.WriteFileDurable(stablePath, data, 0o755); err != nil {
		return fmt.Errorf("service: write stable program %q: %w", stablePath, err)
	}
	info, err := os.Lstat(stablePath)
	if err != nil {
		return fmt.Errorf("service: inspect stable program %q: %w", stablePath, err)
	}
	sum := sha256.Sum256(data)
	return writeStableMeta(metaPath, stableProgramMeta{
		Schema: stableProgramSchema,
		Build:  build,
		Digest: hex.EncodeToString(sum[:]),
		Size:   info.Size(),
		MTime:  info.ModTime().UnixNano(),
	})
}

func writeStableMeta(metaPath string, meta stableProgramMeta) error {
	encoded, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("service: encode stable program meta: %w", err)
	}
	if err := dkdaemon.WriteFileDurable(metaPath, append(encoded, '\n'), 0o600); err != nil {
		return fmt.Errorf("service: write stable program meta %q: %w", metaPath, err)
	}
	return nil
}

func readStableMeta(metaPath string) (stableProgramMeta, error) {
	data, err := os.ReadFile(metaPath) //nolint:gosec // exact service-owned sidecar path
	if err != nil {
		return stableProgramMeta{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var meta stableProgramMeta
	if err := decoder.Decode(&meta); err != nil {
		return stableProgramMeta{}, fmt.Errorf("service: decode stable program meta %q: %w", metaPath, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return stableProgramMeta{}, fmt.Errorf("service: stable program meta %q has trailing JSON", metaPath)
	}
	if meta.Schema != stableProgramSchema || meta.Build == "" || meta.Digest == "" || meta.Size <= 0 {
		return stableProgramMeta{}, fmt.Errorf("service: stable program meta %q is not usable", metaPath)
	}
	return meta, nil
}

func fileDigest(path string) (string, error) {
	file, err := os.Open(path) //nolint:gosec // exact service-owned stable program path
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("service: hash %q: %w", path, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validateStableName(name string) error {
	if name == "" {
		return errors.New("service: stable program name is empty")
	}
	for index, value := range name {
		switch {
		case value >= 'a' && value <= 'z', value >= '0' && value <= '9':
			continue
		case index > 0 && (value == '.' || value == '-' || value == '_'):
			continue
		}
		return fmt.Errorf("service: stable program name %q is not canonical", name)
	}
	return nil
}
