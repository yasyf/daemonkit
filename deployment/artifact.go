// Package deployment owns exact signed application deployment and recovery.
package deployment

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/daemon"
)

var (
	// ErrChecksumMismatch means downloaded bytes do not match the declared digest.
	ErrChecksumMismatch = errors.New("deployment: asset checksum mismatch")
	// ErrUntrusted means the app does not satisfy the exact designated requirement.
	ErrUntrusted = errors.New("deployment: bundle failed designated requirement")
	// ErrVersionMismatch means the signed bundle declares another marketing version.
	ErrVersionMismatch = errors.New("deployment: bundle version mismatch")
	// ErrUnsafeArchive means extraction would escape or traverse the stage root.
	ErrUnsafeArchive = errors.New("deployment: unsafe archive entry")
	// ErrInvalidConfig means the immutable deployment specification is incomplete.
	ErrInvalidConfig = errors.New("deployment: invalid config")
	// ErrInstallConflict means an unmanaged or substituted namespace entry was found.
	ErrInstallConflict = errors.New("deployment: install path conflict")
	// ErrInstallState means durable deployment state is corrupt or inexact.
	ErrInstallState = errors.New("deployment: invalid install state")
)

// SHA256 is an exact asset or policy digest.
type SHA256 [sha256.Size]byte

// ParseSHA256 parses one exact lowercase or uppercase hexadecimal digest.
func ParseSHA256(raw string) (SHA256, error) {
	var digest SHA256
	if len(raw) != hex.EncodedLen(len(digest)) {
		return digest, errors.New("deployment: sha256 must contain 64 hexadecimal characters")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return digest, fmt.Errorf("deployment: parse sha256: %w", err)
	}
	copy(digest[:], decoded)
	return digest, nil
}

func (d SHA256) String() string { return hex.EncodeToString(d[:]) }

// Release identifies one immutable signed app asset.
type Release struct {
	Version string
	URL     string
	SHA256  SHA256
}

// Verifier checks an app against one exact codesign designated requirement.
type Verifier interface {
	Verify(context.Context, string, string) (string, error)
}

type artifactConfig struct {
	Release  Release
	Dir      string
	AppName  string
	Identity codeidentity.CodeIdentity
}

type namespaceOperation func(*os.File, string, *os.File, string) error

func validateArtifactConfig(cfg artifactConfig) (string, error) {
	if err := (stateLocation{Dir: cfg.Dir, AppName: cfg.AppName}).validate(); err != nil {
		return "", err
	}
	if cfg.Release.Version == "" || strings.TrimSpace(cfg.Release.Version) != cfg.Release.Version {
		return "", fmt.Errorf("%w: release version is required and cannot have surrounding whitespace", ErrInvalidConfig)
	}
	parsed, err := url.Parse(cfg.Release.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("%w: release asset URL must be absolute", ErrInvalidConfig)
	}
	if cfg.Release.SHA256 == (SHA256{}) {
		return "", fmt.Errorf("%w: release sha256 is required", ErrInvalidConfig)
	}
	requirement, err := cfg.Identity.DRString()
	if err != nil {
		return "", fmt.Errorf("%w: designated requirement: %w", ErrInvalidConfig, err)
	}
	return requirement, nil
}

func ensureMetadataDir(root, metadata string) error {
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("deployment: stat install dir %q: %w", root, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: install dir %q is not a real directory", ErrInvalidConfig, root)
	}
	for _, dir := range []string{filepath.Dir(metadata), metadata} {
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("deployment: create metadata dir %q: %w", dir, err)
		}
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: metadata path %q is not a real directory", ErrInstallConflict, dir)
		}
		if err := daemon.SyncDir(filepath.Dir(dir)); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) prepareCandidate(
	ctx context.Context,
	cfg Config,
	paths deploymentPaths,
	requirement string,
) (stage string, generation storedGeneration, returnErr error) {
	stage, err := os.MkdirTemp(paths.metadataDir, ".stage-")
	if err != nil {
		return "", storedGeneration{}, fmt.Errorf("deployment: create stage: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			returnErr = errors.Join(returnErr, os.RemoveAll(stage))
		}
	}()
	zipPath, err := c.download(ctx, cfg.Release, paths.metadataDir)
	if err != nil {
		return "", storedGeneration{}, err
	}
	defer func() { _ = os.Remove(zipPath) }()
	if err := unzip(zipPath, stage); err != nil {
		return "", storedGeneration{}, err
	}
	candidate := bundle.AppPath(stage, cfg.AppName)
	if err := requireRealDirectory(candidate); err != nil {
		return "", storedGeneration{}, err
	}
	cdHash, err := c.verifyBundle(ctx, candidate, cfg.Release.Version, requirement)
	if err != nil {
		return "", storedGeneration{}, err
	}
	if err := syncTree(candidate); err != nil {
		return "", storedGeneration{}, err
	}
	if err := errors.Join(daemon.SyncDir(stage), daemon.SyncDir(paths.metadataDir)); err != nil {
		return "", storedGeneration{}, err
	}
	id, err := identifyPath(candidate)
	if err != nil {
		return "", storedGeneration{}, fmt.Errorf("deployment: identify candidate: %w", err)
	}
	bundleDigest, err := bundleTreeDigest(candidate)
	if err != nil {
		return "", storedGeneration{}, err
	}
	keep = true
	return filepath.Base(stage), storedGeneration{
		Path: paths.canonical, Version: cfg.Release.Version, URL: cfg.Release.URL,
		SHA256: cfg.Release.SHA256.String(), DesignatedRequirement: requirement,
		CDHash: cdHash, BundleDigest: bundleDigest.String(), FileID: id,
	}, nil
}

func bundleTreeDigest(root string) (SHA256, error) {
	h := sha256.New()
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return SHA256{}, fmt.Errorf("deployment: open bundle root: %w", err)
	}
	walkErr := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		writeTreeDigestField(h, filepath.ToSlash(relative))
		writeTreeDigestField(h, fmt.Sprintf("%#o", uint32(info.Mode())))
		switch {
		case info.IsDir():
			writeTreeDigestField(h, "directory")
			return nil
		case info.Mode().IsRegular():
			writeTreeDigestField(h, "regular")
			file, err := rootHandle.Open(relative)
			if err != nil {
				return err
			}
			before, statErr := file.Stat()
			content := sha256.New()
			size, copyErr := io.Copy(content, file)
			after, restatErr := file.Stat()
			closeErr := file.Close()
			if err := errors.Join(statErr, copyErr, restatErr, closeErr); err != nil {
				return err
			}
			if !os.SameFile(info, before) || !os.SameFile(before, after) || size != before.Size() ||
				before.Size() != after.Size() || before.ModTime() != after.ModTime() ||
				info.Mode() != before.Mode() || before.Mode() != after.Mode() {
				return fmt.Errorf("deployment: bundle file changed while digesting %q", path)
			}
			writeTreeDigestField(h, fmt.Sprintf("%d", size))
			writeTreeDigestField(h, hex.EncodeToString(content.Sum(nil)))
			return nil
		case info.Mode()&os.ModeSymlink != 0:
			writeTreeDigestField(h, "symlink")
			target, err := rootHandle.Readlink(relative)
			if err != nil {
				return err
			}
			writeTreeDigestField(h, target)
			return nil
		default:
			return fmt.Errorf("deployment: bundle tree contains unsupported entry %q", path)
		}
	})
	closeErr := rootHandle.Close()
	if err := errors.Join(walkErr, closeErr); err != nil {
		return SHA256{}, fmt.Errorf("deployment: digest bundle tree: %w", err)
	}
	var digest SHA256
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func writeTreeDigestField(h hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(value))
}

func (c *Controller) download(ctx context.Context, release Release, dir string) (string, error) {
	body, err := c.get(ctx, release.URL)
	if err != nil {
		return "", err
	}
	defer body.Close()
	tmp, err := os.CreateTemp(dir, ".download-")
	if err != nil {
		return "", fmt.Errorf("deployment: create download: %w", err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(tmp, hash), body)
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(tmp.Name())
		return "", errors.Join(copyErr, syncErr, closeErr)
	}
	var got SHA256
	copy(got[:], hash.Sum(nil))
	if got != release.SHA256 {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("%w: got %s want %s", ErrChecksumMismatch, got.String(), release.SHA256.String())
	}
	return tmp.Name(), nil
}

func (c *Controller) get(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("deployment: request %q: %w", rawURL, err)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("deployment: get %q: %w", rawURL, err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, fmt.Errorf("deployment: get %q: status %s", rawURL, response.Status)
	}
	return response.Body, nil
}

func (c *Controller) verifyBundle(ctx context.Context, path, version, requirement string) (string, error) {
	cdHash, err := c.verifier.Verify(ctx, path, requirement)
	if err != nil {
		return "", fmt.Errorf("deployment: verify signed bundle: %w", err)
	}
	got, err := bundle.ShortVersion(path)
	if err != nil {
		return "", fmt.Errorf("deployment: read bundle version: %w", err)
	}
	if got != version {
		return "", fmt.Errorf("%w: got %q want %q", ErrVersionMismatch, got, version)
	}
	if !validCDHash(cdHash) {
		return "", errors.New("deployment: verifier returned invalid CDHash")
	}
	return strings.ToLower(cdHash), nil
}

func requireRealDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("deployment: inspect directory %q: %w", path, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %q is not a real directory", ErrInstallConflict, path)
	}
	return nil
}

func openVerifiedDirectory(path string) (*os.File, error) {
	if err := requireRealDirectory(path); err != nil {
		return nil, err
	}
	dir, err := openDirectoryNoFollow(path)
	if err != nil {
		return nil, fmt.Errorf("deployment: open directory %q: %w", path, err)
	}
	return dir, nil
}

func syncVerifiedDirectory(path string) error {
	dir, err := openVerifiedDirectory(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func writeJSONDurable(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return daemon.WriteFileDurable(path, append(data, '\n'), 0o600)
}

func removeDurable(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return daemon.SyncDir(filepath.Dir(path))
}

func exactObject(data []byte, fields []string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return nil, errors.New("JSON value is not an object")
	}
	object := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("JSON object key is not a string")
		}
		if _, duplicate := object[key]; duplicate {
			return nil, fmt.Errorf("duplicate JSON object key %q", key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		object[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing JSON data")
	}
	if fields != nil {
		if err := exactFields(object, fields); err != nil {
			return nil, err
		}
	}
	return object, nil
}

func exactFields(object map[string]json.RawMessage, fields []string) error {
	if len(object) != len(fields) {
		return errors.New("JSON field count mismatch")
	}
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return fmt.Errorf("JSON field %q is missing", field)
		}
	}
	return nil
}

func syncTree(root string) error {
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	walkErr := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		file, err := rootHandle.Open(relative)
		if err != nil {
			return err
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		return errors.Join(syncErr, closeErr)
	})
	return errors.Join(walkErr, rootHandle.Close())
}
