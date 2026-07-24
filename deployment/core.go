// Package deployment activates an already-installed fixed signed application.
package deployment

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

var (
	// ErrUntrusted means the app does not satisfy the exact designated requirement.
	ErrUntrusted = errors.New("deployment: bundle failed designated requirement")
	// ErrVersionMismatch means the signed bundle declares another marketing version.
	ErrVersionMismatch = errors.New("deployment: bundle version mismatch")
	// ErrInvalidConfig means the immutable activation specification is incomplete.
	ErrInvalidConfig = errors.New("deployment: invalid config")
	// ErrInstallConflict means installed bytes or durable ownership differ from the request.
	ErrInstallConflict = errors.New("deployment: install path conflict")
	// ErrInstallState means durable activation state is corrupt or inexact.
	ErrInstallState = errors.New("deployment: invalid install state")
	// ErrUnsupported is returned when signed bundle verification is unavailable.
	ErrUnsupported = errors.New("deployment: codesign verification is only supported on macOS")
)

// SHA256 is one exact artifact, entitlement, proof, or policy digest.
type SHA256 [sha256.Size]byte

// ParseSHA256 parses one hexadecimal digest.
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

// String returns the lowercase hexadecimal digest.
func (d SHA256) String() string { return hex.EncodeToString(d[:]) }

func (d SHA256) validate(name string) error {
	if d == (SHA256{}) {
		return fmt.Errorf("%w: %s is required", ErrInvalidConfig, name)
	}
	return nil
}

type signatureAttestation struct {
	CDHash             string
	EntitlementsDigest SHA256
}

type verifier interface {
	Verify(context.Context, string, string) (signatureAttestation, error)
}

type serviceController interface {
	Converge(context.Context, []service.Agent) error
	Status(context.Context, string) (service.Status, error)
	StopRuntime(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error)
	Close(context.Context) error
}

type serviceFactory func(context.Context, service.ControllerConfig) (serviceController, error)

// Controller owns one app's exact activation receipts and launchd state.
type Controller struct {
	verifier    verifier
	openService serviceFactory
	operationID func() (string, error)
	failpoint   func(string) error
}

// New returns a Controller using platform codesign and durable service state.
func New() *Controller {
	return &Controller{
		verifier: newVerifier(),
		openService: func(ctx context.Context, config service.ControllerConfig) (serviceController, error) {
			return service.NewController(ctx, config)
		},
		operationID: newOperationID,
	}
}

type deploymentPaths struct {
	canonical      string
	metadataDir    string
	lock           string
	activation     string
	deactivation   string
	serviceState   string
	serviceProcess string
}

func deploymentPathsForApp(appPath string) deploymentPaths {
	appName := strings.TrimSuffix(filepath.Base(appPath), ".app")
	metadata := filepath.Join(filepath.Dir(appPath), ".daemonkit-deployment", appName)
	return deploymentPaths{
		canonical: appPath, metadataDir: metadata,
		lock:           filepath.Join(metadata, "deployment.lock"),
		activation:     filepath.Join(metadata, "activation.json"),
		deactivation:   filepath.Join(metadata, "deactivation.json"),
		serviceState:   filepath.Join(metadata, "services.db"),
		serviceProcess: filepath.Join(metadata, "service-workers.db"),
	}
}

func validateCanonicalAppPath(appPath string) error {
	if appPath == "" || !filepath.IsAbs(appPath) || filepath.Clean(appPath) != appPath ||
		!strings.HasSuffix(filepath.Base(appPath), ".app") || filepath.Base(appPath) == ".app" {
		return fmt.Errorf("%w: app path must be an exact absolute .app path", ErrInvalidConfig)
	}
	resolved, err := filepath.EvalSymlinks(appPath)
	if err != nil {
		return fmt.Errorf("%w: resolve canonical app path: %w", ErrInstallConflict, err)
	}
	if resolved != appPath {
		return fmt.Errorf("%w: canonical app path contains a symlink", ErrInstallConflict)
	}
	return requireRealDirectory(appPath)
}

func ensureMetadataDir(paths deploymentPaths) error {
	root := filepath.Dir(paths.canonical)
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return fmt.Errorf("%w: install directory is not a canonical real path", ErrInstallConflict)
	}
	for _, dir := range []string{filepath.Dir(paths.metadataDir), paths.metadataDir} {
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("deployment: create metadata directory %q: %w", dir, err)
		}
		if err := requireRealDirectory(dir); err != nil {
			return err
		}
		if err := daemon.SyncDir(filepath.Dir(dir)); err != nil {
			return err
		}
	}
	return nil
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
		writeDigestField(h, filepath.ToSlash(relative))
		writeDigestField(h, fmt.Sprintf("%#o", uint32(info.Mode())))
		switch {
		case info.IsDir():
			writeDigestField(h, "directory")
			return nil
		case info.Mode().IsRegular():
			writeDigestField(h, "regular")
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
			writeDigestField(h, fmt.Sprintf("%d", size))
			writeDigestField(h, hex.EncodeToString(content.Sum(nil)))
			return nil
		case info.Mode()&os.ModeSymlink != 0:
			writeDigestField(h, "symlink")
			target, err := rootHandle.Readlink(relative)
			if err != nil {
				return err
			}
			writeDigestField(h, target)
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

func writeDigestField(h hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(value))
}

func inspectInstalled(
	ctx context.Context,
	verifier verifier,
	appPath, version string,
	identity codeidentity.CodeIdentity,
) (storedGeneration, error) {
	if err := validateCanonicalAppPath(appPath); err != nil {
		return storedGeneration{}, err
	}
	requirement, err := identity.DRString()
	if err != nil {
		return storedGeneration{}, fmt.Errorf("%w: designated requirement: %w", ErrInvalidConfig, err)
	}
	signature, err := verifier.Verify(ctx, appPath, requirement)
	if err != nil {
		return storedGeneration{}, fmt.Errorf("deployment: verify signed bundle: %w", err)
	}
	if !validCDHash(signature.CDHash) {
		return storedGeneration{}, errors.New("deployment: verifier returned invalid CDHash")
	}
	gotVersion, err := bundle.ShortVersion(appPath)
	if err != nil {
		return storedGeneration{}, fmt.Errorf("deployment: read bundle version: %w", err)
	}
	if gotVersion != version {
		return storedGeneration{}, fmt.Errorf("%w: got %q want %q", ErrVersionMismatch, gotVersion, version)
	}
	digest, err := bundleTreeDigest(appPath)
	if err != nil {
		return storedGeneration{}, err
	}
	id, err := identifyPath(appPath)
	if err != nil {
		return storedGeneration{}, fmt.Errorf("deployment: identify canonical app: %w", err)
	}
	return storedGeneration{
		Path: appPath, Version: version, TeamID: identity.TeamID,
		SigningIdentifier: identity.SigningIdentifier, DesignatedRequirement: requirement,
		CDHash: strings.ToLower(signature.CDHash), EntitlementsDigest: signature.EntitlementsDigest.String(),
		BundleDigest: digest.String(), FileID: id,
	}, nil
}

func newOperationID() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("deployment: generate operation id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func validOperationID(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validCDHash(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func verifyGenerationIdentity(path string, expected fileID) error {
	current, err := identifyPath(path)
	if err != nil {
		return err
	}
	if current != expected {
		return errors.New("deployment: app file identity changed")
	}
	return nil
}

func writeJSONDurable(path string, value any) error {
	return writeExactJSON(path, value)
}

var runtimeExecutable = service.CanonicalExecutable

// RuntimeStopControlStore returns the app-scoped process authority store.
func RuntimeStopControlStore() (*proc.FileStore, error) {
	executable, err := runtimeExecutable()
	if err != nil {
		return nil, fmt.Errorf("deployment: resolve runtime executable: %w", err)
	}
	macOS := filepath.Dir(executable)
	contents := filepath.Dir(macOS)
	app := filepath.Dir(contents)
	if filepath.Base(macOS) != "MacOS" || filepath.Base(contents) != "Contents" ||
		filepath.Dir(executable) != macOS || !strings.HasSuffix(filepath.Base(app), ".app") {
		return nil, errors.New("deployment: runtime executable is not a direct child of an app Contents/MacOS directory")
	}
	if err := validateCanonicalAppPath(app); err != nil {
		return nil, err
	}
	return &proc.FileStore{Path: deploymentPathsForApp(app).serviceProcess}, nil
}
