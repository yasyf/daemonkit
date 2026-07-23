// Package fetch installs an exact consumer-owned signed macOS .app release at
// its fixed daemonkit-managed path. It does not package a generic holder app;
// consumers embed the FuseKit holder runtime in their own signed app.
package fetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

const (
	receiptSchema  = 1
	lockDeadline   = 30 * time.Second
	settleDeadline = 30 * time.Second
	statePrepared  = "prepared"
	stateFinal     = "final"
)

var (
	// ErrChecksumMismatch is returned when the asset bytes do not match Release.SHA256.
	ErrChecksumMismatch = errors.New("fetch: asset checksum mismatch")
	// ErrUntrusted is returned when an unpacked bundle fails its designated requirement.
	ErrUntrusted = errors.New("fetch: bundle failed designated requirement")
	// ErrVersionMismatch is returned when CFBundleShortVersionString does not equal Release.Version.
	ErrVersionMismatch = errors.New("fetch: bundle version mismatch")
	// ErrUnsafeArchive is returned when a zip entry would escape its destination.
	ErrUnsafeArchive = errors.New("fetch: unsafe archive entry")
	// ErrInvalidConfig is returned when Config cannot identify one exact release and path.
	ErrInvalidConfig = errors.New("fetch: invalid config")
	// ErrInstallConflict is returned when the canonical path is not managed by fetch.
	ErrInstallConflict = errors.New("fetch: install path conflict")
	// ErrInstallState is returned when durable install metadata contradicts the filesystem.
	ErrInstallState = errors.New("fetch: invalid install state")
)

// SHA256 is an exact artifact digest.
type SHA256 [sha256.Size]byte

// ParseSHA256 parses one 64-character hexadecimal SHA-256 digest.
func ParseSHA256(raw string) (SHA256, error) {
	var digest SHA256
	if len(raw) != hex.EncodedLen(len(digest)) {
		return digest, fmt.Errorf("%w: sha256 must be 64 hexadecimal characters", ErrInvalidConfig)
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return digest, fmt.Errorf("%w: sha256: %w", ErrInvalidConfig, err)
	}
	copy(digest[:], decoded)
	return digest, nil
}

// String returns the lowercase hexadecimal digest.
func (d SHA256) String() string { return hex.EncodeToString(d[:]) }

// Release identifies one immutable release asset.
type Release struct {
	// Version is the exact signed CFBundleShortVersionString, not a release tag.
	Version string
	// URL is the exact asset URL whose bytes SHA256 identifies.
	URL    string
	SHA256 SHA256
}

// Config identifies one signed app release and its canonical install path.
type Config struct {
	Release  Release
	Dir      string
	AppName  string
	Identity codeidentity.CodeIdentity
}

// Installation is the exact release published at Path.
type Installation struct {
	Path    string
	Release Release
}

// Verifier checks that appPath satisfies a codesign designated requirement.
type Verifier interface {
	Verify(ctx context.Context, appPath, requirement string) error
}

// Fetcher installs signed app releases.
type Fetcher struct {
	Client   *http.Client
	Verifier Verifier

	exchange         namespaceOperation
	publishFirst     namespaceOperation
	beforePrepared   func() error
	afterPrepared    func() error
	afterSwap        func() error
	afterFinal       func() error
	afterTransaction func() error
}

type namespaceOperation func(*os.File, string, *os.File, string) error

type receipt struct {
	Schema                int    `json:"schema"`
	State                 string `json:"state"`
	Version               string `json:"version"`
	URL                   string `json:"url"`
	SHA256                string `json:"sha256"`
	AppName               string `json:"app_name"`
	DesignatedRequirement string `json:"designated_requirement"`
	Canonical             fileID `json:"canonical"`
}

type transaction struct {
	Schema    int     `json:"schema"`
	Stage     string  `json:"stage"`
	Candidate fileID  `json:"candidate"`
	Previous  *fileID `json:"previous"`
	Next      receipt `json:"next"`
}

type installPaths struct {
	canonical   string
	metadataDir string
	lock        string
	receipt     string
	transaction string
}

// New returns a Fetcher using the platform codesign verifier.
func New() *Fetcher {
	return &Fetcher{
		Client: http.DefaultClient, Verifier: newVerifier(),
		exchange: exchangePaths, publishFirst: publishExclusive,
	}
}

// Fetch publishes cfg.Release as a real directory at <Dir>/<AppName>.app.
// Reuse requires an exact durable release receipt, declared bundle version,
// and designated requirement. Replacement is serialized and atomic.
func (f *Fetcher) Fetch(ctx context.Context, cfg Config) (Installation, error) {
	if f == nil || f.Client == nil || f.Verifier == nil {
		return Installation{}, fmt.Errorf("%w: fetcher client and verifier are required", ErrInvalidConfig)
	}
	dr, err := validateConfig(cfg)
	if err != nil {
		return Installation{}, err
	}
	paths := pathsFor(cfg)
	if err := ensureMetadataDir(cfg.Dir, paths.metadataDir); err != nil {
		return Installation{}, err
	}
	lock, err := (proc.FileLockSpec{
		Path: paths.lock, Mode: proc.FileLockExclusive, Deadline: lockDeadline,
	}).Acquire(ctx)
	if err != nil {
		return Installation{}, fmt.Errorf("fetch: acquire install lock: %w", err)
	}
	defer lock.Close()

	if err := f.recover(ctx, paths); err != nil {
		return Installation{}, err
	}
	if err := f.cleanupResidue(paths); err != nil {
		return Installation{}, err
	}
	want := newReceipt(cfg, dr)
	current, err := readCurrent(paths)
	if err != nil {
		return Installation{}, err
	}
	if current != nil && current.matches(want) {
		if err := f.verifyInstalled(ctx, paths.canonical, *current); err == nil {
			return Installation{Path: paths.canonical, Release: cfg.Release}, nil
		} else if !errors.Is(err, ErrUntrusted) && !errors.Is(err, ErrVersionMismatch) {
			return Installation{}, err
		}
	}
	if err := validateManagedPath(paths, current); err != nil {
		return Installation{}, err
	}
	if err := f.install(ctx, cfg, paths, want, current); err != nil {
		return Installation{}, err
	}
	return Installation{Path: paths.canonical, Release: cfg.Release}, nil
}

func validateConfig(cfg Config) (string, error) {
	if cfg.Dir == "" || !filepath.IsAbs(cfg.Dir) || filepath.Clean(cfg.Dir) != cfg.Dir || cfg.Dir == string(filepath.Separator) {
		return "", fmt.Errorf("%w: install dir must be exact, absolute, and non-root", ErrInvalidConfig)
	}
	if cfg.AppName == "" || cfg.AppName == "." || cfg.AppName == ".." ||
		filepath.Base(cfg.AppName) != cfg.AppName || strings.HasSuffix(cfg.AppName, ".app") {
		return "", fmt.Errorf("%w: app name must be a basename without .app", ErrInvalidConfig)
	}
	if !within(cfg.Dir, filepath.Join(cfg.Dir, ".daemonkit-fetch", cfg.AppName)) ||
		!within(cfg.Dir, bundle.AppPath(cfg.Dir, cfg.AppName)) {
		return "", fmt.Errorf("%w: app paths escape install dir", ErrInvalidConfig)
	}
	if cfg.Release.Version == "" || strings.TrimSpace(cfg.Release.Version) != cfg.Release.Version {
		return "", fmt.Errorf("%w: release version is required and cannot have surrounding whitespace", ErrInvalidConfig)
	}
	u, err := url.Parse(cfg.Release.URL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("%w: release asset URL must be absolute", ErrInvalidConfig)
	}
	if cfg.Release.SHA256 == (SHA256{}) {
		return "", fmt.Errorf("%w: release sha256 is required", ErrInvalidConfig)
	}
	dr, err := cfg.Identity.DRString()
	if err != nil {
		return "", fmt.Errorf("%w: designated requirement: %w", ErrInvalidConfig, err)
	}
	return dr, nil
}

func pathsFor(cfg Config) installPaths {
	metadataDir := filepath.Join(cfg.Dir, ".daemonkit-fetch", cfg.AppName)
	return installPaths{
		canonical:   bundle.AppPath(cfg.Dir, cfg.AppName),
		metadataDir: metadataDir,
		lock:        filepath.Join(metadataDir, "install.lock"),
		receipt:     filepath.Join(metadataDir, "receipt.json"),
		transaction: filepath.Join(metadataDir, "transaction.json"),
	}
}

func newReceipt(cfg Config, dr string) receipt {
	return receipt{
		Schema: receiptSchema, State: stateFinal, Version: cfg.Release.Version, URL: cfg.Release.URL,
		SHA256: cfg.Release.SHA256.String(), AppName: cfg.AppName, DesignatedRequirement: dr,
	}
}

func (r receipt) matches(want receipt) bool {
	return r.Schema == receiptSchema && r.State == stateFinal &&
		r.Version == want.Version && r.URL == want.URL && r.SHA256 == want.SHA256 &&
		r.AppName == want.AppName && r.DesignatedRequirement == want.DesignatedRequirement
}

func (r receipt) validate(expectedState string) error {
	if r.Schema != receiptSchema || r.State != expectedState {
		return fmt.Errorf("%w: receipt schema or state", ErrInstallState)
	}
	if r.Version == "" || strings.TrimSpace(r.Version) != r.Version {
		return fmt.Errorf("%w: receipt version", ErrInstallState)
	}
	u, err := url.Parse(r.URL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%w: receipt URL", ErrInstallState)
	}
	if len(r.SHA256) != hex.EncodedLen(sha256.Size) {
		return fmt.Errorf("%w: receipt sha256", ErrInstallState)
	}
	if _, err := hex.DecodeString(r.SHA256); err != nil {
		return fmt.Errorf("%w: receipt sha256", ErrInstallState)
	}
	if r.AppName == "" || r.AppName == "." || r.AppName == ".." ||
		filepath.Base(r.AppName) != r.AppName || strings.HasSuffix(r.AppName, ".app") {
		return fmt.Errorf("%w: receipt app name", ErrInstallState)
	}
	if r.DesignatedRequirement == "" || r.Canonical.Device == "" || r.Canonical.Inode == "" {
		return fmt.Errorf("%w: receipt policy or file identity", ErrInstallState)
	}
	return nil
}

func ensureMetadataDir(root, metadataDir string) error {
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("fetch: stat install dir %q: %w", root, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: install dir %q is not a real directory", ErrInvalidConfig, root)
	}
	for _, dir := range []string{filepath.Dir(metadataDir), metadataDir} {
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("fetch: create metadata dir %q: %w", dir, err)
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

func readCurrent(paths installPaths) (*receipt, error) {
	appInfo, appErr := os.Lstat(paths.canonical)
	data, receiptErr := os.ReadFile(paths.receipt)
	if errors.Is(appErr, os.ErrNotExist) && errors.Is(receiptErr, os.ErrNotExist) {
		return nil, nil
	}
	if appErr != nil {
		return nil, fmt.Errorf("%w: canonical app: %w", ErrInstallState, appErr)
	}
	if appInfo.Mode()&os.ModeSymlink != 0 || !appInfo.IsDir() {
		return nil, fmt.Errorf("%w: canonical app %q is not a real directory", ErrInstallConflict, paths.canonical)
	}
	if receiptErr != nil {
		if errors.Is(receiptErr, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: canonical app has no receipt", ErrInstallConflict)
		}
		return nil, fmt.Errorf("%w: read receipt: %w", ErrInstallState, receiptErr)
	}
	var got receipt
	if err := decodeStrict(data, &got); err != nil {
		return nil, fmt.Errorf("%w: decode receipt", ErrInstallState)
	}
	if err := got.validate(stateFinal); err != nil {
		return nil, err
	}
	if got.AppName != strings.TrimSuffix(filepath.Base(paths.canonical), ".app") {
		return nil, fmt.Errorf("%w: receipt app name does not match canonical path", ErrInstallState)
	}
	id, err := identifyPath(paths.canonical)
	if err != nil {
		return nil, fmt.Errorf("%w: identify canonical app: %w", ErrInstallState, err)
	}
	if id != got.Canonical {
		return nil, fmt.Errorf("%w: receipt canonical identity does not match app", ErrInstallState)
	}
	return &got, nil
}

func validateManagedPath(paths installPaths, current *receipt) error {
	_, err := os.Lstat(paths.canonical)
	if current == nil && err == nil {
		return fmt.Errorf("%w: canonical app exists without an exact receipt", ErrInstallConflict)
	}
	if current == nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("fetch: inspect canonical app: %w", err)
	}
	return nil
}

func (f *Fetcher) verifyInstalled(ctx context.Context, appPath string, rec receipt) error {
	if err := f.Verifier.Verify(ctx, appPath, rec.DesignatedRequirement); err != nil {
		return fmt.Errorf("fetch: verify installed bundle: %w", err)
	}
	version, err := bundle.ShortVersion(appPath)
	if err != nil {
		return fmt.Errorf("fetch: read installed bundle version: %w", err)
	}
	if version != rec.Version {
		return fmt.Errorf("%w: got %q want %q", ErrVersionMismatch, version, rec.Version)
	}
	return nil
}

func (f *Fetcher) install(
	ctx context.Context,
	cfg Config,
	paths installPaths,
	want receipt,
	current *receipt,
) error {
	stage, err := os.MkdirTemp(paths.metadataDir, ".stage-")
	if err != nil {
		return fmt.Errorf("fetch: create stage: %w", err)
	}
	keepStage := false
	defer func() {
		if !keepStage {
			_ = os.RemoveAll(stage)
		}
	}()

	zipPath, err := f.download(ctx, cfg.Release, paths.metadataDir)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(zipPath) }()
	if err := unzip(zipPath, stage); err != nil {
		return err
	}
	candidate := bundle.AppPath(stage, cfg.AppName)
	if err := requireRealDirectory(candidate); err != nil {
		return err
	}
	if err := f.verifyInstalled(ctx, candidate, want); err != nil {
		return err
	}
	if err := syncTree(candidate); err != nil {
		return err
	}
	if err := daemon.SyncDir(stage); err != nil {
		return err
	}
	if err := daemon.SyncDir(paths.metadataDir); err != nil {
		return err
	}
	id, err := identifyPath(candidate)
	if err != nil {
		return fmt.Errorf("fetch: identify candidate: %w", err)
	}
	want.State = statePrepared
	want.Canonical = id
	var previous *fileID
	if current != nil {
		previousID := current.Canonical
		previous = &previousID
	}
	tx := transaction{
		Schema: receiptSchema, Stage: filepath.Base(stage), Candidate: id, Previous: previous, Next: want,
	}
	if f.beforePrepared != nil {
		if err := f.beforePrepared(); err != nil {
			return err
		}
	}
	if err := writeJSONDurable(paths.transaction, tx); err != nil {
		return fmt.Errorf("fetch: write transaction: %w", err)
	}
	keepStage = true
	if f.afterPrepared != nil {
		if err := f.afterPrepared(); err != nil {
			return err
		}
	}
	if err := f.publish(paths, tx); err != nil {
		return err
	}
	if f.afterSwap != nil {
		if err := f.afterSwap(); err != nil {
			return err
		}
	}
	if err := f.finish(ctx, paths, tx); err != nil {
		return err
	}
	keepStage = false
	return nil
}

func (f *Fetcher) download(ctx context.Context, release Release, dir string) (string, error) {
	body, err := f.get(ctx, release.URL)
	if err != nil {
		return "", err
	}
	defer body.Close()
	tmp, err := os.CreateTemp(dir, ".download-")
	if err != nil {
		return "", fmt.Errorf("fetch: create download: %w", err)
	}
	h := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(tmp, h), body)
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(tmp.Name())
		return "", errors.Join(copyErr, syncErr, closeErr)
	}
	var got SHA256
	copy(got[:], h.Sum(nil))
	if got != release.SHA256 {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("%w: got %s want %s", ErrChecksumMismatch, got.String(), release.SHA256.String())
	}
	return tmp.Name(), nil
}

func (f *Fetcher) get(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch: request %q: %w", rawURL, err)
	}
	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: get %q: %w", rawURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("fetch: get %q: status %s", rawURL, resp.Status)
	}
	return resp.Body, nil
}

func (f *Fetcher) publish(paths installPaths, tx transaction) error {
	stage := filepath.Join(paths.metadataDir, tx.Stage)
	stageApp := bundle.AppPath(stage, tx.Next.AppName)
	canonicalParent, err := openVerifiedDirectory(filepath.Dir(paths.canonical))
	if err != nil {
		return err
	}
	defer canonicalParent.Close()
	stageParent, err := openVerifiedDirectory(stage)
	if err != nil {
		return err
	}
	defer stageParent.Close()
	canonicalName := filepath.Base(paths.canonical)
	stageName := filepath.Base(stageApp)
	stagedID, err := identifyAt(stageParent, stageName)
	if err != nil || stagedID != tx.Candidate {
		return fmt.Errorf("%w: staged candidate changed before publish", ErrInstallState)
	}
	canonicalID, canonicalErr := identifyAt(canonicalParent, canonicalName)
	if tx.Previous != nil {
		if canonicalErr != nil || canonicalID != *tx.Previous {
			return fmt.Errorf("%w: canonical generation changed before exchange", ErrInstallConflict)
		}
		exchange := f.exchange
		if exchange == nil {
			exchange = exchangePaths
		}
		if err := exchange(canonicalParent, canonicalName, stageParent, stageName); err != nil {
			return fmt.Errorf("fetch: exchange canonical bundle: %w", err)
		}
	} else {
		if !errors.Is(canonicalErr, os.ErrNotExist) {
			return fmt.Errorf("%w: canonical appeared before first publish", ErrInstallConflict)
		}
		publish := f.publishFirst
		if publish == nil {
			publish = publishExclusive
		}
		if err := publish(stageParent, stageName, canonicalParent, canonicalName); err != nil {
			return fmt.Errorf("fetch: publish canonical bundle: %w", err)
		}
	}
	return errors.Join(canonicalParent.Sync(), stageParent.Sync())
}

func (f *Fetcher) finish(ctx context.Context, paths installPaths, tx transaction) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleDeadline)
	defer cancel()
	if err := f.verifyInstalled(settleCtx, paths.canonical, tx.Next); err != nil {
		return fmt.Errorf("fetch: verify published bundle: %w", err)
	}
	canonical, err := identifyPath(paths.canonical)
	if err != nil {
		return fmt.Errorf("fetch: identify published bundle: %w", err)
	}
	if canonical != tx.Candidate {
		return fmt.Errorf("%w: published bundle identity changed", ErrInstallState)
	}
	final := tx.Next
	final.State = stateFinal
	final.Canonical = canonical
	if err := writeJSONDurable(paths.receipt, final); err != nil {
		return fmt.Errorf("fetch: write receipt: %w", err)
	}
	if f.afterFinal != nil {
		if err := f.afterFinal(); err != nil {
			return err
		}
	}
	if err := removeDurable(paths.transaction); err != nil {
		return fmt.Errorf("fetch: clear transaction: %w", err)
	}
	if f.afterTransaction != nil {
		if err := f.afterTransaction(); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(filepath.Join(paths.metadataDir, tx.Stage)); err != nil {
		return err
	}
	return syncVerifiedDirectory(paths.metadataDir)
}

func (f *Fetcher) recover(ctx context.Context, paths installPaths) error {
	tx, err := readTransaction(paths.transaction)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if tx.Next.AppName != strings.TrimSuffix(filepath.Base(paths.canonical), ".app") {
		return fmt.Errorf("%w: transaction app name does not match canonical path", ErrInstallState)
	}
	stage := filepath.Join(paths.metadataDir, tx.Stage)
	if !within(paths.metadataDir, stage) {
		return fmt.Errorf("%w: transaction stage escapes metadata dir", ErrInstallState)
	}
	stageApp := bundle.AppPath(stage, tx.Next.AppName)
	canonicalID, canonicalErr := identifyPath(paths.canonical)
	stageID, stageErr := identifyPath(stageApp)
	switch {
	case canonicalErr == nil && canonicalID == tx.Candidate:
		if err := requireRealDirectory(paths.canonical); err != nil {
			return err
		}
		return f.finish(ctx, paths, tx)
	case stageErr == nil && stageID == tx.Candidate && errors.Is(canonicalErr, os.ErrNotExist):
		if tx.Previous != nil {
			return fmt.Errorf("%w: previous canonical disappeared", ErrInstallConflict)
		}
		if err := f.verifyRecoveryCandidate(ctx, stageApp, tx.Next); err != nil {
			return err
		}
		if err := f.publish(paths, tx); err != nil {
			return err
		}
		return f.finish(ctx, paths, tx)
	case stageErr == nil && stageID == tx.Candidate && canonicalErr == nil:
		if tx.Previous == nil || canonicalID != *tx.Previous {
			return fmt.Errorf("%w: unexpected canonical generation", ErrInstallConflict)
		}
		if err := requireRealDirectory(paths.canonical); err != nil {
			return err
		}
		if err := f.verifyRecoveryCandidate(ctx, stageApp, tx.Next); err != nil {
			return err
		}
		if err := f.publish(paths, tx); err != nil {
			return err
		}
		return f.finish(ctx, paths, tx)
	default:
		return fmt.Errorf("%w: transaction candidate is neither staged nor canonical", ErrInstallState)
	}
}

func readTransaction(path string) (transaction, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return transaction{}, err
	}
	var tx transaction
	if err := decodeStrict(data, &tx); err != nil || tx.Schema != receiptSchema ||
		tx.Next.Canonical != tx.Candidate || filepath.Base(tx.Stage) != tx.Stage || !strings.HasPrefix(tx.Stage, ".stage-") {
		return transaction{}, fmt.Errorf("%w: decode transaction", ErrInstallState)
	}
	if err := tx.Next.validate(statePrepared); err != nil {
		return transaction{}, err
	}
	if tx.Previous != nil && (tx.Previous.Device == "" || tx.Previous.Inode == "" || *tx.Previous == tx.Candidate) {
		return transaction{}, fmt.Errorf("%w: transaction previous identity", ErrInstallState)
	}
	return tx, nil
}

func (f *Fetcher) verifyRecoveryCandidate(ctx context.Context, path string, rec receipt) error {
	if err := requireRealDirectory(path); err != nil {
		return err
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleDeadline)
	defer cancel()
	return f.verifyInstalled(settleCtx, path, rec)
}

func requireRealDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("fetch: inspect bundle %q: %w", path, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: bundle %q is not a real directory", ErrInstallConflict, path)
	}
	return nil
}

func openVerifiedDirectory(path string) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: namespace parent %q is not a real directory", ErrInstallConflict, path)
	}
	dir, err := openDirectoryNoFollow(path)
	if err != nil {
		return nil, fmt.Errorf("fetch: open namespace parent %q: %w", path, err)
	}
	after, err := dir.Stat()
	if err != nil || !os.SameFile(before, after) {
		dir.Close()
		return nil, fmt.Errorf("%w: namespace parent %q changed during open", ErrInstallState, path)
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

func decodeStrict(data []byte, value any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(value); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
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

func (f *Fetcher) cleanupResidue(paths installPaths) error {
	entries, err := os.ReadDir(paths.metadataDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".stage-") || strings.HasPrefix(entry.Name(), ".download-") {
			if err := os.RemoveAll(filepath.Join(paths.metadataDir, entry.Name())); err != nil {
				return fmt.Errorf("fetch: remove residue %q: %w", entry.Name(), err)
			}
		}
	}
	return daemon.SyncDir(paths.metadataDir)
}

func syncTree(root string) error {
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("fetch: open candidate root: %w", err)
	}
	defer rootHandle.Close()
	var dirs []string
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.IsDir() {
			dirs = append(dirs, path)
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
	if err != nil {
		return fmt.Errorf("fetch: sync candidate files: %w", err)
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := daemon.SyncDir(dirs[i]); err != nil {
			return err
		}
	}
	return nil
}
