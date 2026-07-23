// Package fetch downloads a signed macOS .app bundle from a GitHub release,
// verifies its SHA-256 against the release checksums and its signature against
// a pinned designated requirement, and installs it into a caller-managed
// directory. It preserves the asset's build-time signature; it never re-signs.
package fetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
)

// ErrChecksumMismatch is returned when a downloaded asset's SHA-256 does not
// match the release's published checksum.
var ErrChecksumMismatch = errors.New("fetch: asset checksum mismatch")

// ErrChecksumMissing is returned when the checksums file lists no entry for the
// requested asset.
var ErrChecksumMissing = errors.New("fetch: no checksum for asset")

// ErrUntrusted is returned when an unpacked bundle fails its designated
// requirement.
var ErrUntrusted = errors.New("fetch: bundle failed designated requirement")

// ErrUnsafeArchive is returned when a zip entry would escape the destination
// directory (zip-slip).
var ErrUnsafeArchive = errors.New("fetch: unsafe archive entry")

// Verifier checks that the .app at appPath satisfies requirement, a codesign
// designated-requirement string. Production shells to `codesign --verify -R`.
type Verifier interface {
	Verify(ctx context.Context, appPath, requirement string) error
}

// Config describes one signed .app release asset and where it installs.
type Config struct {
	AssetURL     string                    // release asset: a .zip holding <AppName>.app
	ChecksumsURL string                    // release checksums file, sha256sum format
	Dir          string                    // caller-managed install directory
	AppName      string                    // inner bundle basename, without ".app"
	Identity     codeidentity.CodeIdentity // pinned designated requirement
}

// Fetcher downloads and installs signed .app bundles.
type Fetcher struct {
	Client   *http.Client
	Verifier Verifier
}

// New returns a Fetcher wired to http.DefaultClient and the platform's real
// codesign verifier.
func New() *Fetcher {
	return &Fetcher{Client: http.DefaultClient, Verifier: newVerifier()}
}

// Fetch resolves cfg into a verified <Dir>/<AppName>.app, downloading and
// installing it when necessary, and returns the bundle path. It is idempotent:
// an already-installed bundle that passes the designated requirement is reused
// without re-downloading.
func (f *Fetcher) Fetch(ctx context.Context, cfg Config) (string, error) {
	dr, err := cfg.Identity.DRString()
	if err != nil {
		return "", fmt.Errorf("fetch: designated requirement: %w", err)
	}
	appPath := bundle.AppPath(cfg.Dir, cfg.AppName)
	if _, err := os.Stat(appPath); err == nil {
		if f.Verifier.Verify(ctx, appPath, dr) == nil {
			return appPath, nil
		}
	}
	return f.install(ctx, cfg, appPath, dr)
}

func (f *Fetcher) install(ctx context.Context, cfg Config, appPath, dr string) (string, error) {
	u, err := url.Parse(cfg.AssetURL)
	if err != nil {
		return "", fmt.Errorf("fetch: asset url %q: %w", cfg.AssetURL, err)
	}
	asset := path.Base(u.Path)

	want, err := f.expectedSum(ctx, cfg.ChecksumsURL, asset)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return "", fmt.Errorf("fetch: create dir %q: %w", cfg.Dir, err)
	}

	zipPath, err := f.download(ctx, cfg.AssetURL, cfg.Dir)
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(zipPath) }()

	got, err := sha256File(zipPath)
	if err != nil {
		return "", err
	}
	if got != want {
		return "", fmt.Errorf("%w: asset %q got %s want %s", ErrChecksumMismatch, asset, got, want)
	}

	staging, err := os.MkdirTemp(cfg.Dir, ".staging-*")
	if err != nil {
		return "", fmt.Errorf("fetch: staging dir: %w", err)
	}
	defer os.RemoveAll(staging)

	if err := unzip(zipPath, staging); err != nil {
		return "", err
	}
	stagedApp := bundle.AppPath(staging, cfg.AppName)
	if _, err := os.Stat(stagedApp); err != nil {
		return "", fmt.Errorf("fetch: %s.app not found in %q: %w", cfg.AppName, asset, err)
	}
	if err := f.Verifier.Verify(ctx, stagedApp, dr); err != nil {
		return "", fmt.Errorf("%w: %w", ErrUntrusted, err)
	}

	if err := os.RemoveAll(appPath); err != nil {
		return "", fmt.Errorf("fetch: clear existing bundle %q: %w", appPath, err)
	}
	if err := os.Rename(stagedApp, appPath); err != nil {
		return "", fmt.Errorf("fetch: install bundle %q: %w", appPath, err)
	}
	return appPath, nil
}

func (f *Fetcher) expectedSum(ctx context.Context, checksumsURL, asset string) (string, error) {
	body, err := f.get(ctx, checksumsURL)
	if err != nil {
		return "", err
	}
	defer body.Close()
	content, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("fetch: read checksums: %w", err)
	}
	return parseChecksums(string(content), asset)
}

func (f *Fetcher) download(ctx context.Context, assetURL, dir string) (string, error) {
	body, err := f.get(ctx, assetURL)
	if err != nil {
		return "", err
	}
	defer body.Close()

	tmp, err := os.CreateTemp(dir, ".download-*.zip")
	if err != nil {
		return "", fmt.Errorf("fetch: temp file: %w", err)
	}
	_, copyErr := io.Copy(tmp, body)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("fetch: download %q: %w", assetURL, copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("fetch: close download: %w", closeErr)
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

// parseChecksums finds asset's SHA-256 in sha256sum-format content: each line
// is "<hex>  <name>" (or "<hex> *<name>" for binary mode). First match wins,
// which assumes the checksums file is generated fresh per release.
func parseChecksums(content, asset string) (string, error) {
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if filepath.Base(strings.TrimPrefix(fields[1], "*")) == asset {
			return strings.ToLower(fields[0]), nil
		}
	}
	return "", fmt.Errorf("%w: %q", ErrChecksumMissing, asset)
}

func sha256File(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", fmt.Errorf("fetch: open %q: %w", name, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("fetch: hash %q: %w", name, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
