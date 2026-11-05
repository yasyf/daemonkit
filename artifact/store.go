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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/ghrelease"
	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/internal/realhome"
)

const materializeLockDeadline = 5 * time.Minute

// defaultDownloadClient bounds a stalled download so it cannot hold the
// per-artifact lock indefinitely: a host that accepts a connection but never
// sends response headers fails in 30s, while a host that sends headers and then
// drips the body is bounded by the download context (materializeLockDeadline) —
// so a slow host still holds the lock for that whole window, but never longer.
var defaultDownloadClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	},
}

// Store is the on-disk root (~/.daemonkit) holding the content-addressed release
// cache, the version-addressed python-tool store, and per-artifact locks.
type Store struct {
	Root string
}

// DefaultStore returns a Store rooted at ~/.daemonkit.
func DefaultStore() (Store, error) {
	home, err := realhome.Dir()
	if err != nil {
		return Store{}, fmt.Errorf("artifact: resolve home directory: %w", err)
	}
	return Store{Root: filepath.Join(home, ".daemonkit")}, nil
}

// CacheDir is the content-addressed release cache root (Root/cache).
func (s Store) CacheDir() string { return filepath.Join(s.Root, "cache") }

// ToolsDir is the version-addressed python-tool store root (Root/tools).
func (s Store) ToolsDir() string { return filepath.Join(s.Root, "tools") }

func (s Store) locksDir() string { return filepath.Join(s.Root, "locks") }

func (s Store) validate() error {
	if !filepath.IsAbs(s.Root) || filepath.Clean(s.Root) != s.Root {
		return fmt.Errorf("artifact: store root %q is not exact and absolute", s.Root)
	}
	return nil
}

// Resolve materializes desc for the current platform and returns the absolute
// path to its executable entrypoint. It is idempotent: an already-materialized
// artifact is verified and returned without refetching. Resolve never consults a
// repository's latest release — the descriptor pins the exact version.
func (s Store) Resolve(ctx context.Context, desc *Descriptor, opts ...Option) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if err := desc.Validate(); err != nil {
		return "", err
	}
	o := buildOptions(opts)
	version, err := desc.ResolveVersion(ctx)
	if err != nil {
		return "", err
	}
	switch desc.Kind {
	case ReleaseBinary:
		return s.resolveReleaseBinary(ctx, desc, o)
	case PythonTool:
		return s.resolvePythonTool(ctx, desc, version, o)
	case SignedApp:
		return s.resolveSignedApp(ctx, desc, version, o)
	default:
		return "", fmt.Errorf("%w: unknown kind %q", ErrInvalidDescriptor, desc.Kind)
	}
}

// Fetch materializes desc without returning a path, for SessionStart pre-warm.
func (s Store) Fetch(ctx context.Context, desc *Descriptor, opts ...Option) error {
	_, err := s.Resolve(ctx, desc, opts...)
	return err
}

func (s Store) withLock(ctx context.Context, key string, fn func() error) error {
	if err := os.MkdirAll(s.locksDir(), 0o700); err != nil {
		return fmt.Errorf("artifact: create locks directory: %w", err)
	}
	sum := sha256.Sum256([]byte(key))
	lockPath := filepath.Join(s.locksDir(), hex.EncodeToString(sum[:16])+".lock")
	handle, err := (flock.Spec{Path: lockPath, Mode: flock.Exclusive, Deadline: materializeLockDeadline}).Acquire(ctx)
	if err != nil {
		return fmt.Errorf("artifact: lock %q: %w", key, err)
	}
	defer handle.Close()
	return fn()
}

// Option configures a resolution.
type Option func(*options)

type options struct {
	httpClient  *http.Client
	githubToken string
	tokenSource func(context.Context) string
	uv          string
}

func buildOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func (o options) http() *http.Client {
	if o.httpClient != nil {
		return o.httpClient
	}
	return defaultDownloadClient
}

func (o options) apiToken(ctx context.Context) string {
	if o.githubToken != "" || o.tokenSource == nil {
		return o.githubToken
	}
	return o.tokenSource(ctx)
}

func (o options) uvExecutable() string {
	if o.uv != "" {
		return o.uv
	}
	return "uv"
}

// WithHTTPClient overrides the HTTP client used for downloads.
func WithHTTPClient(c *http.Client) Option { return func(o *options) { o.httpClient = c } }

// WithGitHubToken authenticates github-release downloads.
func WithGitHubToken(token string) Option { return func(o *options) { o.githubToken = token } }

// WithGitHubTokenSource supplies a token for the GitHub API download route,
// read only when the release download host has failed and only when no
// WithGitHubToken token is set. An empty token sends the request
// unauthenticated.
func WithGitHubTokenSource(source func(context.Context) string) Option {
	return func(o *options) { o.tokenSource = source }
}

// WithUV overrides the uv executable used by the python-tool backend.
func WithUV(path string) Option { return func(o *options) { o.uv = path } }

type cacheMeta struct {
	Name      string    `json:"name"`
	Tag       string    `json:"tag"`
	Digest    string    `json:"digest"`
	Source    string    `json:"source,omitempty"`
	FetchedAt time.Time `json:"fetched_at"`
}

func writeCacheMeta(digestDir string, meta cacheMeta) error {
	meta.FetchedAt = time.Now().UTC()
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("artifact: encode cache meta: %w", err)
	}
	return durable.WriteFile(filepath.Join(digestDir, "meta.json"), append(data, '\n'), 0o600)
}

func fetchAsset(parent context.Context, provider Provider, o options, path string) (digest string, size int64, err error) {
	ctx, cancel := context.WithTimeout(parent, materializeLockDeadline)
	defer cancel()
	direct, err := provider.URL()
	if err != nil {
		return "", 0, err
	}
	digest, size, directErr := retryDownload(ctx, func() (string, int64, error) {
		return download(ctx, o.http(), direct, o.githubToken, "", path)
	})
	if directErr == nil || parent.Err() != nil {
		return digest, size, directErr
	}
	token := o.apiToken(ctx)
	digest, size, apiErr := retryDownload(ctx, func() (string, int64, error) {
		release, err := (ghrelease.Client{HTTPClient: o.http(), Token: token}).Tagged(ctx, provider.Repo, provider.Tag)
		if err != nil {
			return "", 0, retryable{err}
		}
		for _, asset := range release.Assets {
			if asset.Name == provider.Name {
				return download(ctx, o.http(), asset.APIURL, token, "application/octet-stream", path)
			}
		}
		return "", 0, fmt.Errorf("artifact: release %s of %q has no asset %q", provider.Tag, provider.Repo, provider.Name)
	})
	if apiErr != nil {
		return "", 0, fmt.Errorf("%w: %w; through the GitHub API: %w", ErrDownload, directErr, apiErr)
	}
	slog.Warn("artifact: release download failed, fetched through the GitHub API", "asset", provider.Name, "error", directErr)
	return digest, size, nil
}

var downloadBackoff = time.Second

const downloadAttempts = 3

type retryable struct{ error }

func (r retryable) Unwrap() error { return r.error }

func retryDownload(ctx context.Context, attempt func() (string, int64, error)) (string, int64, error) {
	for i := 1; ; i++ {
		digest, size, err := attempt()
		if err == nil {
			return digest, size, nil
		}
		if i == downloadAttempts || !errors.As(err, new(retryable)) || ctx.Err() != nil {
			return "", 0, fmt.Errorf("attempt %d: %w", i, err)
		}
		select {
		case <-ctx.Done():
			return "", 0, fmt.Errorf("attempt %d: %w; %w", i, err, ctx.Err())
		case <-time.After(downloadBackoff << (i - 1)):
		}
	}
}

func download(ctx context.Context, client *http.Client, url, token, accept, path string) (digest string, size int64, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", 0, fmt.Errorf("artifact: build request: %w", err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if accept != "" {
		request.Header.Set("Accept", accept)
	}
	response, err := client.Do(request)
	if err != nil {
		return "", 0, retryable{fmt.Errorf("artifact: download %q: %w", url, err)}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf("artifact: download %q: status %s", url, response.Status)
		if response.StatusCode >= http.StatusInternalServerError || response.StatusCode == http.StatusTooManyRequests {
			return "", 0, retryable{err}
		}
		return "", 0, err
	}
	dst, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return "", 0, fmt.Errorf("artifact: open download temp: %w", err)
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(dst, hash), response.Body)
	syncErr := dst.Sync()
	closeErr := dst.Close()
	if copyErr != nil {
		return "", 0, retryable{fmt.Errorf("artifact: stream download %q: %w", url, copyErr)}
	}
	if err := errors.Join(syncErr, closeErr); err != nil {
		return "", 0, fmt.Errorf("artifact: stream download: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), written, nil
}
