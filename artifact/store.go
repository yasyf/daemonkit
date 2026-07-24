package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
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
	home, err := os.UserHomeDir()
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
	handle, err := (proc.FileLockSpec{Path: lockPath, Mode: proc.FileLockExclusive, Deadline: materializeLockDeadline}).Acquire(ctx)
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

// WithUV overrides the uv executable used by the python-tool backend.
func WithUV(path string) Option { return func(o *options) { o.uv = path } }

type cacheMeta struct {
	Name      string    `json:"name"`
	Tag       string    `json:"tag"`
	Digest    string    `json:"digest"`
	FetchedAt time.Time `json:"fetched_at"`
}

func writeCacheMeta(digestDir, name string, entry PlatformEntry) error {
	data, err := json.Marshal(cacheMeta{
		Name:      name,
		Tag:       entry.Providers[0].Tag,
		Digest:    entry.Digest,
		FetchedAt: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("artifact: encode cache meta: %w", err)
	}
	return daemon.WriteFileDurable(filepath.Join(digestDir, "meta.json"), append(data, '\n'), 0o600)
}

// syncTree fsyncs every file and directory under root, skipping symlinks, so a
// materialized tree survives a crash before its completion marker or its rename
// into place. Walking and opening through an os.Root keeps every access inside
// root even if a component is a symlink.
func syncTree(root string) error {
	dir, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("artifact: open tree root: %w", err)
	}
	defer dir.Close()
	return fs.WalkDir(dir.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		file, err := dir.Open(path)
		if err != nil {
			return err
		}
		return errors.Join(file.Sync(), file.Close())
	})
}

func download(ctx context.Context, client *http.Client, url, token string, dst *os.File) (digest string, size int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, materializeLockDeadline)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", 0, fmt.Errorf("artifact: build request: %w", err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return "", 0, fmt.Errorf("artifact: download %q: %w", url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("artifact: download %q: status %s", url, response.Status)
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(dst, hash), response.Body)
	syncErr := dst.Sync()
	closeErr := dst.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		return "", 0, fmt.Errorf("artifact: stream download: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), written, nil
}
