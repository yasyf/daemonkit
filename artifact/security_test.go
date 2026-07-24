package artifact

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "script")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeTemp(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "archive")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// #1 — a platform Path that escapes the digest dir is refused, closing both the
// out-of-store write and the fast-path integrity bypass.
func TestResolveReleaseBinaryRejectsPathTraversal(t *testing.T) {
	store := Store{Root: t.TempDir()}
	desc := releaseDescriptor(t, PlatformEntry{
		Size: 5, Hash: "sha256", Digest: strings.Repeat("a", 64), Format: Raw, Path: "../../escape",
	})
	if _, err := store.Resolve(context.Background(), desc); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("Resolve() = %v, want ErrUnsafeArchive", err)
	}
}

// #1 — a non-hex digest can never name a directory outside the shard layout.
func TestValidateRejectsNonHexDigest(t *testing.T) {
	desc := &Descriptor{
		Schema: 1, Name: "t", Kind: ReleaseBinary, Version: VersionSource{Static: "1"},
		Platforms: map[Platform]PlatformEntry{
			"macos-aarch64": {
				Size: 5, Hash: "sha256", Digest: "../" + strings.Repeat("a", 61), Format: Raw, Path: "tool",
				Providers: []Provider{{Type: GitHubRelease, Repo: "o/r", Tag: "v1", Name: "a"}},
			},
		},
	}
	if err := desc.Validate(); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatalf("Validate() = %v, want ErrInvalidDescriptor", err)
	}
}

// #2 — a partial cache entry (entrypoint present, meta.json absent) is not a hit;
// Resolve re-materializes and replaces it with verified content.
func TestResolveReleaseBinaryIgnoresPartialWithoutMeta(t *testing.T) {
	content := []byte("real binary\n")
	store, opt, hits := serveContent(t, content)
	digest := sha256hex(content)
	digestDir := filepath.Join(store.CacheDir(), digest[:2], digest)
	if err := os.MkdirAll(digestDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(digestDir, "tool"), []byte("POISON"), 0o755); err != nil {
		t.Fatal(err)
	}
	desc := releaseDescriptor(t, PlatformEntry{
		Size: int64(len(content)), Hash: "sha256", Digest: digest, Format: Raw, Path: "tool",
	})

	path, err := store.Resolve(context.Background(), desc, opt)
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if hits.Load() == 0 {
		t.Fatal("partial without meta.json was served without re-materializing")
	}
	if got, _ := os.ReadFile(path); string(got) != string(content) {
		t.Fatalf("served poisoned content: %q", got)
	}
	if !regular(filepath.Join(digestDir, "meta.json")) {
		t.Fatal("meta.json missing after re-materialization")
	}
}

// #3 — a uv install that creates the entrypoint then fails is not cached as
// success; a later good install materializes cleanly.
func TestResolvePythonToolFailedInstallNotReused(t *testing.T) {
	store := Store{Root: t.TempDir()}
	desc := &Descriptor{
		Schema: 1, Name: "capt-hook", Kind: PythonTool, Version: VersionSource{Static: "1.2.3"},
		Tool: &ToolSpec{Dist: "capt-hook", Entrypoint: "capt-hookd"},
	}
	failUV := writeScript(t, "#!/bin/sh\n"+
		"real=\"$UV_TOOL_DIR/env/bin\"\n"+
		"mkdir -p \"$real\" \"$UV_TOOL_BIN_DIR\"\n"+
		"printf '#!/bin/sh\\n' > \"$real/capt-hookd\"; chmod +x \"$real/capt-hookd\"\n"+
		"ln -sf \"$real/capt-hookd\" \"$UV_TOOL_BIN_DIR/capt-hookd\"\n"+
		"exit 1\n")
	if _, err := store.Resolve(context.Background(), desc, WithUV(failUV)); err == nil {
		t.Fatal("failed uv install returned success")
	}

	okUV := fakeUV(t, filepath.Join(t.TempDir(), "calls"))
	path, err := store.Resolve(context.Background(), desc, WithUV(okUV))
	if err != nil {
		t.Fatalf("retry after failed install = %v", err)
	}
	if !strings.Contains(path, filepath.Join("capt-hook", "1.2.3")) {
		t.Fatalf("path %q is not under the version store", path)
	}
}

// #4 — a dynamic/static version (or dist) that escapes the tool store is refused
// before uv runs.
func TestResolvePythonToolRejectsVersionTraversal(t *testing.T) {
	store := Store{Root: t.TempDir()}
	desc := &Descriptor{
		Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{Static: "../../../evil"},
		Tool: &ToolSpec{Dist: "t", Entrypoint: "t"},
	}
	if _, err := store.Resolve(context.Background(), desc, WithUV("/bin/true")); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("Resolve() = %v, want ErrUnsafeArchive", err)
	}
}

// #1 family — a signed-app exec that escapes the bundle is refused.
func TestResolveSignedAppRejectsExecTraversal(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	desc := &Descriptor{
		Schema: 1, Name: "Captain Hook", Kind: SignedApp, Version: VersionSource{Static: "12.15.3"},
		App: &AppSpec{Dir: dir, AppName: "Captain Hook", Exec: "../../../../etc/passwd", Cask: "captain-hook"},
	}
	if _, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), desc); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("Resolve() = %v, want ErrUnsafeArchive", err)
	}
}

// #5 — a hung server does not hold the resolver past the context deadline.
func TestResolveReleaseBinaryHungServerRespectsDeadline(t *testing.T) {
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { <-block }))
	defer server.Close()
	defer close(block)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("x")
	store := Store{Root: t.TempDir()}
	opt := WithHTTPClient(&http.Client{Transport: redirectTransport{base: base}})
	desc := releaseDescriptor(t, PlatformEntry{
		Size: int64(len(content)), Hash: "sha256", Digest: sha256hex(content), Format: Raw, Path: "tool",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := store.Resolve(ctx, desc, opt); err == nil {
		t.Fatal("hung server returned success")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Resolve ignored the context deadline (%v)", elapsed)
	}
}

// #6 — extraction is bounded by a decompression budget.
func TestExtractTarGzEnforcesBudget(t *testing.T) {
	src := writeTemp(t, tarGzBytes(t, "tool", make([]byte, 1000)))
	if err := extractTarGz(src, t.TempDir(), 500); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("over-budget extract = %v, want ErrUnsafeArchive", err)
	}
	if err := extractTarGz(src, t.TempDir(), 2000); err != nil {
		t.Fatalf("within-budget extract = %v", err)
	}
}

func TestDecompressionBudget(t *testing.T) {
	if got := decompressionBudget(1); got != minDecompressionBudget {
		t.Errorf("budget(1) = %d, want floor %d", got, minDecompressionBudget)
	}
	if got := decompressionBudget(1 << 30); got != maxDecompressionBudget {
		t.Errorf("budget(1GiB) = %d, want ceiling %d", got, maxDecompressionBudget)
	}
	if got := decompressionBudget(1 << 20); got != (1<<20)*decompressionRatio {
		t.Errorf("budget(1MiB) = %d, want %d", got, (1<<20)*decompressionRatio)
	}
}
