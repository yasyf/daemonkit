package artifact

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

type redirectTransport struct{ base *url.URL }

func (rt redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = rt.base.Scheme
	clone.URL.Host = rt.base.Host
	return http.DefaultTransport.RoundTrip(clone)
}

func serveContent(t *testing.T, content []byte) (Store, Option, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write(content)
	}))
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return Store{Root: t.TempDir()}, WithHTTPClient(&http.Client{Transport: redirectTransport{base: base}}), &hits
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func releaseDescriptor(t *testing.T, entry PlatformEntry) *Descriptor {
	t.Helper()
	platform, err := CurrentPlatform()
	if err != nil {
		t.Fatal(err)
	}
	entry.Providers = []Provider{{Type: GitHubRelease, Repo: "yasyf/tool", Tag: "v1.0.0", Name: "tool_asset"}}
	return &Descriptor{
		Schema: 1, Name: "tool", Kind: ReleaseBinary,
		Version:   VersionSource{Static: "1.0.0"},
		Platforms: map[Platform]PlatformEntry{platform: entry},
	}
}

func tarGzBytes(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(tw.Close(), gz.Close()); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func zipBytes(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	header := &zip.FileHeader{Name: name, Method: zip.Deflate}
	header.SetMode(0o755)
	w, err := zw.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestResolveReleaseBinaryRawCachesAndIsIdempotent(t *testing.T) {
	content := []byte("#!/bin/sh\necho hi\n")
	store, opt, hits := serveContent(t, content)
	desc := releaseDescriptor(t, PlatformEntry{
		Size: int64(len(content)), Hash: "sha256", Digest: sha256hex(content), Format: Raw, Path: "tool",
	})

	path, err := store.Resolve(context.Background(), desc, opt)
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != string(content) {
		t.Fatalf("materialized content = %q, want %q", got, content)
	}
	if info, _ := os.Stat(path); info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("entrypoint mode = %v, want executable", info.Mode())
	}
	if !regular(filepath.Join(filepath.Dir(path), "meta.json")) {
		t.Fatal("missing meta.json sibling")
	}
	want := filepath.Join(store.CacheDir(), sha256hex(content)[:2], sha256hex(content), "tool")
	if path != want {
		t.Fatalf("cache path = %q, want %q", path, want)
	}

	path2, err := store.Resolve(context.Background(), desc, opt)
	if err != nil || path2 != path {
		t.Fatalf("second Resolve = %q, %v; want %q, nil", path2, err, path)
	}
	if hits.Load() != 1 {
		t.Fatalf("download hits = %d, want 1 (second resolve is a cache hit)", hits.Load())
	}
}

func TestResolveReleaseBinaryChecksumMismatch(t *testing.T) {
	served := []byte("aaaaaa")
	other := []byte("bbbbbb") // same length, different bytes
	store, opt, _ := serveContent(t, served)
	desc := releaseDescriptor(t, PlatformEntry{
		Size: int64(len(served)), Hash: "sha256", Digest: sha256hex(other), Format: Raw, Path: "tool",
	})
	if _, err := store.Resolve(context.Background(), desc, opt); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Resolve() = %v, want ErrChecksumMismatch", err)
	}
}

func TestResolveReleaseBinarySizeMismatch(t *testing.T) {
	served := []byte("aaaaaa")
	store, opt, _ := serveContent(t, served)
	desc := releaseDescriptor(t, PlatformEntry{
		Size: int64(len(served)) + 1, Hash: "sha256", Digest: sha256hex(served), Format: Raw, Path: "tool",
	})
	if _, err := store.Resolve(context.Background(), desc, opt); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("Resolve() = %v, want ErrSizeMismatch", err)
	}
}

func TestResolveReleaseBinaryTarGz(t *testing.T) {
	content := []byte("#!/bin/sh\necho archived\n")
	archive := tarGzBytes(t, "tool", content)
	store, opt, _ := serveContent(t, archive)
	desc := releaseDescriptor(t, PlatformEntry{
		Size: int64(len(archive)), Hash: "sha256", Digest: sha256hex(archive), Format: TarGz, Path: "tool",
	})
	path, err := store.Resolve(context.Background(), desc, opt)
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != string(content) {
		t.Fatalf("extracted content = %q, want %q", got, content)
	}
}

func TestResolveReleaseBinaryZip(t *testing.T) {
	content := []byte("#!/bin/sh\necho zipped\n")
	archive := zipBytes(t, "tool", content)
	store, opt, _ := serveContent(t, archive)
	desc := releaseDescriptor(t, PlatformEntry{
		Size: int64(len(archive)), Hash: "sha256", Digest: sha256hex(archive), Format: Zip, Path: "tool",
	})
	path, err := store.Resolve(context.Background(), desc, opt)
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != string(content) {
		t.Fatalf("extracted content = %q, want %q", got, content)
	}
}

func TestResolveReleaseBinaryUnsupportedPlatform(t *testing.T) {
	store, opt, _ := serveContent(t, []byte("x"))
	desc := &Descriptor{
		Schema: 1, Name: "tool", Kind: ReleaseBinary, Version: VersionSource{Static: "1.0.0"},
		Platforms: map[Platform]PlatformEntry{
			"solaris-sparc": {
				Size: 1, Hash: "sha256", Digest: strings.Repeat("a", 64), Path: "tool",
				Providers: []Provider{{Type: GitHubRelease, Repo: "o/r", Tag: "v1", Name: "a"}},
			},
		},
	}
	if _, err := store.Resolve(context.Background(), desc, opt); !errors.Is(err, ErrUnsupportedPlatform) {
		t.Fatalf("Resolve() = %v, want ErrUnsupportedPlatform", err)
	}
}
