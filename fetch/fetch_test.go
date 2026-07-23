package fetch

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
)

var testIdentity = codeidentity.CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.FuseT"}

type fakeVerifier struct {
	err    error
	calls  int
	gotApp string
	gotReq string
}

func (v *fakeVerifier) Verify(_ context.Context, appPath, req string) error {
	v.calls++
	v.gotApp = appPath
	v.gotReq = req
	return v.err
}

// appZip builds an in-memory zip holding <appName>.app with an Info.plist and
// an executable inner Mach-O placeholder.
func appZip(t *testing.T, appName string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	files := []struct {
		name string
		mode os.FileMode
		body string
	}{
		{appName + ".app/Contents/Info.plist", 0o644, "<plist/>"},
		{appName + ".app/Contents/MacOS/" + appName, 0o755, "#!/bin/echo\n"},
	}
	for _, f := range files {
		hdr := &zip.FileHeader{Name: f.name, Method: zip.Deflate}
		hdr.SetMode(f.mode)
		fw, err := w.CreateHeader(hdr)
		if err != nil {
			t.Fatalf("zip create %q: %v", f.name, err)
		}
		if _, err := fw.Write([]byte(f.body)); err != nil {
			t.Fatalf("zip write %q: %v", f.name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// releaseServer serves an asset zip and a checksums file, counting hits.
func releaseServer(t *testing.T, zipBytes []byte, checksums string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/FuseT.zip", func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write(zipBytes)
	})
	mux.HandleFunc("/checksums.txt", func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = fmt.Fprint(w, checksums)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &hits
}

func config(srv *httptest.Server, dir string) Config {
	return Config{
		AssetURL:     srv.URL + "/FuseT.zip",
		ChecksumsURL: srv.URL + "/checksums.txt",
		Dir:          dir,
		AppName:      "FuseT",
		Identity:     testIdentity,
	}
}

func TestFetchHappyPath(t *testing.T) {
	z := appZip(t, "FuseT")
	checksums := fmt.Sprintf("%s  FuseT.zip\n%s  other.zip\n", sha256Hex(z), sha256Hex([]byte("x")))
	srv, hits := releaseServer(t, z, checksums)
	dir := t.TempDir()
	v := &fakeVerifier{}
	f := &Fetcher{Client: srv.Client(), Verifier: v}

	got, err := f.Fetch(context.Background(), config(srv, dir))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	want := bundle.AppPath(dir, "FuseT")
	if got != want {
		t.Fatalf("Fetch = %q, want %q", got, want)
	}
	if _, err := os.Stat(bundle.ExePath(want, "FuseT")); err != nil {
		t.Fatalf("inner exe not installed: %v", err)
	}
	dr, _ := testIdentity.DRString()
	if v.gotReq != dr {
		t.Fatalf("verifier requirement = %q, want %q", v.gotReq, dr)
	}
	if filepath.Base(v.gotApp) != "FuseT.app" {
		t.Fatalf("verifier app = %q, want a FuseT.app path", v.gotApp)
	}
	if hits.Load() != 2 {
		t.Fatalf("server hits = %d, want 2", hits.Load())
	}
	// No staging or download temp files left behind.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("dir has %d entries, want 1 (the .app)", len(entries))
	}
}

func TestFetchChecksumMismatch(t *testing.T) {
	z := appZip(t, "FuseT")
	checksums := fmt.Sprintf("%s  FuseT.zip\n", sha256Hex([]byte("tampered")))
	srv, _ := releaseServer(t, z, checksums)
	dir := t.TempDir()
	f := &Fetcher{Client: srv.Client(), Verifier: &fakeVerifier{}}

	_, err := f.Fetch(context.Background(), config(srv, dir))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("err = %v, want ErrChecksumMismatch", err)
	}
	assertNotInstalled(t, dir)
}

func TestFetchChecksumMissing(t *testing.T) {
	z := appZip(t, "FuseT")
	checksums := fmt.Sprintf("%s  something-else.zip\n", sha256Hex(z))
	srv, _ := releaseServer(t, z, checksums)
	dir := t.TempDir()
	f := &Fetcher{Client: srv.Client(), Verifier: &fakeVerifier{}}

	_, err := f.Fetch(context.Background(), config(srv, dir))
	if !errors.Is(err, ErrChecksumMissing) {
		t.Fatalf("err = %v, want ErrChecksumMissing", err)
	}
	assertNotInstalled(t, dir)
}

func TestFetchDRMismatchRejectedAndCleaned(t *testing.T) {
	z := appZip(t, "FuseT")
	checksums := fmt.Sprintf("%s  FuseT.zip\n", sha256Hex(z))
	srv, _ := releaseServer(t, z, checksums)
	dir := t.TempDir()
	v := &fakeVerifier{err: errors.New("code failed to satisfy requirement")}
	f := &Fetcher{Client: srv.Client(), Verifier: v}

	_, err := f.Fetch(context.Background(), config(srv, dir))
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("err = %v, want ErrUntrusted", err)
	}
	if v.calls != 1 {
		t.Fatalf("verifier calls = %d, want 1", v.calls)
	}
	assertNotInstalled(t, dir)
	// A tampered bundle leaves nothing staged behind.
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("dir not clean after rejection: %v", entries)
	}
}

func TestFetchIdempotentSkipsDownload(t *testing.T) {
	z := appZip(t, "FuseT")
	checksums := fmt.Sprintf("%s  FuseT.zip\n", sha256Hex(z))
	srv, hits := releaseServer(t, z, checksums)
	dir := t.TempDir()
	appPath := bundle.AppPath(dir, "FuseT")
	if err := os.MkdirAll(filepath.Join(appPath, "Contents", "MacOS"), 0o755); err != nil {
		t.Fatal(err)
	}
	v := &fakeVerifier{}
	f := &Fetcher{Client: srv.Client(), Verifier: v}

	got, err := f.Fetch(context.Background(), config(srv, dir))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got != appPath {
		t.Fatalf("Fetch = %q, want %q", got, appPath)
	}
	if hits.Load() != 0 {
		t.Fatalf("server hits = %d, want 0 (idempotent skip)", hits.Load())
	}
	if v.calls != 1 {
		t.Fatalf("verifier calls = %d, want 1", v.calls)
	}
}

func TestFetchReinstallsWhenExistingFailsDR(t *testing.T) {
	z := appZip(t, "FuseT")
	checksums := fmt.Sprintf("%s  FuseT.zip\n", sha256Hex(z))
	srv, hits := releaseServer(t, z, checksums)
	dir := t.TempDir()
	appPath := bundle.AppPath(dir, "FuseT")
	if err := os.MkdirAll(appPath, 0o755); err != nil {
		t.Fatal(err)
	}
	// First Verify (on the stale bundle) fails; the reinstalled one passes.
	v := &sequenceVerifier{errs: []error{errors.New("stale"), nil}}
	f := &Fetcher{Client: srv.Client(), Verifier: v}

	got, err := f.Fetch(context.Background(), config(srv, dir))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got != appPath {
		t.Fatalf("Fetch = %q, want %q", got, appPath)
	}
	if hits.Load() != 2 {
		t.Fatalf("server hits = %d, want 2 (re-download)", hits.Load())
	}
	if _, err := os.Stat(bundle.ExePath(appPath, "FuseT")); err != nil {
		t.Fatalf("reinstalled exe missing: %v", err)
	}
}

func TestParseChecksums(t *testing.T) {
	tests := []struct {
		name    string
		content string
		asset   string
		want    string
		wantErr error
	}{
		{"two-space", "abc123  FuseT.zip\n", "FuseT.zip", "abc123", nil},
		{"binary-star", "ABC123 *FuseT.zip\n", "FuseT.zip", "abc123", nil},
		{"lowercased", "AbCdEf  FuseT.zip", "FuseT.zip", "abcdef", nil},
		{"picks-right-line", "aaa  a.zip\nbbb  FuseT.zip\n", "FuseT.zip", "bbb", nil},
		{"missing", "aaa  other.zip\n", "FuseT.zip", "", ErrChecksumMissing},
		{"blank-lines-ignored", "\n  \nccc  FuseT.zip\n", "FuseT.zip", "ccc", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseChecksums(tt.content, tt.asset)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseChecksums = %q, want %q", got, tt.want)
			}
		})
	}
}

type sequenceVerifier struct {
	errs []error
	n    int
}

func (v *sequenceVerifier) Verify(context.Context, string, string) error {
	err := v.errs[v.n]
	v.n++
	return err
}

func assertNotInstalled(t *testing.T, dir string) {
	t.Helper()
	if _, err := os.Stat(bundle.AppPath(dir, "FuseT")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bundle should not be installed, stat err = %v", err)
	}
}
