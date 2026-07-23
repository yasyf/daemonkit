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
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
	"golang.org/x/sys/unix"
)

var testIdentity = codeidentity.CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.FuseT"}

type fakeVerifier struct {
	mu     sync.Mutex
	err    error
	calls  int
	gotApp string
	gotReq string
}

func (v *fakeVerifier) Verify(_ context.Context, appPath, req string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	v.gotApp = appPath
	v.gotReq = req
	return v.err
}

func (v *fakeVerifier) callCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.calls
}

func appZip(t *testing.T, appName, version, payload string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	plist := fmt.Sprintf(`<plist><dict><key>CFBundleShortVersionString</key><string>%s</string></dict></plist>`, version)
	files := []struct {
		name string
		mode os.FileMode
		body string
	}{
		{appName + ".app/Contents/Info.plist", 0o644, plist},
		{appName + ".app/Contents/MacOS/" + appName, 0o755, payload},
	}
	for _, file := range files {
		hdr := &zip.FileHeader{Name: file.name, Method: zip.Deflate}
		hdr.SetMode(file.mode)
		writer, err := w.CreateHeader(hdr)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte(file.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func digestFor(t *testing.T, data []byte) SHA256 {
	t.Helper()
	digest, err := ParseSHA256(sha256Hex(data))
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func releaseServer(t *testing.T, zipBytes []byte) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write(zipBytes)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func config(t *testing.T, srv *httptest.Server, dir, version string, zipBytes []byte) Config {
	t.Helper()
	realDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return Config{
		Release: Release{Version: version, URL: srv.URL + "/FuseT.zip", SHA256: digestFor(t, zipBytes)},
		Dir:     realDir, AppName: "FuseT", Identity: testIdentity,
	}
}

func newFetcher(srv *httptest.Server, verifier Verifier) *Fetcher {
	return &Fetcher{
		Client: srv.Client(), Verifier: verifier,
		exchange: exchangePaths, publishFirst: publishExclusive,
	}
}

func TestFetchPublishesExactRealBundle(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.2.3", "new")
	srv, hits := releaseServer(t, zipBytes)
	dir := t.TempDir()
	verifier := &fakeVerifier{}
	cfg := config(t, srv, dir, "1.2.3", zipBytes)

	got, err := newFetcher(srv, verifier).Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := bundle.AppPath(cfg.Dir, "FuseT")
	if got.Path != wantPath || got.Release != cfg.Release {
		t.Fatalf("installation = %#v, want path %q release %#v", got, wantPath, cfg.Release)
	}
	info, err := os.Lstat(got.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("canonical mode = %v, want real directory", info.Mode())
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, want 1", hits.Load())
	}
	if verifier.callCount() != 2 {
		t.Fatalf("verifier calls = %d, want staged and published verification", verifier.callCount())
	}
	receiptPath := pathsFor(cfg).receipt
	current, err := readCurrent(pathsFor(cfg))
	if err != nil || current == nil {
		t.Fatalf("receipt %q: %#v, %v", receiptPath, current, err)
	}
	if current.Version != "1.2.3" || current.URL != cfg.Release.URL || current.SHA256 != cfg.Release.SHA256.String() {
		t.Fatalf("receipt = %#v, want exact release", current)
	}
	if current.Identity != receiptIdentity || current.Schema != 1 || current.Fingerprint != receiptFingerprint {
		t.Fatalf("receipt identity = %q/%d/%q", current.Identity, current.Schema, current.Fingerprint)
	}
}

func TestFetchRejectsChecksumMismatch(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	dir := t.TempDir()
	cfg := config(t, srv, dir, "1.0.0", []byte("different"))

	_, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("error = %v, want ErrChecksumMismatch", err)
	}
	assertCanonicalAbsent(t, cfg)
}

func TestFetchRejectsVersionMismatch(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "2.0.0", zipBytes)

	_, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("error = %v, want ErrVersionMismatch", err)
	}
	assertCanonicalAbsent(t, cfg)
}

func TestFetchRejectsUntrustedBundle(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)

	_, err := newFetcher(srv, &fakeVerifier{err: fmt.Errorf("%w: rejected", ErrUntrusted)}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("error = %v, want ErrUntrusted", err)
	}
	assertCanonicalAbsent(t, cfg)
}

func TestFetchRejectsTopLevelAppSymlink(t *testing.T) {
	zipPath := writeZip(t, func(writer *zip.Writer) {
		plist := `<plist><dict><key>CFBundleShortVersionString</key><string>1.0.0</string></dict></plist>`
		addFile(t, writer, "Real.app/Contents/Info.plist", 0o644, plist)
		addFile(t, writer, "Real.app/Contents/MacOS/FuseT", 0o755, "payload")
		header := &zip.FileHeader{Name: "FuseT.app"}
		header.SetMode(os.ModeSymlink | 0o777)
		entry, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write([]byte("Real.app")); err != nil {
			t.Fatal(err)
		}
	})
	zipBytes, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	_, err = newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("error = %v, want ErrInstallConflict", err)
	}
	assertCanonicalAbsent(t, cfg)
}

func TestFetchReusesOnlyExactReceipt(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "one")
	srv, hits := releaseServer(t, zipBytes)
	dir := t.TempDir()
	cfg := config(t, srv, dir, "1.0.0", zipBytes)
	verifier := &fakeVerifier{}
	fetcher := newFetcher(srv, verifier)
	if _, err := fetcher.Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := fetcher.Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, want 1", hits.Load())
	}
	if verifier.callCount() != 3 {
		t.Fatalf("verifier calls = %d, want install twice and exact reuse once", verifier.callCount())
	}
}

func TestFetchSameVersionChangedURLAndSHAReconciles(t *testing.T) {
	dir := t.TempDir()
	firstZip := appZip(t, "FuseT", "1.0.0", "first")
	firstServer, firstHits := releaseServer(t, firstZip)
	firstCfg := config(t, firstServer, dir, "1.0.0", firstZip)
	if _, err := newFetcher(firstServer, &fakeVerifier{}).Fetch(context.Background(), firstCfg); err != nil {
		t.Fatal(err)
	}
	secondZip := appZip(t, "FuseT", "1.0.0", "second")
	secondServer, secondHits := releaseServer(t, secondZip)
	secondCfg := config(t, secondServer, dir, "1.0.0", secondZip)
	if _, err := newFetcher(secondServer, &fakeVerifier{}).Fetch(context.Background(), secondCfg); err != nil {
		t.Fatal(err)
	}
	if firstHits.Load() != 1 || secondHits.Load() != 1 {
		t.Fatalf("asset hits = (%d, %d), want exact identity reconciliation", firstHits.Load(), secondHits.Load())
	}
	body, err := os.ReadFile(bundle.ExePath(bundle.AppPath(secondCfg.Dir, "FuseT"), "FuseT"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "second" {
		t.Fatalf("canonical payload = %q, want second", body)
	}
}

func TestFetchAtomicallyReplacesDifferentRelease(t *testing.T) {
	dir := t.TempDir()
	oldZip := appZip(t, "FuseT", "1.0.0", "old")
	oldServer, _ := releaseServer(t, oldZip)
	oldCfg := config(t, oldServer, dir, "1.0.0", oldZip)
	if _, err := newFetcher(oldServer, &fakeVerifier{}).Fetch(context.Background(), oldCfg); err != nil {
		t.Fatal(err)
	}
	newZip := appZip(t, "FuseT", "2.0.0", "new")
	newServer, _ := releaseServer(t, newZip)
	newCfg := config(t, newServer, dir, "2.0.0", newZip)
	var exchanged atomic.Bool
	fetcher := newFetcher(newServer, &fakeVerifier{})
	fetcher.exchange = func(leftDir *os.File, left string, rightDir *os.File, right string) error {
		leftPath := filepath.Join(leftDir.Name(), left)
		rightPath := filepath.Join(rightDir.Name(), right)
		assertRealDir(t, leftPath)
		assertRealDir(t, rightPath)
		if err := exchangePaths(leftDir, left, rightDir, right); err != nil {
			return err
		}
		assertRealDir(t, leftPath)
		assertRealDir(t, rightPath)
		exchanged.Store(true)
		return nil
	}
	if _, err := fetcher.Fetch(context.Background(), newCfg); err != nil {
		t.Fatal(err)
	}
	if !exchanged.Load() {
		t.Fatal("replacement did not use atomic exchange")
	}
	version, err := bundle.ShortVersion(bundle.AppPath(newCfg.Dir, "FuseT"))
	if err != nil || version != "2.0.0" {
		t.Fatalf("canonical version = %q, %v; want 2.0.0", version, err)
	}
}

func TestFetchExchangeFailurePreservesPriorBundle(t *testing.T) {
	dir := t.TempDir()
	oldZip := appZip(t, "FuseT", "1.0.0", "old")
	oldServer, _ := releaseServer(t, oldZip)
	oldCfg := config(t, oldServer, dir, "1.0.0", oldZip)
	if _, err := newFetcher(oldServer, &fakeVerifier{}).Fetch(context.Background(), oldCfg); err != nil {
		t.Fatal(err)
	}
	newZip := appZip(t, "FuseT", "2.0.0", "new")
	newServer, _ := releaseServer(t, newZip)
	newCfg := config(t, newServer, dir, "2.0.0", newZip)
	fetcher := newFetcher(newServer, &fakeVerifier{})
	fetcher.exchange = func(*os.File, string, *os.File, string) error { return errors.New("injected exchange failure") }
	if _, err := fetcher.Fetch(context.Background(), newCfg); err == nil {
		t.Fatal("Fetch succeeded despite exchange failure")
	}
	version, err := bundle.ShortVersion(bundle.AppPath(newCfg.Dir, "FuseT"))
	if err != nil || version != "1.0.0" {
		t.Fatalf("canonical version = %q, %v; want preserved 1.0.0", version, err)
	}
}

func TestFetchUnsupportedExchangePreservesPriorBundle(t *testing.T) {
	dir := t.TempDir()
	oldZip := appZip(t, "FuseT", "1.0.0", "old")
	oldServer, _ := releaseServer(t, oldZip)
	oldCfg := config(t, oldServer, dir, "1.0.0", oldZip)
	if _, err := newFetcher(oldServer, &fakeVerifier{}).Fetch(context.Background(), oldCfg); err != nil {
		t.Fatal(err)
	}
	newZip := appZip(t, "FuseT", "2.0.0", "new")
	newServer, _ := releaseServer(t, newZip)
	newCfg := config(t, newServer, dir, "2.0.0", newZip)
	fetcher := newFetcher(newServer, &fakeVerifier{})
	fetcher.exchange = func(*os.File, string, *os.File, string) error { return unix.ENOTSUP }
	if _, err := fetcher.Fetch(context.Background(), newCfg); !errors.Is(err, unix.ENOTSUP) {
		t.Fatalf("error = %v, want ENOTSUP", err)
	}
	version, err := bundle.ShortVersion(bundle.AppPath(newCfg.Dir, "FuseT"))
	if err != nil || version != "1.0.0" {
		t.Fatalf("canonical version = %q, %v; want preserved 1.0.0", version, err)
	}
}

func TestFetchUnsupportedFirstPublishLeavesCanonicalAbsent(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	fetcher := newFetcher(srv, &fakeVerifier{})
	fetcher.publishFirst = func(*os.File, string, *os.File, string) error { return unix.ENOTSUP }
	if _, err := fetcher.Fetch(context.Background(), cfg); !errors.Is(err, unix.ENOTSUP) {
		t.Fatalf("error = %v, want ENOTSUP", err)
	}
	assertCanonicalAbsent(t, cfg)
}

func TestFetchFailureBeforePreparedLeavesNoTransaction(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	injected := errors.New("before prepared")
	fetcher := newFetcher(srv, &fakeVerifier{})
	fetcher.beforePrepared = func() error { return injected }
	if _, err := fetcher.Fetch(context.Background(), cfg); !errors.Is(err, injected) {
		t.Fatalf("error = %v, want injected failure", err)
	}
	assertCanonicalAbsent(t, cfg)
	if _, err := os.Stat(pathsFor(cfg).transaction); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("transaction exists: %v", err)
	}
}

func TestFetchRecoversCrashBeforeFirstPublish(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	crash := errors.New("crash after durable journal")
	first := newFetcher(srv, &fakeVerifier{})
	first.afterPrepared = func() error { return crash }
	if _, err := first.Fetch(context.Background(), cfg); !errors.Is(err, crash) {
		t.Fatalf("error = %v, want injected crash", err)
	}
	assertCanonicalAbsent(t, cfg)
	if _, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, want recovered staged download", hits.Load())
	}
	assertRealDir(t, bundle.AppPath(cfg.Dir, cfg.AppName))
}

func TestFetchRejectsPreparedTransactionForDifferentAppName(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	prepared := errors.New("prepared")
	first := newFetcher(srv, &fakeVerifier{})
	first.afterPrepared = func() error { return prepared }
	if _, err := first.Fetch(context.Background(), cfg); !errors.Is(err, prepared) {
		t.Fatalf("error = %v, want prepared failpoint", err)
	}
	tx, err := readTransaction(pathsFor(cfg).transaction)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Identity != transactionIdentity || tx.Schema != 1 || tx.Fingerprint != transactionFingerprint {
		t.Fatalf("transaction identity = %q/%d/%q", tx.Identity, tx.Schema, tx.Fingerprint)
	}
	tx.Next.AppName = "Other"
	if err := writeJSONDurable(pathsFor(cfg).transaction, tx); err != nil {
		t.Fatal(err)
	}
	_, err = newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrInstallState) {
		t.Fatalf("error = %v, want ErrInstallState", err)
	}
	assertCanonicalAbsent(t, cfg)
}

func TestFetchRecoversCrashAfterExchange(t *testing.T) {
	dir := t.TempDir()
	oldZip := appZip(t, "FuseT", "1.0.0", "old")
	oldServer, _ := releaseServer(t, oldZip)
	oldCfg := config(t, oldServer, dir, "1.0.0", oldZip)
	if _, err := newFetcher(oldServer, &fakeVerifier{}).Fetch(context.Background(), oldCfg); err != nil {
		t.Fatal(err)
	}
	newZip := appZip(t, "FuseT", "2.0.0", "new")
	newServer, hits := releaseServer(t, newZip)
	newCfg := config(t, newServer, dir, "2.0.0", newZip)
	crash := errors.New("crash after atomic exchange")
	first := newFetcher(newServer, &fakeVerifier{})
	first.afterSwap = func() error {
		assertRealDir(t, bundle.AppPath(newCfg.Dir, "FuseT"))
		return crash
	}
	if _, err := first.Fetch(context.Background(), newCfg); !errors.Is(err, crash) {
		t.Fatalf("error = %v, want injected crash", err)
	}
	assertRealDir(t, bundle.AppPath(newCfg.Dir, "FuseT"))
	if _, err := newFetcher(newServer, &fakeVerifier{}).Fetch(context.Background(), newCfg); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, want recovery without redownload", hits.Load())
	}
	if _, err := os.Stat(pathsFor(newCfg).transaction); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("transaction remains after recovery: %v", err)
	}
}

func TestFetchRecoveryRejectsChangedCanonicalGeneration(t *testing.T) {
	dir := t.TempDir()
	oldZip := appZip(t, "FuseT", "1.0.0", "old")
	oldServer, _ := releaseServer(t, oldZip)
	oldCfg := config(t, oldServer, dir, "1.0.0", oldZip)
	if _, err := newFetcher(oldServer, &fakeVerifier{}).Fetch(context.Background(), oldCfg); err != nil {
		t.Fatal(err)
	}
	newZip := appZip(t, "FuseT", "2.0.0", "new")
	newServer, _ := releaseServer(t, newZip)
	newCfg := config(t, newServer, dir, "2.0.0", newZip)
	prepared := errors.New("prepared")
	first := newFetcher(newServer, &fakeVerifier{})
	first.afterPrepared = func() error { return prepared }
	if _, err := first.Fetch(context.Background(), newCfg); !errors.Is(err, prepared) {
		t.Fatalf("error = %v, want prepared failpoint", err)
	}
	canonical := bundle.AppPath(newCfg.Dir, "FuseT")
	if err := os.Rename(canonical, canonical+".displaced"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(canonical, 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := newFetcher(newServer, &fakeVerifier{}).Fetch(context.Background(), newCfg)
	if !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("error = %v, want generation conflict", err)
	}
	info, statErr := os.Stat(canonical)
	if statErr != nil || !info.IsDir() {
		t.Fatalf("changed canonical was overwritten: %v, %v", info, statErr)
	}
}

func TestFetchRecoversFinalReceiptBeforeCleanup(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	injected := errors.New("after final receipt")
	first := newFetcher(srv, &fakeVerifier{})
	first.afterFinal = func() error { return injected }
	if _, err := first.Fetch(context.Background(), cfg); !errors.Is(err, injected) {
		t.Fatalf("error = %v, want injected failure", err)
	}
	assertRealDir(t, bundle.AppPath(cfg.Dir, cfg.AppName))
	if _, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, want final receipt recovery", hits.Load())
	}
}

func TestFetchCleansStageAfterFinalTransactionRemoval(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	injected := errors.New("after transaction removal")
	first := newFetcher(srv, &fakeVerifier{})
	first.afterTransaction = func() error { return injected }
	if _, err := first.Fetch(context.Background(), cfg); !errors.Is(err, injected) {
		t.Fatalf("error = %v, want injected failure", err)
	}
	if _, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, want final receipt reuse", hits.Load())
	}
	entries, err := os.ReadDir(pathsFor(cfg).metadataDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".stage-") {
			t.Fatalf("stage residue remains: %q", entry.Name())
		}
	}
}

func TestFetchCancellationAfterSwapSettlesFinalReceipt(t *testing.T) {
	dir := t.TempDir()
	oldZip := appZip(t, "FuseT", "1.0.0", "old")
	oldServer, _ := releaseServer(t, oldZip)
	oldCfg := config(t, oldServer, dir, "1.0.0", oldZip)
	if _, err := newFetcher(oldServer, &fakeVerifier{}).Fetch(context.Background(), oldCfg); err != nil {
		t.Fatal(err)
	}
	newZip := appZip(t, "FuseT", "2.0.0", "new")
	newServer, _ := releaseServer(t, newZip)
	newCfg := config(t, newServer, dir, "2.0.0", newZip)
	ctx, cancel := context.WithCancel(context.Background())
	fetcher := newFetcher(newServer, &fakeVerifier{})
	fetcher.afterSwap = func() error {
		cancel()
		return nil
	}
	installation, err := fetcher.Fetch(ctx, newCfg)
	if err != nil {
		t.Fatal(err)
	}
	if installation.Release != newCfg.Release {
		t.Fatalf("release = %#v, want %#v", installation.Release, newCfg.Release)
	}
	current, err := readCurrent(pathsFor(newCfg))
	if err != nil || current == nil || current.State != stateFinal || current.Version != "2.0.0" {
		t.Fatalf("final receipt = %#v, %v", current, err)
	}
}

func TestFetchSerializesConcurrentExactRelease(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	verifier := &fakeVerifier{}
	fetcher := newFetcher(srv, verifier)
	const callers = 12
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			installation, err := fetcher.Fetch(context.Background(), cfg)
			if err == nil && installation.Path != bundle.AppPath(cfg.Dir, cfg.AppName) {
				err = fmt.Errorf("path = %q", installation.Path)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, want 1", hits.Load())
	}
}

func TestFetchSubprocessWorker(t *testing.T) {
	if os.Getenv("DAEMONKIT_FETCH_CHILD") != "1" {
		t.Skip("subprocess helper")
	}
	digest, err := ParseSHA256(os.Getenv("DAEMONKIT_FETCH_SHA256"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Release: Release{
			Version: os.Getenv("DAEMONKIT_FETCH_VERSION"),
			URL:     os.Getenv("DAEMONKIT_FETCH_URL"), SHA256: digest,
		},
		Dir: os.Getenv("DAEMONKIT_FETCH_DIR"), AppName: "FuseT", Identity: testIdentity,
	}
	if _, err := (&Fetcher{Client: http.DefaultClient, Verifier: &fakeVerifier{}, exchange: exchangePaths, publishFirst: publishExclusive}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestFetchSerializesAcrossProcesses(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(),
		"DAEMONKIT_FETCH_CHILD=1",
		"DAEMONKIT_FETCH_VERSION=1.0.0",
		"DAEMONKIT_FETCH_URL="+srv.URL+"/FuseT.zip",
		"DAEMONKIT_FETCH_SHA256="+digestFor(t, zipBytes).String(),
		"DAEMONKIT_FETCH_DIR="+dir,
	)
	commands := []*exec.Cmd{
		exec.Command(os.Args[0], "-test.run=^TestFetchSubprocessWorker$", "-test.v"),
		exec.Command(os.Args[0], "-test.run=^TestFetchSubprocessWorker$", "-test.v"),
	}
	outputs := make([]bytes.Buffer, len(commands))
	for i, command := range commands {
		command.Env = env
		command.Stdout = &outputs[i]
		command.Stderr = &outputs[i]
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for i, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("child failed: %v\n%s", err, outputs[i].String())
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, want one cross-process download", hits.Load())
	}
}

func TestFetchSerializesConcurrentDifferentReleasesWithoutAbsence(t *testing.T) {
	dir := t.TempDir()
	baseZip := appZip(t, "FuseT", "1.0.0", "base")
	baseServer, _ := releaseServer(t, baseZip)
	baseCfg := config(t, baseServer, dir, "1.0.0", baseZip)
	if _, err := newFetcher(baseServer, &fakeVerifier{}).Fetch(context.Background(), baseCfg); err != nil {
		t.Fatal(err)
	}
	zipA := appZip(t, "FuseT", "2.0.0", "a")
	serverA, hitsA := releaseServer(t, zipA)
	cfgA := config(t, serverA, dir, "2.0.0", zipA)
	zipB := appZip(t, "FuseT", "3.0.0", "b")
	serverB, hitsB := releaseServer(t, zipB)
	cfgB := config(t, serverB, dir, "3.0.0", zipB)

	stop := make(chan struct{})
	missing := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				info, err := os.Lstat(bundle.AppPath(cfgA.Dir, "FuseT"))
				if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					select {
					case missing <- fmt.Errorf("canonical observation: mode=%v error=%v", info, err):
					default:
					}
					return
				}
			}
		}
	}()

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, job := range []struct {
		fetcher *Fetcher
		cfg     Config
	}{{newFetcher(serverA, &fakeVerifier{}), cfgA}, {newFetcher(serverB, &fakeVerifier{}), cfgB}} {
		go func() {
			<-start
			_, err := job.fetcher.Fetch(context.Background(), job.cfg)
			errs <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	select {
	case err := <-missing:
		t.Fatal(err)
	default:
	}
	if hitsA.Load() != 1 || hitsB.Load() != 1 {
		t.Fatalf("asset hits = (%d, %d), want (1, 1)", hitsA.Load(), hitsB.Load())
	}
	current, err := readCurrent(pathsFor(cfgA))
	if err != nil || current == nil || (current.Version != "2.0.0" && current.Version != "3.0.0") {
		t.Fatalf("coherent final receipt = %#v, %v", current, err)
	}
}

func TestFetchRejectsUnmanagedCanonicalPath(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	if err := os.Mkdir(bundle.AppPath(cfg.Dir, cfg.AppName), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("error = %v, want ErrInstallConflict", err)
	}
}

func TestFetchRejectsCanonicalSymlink(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	target := t.TempDir()
	if err := os.Symlink(target, bundle.AppPath(cfg.Dir, cfg.AppName)); err != nil {
		t.Fatal(err)
	}
	_, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("error = %v, want ErrInstallConflict", err)
	}
}

func TestFetchRejectsUnknownReceiptFieldsWithoutDownload(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	if _, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pathsFor(cfg).receipt, []byte(`{"schema":1,"state":"final","unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrInstallState) {
		t.Fatalf("error = %v, want ErrInstallState", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, corrupt receipt must not authorize or trigger download", hits.Load())
	}
}

func TestFetchRejectsTrailingReceiptData(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	if _, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	receiptPath := pathsFor(cfg).receipt
	data, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receiptPath, append(data, []byte("{}")...), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrInstallState) {
		t.Fatalf("error = %v, want ErrInstallState", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, trailing receipt data must not authorize download", hits.Load())
	}
}

func TestFetchRejectsReceiptCanonicalIdentityMismatch(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	if _, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	current, err := readCurrent(pathsFor(cfg))
	if err != nil {
		t.Fatal(err)
	}
	current.Canonical.Inode = "different"
	if err := writeJSONDurable(pathsFor(cfg).receipt, current); err != nil {
		t.Fatal(err)
	}
	_, err = newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg)
	if !errors.Is(err, ErrInstallState) {
		t.Fatalf("error = %v, want ErrInstallState", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, mismatched inode must not authorize download", hits.Load())
	}
}

func TestFetchPropagatesOperationalVerificationFailure(t *testing.T) {
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, hits := releaseServer(t, zipBytes)
	cfg := config(t, srv, t.TempDir(), "1.0.0", zipBytes)
	if _, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	operational := errors.New("codesign unavailable")
	_, err := newFetcher(srv, &fakeVerifier{err: operational}).Fetch(context.Background(), cfg)
	if !errors.Is(err, operational) {
		t.Fatalf("error = %v, want operational verifier error", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("asset hits = %d, operational failure must not reinstall", hits.Load())
	}
}

func TestParseSHA256(t *testing.T) {
	want := sha256.Sum256([]byte("release"))
	got, err := ParseSHA256(hex.EncodeToString(want[:]))
	if err != nil {
		t.Fatal(err)
	}
	if got != SHA256(want) || got.String() != hex.EncodeToString(want[:]) {
		t.Fatalf("digest = %s, want %x", got.String(), want)
	}
	for _, value := range []string{"", "abc", string(bytes.Repeat([]byte{'z'}, 64))} {
		if _, err := ParseSHA256(value); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("ParseSHA256(%q) error = %v, want ErrInvalidConfig", value, err)
		}
	}
}

func assertCanonicalAbsent(t *testing.T, cfg Config) {
	t.Helper()
	if _, err := os.Lstat(bundle.AppPath(cfg.Dir, cfg.AppName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical app exists: %v", err)
	}
}

func assertRealDir(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("%q mode = %v, want real directory", path, info.Mode())
	}
}

func TestMetadataLivesBesideCanonical(t *testing.T) {
	dir := t.TempDir()
	zipBytes := appZip(t, "FuseT", "1.0.0", "new")
	srv, _ := releaseServer(t, zipBytes)
	cfg := config(t, srv, dir, "1.0.0", zipBytes)
	if _, err := newFetcher(srv, &fakeVerifier{}).Fetch(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(filepath.Dir(pathsFor(cfg).metadataDir)) != cfg.Dir {
		t.Fatalf("metadata dir = %q, want under %q", pathsFor(cfg).metadataDir, cfg.Dir)
	}
}
