package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSafeJoinRejectsEscape(t *testing.T) {
	dest := t.TempDir()
	for _, name := range []string{"../evil", "a/../../evil"} {
		if _, err := safeJoin(dest, name); !errors.Is(err, ErrUnsafeArchive) {
			t.Errorf("safeJoin(%q) = %v, want ErrUnsafeArchive", name, err)
		}
	}
	if got, err := safeJoin(dest, "sub/child"); err != nil || got != filepath.Join(dest, "sub", "child") {
		t.Errorf("safeJoin(sub/child) = %q, %v", got, err)
	}
}

func TestExtractTarGzRejectsSymlink(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd", Mode: 0o777}); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(tw.Close(), gz.Close()); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(t.TempDir(), "archive.tar.gz")
	if err := os.WriteFile(src, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := extractTarGz(src, t.TempDir()); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("extractTarGz() = %v, want ErrUnsafeArchive", err)
	}
}
