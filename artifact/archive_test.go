package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestArchiveBudgetRejectsOversizeEntry(t *testing.T) {
	for _, size := range []int64{1001, math.MaxInt64} {
		b := archiveBudget{remaining: 1000, maxBytes: 1000}
		if err := b.admit(size); !errors.Is(err, ErrUnsafeArchive) {
			t.Fatalf("admit(%d) with maxBytes 1000 = %v, want ErrUnsafeArchive", size, err)
		}
	}
}

func TestArchiveBudgetAccumulationDoesNotOverflow(t *testing.T) {
	b := archiveBudget{remaining: 4 << 30, maxBytes: 4 << 30}
	if err := b.admit(4 << 30); err != nil {
		t.Fatalf("admit(maxBytes) = %v, want nil", err)
	}
	if err := b.admit(math.MaxInt64); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("admit(MaxInt64) after a near-full budget = %v, want ErrUnsafeArchive", err)
	}
	if b.declared < 0 {
		t.Fatalf("declared sum wrapped negative: %d", b.declared)
	}
}

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
	if err := extractTarGz(context.Background(), src, t.TempDir(), 1<<20); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("extractTarGz() = %v, want ErrUnsafeArchive", err)
	}
}
