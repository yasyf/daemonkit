package fetch

import (
	"archive/zip"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeZip(t *testing.T, build func(*zip.Writer)) string {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	build(w)
	if err := w.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	path := filepath.Join(t.TempDir(), "a.zip")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write zip: %v", err)
	}
	return path
}

func addFile(t *testing.T, w *zip.Writer, name string, mode os.FileMode, body string) {
	t.Helper()
	hdr := &zip.FileHeader{Name: name, Method: zip.Deflate}
	hdr.SetMode(mode)
	fw, err := w.CreateHeader(hdr)
	if err != nil {
		t.Fatalf("create %q: %v", name, err)
	}
	if _, err := fw.Write([]byte(body)); err != nil {
		t.Fatalf("write %q: %v", name, err)
	}
}

func TestUnzipRejectsPathTraversal(t *testing.T) {
	src := writeZip(t, func(w *zip.Writer) {
		addFile(t, w, "../escape.txt", 0o644, "pwned")
	})
	dest := t.TempDir()
	if err := unzip(src, dest); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("err = %v, want ErrUnsafeArchive", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dest), "escape.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("traversal wrote outside dest")
	}
}

func TestUnzipRejectsEscapingSymlink(t *testing.T) {
	src := writeZip(t, func(w *zip.Writer) {
		addFile(t, w, "FuseT.app/link", 0o777|os.ModeSymlink, "../../../../etc/passwd")
	})
	if err := unzip(src, t.TempDir()); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("err = %v, want ErrUnsafeArchive", err)
	}
}

func TestUnzipExtractsBundle(t *testing.T) {
	src := writeZip(t, func(w *zip.Writer) {
		addFile(t, w, "FuseT.app/Contents/Info.plist", 0o644, "<plist/>")
		addFile(t, w, "FuseT.app/Contents/MacOS/FuseT", 0o755, "bin")
		addFile(t, w, "FuseT.app/Contents/Frameworks/cur", 0o777|os.ModeSymlink, "A")
	})
	dest := t.TempDir()
	if err := unzip(src, dest); err != nil {
		t.Fatalf("unzip: %v", err)
	}
	info, err := os.Stat(filepath.Join(dest, "FuseT.app/Contents/MacOS/FuseT"))
	if err != nil {
		t.Fatalf("stat exe: %v", err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("inner exe not executable: %v", info.Mode())
	}
	link, err := os.Readlink(filepath.Join(dest, "FuseT.app/Contents/Frameworks/cur"))
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if link != "A" {
		t.Fatalf("symlink target = %q, want %q", link, "A")
	}
}
