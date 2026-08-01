package durable

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMkdirTreatsPresenceAsSuccess(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "metadata")
	if err := Mkdir(dir, 0o700); err != nil {
		t.Fatalf("first Mkdir: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("mode = %v, want a 0700 directory", info.Mode())
	}
	if err := Mkdir(dir, 0o700); err != nil {
		t.Fatalf("re-run Mkdir: %v", err)
	}
}

// TestMkdirSyncsThroughAPresentDirectory pins the retry the moved
// implementation carried and its test named: a run whose parent fsync failed
// left an entry the page cache holds and the disk does not, and the ladder's
// next Mkdir is the only thing that re-establishes it. The parent is opened
// either way, so a mode that refuses the open is what makes the second fsync
// observable at all.
func TestMkdirSyncsThroughAPresentDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses the directory mode this test reads the fsync through")
	}
	parent := filepath.Join(t.TempDir(), "metadata")
	dir := filepath.Join(parent, "generation")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o700) })

	if err := Mkdir(dir, 0o700); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Mkdir over a present directory = %v, want the parent fsync to run and fail", err)
	}
}

func TestMkdirRefusesAnAbsentParent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent", "metadata")
	if err := Mkdir(dir, 0o700); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Mkdir under an absent parent = %v, want os.ErrNotExist", err)
	}
}

func TestRemoveTreatsAbsenceAsSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "record.json")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("path survived Remove: %v", err)
	}
	if err := Remove(path); err != nil {
		t.Fatalf("re-run Remove: %v", err)
	}
	if err := Remove(filepath.Join(dir, "absent", "record.json")); err != nil {
		t.Fatalf("Remove under an absent directory: %v", err)
	}
}

func TestRemoveTreeTreatsAbsenceAsSuccess(t *testing.T) {
	dir := t.TempDir()
	tree := filepath.Join(dir, "prior.app")
	if err := os.MkdirAll(filepath.Join(tree, "Contents", "MacOS"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := RemoveTree(tree); err != nil {
		t.Fatalf("RemoveTree: %v", err)
	}
	if _, err := os.Stat(tree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tree survived RemoveTree: %v", err)
	}
	if err := RemoveTree(tree); err != nil {
		t.Fatalf("re-run RemoveTree: %v", err)
	}
	if err := RemoveTree(filepath.Join(dir, "absent", "prior.app")); err != nil {
		t.Fatalf("RemoveTree under an absent directory: %v", err)
	}
}

func TestRenameMoves(t *testing.T) {
	tests := []struct {
		name  string
		cross bool
	}{
		{"within one directory", false},
		{"across directories", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			target := source
			if test.cross {
				target = filepath.Join(root, "target")
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Mkdir(source, 0o700); err != nil {
				t.Fatal(err)
			}
			from := filepath.Join(source, "generation.app")
			to := filepath.Join(target, "prior.app")
			if err := os.Mkdir(from, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := Rename(from, to); err != nil {
				t.Fatalf("Rename: %v", err)
			}
			if _, err := os.Stat(from); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source entry survived: %v", err)
			}
			if _, err := os.Stat(to); err != nil {
				t.Fatalf("destination missing: %v", err)
			}
		})
	}
}

// TestRenameSurfacesTheOsCondition pins the one-shot archive ladder in deploy,
// which reads os.ErrNotExist and os.ErrExist off a failed rename to decide
// whether the tree is already gone or the next name has to be tried.
func TestRenameSurfacesTheOsCondition(t *testing.T) {
	root := t.TempDir()
	occupied := filepath.Join(root, "occupied")
	for _, dir := range []string{filepath.Join(root, "source"), occupied} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(occupied, "tenant"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Rename(filepath.Join(root, "absent"), filepath.Join(root, "target")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Rename of an absent source = %v, want os.ErrNotExist", err)
	}
	if err := Rename(filepath.Join(root, "source"), occupied); !errors.Is(err, os.ErrExist) {
		t.Fatalf("Rename onto an occupied directory = %v, want os.ErrExist", err)
	}
}
