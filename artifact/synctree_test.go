package artifact

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func TestWalkSyncTreeContainsTheExactNonsymlinkPopulation(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a", "b"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"file", filepath.Join("a", "b", "file")} {
		if err := os.WriteFile(filepath.Join(root, path), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range map[string]string{"outside": t.TempDir(), "dangling": "missing"} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	if err := walkSyncTree(root, func(file *os.File) error {
		path, err := filepath.Rel(root, file.Name())
		if err != nil {
			return errors.Join(err, file.Close())
		}
		got = append(got, path)
		return file.Close()
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	want := []string{".", "a", filepath.Join("a", "b"), filepath.Join("a", "b", "file"), "file"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("visited = %v, want %v", got, want)
	}
}

func TestSyncTreeReportsAMissingRoot(t *testing.T) {
	if err := syncTree(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("syncTree = %v, want missing root", err)
	}
}
