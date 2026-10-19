//go:build darwin

package durable

import (
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileNewPublishes(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		perm os.FileMode
	}{
		{"payload", []byte("published"), 0o600},
		{"empty", []byte{}, 0o640},
		{"nil", nil, 0o644},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "agent.plist")

			published, err := WriteFileNew(path, test.data, test.perm)
			if err != nil {
				t.Fatalf("WriteFileNew: %v", err)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(test.data) {
				t.Fatalf("contents = %q, want %q", got, test.data)
			}
			info, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != test.perm {
				t.Fatalf("mode = %#o, want %#o", info.Mode().Perm(), test.perm)
			}
			if !os.SameFile(published, info) {
				t.Fatal("the returned FileInfo is not the file published at the path")
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0].Name() != "agent.plist" {
				t.Fatalf("directory holds %v, want only agent.plist", entries)
			}
		})
	}
}

// TestWriteFileNewRefusesAnOccupiedPath holds the refusal to leaving the file
// already at the path exactly as it was, down to its inode and mode, with the
// temp the refused write staged through gone.
func TestWriteFileNewRefusesAnOccupiedPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.plist")
	if err := os.WriteFile(path, []byte("incumbent"), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}

	published, err := WriteFileNew(path, []byte("challenger"), 0o600)

	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("WriteFileNew onto an occupied path = %v, want os.ErrExist", err)
	}
	if published != nil {
		t.Fatalf("WriteFileNew returned %v for a write it refused", published)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "incumbent" {
		t.Fatalf("contents = %q, want the incumbent's %q", got, "incumbent")
	}
	after, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || after.Mode().Perm() != 0o644 {
		t.Fatalf("the incumbent was replaced or rechmodded: mode %#o", after.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "agent.plist" {
		t.Fatalf("directory holds %v, want only agent.plist and no temp", entries)
	}
}

// TestWriteFileNewReportsAnUnconfirmedDirectoryFsync is a directory fsync that
// fails once the rename has landed, made observable by a mode that refuses the
// directory's open. The file is published all the same, so the error comes
// back with the published file's FileInfo rather than instead of it.
func TestWriteFileNewReportsAnUnconfirmedDirectoryFsync(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("this suite does not run as root: root bypasses the directory mode this test reads the fsync through")
	}
	dir := filepath.Join(t.TempDir(), "LaunchAgents")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "agent.plist")
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	published, err := WriteFileNew(path, []byte("published"), 0o600)

	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("WriteFileNew = %v, want the directory fsync to run and fail", err)
	}
	if published == nil || !os.SameFile(published, lstat(t, path)) {
		t.Fatalf("WriteFileNew returned %v, want the FileInfo of the file it published", published)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "published" {
		t.Fatalf("contents = %q, want %q", got, "published")
	}
}

func TestRenameNewPublishesOntoAnAbsentTarget(t *testing.T) {
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
			from := stagedTree(t, filepath.Join(root, "stage"))
			target := root
			if test.cross {
				target = filepath.Join(root, "install")
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			to := filepath.Join(target, "candidate.app")
			staged, tree := lstat(t, from), treeOf(t, from)

			if err := RenameNew(from, to); err != nil {
				t.Fatalf("RenameNew onto an absent target: %v", err)
			}

			if _, err := os.Lstat(from); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the source survived the rename: %v", err)
			}
			if !os.SameFile(staged, lstat(t, to)) || !maps.Equal(treeOf(t, to), tree) {
				t.Fatal("the target is not the tree renamed from the source")
			}
		})
	}
}

// TestRenameNewRefusesAnOccupiedTarget holds the refusal to leaving both paths
// exactly as they were. The target keeps its inode and every byte, an empty
// directory among them, which a plain rename replaces, and the source tree
// stays where it was staged.
func TestRenameNewRefusesAnOccupiedTarget(t *testing.T) {
	tests := []struct {
		name  string
		plant func(t *testing.T, path string)
	}{
		{"an empty directory", func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{"a non-empty directory", func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(path, "incumbent"), []byte("incumbent"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"a file", func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte("incumbent"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			from := stagedTree(t, filepath.Join(root, "stage"))
			to := filepath.Join(root, "candidate.app")
			test.plant(t, to)
			staged, stagedFiles := lstat(t, from), treeOf(t, from)
			incumbent, incumbentFiles := lstat(t, to), treeOf(t, to)

			err := RenameNew(from, to)

			if !errors.Is(err, os.ErrExist) {
				t.Fatalf("RenameNew onto %s = %v, want os.ErrExist", test.name, err)
			}
			if !os.SameFile(incumbent, lstat(t, to)) || !maps.Equal(treeOf(t, to), incumbentFiles) {
				t.Fatalf("the target was replaced or rewritten: %v", treeOf(t, to))
			}
			if !os.SameFile(staged, lstat(t, from)) || !maps.Equal(treeOf(t, from), stagedFiles) {
				t.Fatalf("the source moved or changed: %v", treeOf(t, from))
			}
		})
	}
}

func stagedTree(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(path, "Contents"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "Contents", "Info.plist"), []byte("staged"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func lstat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func treeOf(t *testing.T, root string) map[string]string {
	t.Helper()
	tree := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		state := info.Mode().String()
		if info.Mode().IsRegular() {
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			state += " " + string(content)
		}
		tree[rel] = state
		return nil
	})
	if err != nil {
		t.Fatalf("walk %q: %v", root, err)
	}
	return tree
}
