package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalExecutablePath(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "real")
	if err := os.WriteFile(target, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(root, "chain")
	if err := os.Symlink(link, chain); err != nil {
		t.Fatal(err)
	}
	broken := filepath.Join(root, "broken")
	if err := os.Symlink(filepath.Join(root, "missing"), broken); err != nil {
		t.Fatal(err)
	}
	nonExecutable := filepath.Join(root, "non-executable")
	if err := os.WriteFile(nonExecutable, []byte("no"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		path string
		want string
		err  string
	}{
		{name: "real path", path: target, want: target},
		{name: "absolute symlink", path: link, want: target},
		{name: "symlink chain", path: chain, want: target},
		{name: "broken symlink", path: broken, err: "resolve executable path"},
		{name: "relative path", path: "worker", err: "not exact and absolute"},
		{name: "directory", path: root, err: "not a regular file"},
		{name: "non-executable", path: nonExecutable, err: "not executable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := canonicalExecutablePath(test.path)
			if test.err != "" {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("canonicalExecutablePath(%q) error = %v, want %q", test.path, err, test.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("canonicalExecutablePath(%q) = %q, want %q", test.path, got, test.want)
			}
		})
	}
}

func TestCanonicalExecutableResolvesCurrentProcess(t *testing.T) {
	raw, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := CanonicalExecutable()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("CanonicalExecutable() = %q, want %q", got, want)
	}
}
