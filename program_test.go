package daemonkit

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/internal/realhome"
)

func TestStaged(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)

	program, err := Staged()
	if err != nil {
		t.Fatalf("Staged() error = %v", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	source, err := os.ReadFile(exe)
	if err != nil {
		t.Fatalf("read executable: %v", err)
	}
	sum := sha256.Sum256(source)
	want := filepath.Join(home, ".daemonkit", "staged", hex.EncodeToString(sum[:]), filepath.Base(exe))
	if program.path != want {
		t.Fatalf("path = %q, want %q", program.path, want)
	}

	staged, err := os.ReadFile(program.path)
	if err != nil {
		t.Fatalf("read staged copy: %v", err)
	}
	if sha256.Sum256(staged) != sum {
		t.Fatal("staged bytes differ from the executable")
	}
	info, err := os.Lstat(program.path)
	if err != nil {
		t.Fatalf("inspect staged copy: %v", err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("staged copy mode = %v, want a regular file", info.Mode())
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("staged copy mode = %v, want owner-executable", info.Mode())
	}

	again, err := Staged()
	if err != nil {
		t.Fatalf("second Staged() error = %v", err)
	}
	if again.path != program.path {
		t.Fatalf("second path = %q, want %q", again.path, program.path)
	}
	restaged, err := os.ReadFile(again.path)
	if err != nil {
		t.Fatalf("read staged copy: %v", err)
	}
	if sha256.Sum256(restaged) != sum {
		t.Fatal("second Staged() left bytes that differ from the executable")
	}
}

func TestInBundle(t *testing.T) {
	app := filepath.Join(t.TempDir(), "Fake.app")
	exe := filepath.Join(app, "Contents", "MacOS", "fake")
	if err := os.MkdirAll(filepath.Dir(exe), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	plain := filepath.Join(app, "Contents", "Info.plist")
	if err := os.WriteFile(plain, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		app     string
		rel     string
		want    string
		wantErr string
	}{
		{"executable in bundle", app, filepath.Join("Contents", "MacOS", "fake"), exe, ""},
		{"relative app", "Fake.app", filepath.Join("Contents", "MacOS", "fake"), "", "not absolute"},
		{"escaping rel", app, filepath.Join("..", "other"), "", "does not stay inside"},
		{"absolute rel", app, exe, "", "does not stay inside"},
		{"missing executable", app, filepath.Join("Contents", "MacOS", "gone"), "", "bundle executable"},
		{"directory", app, "Contents", "", "not a regular file"},
		{"non-executable file", app, filepath.Join("Contents", "Info.plist"), "", "not executable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			program, err := InBundle(tt.app, tt.rel)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("InBundle() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("InBundle() error = %v", err)
			}
			if program.path != tt.want {
				t.Errorf("path = %q, want %q", program.path, tt.want)
			}
		})
	}

	if _, err := InBundle(app, filepath.Join("Contents", "MacOS", "gone")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("missing executable error = %v, want fs.ErrNotExist", err)
	}
}

func TestStagedReplacesPlantedBytes(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	source, err := os.ReadFile(exe)
	if err != nil {
		t.Fatalf("read executable: %v", err)
	}
	sum := sha256.Sum256(source)
	dir := filepath.Join(home, ".daemonkit", "staged", hex.EncodeToString(sum[:]))
	target := filepath.Join(dir, filepath.Base(exe))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("not the executable"), 0o600); err != nil {
		t.Fatal(err)
	}

	program, err := Staged()
	if err != nil {
		t.Fatalf("Staged() error = %v", err)
	}
	staged, err := os.ReadFile(program.path)
	if err != nil {
		t.Fatalf("read staged copy: %v", err)
	}
	if sha256.Sum256(staged) != sum {
		t.Error("staged bytes do not hash to the digest the path names")
	}
	info, err := os.Stat(program.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Errorf("staged copy mode = %v, want owner-executable", info.Mode())
	}
}

func TestInBundleSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	app := filepath.Join(root, "Fake.app")
	if err := os.MkdirAll(filepath.Join(app, "Contents", "MacOS"), 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(app, "Contents", "MacOS", "escape")); err != nil {
		t.Fatal(err)
	}

	_, err := InBundle(app, filepath.Join("Contents", "MacOS", "escape"))
	if err == nil || !strings.Contains(err.Error(), "does not stay inside") {
		t.Errorf("InBundle() error = %v, want containment refusal", err)
	}
}

func TestInBundleSymlinkedAppRoot(t *testing.T) {
	root := t.TempDir()
	realApp := filepath.Join(root, "Real.app")
	exe := filepath.Join(realApp, "Contents", "MacOS", "fake")
	if err := os.MkdirAll(filepath.Dir(exe), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "Alias.app")
	if err := os.Symlink(realApp, alias); err != nil {
		t.Fatal(err)
	}

	program, err := InBundle(alias, filepath.Join("Contents", "MacOS", "fake"))
	if err != nil {
		t.Fatalf("InBundle() error = %v", err)
	}
	if want := filepath.Join(alias, "Contents", "MacOS", "fake"); program.path != want {
		t.Errorf("path = %q, want %q", program.path, want)
	}
}

func TestInBundleInternalSymlink(t *testing.T) {
	app := filepath.Join(t.TempDir(), "Fake.app")
	exe := filepath.Join(app, "Contents", "MacOS", "fake")
	if err := os.MkdirAll(filepath.Dir(exe), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(exe, filepath.Join(app, "Contents", "MacOS", "current")); err != nil {
		t.Fatal(err)
	}

	if _, err := InBundle(app, filepath.Join("Contents", "MacOS", "current")); err != nil {
		t.Errorf("InBundle() error = %v, want an in-bundle symlink accepted", err)
	}
}
