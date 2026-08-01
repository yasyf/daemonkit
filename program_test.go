package daemonkit

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/realhome"
)

const placedLabel = Label("com.example.placed")

func placedElement(t *testing.T) element {
	t.Helper()
	return mustElement(t, placedLabel)
}

func selfDigest(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	source, err := os.ReadFile(exe)
	if err != nil {
		t.Fatalf("read executable: %v", err)
	}
	return digest(source)
}

func placedAt(t *testing.T, home string) string {
	t.Helper()
	return filepath.Join(home, ".daemonkit", "bin", string(placedLabel))
}

func inode(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return info.Sys().(*syscall.Stat_t).Ino
}

// TestTwoDaemonsCannotShareAProgramPath is the third consequence of a free-form
// program name: two consumers that pick the same one deploy over each other's
// binary and evict each other's daemon, with nothing on the shared root to say
// whose it is. The leaf is the daemon's Label, which launchd already enforces
// unique, so the collision is unrepresentable rather than undetected.
func TestTwoDaemonsCannotShareAProgramPath(t *testing.T) {
	t.Setenv(realhome.EnvOverride, t.TempDir())

	first, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	second, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	one, err := (Daemon{Label: "com.example.one", Program: first}).agent()
	if err != nil {
		t.Fatalf("agent() error = %v", err)
	}
	two, err := (Daemon{Label: "com.example.two", Program: second}).agent()
	if err != nil {
		t.Fatalf("agent() error = %v", err)
	}
	if one.Program == two.Program {
		t.Fatalf("two labels deploy to the same program path %q", one.Program)
	}
}

func TestStablePlacesTheInvokingExecutable(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	deployed := selfDigest(t)

	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	path, err := program.path(placedElement(t))
	if err != nil {
		t.Fatalf("path() error = %v", err)
	}
	if want := placedAt(t, home); path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
	build, err := program.build()
	if err != nil {
		t.Fatalf("build() error = %v", err)
	}
	if build != deployed {
		t.Fatalf("build = %q, want the current executable's %q", build, deployed)
	}

	replaced, err := program.place(placedElement(t))
	if err != nil {
		t.Fatalf("place() error = %v", err)
	}
	if !replaced {
		t.Fatal("place() reported no write against a path that held nothing")
	}
	landed, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read placed copy: %v", err)
	}
	if digest(landed) != deployed {
		t.Fatal("placed bytes differ from the executable")
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("inspect placed copy: %v", err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("placed copy mode = %v, want a regular file", info.Mode())
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("placed copy mode = %v, want 0700", info.Mode())
	}
}

// TestPlaceLeavesMatchingBytesAlone is placement's idempotence, asserted on the
// inode: every write lands by rename, so a path that still carries the inode it
// carried before was not rewritten under the daemon already running from it.
func TestPlaceLeavesMatchingBytesAlone(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)

	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	if _, err := program.place(placedElement(t)); err != nil {
		t.Fatalf("place() error = %v", err)
	}
	target := placedAt(t, home)
	before := inode(t, target)

	replaced, err := program.place(placedElement(t))
	if err != nil {
		t.Fatalf("second place() error = %v", err)
	}
	if replaced {
		t.Fatal("place() rewrote a path that already held these bytes")
	}
	if after := inode(t, target); after != before {
		t.Fatalf("program inode = %d, want the untouched %d", after, before)
	}
}

// TestPlaceReplacesPlantedBytes pins that the path is written, never trusted: a
// same-uid writer can leave anything at a path that outlives every upgrade, and
// what launchd receives is always this binary, executable, in full.
func TestPlaceReplacesPlantedBytes(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	deployed := selfDigest(t)

	target := placedAt(t, home)
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("not the executable"), 0o600); err != nil {
		t.Fatal(err)
	}

	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	replaced, err := program.place(placedElement(t))
	if err != nil {
		t.Fatalf("place() error = %v", err)
	}
	if !replaced {
		t.Fatal("place() left planted bytes in place")
	}
	landed, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read placed copy: %v", err)
	}
	if digest(landed) != deployed {
		t.Error("placed bytes are not the current executable's")
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("placed copy mode = %v, want 0700 rather than the planted file's", info.Mode())
	}
}

// TestPlaceReplacesAPlantedSymlink is the planted-bytes case the idempotence
// shortcut would otherwise wave through. A link whose target holds these exact
// bytes reads as settled, and its target is swapped afterwards for whatever
// launchd then execs from the plist's Program path — so the final component is
// never followed and a link is replaced like any other bytes that are not
// these.
func TestPlaceReplacesAPlantedSymlink(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	deployed := selfDigest(t)

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	elsewhere := filepath.Join(t.TempDir(), "planted")
	data, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(elsewhere, data, 0o700); err != nil {
		t.Fatal(err)
	}
	target := placedAt(t, home)
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, target); err != nil {
		t.Fatal(err)
	}

	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	replaced, err := program.place(placedElement(t))
	if err != nil {
		t.Fatalf("place() error = %v", err)
	}
	if !replaced {
		t.Fatal("place() left a symlink standing at the program path")
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("program path mode = %v, want a regular file", info.Mode())
	}
	landed, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if digest(landed) != deployed {
		t.Error("placed bytes are not the current executable's")
	}
}

// TestPlaceSweepsOnlyItsOwnStumps covers the crash between the temp file and
// the rename. The stump is named for this target, so the next placement that
// writes reclaims it — and a neighbour's, on a root every daemonkit consumer
// shares, is not this label's to remove, nor is a temp another launcher is
// still writing.
func TestPlaceSweepsOnlyItsOwnStumps(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)

	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	target := placedAt(t, home)
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	mine := filepath.Join(dir, "."+string(placedLabel)+".123456")
	neighbours := []string{
		filepath.Join(dir, ".com.example.other.123456"),
		filepath.Join(dir, "."+string(placedLabel)+".helper.123456"),
	}
	for _, stump := range append([]string{mine}, neighbours...) {
		if err := os.WriteFile(stump, []byte("half a binary"), 0o600); err != nil {
			t.Fatal(err)
		}
		stale := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(stump, stale, stale); err != nil {
			t.Fatal(err)
		}
	}
	inFlight := filepath.Join(dir, "."+string(placedLabel)+".789012")
	if err := os.WriteFile(inFlight, []byte("half a binary"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := program.place(placedElement(t)); err != nil {
		t.Fatalf("place() error = %v", err)
	}
	if _, err := os.Stat(mine); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("stat own stump = %v, want it swept", err)
	}
	for _, neighbour := range neighbours {
		if _, err := os.Stat(neighbour); err != nil {
			t.Errorf("stat neighbour's stump %q = %v, want it untouched", neighbour, err)
		}
	}
	if _, err := os.Stat(inFlight); err != nil {
		t.Errorf("stat a concurrent launcher's temp %q = %v, want it untouched", inFlight, err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Errorf("stat the program = %v, want the sweep to have spared it", err)
	}
}

// TestPlaceRefusesAnExecutableReplacedUnderTheLauncher keeps the promise the
// constructor made: the build Ensure converges on is the digest Stable took, so
// bytes that no longer hash to it are refused rather than deployed as a build
// no launcher asked for.
func TestPlaceRefusesAnExecutableReplacedUnderTheLauncher(t *testing.T) {
	t.Setenv(realhome.EnvOverride, t.TempDir())

	body := []byte("#!/bin/sh\nexit 0\n")
	source := filepath.Join(t.TempDir(), "launcher")
	if err := os.WriteFile(source, body, 0o700); err != nil {
		t.Fatal(err)
	}
	program := Program{policy: copied{source: source, digest: digest(body)}}
	if _, err := program.place(placedElement(t)); err != nil {
		t.Fatalf("place() error = %v", err)
	}
	if err := os.WriteFile(source, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := program.place(mustElement(t, "com.example.moved")); err == nil ||
		!strings.Contains(err.Error(), "this Program was built from") {
		t.Fatalf("place() error = %v, want the replaced executable refused", err)
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
			path, err := program.path(placedElement(t))
			if err != nil {
				t.Fatalf("path() error = %v", err)
			}
			if path != tt.want {
				t.Errorf("path = %q, want %q", path, tt.want)
			}
			build, err := program.build()
			if err != nil {
				t.Fatalf("build() error = %v", err)
			}
			if want := digest([]byte("#!/bin/sh\n")); build != want {
				t.Errorf("build = %q, want %q", build, want)
			}
		})
	}

	if _, err := InBundle(app, filepath.Join("Contents", "MacOS", "gone")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("missing executable error = %v, want fs.ErrNotExist", err)
	}
}

// TestInBundlePlacesNothing is the other half of the two policies: a bundled
// executable is already where launchd runs it, the Label names nothing about
// it, and Ensure's placement step has nothing to do.
func TestInBundlePlacesNothing(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	app := filepath.Join(t.TempDir(), "Fake.app")
	exe := filepath.Join(app, "Contents", "MacOS", "fake")
	if err := os.MkdirAll(filepath.Dir(exe), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	program, err := InBundle(app, filepath.Join("Contents", "MacOS", "fake"))
	if err != nil {
		t.Fatalf("InBundle() error = %v", err)
	}
	replaced, err := program.place(placedElement(t))
	if err != nil {
		t.Fatalf("place() error = %v", err)
	}
	if replaced {
		t.Fatal("place() reported a write for a bundled executable")
	}
	if _, err := os.Stat(filepath.Join(home, ".daemonkit")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("stat the program root = %v, want a bundled program to have made none", err)
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
	path, err := program.path(placedElement(t))
	if err != nil {
		t.Fatalf("path() error = %v", err)
	}
	if want := filepath.Join(alias, "Contents", "MacOS", "fake"); path != want {
		t.Errorf("path = %q, want %q", path, want)
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
