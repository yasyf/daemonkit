package daemonkit

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/internal/realhome"
)

// Program is the executable launchd runs. Its two constructors are its two
// policies; no field is settable.
type Program struct{ path string }

// Staged copies the current executable to a digest-keyed path outside any
// versioned install directory, so a package upgrade cannot delete the running
// program. The path is the content's digest and every call rewrites the copy
// atomically, so pre-existing bytes at the path are replaced, never trusted:
// what launchd receives is always this binary, executable, in full.
func Staged() (Program, error) {
	exe, err := os.Executable()
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: resolve current executable: %w", err)
	}
	data, err := os.ReadFile(exe)
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: read executable %q: %w", exe, err)
	}
	home, err := realhome.Dir()
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: stage %q: %w", exe, err)
	}
	sum := sha256.Sum256(data)
	dir := filepath.Join(home, ".daemonkit", "staged", hex.EncodeToString(sum[:]))
	target := filepath.Join(dir, filepath.Base(exe))
	if err := stage(dir, target, data); err != nil {
		return Program{}, fmt.Errorf("daemonkit: stage %q: %w", exe, err)
	}
	return Program{path: target}, nil
}

// InBundle names an executable inside a signed .app and never copies it:
// macOS keys TCC and notification grants to identifier, team, and path.
// Containment is checked on the symlink-resolved path, so a link inside the
// bundle cannot smuggle in an executable outside it; a bundle path that is
// itself a symlink to a real bundle stays valid.
func InBundle(app, rel string) (Program, error) {
	if !filepath.IsAbs(app) {
		return Program{}, fmt.Errorf("daemonkit: bundle path %q is not absolute", app)
	}
	if !filepath.IsLocal(rel) {
		return Program{}, fmt.Errorf("daemonkit: %q does not stay inside %q", rel, app)
	}
	path := filepath.Join(app, rel)
	info, err := os.Stat(path)
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: bundle executable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Program{}, fmt.Errorf("daemonkit: bundle executable %q is not a regular file", path)
	}
	if info.Mode().Perm()&0o111 == 0 {
		return Program{}, fmt.Errorf("daemonkit: bundle executable %q is not executable", path)
	}
	root, err := filepath.EvalSymlinks(app)
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: resolve bundle %q: %w", app, err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: resolve bundle executable %q: %w", path, err)
	}
	if within, err := filepath.Rel(root, resolved); err != nil || !filepath.IsLocal(within) {
		return Program{}, fmt.Errorf("daemonkit: %q does not stay inside %q", rel, app)
	}
	return Program{path: path}, nil
}

func stage(dir, target string, data []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil { //nolint:gosec // G703: passwd home + fixed literals + hex digest, never caller input
		return err
	}
	tmp, err := os.CreateTemp(dir, ".staging-")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }() //nolint:gosec // G703: CreateTemp's own name inside the digest dir
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o700); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), target); err != nil { //nolint:gosec // G703: both ends derive from the digest dir and filepath.Base
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // G703: passwd home + fixed literals + hex digest, never caller input
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
