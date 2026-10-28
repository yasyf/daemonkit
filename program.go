package daemonkit

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/internal/realhome"
)

// Program is the executable launchd runs, as a policy and never as an action:
// no constructor writes anything. Its two constructors are its two policies, a
// Program carries exactly one of them, and no field is settable. The one
// policy that needs a file put somewhere has that done by Ensure, under the
// start lock that already serializes every other transition of the live
// daemon.
type Program struct{ policy placement }

// placement is one Program's whole policy: where launchd runs the executable
// from, which build that is, and what putting it there takes. Its two
// implementations are Program's two constructors, and nothing outside this
// file can write a third.
type placement interface {
	// path is where the plist's Program key points. It answers before anything
	// is written, so the LaunchAgent can be declared and the process table
	// queried without a placement having run.
	path(el element) (string, error)
	// build is the Health.Build the daemon this program becomes publishes:
	// Serve reports its own Program's build, so the launcher and the daemon
	// read the same answer.
	build() (string, error)
	// place makes path hold build's bytes and reports whether that took a
	// write. Ensure is its only caller and holds the start lock across it.
	place(el element) (bool, error)
}

// Stable is the policy for a daemon launchd runs from a copy of the invoking
// executable, kept at ~/.daemonkit/bin/<Label>. build is the version string
// the executable reports for itself, stamped at build time; it is the whole
// build identity, so a build that should replace a running daemon must carry
// a different one. Stable reads nothing: the copy is Ensure's, made under the
// start lock, because replacing the file launchd is currently executing is a
// transition of the live daemon and belongs to the convergence that owns every
// other one.
//
// The path lies outside any versioned install directory, so a package upgrade
// that deletes the directory this binary was installed into cannot delete the
// program launchd runs; and it does not change across upgrades, so the plist's
// Program key stays put and the TCC grants a bare Mach-O carries on its
// absolute path survive one. Its leaf is the daemon's own Label — the name
// launchd itself enforces unique — so two consumers deploying over each other
// is not a collision to be detected on a root they share, but a state neither
// can write down.
func Stable(build string) (Program, error) {
	if build == "" {
		return Program{}, errors.New("daemonkit: Stable requires the executable's build version")
	}
	exe, err := os.Executable()
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: resolve current executable: %w", err)
	}
	return Program{policy: copied{source: exe, version: build}}, nil
}

// InBundle names an executable inside a signed .app and never copies it: an
// executable relocated out of its bundle loses the entitlements and App Group
// membership it inherits from that bundle, and lands on a different absolute
// path besides. Containment is checked on the symlink-resolved path, so a link
// inside the bundle cannot smuggle in an executable outside it; a bundle path
// that is itself a symlink to a real bundle stays valid.
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
	return Program{policy: bundled{app: app, file: path}}, nil
}

// build is the build Ensure converges on, and the last point at which a Daemon
// whose Program no constructor built is still nameable as the cause: every
// step past here holds an empty path and reports it as one.
func (p Program) build() (string, error) {
	if p.policy == nil {
		return "", errors.New("daemonkit: Daemon.Program is unset: build one with Stable or InBundle")
	}
	return p.policy.build()
}

func (p Program) path(el element) (string, error) { return p.policy.path(el) }

func (p Program) place(el element) (bool, error) { return p.policy.place(el) }

// query is the executable-scoped inventory's query: absolute, because
// DAEMONKIT_HOME is honored verbatim and the kernel names no relative path,
// and otherwise unresolved, because the inventory resolves it itself and
// answers a path naming no file rather than refusing it.
func (p Program) query(el element) (string, error) {
	path, err := p.path(el)
	if err != nil {
		return "", err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("daemonkit: resolve program %q: %w", path, err)
	}
	return absolute, nil
}

// copied runs the daemon from a copy of source, kept at a Label-derived path
// under the one program root every daemonkit consumer shares, with the build
// it holds recorded beside it.
type copied struct {
	source  string
	version string
}

func (c copied) build() (string, error) { return c.version, nil }

func (c copied) path(el element) (string, error) {
	home, err := realhome.Dir()
	if err != nil {
		return "", fmt.Errorf("daemonkit: resolve home directory: %w", err)
	}
	return filepath.Join(home, ".daemonkit", "bin", el.label), nil
}

// place copies source over the program path, and does so only when the build
// recorded beside that path is not this one: re-converging a settled system
// reads one small file, rewrites nothing, and leaves the inode launchd already
// knows alone. The executable is written before its build record, so a crash
// between the two leaves a record naming the old build and the next place
// copies again.
func (c copied) place(el element) (bool, error) {
	target, err := c.path(el)
	if err != nil {
		return false, err
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, fmt.Errorf("daemonkit: create program dir %q: %w", dir, err)
	}
	held, err := placedBuild(target)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, fmt.Errorf("daemonkit: read program %q: %w", target, err)
	}
	if held == c.version {
		return false, nil
	}
	data, err := os.ReadFile(c.source)
	if err != nil {
		return false, fmt.Errorf("daemonkit: read executable %q: %w", c.source, err)
	}
	if err := durable.WriteFile(target, data, 0o700); err != nil {
		return false, fmt.Errorf("daemonkit: place %q at %q: %w", c.source, target, err)
	}
	if err := durable.WriteFile(buildRecord(target), []byte(c.version), 0o600); err != nil {
		return false, fmt.Errorf("daemonkit: record the build placed at %q: %w", target, err)
	}
	return true, nil
}

// bundled runs the daemon from an executable already in place inside a signed
// .app: the bundle names the path, so the Label decides nothing, and there is
// nothing to write.
type bundled struct {
	app  string
	file string
}

// build re-reads the bundle's CFBundleShortVersionString every time it is
// asked, and carries nothing to go stale: an .app upgraded under a long-lived
// Client is the build the daemon already publishes.
func (b bundled) build() (string, error) {
	version, err := bundle.ShortVersion(b.app)
	if err != nil {
		return "", fmt.Errorf("daemonkit: read bundle version of %q: %w", b.app, err)
	}
	return version, nil
}

func (b bundled) path(element) (string, error) { return b.file, nil }

func (b bundled) place(element) (bool, error) { return false, nil }

// placedBuild is the build recorded beside path, and empty when path itself is
// not a regular file: a program path that outlives every upgrade is a name a
// writer can point somewhere else, and anything but a regular file holds none
// of the bytes this build placed, so the rename replaces it.
func placedBuild(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", nil
	}
	data, err := os.ReadFile(buildRecord(path))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func buildRecord(path string) string { return path + ".build" }
