package daemonkit

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

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
	// build is the Health.Build the daemon this program becomes will publish,
	// since Serve digests its own executable. When it is read is the policy's
	// own answer and not one arm's inherited from the other: a policy that
	// deploys bytes owes the launcher the ones it deployed and freezes them,
	// and a policy that deploys nothing owes the launcher whatever is at the
	// path launchd execs and re-reads it.
	build() (string, error)
	// place makes path hold build's bytes and reports whether that took a
	// write. Ensure is its only caller and holds the start lock across it.
	place(el element) (bool, error)
}

// Stable is the policy for a daemon launchd runs from a copy of the invoking
// executable, kept at ~/.daemonkit/bin/<Label>. It reads that executable to
// learn which build it is and places nothing: the copy is Ensure's, made under
// the start lock, because replacing the file launchd is currently executing is
// a transition of the live daemon and belongs to the convergence that owns
// every other one.
//
// The path lies outside any versioned install directory, so a package upgrade
// that deletes the directory this binary was installed into cannot delete the
// program launchd runs; and it does not change across upgrades, so the plist's
// Program key stays put and the TCC grants a bare Mach-O carries on its
// absolute path survive one. Its leaf is the daemon's own Label — the name
// launchd itself enforces unique — so two consumers deploying over each other
// is not a collision to be detected on a root they share, but a state neither
// can write down.
func Stable() (Program, error) {
	exe, err := os.Executable()
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: resolve current executable: %w", err)
	}
	data, err := os.ReadFile(exe)
	if err != nil {
		return Program{}, fmt.Errorf("daemonkit: read executable %q: %w", exe, err)
	}
	return Program{policy: copied{source: exe, digest: digest(data)}}, nil
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
	return Program{policy: bundled{file: path}}, nil
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

// resolved is the program path in the form the kernel reports an executable:
// absolute and symlink-free. A path that resolves to nothing is an error, never
// an empty answer — a caller comparing against the kernel's own paths would
// otherwise match nothing and read that as proof.
func (p Program) resolved(el element) (string, error) {
	path, err := p.path(el)
	if err != nil {
		return "", err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("daemonkit: resolve program %q: %w", path, err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("daemonkit: resolve program %q: %w", path, err)
	}
	return resolved, nil
}

// copied runs the daemon from a copy of source, kept at a Label-derived path
// under the one program root every daemonkit consumer shares.
type copied struct {
	source string
	digest string
}

// build is frozen at construction. The source is this launcher's own
// executable, so bytes that no longer hash to it mean someone replaced the
// binary while it was running — an anomaly place refuses out loud, never a new
// intent to deploy.
func (c copied) build() (string, error) { return c.digest, nil }

func (c copied) path(el element) (string, error) {
	home, err := realhome.Dir()
	if err != nil {
		return "", fmt.Errorf("daemonkit: resolve home directory: %w", err)
	}
	return filepath.Join(home, ".daemonkit", "bin", el.label), nil
}

// place copies source over the program path, and does so only when that path
// does not already hold these bytes: re-converging a settled system rewrites
// nothing and leaves the inode launchd already knows alone.
//
// The source is re-read here rather than carried, so a Program held by a
// long-lived daemon pins no executable image in memory, and the digest the
// constructor took is what says the two reads saw the same build — an
// executable replaced under a running launcher is refused rather than deployed
// as a build nobody asked for.
func (c copied) place(el element) (bool, error) {
	target, err := c.path(el)
	if err != nil {
		return false, err
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, fmt.Errorf("daemonkit: create program dir %q: %w", dir, err)
	}
	held, err := placedDigest(target)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, fmt.Errorf("daemonkit: read program %q: %w", target, err)
	}
	if held == c.digest {
		return false, nil
	}
	data, err := os.ReadFile(c.source)
	if err != nil {
		return false, fmt.Errorf("daemonkit: read executable %q: %w", c.source, err)
	}
	if placing := digest(data); placing != c.digest {
		return false, fmt.Errorf(
			"daemonkit: executable %q is now build %q, this Program was built from %q",
			c.source, placing, c.digest,
		)
	}
	if err := durable.WriteFile(target, data, 0o700); err != nil {
		return false, fmt.Errorf("daemonkit: place %q at %q: %w", c.source, target, err)
	}
	return true, nil
}

// bundled runs the daemon from an executable already in place inside a signed
// .app: the bundle names the path, so the Label decides nothing, and there is
// nothing to write.
type bundled struct{ file string }

// build re-reads the bundle every time it is asked, and carries no digest to go
// stale. Nothing here deploys, so the bytes at file are the ones launchd execs
// whoever put them there: an .app upgraded under a long-lived Client is the
// build the daemon already publishes. The freeze copied needs is a launcher's
// own deployment intent being overwritten by another launcher's copy, which is
// a hazard of copying.
//
// The residual the re-read carries is a daemon launchd restarted at the instant
// an installer replaced the bundle executable. The process execs the old image
// while serve.go's buildDigest reads the new bytes off the same path
// microseconds later, so it publishes and records a build it is not running.
// This read agrees with that record, decide answers ActionNothing, and Ensure
// reports converged on a build the daemon does not execute — permanently, since
// nothing on this policy ever disagrees. copied answers the same interleaving
// with an eviction it can force; bundled cannot, because daemonkit does not
// deploy those bytes. Closing it is serve.go's TODO.
func (b bundled) build() (string, error) {
	data, err := os.ReadFile(b.file)
	if err != nil {
		return "", fmt.Errorf("daemonkit: read bundle executable %q: %w", b.file, err)
	}
	return digest(data), nil
}

func (b bundled) path(element) (string, error) { return b.file, nil }

func (b bundled) place(element) (bool, error) { return false, nil }

// placedDigest is the digest of the bytes path itself holds, and it does not
// follow the final component: a program path that outlives every upgrade is a
// name a same-uid writer can point somewhere else, and reading through that
// link would answer with the bytes it aims at while launchd execs whatever
// they are swapped for afterwards. Anything but a regular file holds none of
// these bytes, so the rename replaces it.
func placedDigest(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return digest(data), nil
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
