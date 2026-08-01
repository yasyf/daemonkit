//go:build darwin

package launchd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/durablefile"
)

const (
	bootstrapAttempts  = 3
	bootstrapBaseDelay = 200 * time.Millisecond
	launchctlPath      = "/bin/launchctl"
)

// ownerMarkerKey is the rendered EnvironmentVariables key every daemonkit plist
// carries. Its presence in the plist at one exact label is that label's
// store-free proof of daemonkit ownership; it is never an index of what
// daemonkit owns machine-wide, which daemonkit never asks.
var ownerMarkerKey = []byte("<key>" + OwnerEnvKey + "</key>")

// ErrNotOwned means the plist sitting at daemonkit's path for a label carries
// no ownership marker. Remove refuses it untouched, since the job launchd
// holds there is a third party's to stop.
var ErrNotOwned = errors.New("launchd: agent plist carries no " + OwnerEnvKey + " ownership marker")

// Runner runs one command to completion and returns its combined stdout and
// stderr, its exit code, and any error that prevented it from running. Apply and
// Remove drive /bin/launchctl through a Runner so the process boundary stays
// mockable and the caller owns durable process supervision.
//
// The code is launchctl's own exit status and nothing else: a command that
// never ran produced none, so a Runner reporting the error leaves the code at
// its zero value rather than inventing a status. Only a positive code is read
// as one launchctl exited with.
type Runner func(ctx context.Context, path string, args ...string) (output string, code int, err error)

// Apply installs or repairs exactly the one named agent and always kickstarts
// it. A drifted, missing, or unloaded agent is written and reloaded; an agent
// whose on-disk plist is already byte-exact and whose job launchd reports loaded
// is kickstarted anyway, because a RestartOnFailure or NoRestart job that exited
// cleanly is byte-exact, loaded, and dead. A pre-cut markerless plist at the
// label is adopted, and a foreign one is archived aside, so daemonkit takes a
// label over without orphaning what sat there.
//
// Apply touches no label but this one: daemonkit never scans for what it owns,
// so one consumer's agents can never collide with another's.
func Apply(ctx context.Context, run Runner, agent Agent) error {
	if run == nil {
		return errors.New("launchd: apply runner is required")
	}
	desired, err := canonicalAgent(agent)
	if err != nil {
		return err
	}
	c := applier{run: run, wait: waitRetry}
	return c.apply(ctx, desired)
}

// Verify reports whether the one named agent is already exactly applied: a
// serviceable program, the byte-exact plist at 0600 where launchd reads it, and
// launchd itself reporting the job bootstrapped. It is [Apply]'s own
// observation, mutating nothing, so a caller that decides from what is on the
// system observes here and repairs there rather than re-deriving the answer.
//
// Like Apply it touches no label but this one, and a launchd that could not be
// asked at all is an error. A missing plist is reported as simply not applied,
// the drift Apply heals. A missing program is reported as not applied too, but
// it is not drift Apply heals: Apply validates the program tree first and
// refuses outright, because installing an agent whose program is not there
// only hands launchd a job that cannot run.
func Verify(ctx context.Context, run Runner, agent Agent) (bool, error) {
	if run == nil {
		return false, errors.New("launchd: verify runner is required")
	}
	return applier{run: run, wait: waitRetry}.verify(ctx, agent)
}

// Remove boots out and deletes exactly the one plist daemonkit owns at the
// named label. Ownership is proved by the marker in the plist at daemonkit's
// own path and nowhere else: a plist that carries none is refused untouched
// with [ErrNotOwned], and a label with no plist there is success — daemonkit
// registered nothing under it, and it never boots out a job it cannot prove is
// its own. A label launchd does not know is success too, so a repeated Remove
// is a no-op.
func Remove(ctx context.Context, run Runner, label string) error {
	if run == nil {
		return errors.New("launchd: remove runner is required")
	}
	if err := validateLabel(label); err != nil {
		return err
	}
	c := applier{run: run, wait: waitRetry}
	return c.remove(ctx, label)
}

type applier struct {
	run  Runner
	wait func(context.Context, time.Duration) error
}

func (c applier) apply(ctx context.Context, agent Agent) error {
	exact, err := c.verify(ctx, agent)
	if err != nil {
		return fmt.Errorf("launchd: verify agent %q: %w", agent.Label, err)
	}
	if exact {
		if err := c.kickstart(ctx, agent.Label); err != nil {
			return fmt.Errorf("launchd: kickstart agent %q: %w", agent.Label, err)
		}
		return nil
	}
	if err := c.install(ctx, agent); err != nil {
		return fmt.Errorf("launchd: install agent %q: %w", agent.Label, err)
	}
	return nil
}

func (c applier) remove(ctx context.Context, label string) error {
	path, err := plistPath(label)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path) //nolint:gosec // exact agent-owned plist path
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("launchd: inspect agent plist %q: %w", path, err)
	}
	if !plistHasOwnerMarker(data) {
		return fmt.Errorf("%w: %q", ErrNotOwned, path)
	}
	if bootout := c.launchctl(ctx, "bootout", serviceTarget(label)); !bootout.settled() {
		return fmt.Errorf("launchd: remove agent %q: %w", label, bootout.fail())
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("launchd: remove agent plist: %w", err)
	}
	if err := durablefile.SyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("launchd: persist agent plist removal: %w", err)
	}
	return nil
}

// verify observes without mutating: it reports a stale stored program as drift
// so a later Apply with a serviceable program can heal it, and leaves every
// launchd mutation to install.
func (c applier) verify(ctx context.Context, agent Agent) (bool, error) {
	if err := validateProgramTree(agent); err != nil {
		if programMissing(err) {
			return false, nil
		}
		return false, err
	}
	want, err := agent.Plist()
	if err != nil {
		return false, err
	}
	path, err := agent.PlistPath()
	if err != nil {
		return false, err
	}
	got, err := os.ReadFile(path) //nolint:gosec // exact agent-owned plist path
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("launchd: read agent plist: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("launchd: inspect agent plist: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !bytes.Equal(got, want) {
		return false, nil
	}
	outcome := c.launchctl(ctx, "print", serviceTarget(agent.Label))
	if outcome.kind == launchctlNotLoaded {
		return false, nil
	}
	if outcome.kind != launchctlLoaded {
		return false, outcome.fail()
	}
	return true, nil
}

func (c applier) install(ctx context.Context, agent Agent) error {
	if err := validateProgramTree(agent); err != nil {
		return err
	}
	plist, err := agent.Plist()
	if err != nil {
		return err
	}
	path, err := agent.PlistPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(agent.LogPath), 0o700); err != nil {
		return fmt.Errorf("launchd: create log directory: %w", err)
	}
	if err := archiveForeignPlist(path); err != nil {
		return err
	}
	if err := durablefile.WriteFileDurable(path, plist, 0o600); err != nil {
		return fmt.Errorf("launchd: write agent plist: %w", err)
	}
	return c.reload(ctx, agent, path)
}

func (c applier) reload(ctx context.Context, agent Agent, path string) error {
	return c.settle(ctx, fmt.Sprintf("bootstrapping %q", path), func() launchctlResult {
		return c.loadOnce(ctx, agent, path)
	})
}

func (c applier) kickstart(ctx context.Context, label string) error {
	return c.settle(ctx, fmt.Sprintf("kickstarting %q", label), func() launchctlResult {
		return c.launchctl(ctx, "kickstart", serviceTarget(label))
	})
}

// settle runs one launchctl pass until launchd stops reporting the operation in
// progress, the only status that is positive evidence the same call can later
// succeed. Every other failure is returned on the first attempt.
func (c applier) settle(ctx context.Context, subject string, once func() launchctlResult) error {
	delay := bootstrapBaseDelay
	var lastErr error
	for attempt := 1; attempt <= bootstrapAttempts; attempt++ {
		outcome := once()
		lastErr = outcome.fail()
		if lastErr == nil {
			return nil
		}
		if outcome.kind != launchctlInFlux {
			return lastErr
		}
		if attempt == bootstrapAttempts {
			break
		}
		if err := c.wait(ctx, delay); err != nil {
			return err
		}
		delay *= 2
	}
	return fmt.Errorf(
		"%w (gave up after %d attempts %s; launchd reported the operation still in progress every time)",
		lastErr, bootstrapAttempts, subject,
	)
}

// loadOnce runs one bootout → enable → bootstrap → kickstart pass and returns
// the classified outcome that stopped it, or the successful kickstart.
func (c applier) loadOnce(ctx context.Context, agent Agent, path string) launchctlResult {
	bootout := c.launchctl(ctx, "bootout", serviceTarget(agent.Label))
	if !bootout.settled() {
		return bootout
	}
	if enable := c.launchctl(ctx, "enable", serviceTarget(agent.Label)); enable.fail() != nil {
		return enable
	}
	if bootstrap := c.launchctl(ctx, "bootstrap", domainTarget(), path); bootstrap.fail() != nil {
		return bootstrap
	}
	return c.launchctl(ctx, "kickstart", serviceTarget(agent.Label))
}

func (c applier) launchctl(ctx context.Context, args ...string) launchctlResult {
	out, code, err := c.run(ctx, launchctlPath, args...)
	return launchctlOutcome(args[0], out, code, err)
}

// archiveForeignPlist renames aside any existing plist at path that does not
// carry the ownership marker, so daemonkit adopts a label without clobbering a
// pre-marker or third-party file. An already-owned plist is left for install to
// overwrite in place.
func archiveForeignPlist(path string) error {
	data, err := os.ReadFile(path) //nolint:gosec // exact agent-owned plist path
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("launchd: inspect existing plist %q: %w", path, err)
	}
	if plistHasOwnerMarker(data) {
		return nil
	}
	backup := path + ".daemonkit-archived-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := os.Rename(path, backup); err != nil {
		return fmt.Errorf("launchd: archive foreign plist %q: %w", path, err)
	}
	if err := durablefile.SyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("launchd: persist foreign plist archive: %w", err)
	}
	slog.Warn("launchd: archived foreign LaunchAgent plist before adopting the label", "path", path, "backup", backup)
	return nil
}

func plistHasOwnerMarker(data []byte) bool {
	return bytes.Contains(data, ownerMarkerKey)
}

func desiredAgents(agents []Agent) (map[string]Agent, error) {
	desired := make(map[string]Agent, len(agents))
	for _, agent := range agents {
		canonical, err := canonicalAgent(agent)
		if err != nil {
			return nil, err
		}
		if _, duplicate := desired[canonical.Label]; duplicate {
			return nil, fmt.Errorf("launchd: duplicate agent label %q", canonical.Label)
		}
		desired[canonical.Label] = canonical
	}
	return desired, nil
}

// canonicalAgent validates one agent against both the plist grammar and the
// live filesystem, and returns it with every reference-typed field copied so a
// caller's later mutation cannot reach what daemonkit applies.
func canonicalAgent(agent Agent) (Agent, error) {
	if _, err := agent.Plist(); err != nil {
		return Agent{}, fmt.Errorf("launchd: validate agent %q: %w", agent.Label, err)
	}
	if err := validateProgramTree(agent); err != nil {
		return Agent{}, fmt.Errorf("launchd: validate agent %q: %w", agent.Label, err)
	}
	acceptIgnoredSessionType(&agent)
	agent.Args = append([]string(nil), agent.Args...)
	agent.Env = cloneStrings(agent.Env)
	agent.AssociatedBundleIdentifiers, _ = canonicalAssociatedBundleIdentifiers(
		agent.AssociatedBundleIdentifiers,
	)
	return agent, nil
}

// validateProgramTree is a live-filesystem check: it walks the program path from
// the root, refusing any symlinked component, a non-directory ancestor, or a
// final component that is not a regular executable file. It is Apply's
// serviceability gate, and NewPlan invokes it to require program residency at
// plan creation.
func validateProgramTree(agent Agent) error {
	program, err := agent.programPath()
	if err != nil {
		return err
	}
	current := string(filepath.Separator)
	root, err := os.Lstat(current)
	if err != nil {
		return fmt.Errorf("launchd: inspect program root: %w", err)
	}
	if root.Mode()&os.ModeSymlink != 0 || !root.IsDir() {
		return errors.New("launchd: program root is not a real directory")
	}
	parts := strings.Split(strings.TrimPrefix(program, current), current)
	for index, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("launchd: inspect program path %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("launchd: program path %q is a symlink", current)
		}
		if index < len(parts)-1 {
			if !info.IsDir() {
				return fmt.Errorf("launchd: program ancestor %q is not a directory", current)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("launchd: program %q is not a regular file", current)
		}
		if info.Mode().Perm()&0o111 == 0 {
			return fmt.Errorf("launchd: program %q is not executable", current)
		}
	}
	return nil
}

func programMissing(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func domainTarget() string { return "gui/" + strconv.Itoa(os.Getuid()) }

func serviceTarget(label string) string { return domainTarget() + "/" + label }
