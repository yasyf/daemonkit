package launchd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/durablefile"
	"github.com/yasyf/daemonkit/internal/realhome"
)

const (
	bootstrapAttempts  = 3
	bootstrapBaseDelay = 200 * time.Millisecond
	launchctlPath      = "/bin/launchctl"
)

// ownerMarkerKey is the rendered EnvironmentVariables key every daemonkit plist
// carries. Its presence in an on-disk plist is convergence's store-free proof
// of daemonkit ownership.
var ownerMarkerKey = []byte("<key>" + OwnerEnvKey + "</key>")

// Runner runs one command to completion and returns its combined stdout and
// stderr, its exit code, and any error that prevented it from running. Converge
// drives /bin/launchctl through a Runner so the process boundary stays mockable
// and the caller owns durable process supervision.
type Runner func(ctx context.Context, path string, args ...string) (output string, code int, err error)

// Converge drives launchd to exactly the desired agent set. It is the stateless
// reconcile primitive: it discovers the currently installed daemonkit agents
// from ~/Library/LaunchAgents by the DAEMONKIT_AGENT_OWNER marker, removes any
// marked agent absent from the desired set, and installs or repairs each desired
// agent. A pre-marker plist (or any foreign plist) occupying a desired label is
// archived aside before daemonkit takes the label over, so nothing is orphaned.
func Converge(ctx context.Context, run Runner, agents []Agent) error {
	if run == nil {
		return errors.New("launchd: converge runner is required")
	}
	desired, err := desiredAgents(agents)
	if err != nil {
		return err
	}
	c := converger{run: run, wait: waitRetry}
	return c.reconcile(ctx, desired)
}

type converger struct {
	run  Runner
	wait func(context.Context, time.Duration) error
}

func (c converger) reconcile(ctx context.Context, desired map[string]Agent) error {
	owned, err := discoverOwnedLabels()
	if err != nil {
		return err
	}
	for _, label := range slices.Sorted(maps.Keys(owned)) {
		if _, keep := desired[label]; keep {
			continue
		}
		if err := c.uninstall(ctx, label); err != nil {
			return fmt.Errorf("launchd: remove stale agent %q: %w", label, err)
		}
	}
	for _, label := range slices.Sorted(maps.Keys(desired)) {
		agent := desired[label]
		if err := validateProgramTree(agent); err != nil {
			if !programMissing(err) {
				return err
			}
			slog.Warn("launchd: desired agent program is not serviceable; awaiting reconverge", "label", label, "error", err)
			continue
		}
		exact, err := c.verify(ctx, agent)
		if err != nil {
			return fmt.Errorf("launchd: verify agent %q: %w", label, err)
		}
		if exact {
			continue
		}
		if err := c.install(ctx, agent); err != nil {
			return fmt.Errorf("launchd: install agent %q: %w", label, err)
		}
	}
	return nil
}

// verify observes without mutating: it reports a stale stored program as drift
// so a later Converge with a serviceable program can heal it, and leaves every
// launchd mutation to install.
func (c converger) verify(ctx context.Context, agent Agent) (bool, error) {
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

func (c converger) install(ctx context.Context, agent Agent) error {
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

func (c converger) uninstall(ctx context.Context, label string) error {
	if bootout := c.launchctl(ctx, "bootout", serviceTarget(label)); !bootout.settled() {
		return bootout.fail()
	}
	path, err := plistPath(label)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("launchd: remove agent plist: %w", err)
	} else if err == nil {
		if err := durablefile.SyncDir(filepath.Dir(path)); err != nil {
			return fmt.Errorf("launchd: persist agent plist removal: %w", err)
		}
	}
	return nil
}

func (c converger) reload(ctx context.Context, agent Agent, path string) error {
	delay := bootstrapBaseDelay
	var lastErr error
	for attempt := 1; attempt <= bootstrapAttempts; attempt++ {
		outcome := c.loadOnce(ctx, agent, path)
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
		"%w (gave up after %d attempts bootstrapping %q; launchd reported the operation still in progress every time)",
		lastErr, bootstrapAttempts, path,
	)
}

// loadOnce runs one bootout → enable → bootstrap → kickstart pass and returns
// the classified outcome that stopped it, or the successful kickstart.
func (c converger) loadOnce(ctx context.Context, agent Agent, path string) launchctlResult {
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

func (c converger) launchctl(ctx context.Context, args ...string) launchctlResult {
	out, code, err := c.run(ctx, launchctlPath, args...)
	return launchctlOutcome(args[0], out, code, err)
}

// discoverOwnedLabels returns the label → plist path of every daemonkit-owned
// LaunchAgent on disk, identified by the ownership marker. It is convergence's
// store-free view of what is currently applied.
func discoverOwnedLabels() (map[string]string, error) {
	home, err := realhome.Dir()
	if err != nil {
		return nil, fmt.Errorf("launchd: resolve home dir: %w", err)
	}
	dir := filepath.Join(home, "Library", "LaunchAgents")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("launchd: list launch agents: %w", err)
	}
	owned := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".plist") {
			continue
		}
		label := strings.TrimSuffix(name, ".plist")
		if validateLabel(label) != nil {
			continue
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path) //nolint:gosec // scanning the caller's own LaunchAgents directory
		if err != nil {
			return nil, fmt.Errorf("launchd: read launch agent %q: %w", path, err)
		}
		if plistHasOwnerMarker(data) {
			owned[label] = path
		}
	}
	return owned, nil
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
		if _, err := agent.Plist(); err != nil {
			return nil, fmt.Errorf("launchd: validate agent %q: %w", agent.Label, err)
		}
		if err := validateProgramTree(agent); err != nil {
			return nil, fmt.Errorf("launchd: validate agent %q: %w", agent.Label, err)
		}
		if _, duplicate := desired[agent.Label]; duplicate {
			return nil, fmt.Errorf("launchd: duplicate agent label %q", agent.Label)
		}
		acceptIgnoredSessionType(&agent)
		agent.Args = append([]string(nil), agent.Args...)
		agent.Env = cloneStrings(agent.Env)
		agent.AssociatedBundleIdentifiers, _ = canonicalAssociatedBundleIdentifiers(
			agent.AssociatedBundleIdentifiers,
		)
		desired[agent.Label] = agent
	}
	return desired, nil
}

// validateProgramTree is a live-filesystem check: it walks the program path from
// the root, refusing any symlinked component, a non-directory ancestor, or a
// final component that is not a regular executable file. It is convergence's
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
