package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const (
	controllerCloseBound   = 30 * time.Second
	bootstrapAttempts      = 6
	bootstrapBaseDelay     = 200 * time.Millisecond
	launchctlNotLoadedExit = 3
	launchctlInFluxExit    = 5
)

// ErrControllerClosed means a service convergence request arrived after drain began.
var ErrControllerClosed = errors.New("service: controller is closed")

// ControllerConfig names the durable service and worker state plus the bounded
// disposable-worker capacity. Every path must be exact, absolute, and distinct.
type ControllerConfig struct {
	StatePath   string
	ProcessPath string
	WorkerLimit int
}

func (c ControllerConfig) validate() error {
	if err := exactControllerPath(c.StatePath); err != nil {
		return err
	}
	if err := exactControllerPath(c.ProcessPath); err != nil {
		return fmt.Errorf("service: process state: %w", err)
	}
	if c.StatePath == c.ProcessPath {
		return errors.New("service: controller and process state paths must be distinct")
	}
	if c.WorkerLimit <= 0 {
		return errors.New("service: controller worker limit must be positive")
	}
	return nil
}

type controllerRuntime interface {
	supervise.TaskRunner
	Recover(context.Context) error
	Close()
	Cancel()
	Wait(context.Context) error
}

type serviceReceiptRecovery interface {
	RecoverReapReceipts(
		context.Context,
		proc.RecoveryClass,
		func(context.Context, proc.ReapReceipt) error,
	) (proc.ReapReceiptFloor, error)
}

// Controller owns one exact durable LaunchAgent set and every launchctl worker
// used to converge it.
type Controller struct {
	config     ControllerConfig
	runtime    controllerRuntime
	receipts   serviceReceiptRecovery
	store      controllerStateStore
	retryWait  func(context.Context, time.Duration) error
	stopReaper *proc.Reaper
	stopTiming stopControlTiming

	replacementProcesses func(string) ([]proc.Identity, error)
	replacementNow       func() time.Time
	replacementWait      func(context.Context, time.Duration) error

	// Lock order is opMu, then store/launchctl/process inventory. Product stop,
	// readiness, and deployment callbacks run only between controller calls.
	opMu  sync.Mutex
	state controllerState

	mu           sync.Mutex
	closed       bool
	activeCancel context.CancelFunc
	activeDone   chan struct{}

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// Status is the controller-owned launch service state. Product runtime health
// remains the authority for application readiness.
type Status struct {
	Label   string
	Desired bool
	Applied bool
	Loaded  bool
	Exact   bool
}

// NewController opens the durable controller, settles prior workers, restores
// the persisted exact desired set, and acknowledges service recovery receipts
// only after that state has converged.
func NewController(ctx context.Context, config ControllerConfig) (*Controller, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	store, err := openControllerStore(ctx, config.StatePath)
	if err != nil {
		return nil, err
	}
	generation, err := proc.ProcessGeneration()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("service: derive process generation: %w", err)
	}
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: config.ProcessPath},
		Generation: generation,
	}
	runtime, err := supervise.NewPool(config.WorkerLimit, reaper)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	controller, err := newControllerWithRuntime(ctx, config, runtime, reaper, store)
	if err != nil {
		return nil, err
	}
	controller.stopReaper = reaper
	return controller, nil
}

func newControllerWithRuntime(
	ctx context.Context,
	config ControllerConfig,
	runtime controllerRuntime,
	receipts serviceReceiptRecovery,
	store controllerStateStore,
) (_ *Controller, err error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if runtime == nil || receipts == nil || store == nil {
		return nil, errors.New("service: controller runtime, receipt recovery, and state store are required")
	}
	controller := &Controller{
		config: config, runtime: runtime, receipts: receipts, store: store,
		retryWait:            waitServiceRetry,
		replacementProcesses: proc.ExecutableIdentities,
		replacementNow:       time.Now,
		replacementWait:      defaultReplacementWait,
		closeDone:            make(chan struct{}),
	}
	defer func() {
		if err == nil {
			return
		}
		runtime.Close()
		runtime.Cancel()
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controllerCloseBound)
		defer cancel()
		_ = runtime.Wait(waitCtx)
		_ = store.Close()
	}()
	if err := runtime.Recover(ctx); err != nil {
		return nil, err
	}
	state, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	controller.state = state
	if err := controller.reconcile(ctx, state.Applied, state.Desired); err != nil {
		return nil, fmt.Errorf("service: recover desired set: %w", err)
	}
	if _, err := receipts.RecoverReapReceipts(
		ctx,
		proc.RecoveryService,
		func(context.Context, proc.ReapReceipt) error { return nil },
	); err != nil {
		return nil, fmt.Errorf("service: acknowledge recovered service workers: %w", err)
	}
	if _, err := receipts.RecoverReapReceipts(
		ctx,
		proc.RecoveryStopControl,
		func(context.Context, proc.ReapReceipt) error { return nil },
	); err != nil {
		return nil, fmt.Errorf("service: acknowledge recovered stop controls: %w", err)
	}
	return controller, nil
}

// Converge durably records agents as the complete desired set before applying
// any effects, then records that exact set as applied only after every effect
// succeeds. Repeating the same set verifies its plist and launchd registration,
// repairing drift without reloading an already exact agent.
func (c *Controller) Converge(ctx context.Context, agents []Agent) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if c.state.Replacement != nil {
		return ErrQuiesced
	}
	desired, err := desiredAgents(agents)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(c.state.Desired, desired) {
		prior, err := c.store.ReplaceDesired(opCtx, desired)
		if err != nil {
			return err
		}
		c.state = controllerState{Desired: copyAgents(desired), Applied: copyAgents(prior.Applied)}
	}
	return c.reconcile(opCtx, copyAgents(c.state.Applied), c.state.Desired)
}

// Status reports durable desired/applied state and current launchd ownership
// without opening a protected runtime connection.
func (c *Controller) Status(ctx context.Context, label string) (Status, error) {
	if err := validateLabel(label); err != nil {
		return Status{}, err
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return Status{}, err
	}
	defer finish()

	desired, isDesired := c.state.Desired[label]
	applied, isApplied := c.state.Applied[label]
	status := Status{Label: label, Desired: isDesired, Applied: isApplied}
	_, printErr := c.launchctl(opCtx, "print", serviceTarget(label))
	switch launchctlExitCode(printErr) {
	case launchctlNotLoadedExit:
		return status, nil
	case -1:
		if printErr != nil {
			return Status{}, fmt.Errorf("service: inspect agent %q: %w", label, printErr)
		}
	default:
		return Status{}, fmt.Errorf("service: inspect agent %q: %w", label, printErr)
	}
	status.Loaded = true
	if !isDesired || !isApplied || !reflect.DeepEqual(desired, applied) {
		return status, nil
	}
	exact, err := c.verify(opCtx, desired)
	if err != nil {
		return Status{}, fmt.Errorf("service: verify agent %q: %w", label, err)
	}
	status.Exact = exact
	return status, nil
}

// Close rejects new convergence, waits for admitted service workers, cancels
// them only when the caller's earlier live deadline or the internal bound is
// reached, then releases durable controller ownership.
func (c *Controller) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		activeDone := c.activeDone
		activeCancel := c.activeCancel
		c.mu.Unlock()
		closeCtx, cancel := controllerCloseContext(ctx)
		go func() {
			defer close(c.closeDone)
			defer cancel()
			c.closeErr = c.settleClose(closeCtx, activeDone, activeCancel)
		}()
	})
	select {
	case <-c.closeDone:
		return c.closeErr
	case <-ctx.Done():
		// The owned close continues under its fresh bounded context.
		<-c.closeDone
		return errors.Join(ctx.Err(), c.closeErr)
	}
}

func (c *Controller) settleClose(
	ctx context.Context,
	activeDone <-chan struct{},
	activeCancel context.CancelFunc,
) error {
	var closeErr error
	if activeDone != nil {
		select {
		case <-activeDone:
		case <-ctx.Done():
			activeCancel()
			<-activeDone
			closeErr = ctx.Err()
		}
	}
	c.runtime.Close()
	waited := make(chan error, 1)
	go func() { waited <- c.runtime.Wait(context.WithoutCancel(ctx)) }()
	var waitErr error
	select {
	case waitErr = <-waited:
	case <-ctx.Done():
		c.runtime.Cancel()
		waitErr = errors.Join(ctx.Err(), <-waited)
	}
	return errors.Join(closeErr, waitErr, c.store.Close())
}

func controllerCloseContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok && ctx.Err() == nil && time.Until(deadline) < controllerCloseBound {
		return context.WithDeadline(base, deadline)
	}
	return context.WithTimeout(base, controllerCloseBound)
}

func (c *Controller) admit(ctx context.Context) (context.Context, func(), error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, ErrControllerClosed
	}
	opCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.activeCancel = cancel
	c.activeDone = done
	c.mu.Unlock()
	finish := func() {
		cancel()
		c.mu.Lock()
		if c.activeDone == done {
			c.activeCancel = nil
			c.activeDone = nil
		}
		close(done)
		c.mu.Unlock()
	}
	return opCtx, finish, nil
}

func (c *Controller) reconcile(
	ctx context.Context,
	applied map[string]Agent,
	desired map[string]Agent,
) error {
	for _, label := range slices.Sorted(maps.Keys(applied)) {
		if _, keep := desired[label]; keep {
			continue
		}
		if err := c.uninstall(ctx, applied[label]); err != nil {
			return fmt.Errorf("service: remove stale agent %q: %w", label, err)
		}
		if err := c.store.SetApplied(ctx, label, nil); err != nil {
			return fmt.Errorf("service: commit removed agent %q: %w", label, err)
		}
		delete(c.state.Applied, label)
	}
	for _, label := range slices.Sorted(maps.Keys(desired)) {
		if previous, ok := applied[label]; ok && reflect.DeepEqual(previous, desired[label]) {
			verified, err := c.verify(ctx, desired[label])
			if err != nil {
				return fmt.Errorf("service: verify agent %q: %w", label, err)
			}
			if verified {
				continue
			}
		}
		if err := c.install(ctx, desired[label]); err != nil {
			return fmt.Errorf("service: install agent %q: %w", label, err)
		}
		agent := desired[label]
		if err := c.store.SetApplied(ctx, label, &agent); err != nil {
			return fmt.Errorf("service: commit applied agent %q: %w", label, err)
		}
		c.state.Applied[label] = agent
	}
	return nil
}

func (c *Controller) verify(ctx context.Context, agent Agent) (bool, error) {
	if err := validateProgramTree(agent); err != nil {
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
	got, err := os.ReadFile(path) //nolint:gosec // exact controller-owned plist path
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read agent plist: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("inspect agent plist: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !bytes.Equal(got, want) {
		return false, nil
	}
	out, err := c.launchctl(ctx, "print", serviceTarget(agent.Label))
	if err != nil {
		if launchctlExitCode(err) == launchctlNotLoadedExit {
			return false, nil
		}
		return false, fmt.Errorf("launchctl print: %w: %s", err, strings.TrimSpace(out))
	}
	return true, nil
}

func (c *Controller) install(ctx context.Context, agent Agent) error {
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
		return fmt.Errorf("service: create log directory: %w", err)
	}
	if err := dkdaemon.WriteFileDurable(path, plist, 0o600); err != nil {
		return fmt.Errorf("service: write agent plist: %w", err)
	}
	if err := c.reload(ctx, agent, path); err != nil {
		return err
	}
	if out, err := c.launchctl(ctx, "enable", serviceTarget(agent.Label)); err != nil {
		return fmt.Errorf("launchctl enable: %w: %s", err, strings.TrimSpace(out))
	}
	if out, err := c.launchctl(ctx, "kickstart", serviceTarget(agent.Label)); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, strings.TrimSpace(out))
	}
	return nil
}

func validateProgramTree(agent Agent) error {
	program, err := agent.programPath()
	if err != nil {
		return err
	}
	current := string(filepath.Separator)
	root, err := os.Lstat(current)
	if err != nil {
		return fmt.Errorf("service: inspect program root: %w", err)
	}
	if root.Mode()&os.ModeSymlink != 0 || !root.IsDir() {
		return errors.New("service: program root is not a real directory")
	}
	parts := strings.Split(strings.TrimPrefix(program, current), current)
	for index, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("service: inspect program path %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("service: program path %q is a symlink", current)
		}
		if index < len(parts)-1 {
			if !info.IsDir() {
				return fmt.Errorf("service: program ancestor %q is not a directory", current)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("service: program %q is not a regular file", current)
		}
		if info.Mode().Perm()&0o111 == 0 {
			return fmt.Errorf("service: program %q is not executable", current)
		}
	}
	return nil
}

func (c *Controller) uninstall(ctx context.Context, agent Agent) error {
	if out, err := c.launchctl(ctx, "bootout", serviceTarget(agent.Label)); err != nil &&
		launchctlExitCode(err) != launchctlNotLoadedExit {
		return fmt.Errorf("launchctl bootout: %w: %s", err, strings.TrimSpace(out))
	}
	path, err := agent.PlistPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("service: remove agent plist: %w", err)
	} else if err == nil {
		if err := dkdaemon.SyncDir(filepath.Dir(path)); err != nil {
			return fmt.Errorf("service: persist agent plist removal: %w", err)
		}
	}
	return nil
}

func (c *Controller) reload(ctx context.Context, agent Agent, path string) error {
	delay := bootstrapBaseDelay
	var lastErr error
	for attempt := 1; attempt <= bootstrapAttempts; attempt++ {
		out, err := c.launchctl(ctx, "bootout", serviceTarget(agent.Label))
		if err != nil && launchctlExitCode(err) != launchctlNotLoadedExit {
			lastErr = fmt.Errorf("launchctl bootout before bootstrap: %w: %s", err, strings.TrimSpace(out))
			if launchctlExitCode(err) != launchctlInFluxExit {
				return lastErr
			}
		} else {
			out, err = c.launchctl(ctx, "bootstrap", domainTarget(), path)
			if err == nil {
				return nil
			}
			lastErr = fmt.Errorf("launchctl bootstrap: %w: %s", err, strings.TrimSpace(out))
			if launchctlExitCode(err) != launchctlInFluxExit {
				return lastErr
			}
		}
		if attempt == bootstrapAttempts {
			break
		}
		if err := c.retryWait(ctx, delay); err != nil {
			return err
		}
		delay *= 2
	}
	manager, managerErr := c.launchctl(ctx, "managername")
	if managerErr != nil {
		return errors.Join(lastErr, fmt.Errorf("launchctl managername: %w: %s", managerErr, strings.TrimSpace(manager)))
	}
	current, parseErr := ParseSessionType(manager)
	if parseErr != nil {
		return errors.Join(lastErr, parseErr)
	}
	desired, _ := agent.LimitLoadToSessionType.plistValue()
	currentName, _ := current.plistValue()
	if desired == "" {
		desired = "unrestricted"
	}
	return fmt.Errorf(
		"%w (launchd EIO after %d attempts; desired session %q, current manager %q)",
		lastErr, bootstrapAttempts, desired, currentName,
	)
}

func (c *Controller) launchctl(ctx context.Context, args ...string) (string, error) {
	return runCombined(ctx, c.runtime, proc.RecoveryService, "/bin/launchctl", args...)
}

func desiredAgents(agents []Agent) (map[string]Agent, error) {
	desired := make(map[string]Agent, len(agents))
	for _, agent := range agents {
		if _, err := agent.Plist(); err != nil {
			return nil, fmt.Errorf("service: validate agent %q: %w", agent.Label, err)
		}
		if _, duplicate := desired[agent.Label]; duplicate {
			return nil, fmt.Errorf("service: duplicate agent label %q", agent.Label)
		}
		agent.Args = append([]string(nil), agent.Args...)
		agent.Env = cloneStrings(agent.Env)
		agent.AssociatedBundleIdentifiers, _ = canonicalAssociatedBundleIdentifiers(
			agent.AssociatedBundleIdentifiers,
		)
		desired[agent.Label] = agent
	}
	return desired, nil
}

func launchctlExitCode(err error) int {
	var exitErr *supervise.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func waitServiceRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
