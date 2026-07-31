package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/converge"
	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/launchd"
)

const (
	// observeShare bounds waiting out a transitional incumbent — one starting
	// or draining — before it is replaced instead of waited on further.
	observeShare = 0.25
	// evictDrainShare is the slice of an eviction spent on the drain verb and
	// its observation, leaving the rest for the session-less settlement.
	evictDrainShare = 0.5
	// proveSettleShare is the slice of a proof spent observing departure before
	// a wedged incumbent is signalled.
	proveSettleShare = 0.5
	// proveReproofShare is the slice of what the proof has left spent observing
	// the signalled incumbent depart, so an incumbent that never leaves cannot
	// spend the apply and the readiness wait's budget on being watched.
	proveReproofShare = 0.5
	// minPassSlice is the smallest slice of the caller's budget a ladder pass is
	// re-entered on. A pass attaches, evicts, execs launchctl, and subscribes to
	// the new daemon's phase stream; below this every deadline it derives is
	// already in the past, so it observes nothing, evicts nothing, and reports
	// its dials' i/o timeouts in place of the race it was re-entered to lose
	// again. A remainder that small is waited out rather than spent, so the race
	// is reported joined with the deadline that actually ended it.
	minPassSlice = 10 * time.Millisecond
)

// Ensured is what Ensure did. A caller learns nothing by discarding it.
type Ensured struct {
	// Before is the incumbent Ensure found, zero when nothing was serving.
	Before Health
	// Did is what the ladder performed to reach After.
	Did Action
	// After is the daemon serving when Ensure returned. When Did is
	// ActionNothing it is Before, re-stated rather than re-read: nothing moved.
	After Health
}

// Ensure makes the daemon this client names be the exact build of its own
// Program, ready and serving, and reports what that took.
//
// The ladder is fixed and every step observes rather than assumes. The state
// directory's start lock serializes concurrent Ensures. The world is
// re-derived from the socket, the durable owner record, and launchd's own view
// of the LaunchAgent — never from stored intent. A pure decision reads the
// observed Health (decide). An incumbent that decision replaces is evicted
// through Control.Drain, pinned to the build and generation just observed so a
// replacement that raced in is refused rather than stopped, and proven out of
// the process table. The daemon's own LaunchAgent is then applied, and
// readiness is a subscription on the new daemon's phase stream rather than a
// poll.
//
// Applying rewrites a drifted plist and re-bootstraps the job, which boots a
// live incumbent out — so an incumbent is always drained first, and a daemon
// that is already the wanted build but whose LaunchAgent has drifted is
// therefore restarted, not repaired underneath.
//
// Ensure and an explicit Control.Drain are the only evictors; Serve refuses a
// live incumbent with ErrBusy. Eviction proves exactly one pinned identity
// left the process table: a caller gating an irreversible action on absence
// needs the executable-scoped inventory besides (see Stopped). Every eviction
// is addressed to a completely named incumbent — build and generation both —
// so a runtime this Ensure never observed is never drained, never settled, and
// never signalled.
//
// Ensure touches exactly one LaunchAgent, this daemon's own label. It never
// asks what daemonkit owns machine-wide, so one consumer's Ensure cannot
// disturb another product's agents.
func (c *Client) Ensure(ctx context.Context) (Ensured, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Ensured{}, errors.New("daemonkit: Ensure requires a context deadline")
	}
	want, err := c.daemon.Program.build()
	if err != nil {
		return Ensured{}, err
	}
	agent, err := c.daemon.agent()
	if err != nil {
		return Ensured{}, err
	}
	statePaths := c.daemon.statePaths()
	if err := statePaths.EnsureLockDir(); err != nil {
		return Ensured{}, fmt.Errorf("daemonkit: create lock dir: %w", err)
	}
	lock, err := flock.Spec{
		Path:     statePaths.StartLockPath(),
		Mode:     flock.Exclusive,
		Deadline: left(ctx),
	}.Acquire(ctx)
	if err != nil {
		return Ensured{}, fmt.Errorf("daemonkit: serialize ensure: %w", err)
	}
	defer func() { _ = lock.Close() }()
	timer := time.NewTimer(attachCadence(ctx))
	defer timer.Stop()
	for {
		ensured, err := c.ensureOnce(ctx, want, agent)
		if !moved(err) {
			return ensured, err
		}
		timer.Reset(attachCadence(ctx))
		select {
		case <-ctx.Done():
			return Ensured{}, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		if left(ctx) < minPassSlice {
			<-ctx.Done()
			return Ensured{}, errors.Join(err, ctx.Err())
		}
	}
}

// moved names the two benign races that mean the incumbent changed between an
// observation and the verb aimed at it: a non-zero Expect the pinned runtime
// disagreed with, and a session whose peer is no longer the process the attach
// pinned. Neither stopped anything and neither is the caller's error — the
// whole repair is to observe again and decide again, which the ladder does on
// its own re-observation cadence rather than as fast as the race can be lost.
func moved(err error) bool {
	return errors.Is(err, ErrWrongIncumbent) || errors.Is(err, errPinMoved)
}

func (c *Client) ensureOnce(ctx context.Context, want string, agent launchd.Agent) (Ensured, error) {
	world, action, err := c.settle(ctx, want, agent)
	if err != nil {
		return Ensured{}, err
	}
	before := healthFromReport(world.Health)
	if action == ActionNothing && world.Applied {
		return Ensured{Before: before, Did: ActionNothing, After: before}, nil
	}
	if world.Serving() {
		if action == ActionNothing {
			action = ActionRestarted
		}
		if err := c.evict(ctx, before, world.Observed()); err != nil {
			return Ensured{}, err
		}
	} else if err := c.proveRecorded(ctx, world); err != nil {
		return Ensured{}, err
	}
	if err := launchd.Apply(ctx, c.launchctl, agent); err != nil {
		return Ensured{}, fmt.Errorf("daemonkit: apply %q: %w", agent.Label, err)
	}
	after, err := c.WaitReady(ctx)
	if err != nil {
		return Ensured{}, err
	}
	if after.Build != want || after.Phase != PhaseReady {
		return Ensured{}, fmt.Errorf(
			"daemonkit: daemon came up as build %q phase %d, want build %q phase %d",
			after.Build, after.Phase, want, PhaseReady,
		)
	}
	return Ensured{Before: before, Did: action, After: after}, nil
}

// settle observes until the decision is knowable. A transitional incumbent is
// re-observed on a cadence derived from the caller's own deadline; one still
// transitioning when the observation share runs out is replaced rather than
// waited on further, which is what the launcher this ladder replaces did.
//
// An attach that only ran out of budget is the deadline and no observation at
// all: it is classified before absence, because a dial that never left this
// process must never read as nothing serving and start a second daemon over a
// live one.
func (c *Client) settle(ctx context.Context, want string, agent launchd.Agent) (converge.World, Action, error) {
	observing := Grace(left(ctx)).mint("ensure").Share("observe", observeShare)
	timer := time.NewTimer(attachCadence(ctx))
	defer timer.Stop()
	for {
		world, err := c.observeWorld(ctx, agent)
		if err != nil {
			return converge.World{}, actionInvalid, err
		}
		if !world.Serving() {
			if spent(ctx, world.Attach) {
				return converge.World{}, actionInvalid, errors.Join(world.Attach, context.DeadlineExceeded)
			}
			if errors.Is(world.Attach, ErrAbsent) || errors.Is(world.Attach, ErrDraining) {
				return world, ActionStarted, nil
			}
			return converge.World{}, actionInvalid, world.Attach
		}
		action, err := decide(healthFromReport(world.Health), want)
		if err != nil {
			return converge.World{}, actionInvalid, err
		}
		if action != actionObserve {
			return world, action, nil
		}
		if observing.Left() <= 0 {
			return world, ActionRestarted, nil
		}
		timer.Reset(attachCadence(ctx))
		select {
		case <-ctx.Done():
			return converge.World{}, actionInvalid, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) observeWorld(ctx context.Context, agent launchd.Agent) (converge.World, error) {
	return converge.Observe(ctx, converge.Sources{
		Serving:    c.serving,
		Recorded:   c.readOwner,
		RecordPath: c.recordPath,
		Agent:      agent,
		Launchctl:  c.launchctl,
	})
}

// servedHealth attaches the control lane, reads the pinned incumbent's report,
// and lets the session go. The attach is what judges the serving process
// against Trust.Serving and pins the kernel process the report describes, so
// this observation names one instance rather than whatever answered.
func (c *Client) servedHealth(ctx context.Context) (wire.HealthReport, error) {
	control, err := c.Control(ctx)
	if err != nil {
		return wire.HealthReport{}, err
	}
	defer func() { _ = control.Close(ctx) }()
	report, err := control.session.Health(ctx)
	if err != nil {
		return wire.HealthReport{}, err
	}
	if err := control.pinnedBy(report); err != nil {
		return wire.HealthReport{}, err
	}
	return report, nil
}

// evict removes the observed incumbent and proves it left the process table.
// Expect pins the drain to the build and generation just observed, so a
// replacement that raced in between the observation and the verb is refused
// with ErrWrongIncumbent — nothing is dispatched and Ensure re-decides. A
// drain already in flight or a husk whose listener is gone leaves no session
// to pin, and the same Reap-bearing proof comes from the durable owner record
// instead. A drain slice that ran out — before the dial or under the verb —
// leaves no session either: its i/o timeout is the deadline's, so the proof
// falls to the record on what the whole eviction has left rather than being
// reported as something the incumbent did.
//
// Whichever arm it takes, the eviction carries a pinned identity into the
// proof: the peer this attach pinned when there was a session, and otherwise
// observed — what the pass that reached here already named. It is the one thing
// that says whose an unnameable process is when the record turns out to name
// nobody, and a session this ladder held is exactly the evidence no record file
// can forge.
func (c *Client) evict(ctx context.Context, before Health, observed proc.Identity) error {
	target, err := pin(before.Build, before.Generation)
	if err != nil {
		return err
	}
	drainCtx, cancel := Grace(left(ctx)).mint("evict").Share("drain", evictDrainShare).Context(ctx)
	defer cancel()
	control, err := c.Control(drainCtx)
	switch {
	case err == nil:
		defer func() { _ = control.Close(drainCtx) }()
		_, drainErr := control.Drain(drainCtx, target.expect())
		if drainErr == nil {
			return nil
		}
		if !errors.Is(drainErr, ErrUnsettled) && !spent(drainCtx, drainErr) {
			return drainErr
		}
		return c.prove(ctx, target, control.pinned)
	case errors.Is(err, ErrAbsent), errors.Is(err, ErrDraining), spent(drainCtx, err):
	default:
		return err
	}
	return c.prove(ctx, target, observed)
}

// incumbent is one completely named runtime: the build stamp and the instance
// generation both. It is the only thing daemonkit evicts, so no settlement and
// no signal can run against a partial expectation — one whose empty fields
// match whatever the same-UID-writable owner record happens to say at the
// moment it is read.
type incumbent struct {
	build      string
	generation uint64
}

// pin names the runtime an eviction is entitled to act on. The completeness it
// requires is the whole authorization: an empty build or a zero generation
// leaves that field unchecked, and an unchecked field is a SIGTERM addressed to
// whichever process the record names next.
func pin(build string, generation uint64) (incumbent, error) {
	if build == "" || generation == 0 {
		return incumbent{}, fmt.Errorf(
			"daemonkit: refusing to evict an unpinned incumbent: build=%q generation=%d",
			build, generation,
		)
	}
	return incumbent{build: build, generation: generation}, nil
}

func (i incumbent) expect() Expect { return Expect{Build: i.build, Generation: i.generation} }

// proveRecorded settles the incumbent the observation actually found recorded.
// An observation that found no record names nobody to settle and nobody to
// signal, so absence is the executable-scoped inventory's question alone: a
// record appearing between that observation and this one describes a runtime
// this Ensure never saw and holds no authority over.
func (c *Client) proveRecorded(ctx context.Context, world converge.World) error {
	if !world.Recorded {
		return c.inventoryClear(world.Observed())
	}
	target, err := pin(world.Owner.Build, world.Owner.Generation)
	if err != nil {
		return err
	}
	return c.prove(ctx, target, world.Observed())
}

// prove settles target out of the process table without a session. A record
// that no longer names target refuses with ErrWrongIncumbent and Ensure
// re-observes; a record still naming it and still live is a wedged daemon, and
// the one signal daemonkit ever sends goes out addressed to the recorded
// identity before departure is observed again. No record at all leaves absence
// to the executable-scoped inventory, which reads the kernel and no
// same-UID-writable record file can forge.
//
// observed is what the ladder pinned on its way here — the session's peer, or
// the identity the observation read out of the record — and it is the only
// thing that can attribute an unnameable process to this daemon once the record
// names nobody. It is required rather than optional because a call site that
// forgets it forgets the whole correlation silently; a pass that genuinely
// pinned nothing names the zero identity, which no live process matches.
func (c *Client) prove(ctx context.Context, target incumbent, observed proc.Identity) error {
	proving := Grace(left(ctx)).mint("prove")
	settleCtx, cancel := proving.Share("settle", proveSettleShare).Context(ctx)
	defer cancel()
	_, err := c.Settle(settleCtx, target.expect())
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrUnrecorded):
		return c.inventoryClear(observed)
	case errors.Is(err, ErrUnsettled):
		if err := repairWedged(c.recordPath, target, c.readOwner, c.probe, c.kill); err != nil {
			return err
		}
		reproofCtx, cancelReproof := proving.Share("reproof", proveReproofShare).Context(ctx)
		defer cancelReproof()
		_, err = c.Settle(reproofCtx, target.expect())
		return err
	default:
		return err
	}
}

// repairWedged terminates a recorded incumbent that will not leave. Two
// cross-checks stand between the record and the signal: the record must still
// name target — build and generation both — so a runtime the caller never
// observed is never signalled on a record it can write itself, and the recorded
// {pid, start, boot} must still be live and match, so a PID the kernel has
// since handed to a stranger is never signalled either. The identity, not the
// number, is the address.
func repairWedged(
	recordPath string,
	target incumbent,
	readOwner func(string) (proc.Owner, bool, error),
	probe func(int) (proc.Identity, error),
	kill func(int, syscall.Signal) error,
) error {
	owner, ok, err := readOwner(recordPath)
	if err != nil {
		return err
	}
	if !ok {
		return ErrUnrecorded
	}
	if target.expect().mismatch(owner.Build, owner.Generation) {
		return ErrWrongIncumbent
	}
	live, err := probe(owner.PID)
	if errors.Is(err, proc.ErrNoProcess) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("daemonkit: probe recorded incumbent %d: %w", owner.PID, err)
	}
	if live.Start != owner.Start || live.Boot != owner.Boot {
		return nil
	}
	if err := kill(owner.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("daemonkit: terminate wedged incumbent %d: %w", owner.PID, err)
	}
	return nil
}

// inventoryClear proves absence the one way a record file cannot forge: the
// executable-scoped process inventory over the path this daemon's program runs
// from. The inventory compares against the path the kernel holds, which is
// fully symlink-resolved, so the program is resolved to that same form first —
// an unresolved component matches no process at all and would report a clear
// inventory for a daemon still running. This gate authorizes irreversible
// actions, so a path that cannot be resolved is an error, never an empty
// answer.
//
// The query is this daemon's own program and nothing else. A path guessed by
// name — a sibling under the staging root every daemonkit consumer shares — is
// wrong in both directions: it misses a build staged under another basename,
// and it holds another product's daemon against this gate. What covers a live
// process this daemon's program path does not name is observed: a process
// nothing could name at all counts against the gate exactly when its pin is one
// this ladder pinned on its way here, and a stranger's husk never is. The owner
// record is not re-read for that correlation — every path into this gate is
// entered precisely because the record named nobody, so a re-read would
// correlate against nothing — which is why every caller hands down what it
// observed, and the zero identity when it observed nothing.
//
// The residual is exact and stated rather than papered over: a husk this ladder
// never observed is attributable to nothing, and no scan of the process table
// can attribute it. Settling a recorded identity out of the table is the half
// that covers a recorded process whose executable is gone.
func (c *Client) inventoryClear(observed proc.Identity) error {
	program, err := c.daemon.Program.resolved()
	if err != nil {
		return err
	}
	found, err := c.identities(program)
	if err != nil {
		return fmt.Errorf("daemonkit: inventory %q: %w", program, err)
	}
	live := slices.Clone(found.Matched)
	for _, husk := range found.Unnameable {
		if proc.SameInstance(husk, observed) {
			live = append(live, husk)
		}
	}
	if len(live) == 0 {
		return nil
	}
	names := make([]string, len(live))
	for i, identity := range live {
		names[i] = identity.String()
	}
	return fmt.Errorf("%w: live process(es) remain: %s", ErrUnsettled, strings.Join(names, ", "))
}

// agent is the LaunchAgent that runs this daemon. Every field is already on
// the Daemon, so nothing about the job is declared twice; an unset Log sinks
// to the state directory's daemon.log.
func (d Daemon) agent() (launchd.Agent, error) {
	restart, err := d.Restart.launchd()
	if err != nil {
		return launchd.Agent{}, err
	}
	log := d.Log
	if log == "" {
		log = d.statePaths().LogPath()
	}
	return launchd.Agent{
		Label:         string(d.Label),
		Program:       d.Program.path,
		Args:          d.Args,
		LogPath:       log,
		RestartPolicy: restart,
		ExitTimeOut:   d.exitTimeOut(),
	}, nil
}

// exitTimeOut is Shutdown as launchd's own ExitTimeOut key: the SIGKILL that
// backstops a drain which wedges past the budget the daemon was promised.
// launchd counts it in whole seconds and kills the instant they elapse, so a
// sub-second remainder rounds up — truncating it would fire the backstop on the
// very grace it exists to protect.
func (d Daemon) exitTimeOut() time.Duration {
	grace := time.Duration(d.shutdownGrace())
	if remainder := grace % time.Second; remainder != 0 {
		return grace + time.Second - remainder
	}
	return grace
}

func (r Restart) launchd() (launchd.RestartPolicy, error) {
	switch r {
	case RestartNever:
		return launchd.NoRestart, nil
	case RestartOnFailure:
		return launchd.RestartOnFailure, nil
	case RestartAlways:
		return launchd.RestartAlways, nil
	default:
		return 0, fmt.Errorf("daemonkit: unknown restart policy %d", r)
	}
}

// launchctl runs one /bin/launchctl invocation to completion. An exit code is
// an answer, not a failure: only a launchctl that could not be run at all is
// an error, which is the boundary launchd's single outcome classifier reads.
func launchctl(ctx context.Context, path string, args ...string) (string, int, error) {
	out, err := exec.CommandContext(ctx, path, args...).CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(out), exit.ExitCode(), nil
	}
	if err != nil {
		return string(out), 0, err
	}
	return string(out), 0, nil
}

func left(ctx context.Context) time.Duration {
	deadline, _ := ctx.Deadline()
	return time.Until(deadline)
}

// spent reports a failure that is only the budget ending. A dial on a context
// whose deadline has passed and a socket read past the deadline set on it both
// raise "i/o timeout" — net's own error for the first, os.ErrDeadlineExceeded
// for the second — so a verb that never left this process arrives looking
// exactly like a peer that stopped answering, and a ladder that passed it on
// would report a socket timeout as an answer about the incumbent.
//
// The deadline decides, never ctx.Err(): a context publishes its expiry from a
// timer goroutine, so under load the transport times out on the deadline it
// read while the context it was derived from still reports no error at all.
func spent(ctx context.Context, err error) bool {
	if left(ctx) > 0 {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
}
