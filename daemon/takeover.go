package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/version"
)

// DefaultGrace bounds the wait between a RequestDaemon Shutdown and its SIGKILL.
const DefaultGrace = 5 * time.Second

// DefaultWaitTimeout bounds a takeover's wait for the incumbent to release.
const DefaultWaitTimeout = 30 * time.Second

const takeoverPollInterval = 100 * time.Millisecond

// Contract classifies whether an incumbent owns external resources, which
// decides how a strictly-newer successor may evict it.
type Contract int

const (
	// RequestDaemon owns no external resources, so an older one is evicted with
	// the Shutdown -> grace -> PID-revalidated SIGKILL ladder.
	RequestDaemon Contract = iota
	// ResourceOwner owns external resources, so an older one is never killed for
	// being older: the takeover defers unless external proof confirms it dead.
	ResourceOwner
)

// WaitMode selects how a takeover detects the incumbent releasing the socket.
type WaitMode int

const (
	// SocketRelease waits until the incumbent no longer answers Health.
	SocketRelease WaitMode = iota
	// PIDExit waits until the incumbent process exits, for consumers whose
	// readiness rides a publishable handshake rather than socket liveness.
	PIDExit
)

// Outcome is a takeover verdict.
type Outcome int

const (
	// ExitSelf means the incumbent is same-or-newer; the caller must exit.
	ExitSelf Outcome = iota + 1
	// Bind means the socket is clear; the caller may bind.
	Bind
	// Defer means an older busy ResourceOwner holds the socket; back off and retry.
	Defer
)

// String renders an Outcome for logs and test failures.
func (o Outcome) String() string {
	switch o {
	case ExitSelf:
		return "exit-self"
	case Bind:
		return "bind"
	case Defer:
		return "defer"
	default:
		return fmt.Sprintf("Outcome(%d)", int(o))
	}
}

// TakeoverConfig drives one successor-initiated takeover attempt.
type TakeoverConfig struct {
	// Self is this successor's own build version string.
	Self string
	// Peer adapts whoever currently answers the socket. Required.
	Peer Peer
	// Contract classifies the incumbent for the no-handoff eviction split.
	Contract Contract
	// WaitMode selects release detection after a handoff.
	WaitMode WaitMode
	// Grace bounds a RequestDaemon Shutdown->SIGKILL wait; zero means DefaultGrace.
	Grace time.Duration
	// WaitTimeout bounds the post-handoff release wait; zero means DefaultWaitTimeout.
	WaitTimeout time.Duration
	// ConfirmedDead supplies external proof that an incumbent still answering the
	// socket is actually dead (a stale socket over a crashed ResourceOwner). Only
	// a true result lets a ResourceOwner takeover force in; nil never forces.
	ConfirmedDead func(ctx context.Context, h Health) (bool, error)

	clock    clock
	prober   prober
	signaler signaler
}

// Run probes the incumbent and returns the takeover verdict: ExitSelf when the
// incumbent is same-or-newer (ties never evict), otherwise it evicts a
// strictly-older incumbent — via handoff when Features advertises it, else per
// Contract — and returns Bind once the socket is clear, or Defer for an older
// busy ResourceOwner. Every wait honors ctx and the clock seam.
func Run(ctx context.Context, cfg TakeoverConfig) (Outcome, error) {
	h, err := cfg.Peer.Health(ctx)
	if err != nil {
		// No reachable incumbent: nothing to take over, the caller may bind. The
		// start flock arbitrates a genuine race at bind time.
		return Bind, nil
	}
	if !version.Newer(cfg.Self, h.Version) {
		return ExitSelf, nil
	}
	if h.HasFeature(FeatureHandoff) {
		if err := cfg.Peer.Handoff(ctx); err != nil {
			return 0, fmt.Errorf("request handoff: %w", err)
		}
		if err := cfg.waitRelease(ctx, h); err != nil {
			return 0, err
		}
		return Bind, nil
	}
	switch cfg.Contract {
	case RequestDaemon:
		return cfg.evictRequestDaemon(ctx, h)
	case ResourceOwner:
		return cfg.evictResourceOwner(ctx, h)
	default:
		return 0, fmt.Errorf("%w: %d", ErrUnknownContract, cfg.Contract)
	}
}

// evictRequestDaemon runs the Shutdown -> grace -> PID-revalidated SIGKILL ladder
// against a strictly-older, no-handoff RequestDaemon.
func (cfg TakeoverConfig) evictRequestDaemon(ctx context.Context, h Health) (Outcome, error) {
	victim := h.PID
	if victim <= 1 || victim == os.Getpid() {
		return 0, fmt.Errorf("%w: pid %d", ErrRefuseVictim, victim)
	}
	id, err := cfg.prb().probe(victim)
	if errors.Is(err, proc.ErrNoProcess) {
		return Bind, nil
	}
	if err != nil {
		return 0, fmt.Errorf("probe incumbent %d: %w", victim, err)
	}
	if err := cfg.Peer.Shutdown(ctx); err != nil {
		return 0, fmt.Errorf("request shutdown: %w", err)
	}
	if err := cfg.sleep(ctx, cfg.grace()); err != nil {
		return 0, err
	}
	return cfg.killIfSameOwner(ctx, victim, id.StartTime)
}

// evictResourceOwner never kills an older ResourceOwner for age: it defers unless
// ConfirmedDead proves the still-answering incumbent is actually dead.
func (cfg TakeoverConfig) evictResourceOwner(ctx context.Context, h Health) (Outcome, error) {
	if cfg.ConfirmedDead == nil {
		return Defer, nil
	}
	dead, err := cfg.ConfirmedDead(ctx, h)
	if err != nil {
		return 0, fmt.Errorf("confirm incumbent dead: %w", err)
	}
	if !dead {
		return Defer, nil
	}
	victim := h.PID
	if victim <= 1 || victim == os.Getpid() {
		return 0, fmt.Errorf("%w: pid %d", ErrRefuseVictim, victim)
	}
	id, err := cfg.prb().probe(victim)
	if errors.Is(err, proc.ErrNoProcess) {
		return Bind, nil
	}
	if err != nil {
		return 0, fmt.Errorf("probe incumbent %d: %w", victim, err)
	}
	return cfg.killIfSameOwner(ctx, victim, id.StartTime)
}

// killIfSameOwner re-reads the socket peer and SIGKILLs victim only if the same
// {pid, start_time} still holds it; a released socket, a vanished process, or a
// reused PID means no signal. ESRCH counts as success.
func (cfg TakeoverConfig) killIfSameOwner(ctx context.Context, victim int, startTime string) (Outcome, error) {
	h2, err := cfg.Peer.Health(ctx)
	if err != nil {
		return Bind, nil // socket released during grace
	}
	if h2.PID != victim {
		return Bind, nil // a different owner answers now: never kill it
	}
	id2, err := cfg.prb().probe(victim)
	if err != nil || id2.StartTime != startTime {
		return Bind, nil // gone, reused, or unresolvable: fail closed, no kill
	}
	if err := cfg.kill(victim); err != nil {
		return 0, fmt.Errorf("sigkill incumbent %d: %w", victim, err)
	}
	return Bind, nil
}

// kill sends SIGKILL, mapping ESRCH (already gone) to success.
func (cfg TakeoverConfig) kill(pid int) error {
	err := cfg.sig().signal(pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

// waitRelease polls until the incumbent releases the socket per WaitMode,
// bounded by WaitTimeout and ctx.
func (cfg TakeoverConfig) waitRelease(ctx context.Context, h Health) error {
	var baseline string
	if cfg.WaitMode == PIDExit {
		id, err := cfg.prb().probe(h.PID)
		if errors.Is(err, proc.ErrNoProcess) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("probe incumbent %d: %w", h.PID, err)
		}
		baseline = id.StartTime
	}
	clk := cfg.clk()
	deadline := clk.Now().Add(cfg.waitTimeout())
	for {
		released, err := cfg.releasedOnce(ctx, h, baseline)
		if err != nil {
			return err
		}
		if released {
			return nil
		}
		if clk.Now().After(deadline) {
			return fmt.Errorf("%w after %s", ErrReleaseTimeout, cfg.waitTimeout())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(takeoverPollInterval):
		}
	}
}

// releasedOnce reports whether the incumbent has released the socket for this
// poll tick under the configured WaitMode.
func (cfg TakeoverConfig) releasedOnce(ctx context.Context, h Health, baseline string) (bool, error) {
	switch cfg.WaitMode {
	case SocketRelease:
		h2, err := cfg.Peer.Health(ctx)
		if err != nil {
			return true, nil
		}
		return h2.PID != h.PID, nil
	case PIDExit:
		id, err := cfg.prb().probe(h.PID)
		if errors.Is(err, proc.ErrNoProcess) {
			return true, nil
		}
		if err != nil {
			return false, nil // Undetermined: not provably released, keep waiting
		}
		return id.StartTime != baseline, nil
	default:
		return false, fmt.Errorf("%w: %d", ErrUnknownWaitMode, cfg.WaitMode)
	}
}

// sleep blocks for d honoring ctx and the clock seam.
func (cfg TakeoverConfig) sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-cfg.clk().After(d):
		return nil
	}
}

func (cfg TakeoverConfig) clk() clock    { return clockOrReal(cfg.clock) }
func (cfg TakeoverConfig) prb() prober   { return proberOrSys(cfg.prober) }
func (cfg TakeoverConfig) sig() signaler { return signalerOrSys(cfg.signaler) }
func (cfg TakeoverConfig) grace() time.Duration {
	if cfg.Grace > 0 {
		return cfg.Grace
	}
	return DefaultGrace
}

func (cfg TakeoverConfig) waitTimeout() time.Duration {
	if cfg.WaitTimeout > 0 {
		return cfg.WaitTimeout
	}
	return DefaultWaitTimeout
}
