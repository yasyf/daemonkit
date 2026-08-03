package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/launchd"
)

// Stop makes nothing serve at this client's label and removes the label's
// LaunchAgent. It is Ensure's inverse and serializes on the same start lock,
// held from the first observation through the agent's removal — so a Stop and
// an Ensure racing one label take turns: an Ensure cannot re-apply the agent a
// Stop just removed, and a Stop cannot remove the replacement an Ensure just
// started.
//
// The ladder is Ensure's own, aimed at absence, and every step observes rather
// than assumes. A serving incumbent is drained through the control lane,
// pinned to the build and generation just observed, so a replacement that
// raced in is re-observed rather than stopped. One already draining, or a husk
// whose listener is gone, is settled out of the process table from the durable
// owner record — a recorded incumbent that will not leave is signalled at its
// recorded identity first. No record at all leaves absence to the
// executable-scoped inventory over this daemon's own program. Stopping a
// stopped daemon is success. An attach the ladder cannot classify — an
// untrusted peer holding the socket — propagates, and nothing is removed over
// it.
//
// The LaunchAgent goes last, after departure is proven, so launchd never
// relaunches what was just drained — and a relaunch that races the removal is
// booted out by the removal itself. A markerless plist at the label, the shape
// every pre-marker install left behind, is removed through
// [launchd.RemoveUnmarked]: this client's Daemon.Label is the caller's
// assertion that the label is its own, which is the only ownership proof a
// markerless plist admits. A marked plist travels [launchd.Remove]'s ordinary
// ownership rules.
//
// Stop takes down the daemon and its LaunchAgent and nothing else: the state
// directory and the owner record stay, and sealed removal remains
// deploy.Uninstall's. Like every verb it requires a context deadline — the
// whole stop budget, from which every stall bound inside derives.
func (c *Client) Stop(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("daemonkit: Stop requires a context deadline")
	}
	el, err := c.daemon.Label.element()
	if err != nil {
		return err
	}
	agent, err := c.daemon.agent()
	if err != nil {
		return err
	}
	statePaths := el.state()
	if err := statePaths.EnsureLockDir(); err != nil {
		return fmt.Errorf("daemonkit: create lock dir: %w", err)
	}
	lock, err := flock.Spec{
		Path:     statePaths.StartLockPath(),
		Mode:     flock.Exclusive,
		Deadline: left(ctx),
	}.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("daemonkit: serialize stop: %w", err)
	}
	defer func() { _ = lock.Close() }()
	timer := time.NewTimer(attachCadence(ctx))
	defer timer.Stop()
	for {
		err := c.stopOnce(ctx, agent)
		if err == nil {
			break
		}
		if !moved(err) {
			return err
		}
		timer.Reset(attachCadence(ctx))
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		if left(ctx) < minPassSlice {
			<-ctx.Done()
			return errors.Join(err, ctx.Err())
		}
	}
	return c.removeAgent(ctx, el.label)
}

// stopOnce proves nothing of this daemon serves, classifying the world exactly
// as Ensure's settle does: a spent attach is the deadline and never absence, a
// serving incumbent is evicted, and an absent or draining one is settled from
// the record.
func (c *Client) stopOnce(ctx context.Context, agent launchd.Agent) error {
	world, err := c.observeWorld(ctx, agent)
	if err != nil {
		return err
	}
	if world.Serving() {
		return c.evict(ctx, healthFromReport(world.Health), world.Observed())
	}
	if spent(ctx, world.Attach) {
		return errors.Join(world.Attach, context.DeadlineExceeded)
	}
	if errors.Is(world.Attach, ErrAbsent) || errors.Is(world.Attach, ErrDraining) {
		return c.proveRecorded(ctx, world)
	}
	return world.Attach
}

// removeAgent takes the label's LaunchAgent down once departure is proven,
// falling back to the markerless escape when the plist predates the marker.
func (c *Client) removeAgent(ctx context.Context, label string) error {
	err := launchd.Remove(ctx, c.launchctl, label)
	if err == nil {
		return nil
	}
	if !errors.Is(err, launchd.ErrNotOwned) {
		return fmt.Errorf("daemonkit: remove agent %q: %w", label, err)
	}
	if err := launchd.RemoveUnmarked(ctx, c.launchctl, label); err != nil {
		return fmt.Errorf("daemonkit: remove legacy agent %q: %w", label, err)
	}
	return nil
}
