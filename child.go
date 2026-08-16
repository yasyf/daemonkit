package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/yasyf/daemonkit/internal/proc"
)

// Reclaimed is one prior-generation child settled during recovery.
type Reclaimed struct {
	PID  int
	Exit Exit
}

// Child is one running owned process. Its identity model — {pid, start, boot}
// — is sealed: PID is the only identity fact exposed, and every proof that
// needs more (anti-PID-reuse observation, signal targeting, reclaim) runs
// inside, where it cannot be recomposed wrongly.
type Child struct {
	child   *proc.Child
	channel Channel
	// nonce is the attach nonce a ChannelHandoff spawn conveyed to the child.
	// It is not a secret: any same-UID process reads a peer's environment via
	// KERN_PROCARGS2. Its role is fd-mixup defence — proof that the attaching
	// peer inherited fd 3 from this exec rather than some other descriptor
	// plumbing.
	nonce  []byte
	limits Limits
	// token is the child's audit token, read while the spawn held it
	// suspended — before release, where the PID provably cannot have been
	// reaped — so later uses cannot race PID reuse.
	token proc.AuditToken
}

// PID returns the child's process id.
func (c *Child) PID() int { return c.child.PID() }

// Conn returns the spawn's channel as a net.Conn with working deadlines:
// the socketpair parent end for ChannelHandoff, the joined stdio pair for
// ChannelStdio. Single-take, and mutually exclusive with Child.Business,
// which consumes the same end: a second take of either, in either order, is
// a named refusal. A ChannelNone child has none.
func (c *Child) Conn() (net.Conn, error) {
	if c.channel == ChannelNone {
		return nil, errors.New("daemonkit: this child was spawned on ChannelNone and has no channel")
	}
	return c.takeChannel()
}

// takeChannel is the single take Conn and Business share, so the collision is
// one named refusal in either order.
func (c *Child) takeChannel() (net.Conn, error) {
	conn, err := c.child.TakeChannel()
	if err != nil {
		return nil, fmt.Errorf(
			"daemonkit: Child.Conn and Child.Business consume the one channel end, once between them: %w", err,
		)
	}
	return conn, nil
}

// Done yields the terminal exactly once per subscription; a subscriber that
// arrives after settlement still receives it.
func (c *Child) Done() <-chan Exit {
	terminal := make(chan Exit, 1)
	settled := c.child.Done()
	go func() { terminal <- exitOf(<-settled) }()
	return terminal
}

// StderrErr reports the stderr copy's failure, nil while the copy is healthy.
// A failed copy never kills the child — losing diagnostics is not a reason to
// kill a working process — so a caller that cares polls here or checks after
// the terminal.
func (c *Child) StderrErr() error { return c.child.StderrErr() }

// Stop demands termination and blocks until the exit is proven, bounded by
// ctx, which must carry a deadline. Idempotent: every call converges on the
// same terminal. An exit not proven within ctx returns ErrUnsettled joined
// with ctx.Err(); the settlement ladder keeps running and the record is
// reclaimed by the next generation.
func (c *Child) Stop(ctx context.Context) (Exit, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return Exit{}, errors.New("daemonkit: Stop requires a context deadline")
	}
	c.child.TerminateBy(deadline)
	select {
	case exit := <-c.child.Done():
		return exitOf(exit), nil
	case <-ctx.Done():
		return Exit{}, errors.Join(ErrUnsettled, ctx.Err())
	}
}
