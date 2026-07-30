package proc

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// settleAllowance is the driver's fixed internal settlement allowance:
	// termination demands and post-exit session settlement subdivide it as
	// fractions, mirroring the deliberately store-free settlement of v1.
	settleAllowance = 5 * time.Second
	termShare       = 0.6
	retireShare     = 0.4
)

// Reap is what observation of the process table proved. Zero is undetermined:
// it is published only when a demanded settlement timed out with nothing
// proved, and then the record is kept for the next open to reclaim.
type Reap uint8

const (
	reapUndetermined Reap = iota
	// ReapAbsent means the process was observed gone or already reaped.
	ReapAbsent
	// ReapCrossBoot means the record's boot session is not this one: the
	// process cannot exist and is never probed or signaled.
	ReapCrossBoot
	// ReapReused means the PID now names a different process instance, which
	// is never signaled.
	ReapReused
	// ReapTerminated means the ladder delivered signals and then observed the
	// exact instance leave the process table.
	ReapTerminated
)

// RecordFate is what the post-write re-read of the store proved.
type RecordFate uint8

const (
	recordInvalid RecordFate = iota
	// RecordRemoved means the re-read proved the record gone.
	RecordRemoved
	// RecordAbandoned means removal was not confirmed within the bounded
	// tail; the next open reclaims it.
	RecordAbandoned
)

// Exit is a child's terminal value, published exactly once on Done.
type Exit struct {
	Code   int
	Reap   Reap
	Record RecordFate
}

// Reclaimed is one prior-generation child Recover settled.
type Reclaimed struct {
	PID  int
	Exit Exit
}

// Child is a running owned process. It has no record, store, or reaper field:
// settlement's only executor is the driver goroutine Spawn started, which
// closes over all three.
type Child struct {
	pid    int
	demand chan time.Time
	stdin  <-chan error // nil unless Cmd.Stdin was delivered; carries the delivery outcome

	settled chan struct{}
	exit    Exit

	handoffMu    sync.Mutex
	handoff      *os.File
	handoffTaken bool
}

// PID returns the child's process id.
func (c *Child) PID() int { return c.pid }

// Terminate is a demand: non-blocking, idempotent, unordered with Done.
func (c *Child) Terminate() { c.demandBy(time.Now().Add(settleAllowance)) }

func (c *Child) demandBy(deadline time.Time) {
	select {
	case c.demand <- deadline:
	default:
	}
}

// Done yields the terminal exactly once per subscription and is never pinned
// by an fsync: the fate wait is bounded before the publish.
func (c *Child) Done() <-chan Exit {
	ch := make(chan Exit, 1)
	go func() {
		<-c.settled
		ch <- c.exit
	}()
	return ch
}

// Handoff returns the parent end of the Cmd.Handoff socketpair, single-take.
func (c *Child) Handoff() (*os.File, error) {
	c.handoffMu.Lock()
	defer c.handoffMu.Unlock()
	if c.handoff == nil {
		if c.handoffTaken {
			return nil, errors.New("proc: handoff endpoint already taken")
		}
		return nil, errors.New("proc: child was spawned without a handoff")
	}
	f := c.handoff
	c.handoff = nil
	c.handoffTaken = true
	return f, nil
}

// Reap authority lives only in this goroutine's closure: identity, writer,
// and ladder are locals no Child field can reach. An undetermined terminal —
// the demanded settlement timed out — publishes without retiring, so the next
// open reclaims the record.
func (s *Store) drive(c *Child, id identity, session int) {
	clk := clockOrReal(s.clock)
	exited := make(chan int, 1)
	go func() { exited <- awaitExit(c.pid) }()

	code, reap := s.awaitTerminal(c, exited, clk)

	settled := reap != reapUndetermined
	sessionSettled := true
	if settled && session != 0 {
		outcome, ok := s.settleSessionSurvivors(session, id.boot, clk)
		sessionSettled = ok
		if outcome == ReapTerminated {
			reap = ReapTerminated
		}
	}

	fate := RecordAbandoned
	if settled && sessionSettled {
		select {
		case fate = <-s.retire(id):
		case <-clk.After(fractionOf(settleAllowance, retireShare)):
		}
	}

	c.exit = Exit{Code: code, Reap: reap, Record: fate}
	close(c.settled)
}

func (s *Store) awaitTerminal(c *Child, exited <-chan int, clk clock) (int, Reap) {
	select {
	case code := <-exited:
		return code, ReapAbsent
	case deadline := <-c.demand:
		return s.terminateChild(c.pid, deadline, exited, clk)
	}
}

// Until wait4 returns, the kernel holds the pid — but a demand can race an
// already-reaped self-exit, so a ready exit must win before any signal; and a
// child SIGKILL cannot dislodge must time out as undetermined by the demand's
// deadline, never hold Done open.
func (s *Store) terminateChild(pid int, deadline time.Time, exited <-chan int, clk clock) (int, Reap) {
	select {
	case code := <-exited:
		return code, ReapAbsent
	default:
	}
	_, _ = s.signalGone(pid, syscall.SIGTERM)
	grace := fractionOf(deadline.Sub(clk.Now()), termShare)
	select {
	case code := <-exited:
		return code, ReapTerminated
	case <-clk.After(grace):
	}
	_, _ = s.signalGone(pid, syscall.SIGKILL)
	select {
	case code := <-exited:
		return code, ReapTerminated
	case <-clk.After(deadline.Sub(clk.Now())):
		return -1, reapUndetermined
	}
}

// settleSessionSurvivors settles the dedicated session after its leader's
// exit: the leader's exit is not the group's. A false return keeps the record
// for the next open to reclaim.
func (s *Store) settleSessionSurvivors(session int, boot uint64, clk clock) (Reap, bool) {
	ctx, cancel := context.WithDeadline(context.Background(), clk.Now().Add(settleAllowance))
	defer cancel()
	outcome, err := s.settleSession(ctx, session, boot)
	if err != nil {
		slog.Warn("proc: dedicated session did not settle; record kept", "session", session, "err", err)
		return reapUndetermined, false
	}
	return outcome, true
}

func awaitExit(pid int) int {
	var status unix.WaitStatus
	for {
		wpid, err := unix.Wait4(pid, &status, 0, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return -1
		}
		if wpid != pid {
			continue
		}
		break
	}
	switch {
	case status.Exited():
		return status.ExitStatus()
	case status.Signaled():
		return 128 + int(status.Signal())
	default:
		return -1
	}
}

func fractionOf(left time.Duration, share float64) time.Duration {
	if left <= 0 {
		return 0
	}
	return time.Duration(share * float64(left))
}
