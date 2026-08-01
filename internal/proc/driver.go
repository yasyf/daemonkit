package proc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// SettleGrace is the budget every settlement runs on that no caller deadline
// covers: a child abandoned mid-spawn, and the driver's own post-exit session
// settlement and record retirement, which subdivide it as fractions.
const SettleGrace = 5 * time.Second

const (
	termShare   = 0.6
	retireShare = 0.4
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

// Exit is a child's terminal value, published exactly once on Done. Code is -1
// when the child died by signal, and Signal is then the fatal one.
type Exit struct {
	Code   int
	Signal syscall.Signal
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
	stderr *stderrCopy  // nil unless Spawn was given a stderr writer

	settled  chan struct{}
	exit     Exit
	demanded time.Time // the deadline of the demand the driver acted on; published with exit

	channelMu    sync.Mutex
	channel      net.Conn
	channelTaken bool
}

// PID returns the child's process id.
func (c *Child) PID() int { return c.pid }

// TerminateBy is a demand bounded by the caller's own settlement deadline:
// non-blocking, idempotent, unordered with Done.
func (c *Child) TerminateBy(deadline time.Time) { c.demandBy(deadline) }

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

// StderrErr reports the stderr copy's failure, nil while the copy is healthy
// and nil for a child spawned without one.
func (c *Child) StderrErr() error {
	if c.stderr == nil {
		return nil
	}
	return c.stderr.err()
}

// TakeChannel returns the parent end of the spawn's channel, single-take. A
// second take, and a take on a channel-less child, are distinct refusals.
func (c *Child) TakeChannel() (net.Conn, error) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
	if c.channel == nil {
		if c.channelTaken {
			return nil, errors.New("proc: channel endpoint already taken")
		}
		return nil, errors.New("proc: child was spawned without a channel")
	}
	conn := c.channel
	c.channel = nil
	c.channelTaken = true
	return conn, nil
}

// stderrCopy drains a spawned child's stderr into the caller's writer for the
// child's whole life. A copy failure is recorded and never kills the child:
// losing diagnostics is not a reason to kill a working process.
type stderrCopy struct {
	reader *os.File

	mu     sync.Mutex
	failed error
}

func startStderrCopy(reader *os.File, sink io.Writer) *stderrCopy {
	drain := &stderrCopy{reader: reader}
	go func() {
		_, err := io.Copy(sink, reader)
		_ = reader.Close()
		if err != nil && !errors.Is(err, os.ErrClosed) {
			drain.mu.Lock()
			drain.failed = err
			drain.mu.Unlock()
		}
	}()
	return drain
}

func (c *stderrCopy) err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

func (c *stderrCopy) abort() { _ = c.reader.Close() }

// Reap authority lives only in this goroutine's closure: identity, writer,
// and ladder are locals no Child field can reach. An undetermined terminal —
// the demanded settlement timed out — publishes without retiring, so the next
// open reclaims the record.
func (s *Store) drive(c *Child, id identity, session int) {
	clk := clockOrReal(s.clock)
	exited := make(chan status, 1)
	go func() { exited <- awaitExit(c.pid) }()

	terminal, reap := s.awaitTerminal(c, exited, clk)

	settled := reap != reapUndetermined
	sessionSettled := true
	if settled && session != 0 {
		outcome, ok := s.settleSessionSurvivors(session, id.boot, clk)
		sessionSettled = ok
		switch {
		case !ok:
			reap = reapUndetermined
		case outcome == ReapTerminated:
			reap = ReapTerminated
		}
	}

	fate := RecordAbandoned
	if settled && sessionSettled {
		select {
		case fate = <-s.retire(id):
		case <-clk.After(fractionOf(SettleGrace, retireShare)):
		}
	}

	c.exit = Exit{Code: terminal.code, Signal: terminal.signal, Reap: reap, Record: fate}
	close(c.settled)
}

func (s *Store) awaitTerminal(c *Child, exited <-chan status, clk clock) (status, Reap) {
	select {
	case terminal := <-exited:
		return terminal, ReapAbsent
	case deadline := <-c.demand:
		c.demanded = deadline
		return s.terminateChild(c.pid, deadline, exited, clk)
	}
}

// Until wait4 returns, the kernel holds the pid — but a demand can race an
// already-reaped self-exit, so a ready exit must win before any signal; and a
// child SIGKILL cannot dislodge must time out as undetermined by the demand's
// deadline, never hold Done open.
func (s *Store) terminateChild(pid int, deadline time.Time, exited <-chan status, clk clock) (status, Reap) {
	select {
	case terminal := <-exited:
		return terminal, ReapAbsent
	default:
	}
	_, _ = s.signalGone(pid, syscall.SIGTERM)
	grace := fractionOf(deadline.Sub(clk.Now()), termShare)
	select {
	case terminal := <-exited:
		return terminal, ReapTerminated
	case <-clk.After(grace):
	}
	_, _ = s.signalGone(pid, syscall.SIGKILL)
	select {
	case terminal := <-exited:
		return terminal, ReapTerminated
	case <-clk.After(deadline.Sub(clk.Now())):
		return status{code: -1}, reapUndetermined
	}
}

// settleSessionSurvivors settles the dedicated session after its leader's
// exit: the leader's exit is not the group's, so a false return publishes an
// undetermined terminal and keeps the record for the next open to reclaim —
// a leader proven gone over survivors that were not is not a proof.
func (s *Store) settleSessionSurvivors(session int, boot uint64, clk clock) (Reap, bool) {
	ctx, cancel := context.WithDeadline(context.Background(), clk.Now().Add(SettleGrace))
	defer cancel()
	outcome, err := s.settleSession(ctx, session, boot)
	if err != nil {
		slog.Warn("proc: dedicated session did not settle; record kept", "session", session, "err", err)
		return reapUndetermined, false
	}
	return outcome, true
}

// status is one reaped child's terminal, as the kernel reported it: a signal
// death carries code -1, so no exit status can be mistaken for a signal.
type status struct {
	code   int
	signal syscall.Signal
}

func awaitExit(pid int) status {
	var wstatus unix.WaitStatus
	for {
		wpid, err := unix.Wait4(pid, &wstatus, 0, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return status{code: -1}
		}
		if wpid != pid {
			continue
		}
		break
	}
	switch {
	case wstatus.Exited():
		return status{code: wstatus.ExitStatus()}
	case wstatus.Signaled():
		return status{code: -1, signal: wstatus.Signal()}
	default:
		return status{code: -1}
	}
}

func fractionOf(left time.Duration, share float64) time.Duration {
	if left <= 0 {
		return 0
	}
	return time.Duration(share * float64(left))
}
