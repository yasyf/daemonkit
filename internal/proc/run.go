package proc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	defaultRunLimit = Bytes(4 << 20)
	runTailShare    = 0.15
	collectChunk    = 32 * 1024
)

// Result is one bounded Run. Overflow is data, never an error: a truncated
// stream still settles its exit.
type Result struct {
	Exit           Exit
	Stdout, Stderr []byte
	Truncated      bool
	// Expired means the run's own budget, not the command, ended it: the
	// terminate ladder ran inside the reserved tail, so the command settled
	// before the caller's deadline and ctx carries no error of its own.
	Expired bool
}

// Run executes one bounded disposable command in a dedicated session, reap
// included. ctx must carry a deadline; the run's whole budget derives from it,
// with the terminate ladder reserved a tail fraction so settlement is never
// starved. The session is Run's, not the caller's: a disposable command that
// outlives itself through a fork is a leak, so Cmd.Session is set here rather
// than asked for, and the run does not settle until its whole session has.
// hold takes the live child before the run waits on it, and cannot refuse: an
// owner admits a run before it can spawn, so the child a spawn produces is
// registered unconditionally and an owner settling its scope terminates a run
// in flight on its own budget rather than answering over it.
func (s *Store) Run(ctx context.Context, c Cmd, hold func(*Child)) (Result, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return Result{}, errors.New("proc: run requires a context deadline")
	}
	clk := clockOrReal(s.clock)
	if c.MaxOutput < 0 {
		return Result{}, fmt.Errorf("proc: negative output limit (MaxOutput %d)", c.MaxOutput)
	}
	if c.Channel != ChannelNone {
		return Result{}, errors.New("proc: run has no channel; its streams are its output")
	}
	c.Session = true
	outR, outW, err := os.Pipe()
	if err != nil {
		return Result{}, fmt.Errorf("proc: create stdout pipe: %w", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		_ = outR.Close()
		_ = outW.Close()
		return Result{}, fmt.Errorf("proc: create stderr pipe: %w", err)
	}
	child, err := s.spawn(ctx, c, outW, errW)
	if err != nil {
		_ = outR.Close()
		_ = errR.Close()
		return Result{}, err
	}
	hold(child)

	// A stream hitting its cap demands termination through this channel: a
	// bounded command is disposable once its retained output is full, so the
	// child is torn down at once rather than drained to the deadline.
	capped := make(chan struct{}, 1)
	onCap := func() {
		select {
		case capped <- struct{}{}:
		default:
		}
	}

	var wg sync.WaitGroup
	var stdout, stderr streamResult
	wg.Add(2)
	go func() {
		defer wg.Done()
		stdout = collectBounded(outR, runLimit(c.MaxOutput), onCap)
		_ = outR.Close()
	}()
	go func() {
		defer wg.Done()
		stderr = collectBounded(errR, runLimit(c.MaxOutput), onCap)
		_ = errR.Close()
	}()

	collected := make(chan struct{})
	go func() {
		wg.Wait()
		close(collected)
	}()

	settled := child.Done()
	work := deadline.Sub(clk.Now()) - fractionOf(deadline.Sub(clk.Now()), runTailShare)
	var exit Exit
	expired := false
	select {
	case exit = <-settled:
	case <-capped:
		child.demandBy(deadline)
		exit = <-settled
	case <-ctx.Done():
		expired = true
		child.demandBy(deadline)
		exit = <-settled
	case <-clk.After(work):
		expired = true
		child.demandBy(deadline)
		exit = <-settled
	}
	// An unowned descendant can inherit a pipe's end and hold it past the
	// child's exit, so both remaining waits are bounded by whichever budget
	// actually governs: the settlement deadline the terminal demand carried,
	// since an owner that already settled its scope must not be answered over
	// for the rest of the caller's run, and the run's own deadline otherwise.
	// On expiry the read ends are severed and the bytes so far stand, truncated.
	drainBy := deadline
	if settlement := child.demanded; !settlement.IsZero() && settlement.Before(drainBy) {
		drainBy = settlement
	}
	select {
	case <-collected:
	case <-clk.After(drainBy.Sub(clk.Now())):
		_ = outR.Close()
		_ = errR.Close()
		<-collected
		stdout.sever()
		stderr.sever()
	}

	var stdinErr error
	if child.stdin != nil {
		select {
		case stdinErr = <-child.stdin:
		case <-clk.After(drainBy.Sub(clk.Now())):
		}
	}

	result := Result{
		Exit:      exit,
		Stdout:    stdout.data,
		Stderr:    stderr.data,
		Truncated: stdout.limited || stderr.limited,
		Expired:   expired,
	}
	if stdinErr != nil {
		return result, fmt.Errorf("proc: deliver stdin: %w", stdinErr)
	}
	if err := errors.Join(stdout.err, stderr.err); err != nil {
		return result, fmt.Errorf("proc: collect run output: %w", err)
	}
	return result, nil
}

func runLimit(limit Bytes) int {
	if limit == 0 {
		limit = defaultRunLimit
	}
	return int(limit)
}

type streamResult struct {
	data    []byte
	limited bool
	err     error
}

func (r *streamResult) sever() {
	r.limited = true
	if errors.Is(r.err, os.ErrClosed) {
		r.err = nil
	}
}

// collectBounded retains at most limit bytes. Retention grows geometrically
// with every growth clamped to the cap, so a silent stream costs nothing and a
// stream that fills allocates the cap once rather than append's doubling past
// it.
func collectBounded(reader io.Reader, limit int, onCap func()) streamResult {
	var data []byte
	buffer := make([]byte, collectChunk)
	limited := false
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			remaining := min(limit-len(data), n)
			if remaining > 0 {
				data = append(reserved(data, remaining, limit), buffer[:remaining]...)
			}
			if n > remaining && !limited {
				limited = true
				onCap()
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return streamResult{data: data, limited: limited, err: err}
		}
	}
}

func reserved(data []byte, add, limit int) []byte {
	need := len(data) + add
	if need <= cap(data) {
		return data
	}
	sized := make([]byte, len(data), min(max(2*cap(data), need, collectChunk), limit))
	copy(sized, data)
	return sized
}
