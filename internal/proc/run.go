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
	defaultRunLimit = Bytes(1 << 20)
	runTailShare    = 0.15
)

// Result is one bounded Run. Overflow is data, never an error: a truncated
// stream still settles its exit.
type Result struct {
	Exit           Exit
	Stdout, Stderr []byte
	Truncated      bool
}

// Run executes one bounded disposable command, reap included. ctx must carry
// a deadline; the run's whole budget derives from it, with the terminate
// ladder reserved a tail fraction so settlement is never starved.
func (s *Store) Run(ctx context.Context, c Cmd) (Result, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return Result{}, errors.New("proc: run requires a context deadline")
	}
	if c.MaxStdout < 0 || c.MaxStderr < 0 {
		return Result{}, fmt.Errorf("proc: negative output limit (MaxStdout %d, MaxStderr %d)", c.MaxStdout, c.MaxStderr)
	}
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
	child, err := s.spawn(c, outW, errW) //nolint:contextcheck // the driver outlives the caller's ctx by design: settlement is never cancelled
	if err != nil {
		_ = outR.Close()
		_ = errR.Close()
		return Result{}, err
	}

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
		stdout = collectBounded(outR, runLimit(c.MaxStdout), onCap)
		_ = outR.Close()
	}()
	go func() {
		defer wg.Done()
		stderr = collectBounded(errR, runLimit(c.MaxStderr), onCap)
		_ = errR.Close()
	}()

	collected := make(chan struct{})
	go func() {
		wg.Wait()
		close(collected)
	}()

	clk := clockOrReal(s.clock)
	settled := child.Done()
	work := deadline.Sub(clk.Now()) - fractionOf(deadline.Sub(clk.Now()), runTailShare)
	var exit Exit
	select {
	case exit = <-settled:
	case <-capped:
		child.demandBy(deadline)
		exit = <-settled
	case <-ctx.Done():
		child.demandBy(deadline)
		exit = <-settled
	case <-clk.After(work):
		child.demandBy(deadline)
		exit = <-settled
	}
	// An unowned descendant can inherit a pipe's write end and hold it past
	// the child's exit, so the EOF drain is bounded by the run's deadline: on
	// expiry the read ends are severed and the bytes so far stand, truncated.
	select {
	case <-collected:
	case <-clk.After(deadline.Sub(clk.Now())):
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
		case <-clk.After(deadline.Sub(clk.Now())):
		}
	}

	result := Result{
		Exit:      exit,
		Stdout:    stdout.data,
		Stderr:    stderr.data,
		Truncated: stdout.limited || stderr.limited,
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

func collectBounded(reader io.Reader, limit int, onCap func()) streamResult {
	data := make([]byte, 0, limit)
	buffer := make([]byte, 32*1024)
	limited := false
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			remaining := min(limit-len(data), n)
			if remaining > 0 {
				data = append(data, buffer[:remaining]...)
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
