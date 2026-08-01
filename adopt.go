package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/yasyf/daemonkit/internal/proc"
)

// Tracked is one adopted process: recorded, terminable, reclaimable — never
// waited.
type Tracked struct {
	adopted *proc.Adopted
	owner   *Owned
	once    sync.Once
}

// PID returns the adopted process id.
func (t *Tracked) PID() int { return t.adopted.PID() }

// Stop signals the recorded identity and observes it gone, bounded by ctx.
// The proof is observational (ReapAbsent, ReapReused, ReapCrossBoot,
// ReapTerminated) — the caller's own Wait still reaps the zombie.
func (t *Tracked) Stop(ctx context.Context) (Reap, error) {
	if _, ok := ctx.Deadline(); !ok {
		return ReapUndetermined, errors.New("daemonkit: Stop requires a context deadline")
	}
	reap, err := t.adopted.Stop(ctx)
	if err != nil {
		return Reap(reap), errors.Join(ErrUnsettled, err)
	}
	t.retire()
	return Reap(reap), nil
}

// Release retires the record without touching the process, for a caller whose
// own Wait already proved the exit.
func (t *Tracked) Release() error {
	if err := t.adopted.Release(); err != nil {
		return err
	}
	t.retire()
	return nil
}

func (t *Tracked) retire() { t.once.Do(func() { t.owner.untrack(t) }) }

// gateWrapper parks before the target: it signals readiness on fd 3, waits for
// the release line on fd 4, and only then execs the target with the argv tail
// it was given. A gate closed without a release fails the read and the
// wrapper exits without ever running the target.
const gateWrapper = `
printf r >&3
exec 3>&-
if ! IFS= read -r marker <&4 || [ "$marker" != start ]; then exit 125; fi
exec 4<&-
exec "$@"
`

const gateReleaseLine = "start\n"

// NewGate wraps argv so the target cannot execute an instruction before
// Release: the wrapper signals readiness, parks, and execs the target only
// when released. It is the record-before-first-instruction guarantee for a
// caller who must own the fork (creack/pty). Flow: place Files at fds 3 and 4
// (exec.Cmd.ExtraFiles), start the command, Ready, Adopt the PID, Release.
func NewGate(argv []string) (*Gate, error) {
	if len(argv) == 0 {
		return nil, errors.New("daemonkit: NewGate requires the target argv")
	}
	readySignal, readyWrite, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("daemonkit: create gate readiness pipe: %w", err)
	}
	releaseRead, releaseGate, err := os.Pipe()
	if err != nil {
		_ = readySignal.Close()
		_ = readyWrite.Close()
		return nil, fmt.Errorf("daemonkit: create gate release pipe: %w", err)
	}
	return &Gate{
		argv:    append([]string{"/bin/sh", "-c", gateWrapper, "daemonkit-gate"}, argv...),
		child:   []*os.File{readyWrite, releaseRead},
		ready:   readySignal,
		release: releaseGate,
	}, nil
}

// Gate is one parked wrapper waiting to exec its target.
type Gate struct {
	argv    []string
	child   []*os.File
	ready   *os.File
	release *os.File

	readyOnce sync.Once
	doneOnce  sync.Once
	released  bool
	err       error
}

// Argv is the wrapper argv to start in place of the target's.
func (g *Gate) Argv() []string { return append([]string(nil), g.argv...) }

// Files are the two descriptors the wrapper expects at fds 3 and 4.
func (g *Gate) Files() []*os.File { return append([]*os.File(nil), g.child...) }

// Ready blocks until the wrapper is parked before the target, bounded by ctx.
func (g *Gate) Ready(ctx context.Context) error {
	signalled := make(chan error, 1)
	go func() {
		var marker [1]byte
		_, err := io.ReadFull(g.ready, marker[:])
		if err == nil && marker[0] != 'r' {
			err = fmt.Errorf("gate wrapper signalled %q", marker[0])
		}
		signalled <- err
	}()
	var err error
	select {
	case err = <-signalled:
	case <-ctx.Done():
		g.closeReady()
		<-signalled
		err = ctx.Err()
	}
	g.closeReady()
	if err != nil {
		return fmt.Errorf("daemonkit: await gate readiness: %w", err)
	}
	return nil
}

// Release lets the target exec. A Release after Close is the named refusal:
// the wrapper has already left without running the target, and reporting that
// as a release would hand the caller a process that never existed.
func (g *Gate) Release() error {
	g.doneOnce.Do(func() {
		g.released = true
		_, writeErr := io.WriteString(g.release, gateReleaseLine)
		g.err = errors.Join(writeErr, g.teardown())
		if g.err != nil {
			g.err = fmt.Errorf("daemonkit: release the gate: %w", g.err)
		}
	})
	if !g.released {
		return errors.New("daemonkit: the gate was closed before Release; the wrapper exited without running the target")
	}
	return g.err
}

// Close before Release aborts the gate; the parked wrapper exits without ever
// running the target. After Release it is the no-op that reports the release's
// own outcome.
func (g *Gate) Close() error {
	g.doneOnce.Do(func() {
		if err := g.teardown(); err != nil {
			g.err = fmt.Errorf("daemonkit: close the gate: %w", err)
		}
	})
	return g.err
}

func (g *Gate) teardown() error {
	closes := make([]error, 0, len(g.child)+1)
	closes = append(closes, g.release.Close())
	for _, f := range g.child {
		closes = append(closes, f.Close())
	}
	g.closeReady()
	return errors.Join(closes...)
}

func (g *Gate) closeReady() { g.readyOnce.Do(func() { _ = g.ready.Close() }) }
