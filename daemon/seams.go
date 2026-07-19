package daemon

import (
	"errors"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// clock is the package time seam: real wall time in production, a fake in tests
// so bounded polls and timers run deterministically without real sleeps.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func clockOrReal(c clock) clock {
	if c == nil {
		return realClock{}
	}
	return c
}

// prober reads a process's revalidation identity. Production wraps proc.Probe;
// tests substitute a fake to control identity and inject ErrNoProcess.
type prober interface {
	probe(pid int) (proc.Identity, error)
}

type sysProber struct{}

func (sysProber) probe(pid int) (proc.Identity, error) { return proc.Probe(pid) }

func proberOrSys(p prober) prober {
	if p == nil {
		return sysProber{}
	}
	return p
}

// signaler delivers a signal to a process. Production wraps kill(2); tests
// substitute a fake to observe the ladder and inject ESRCH.
type signaler interface {
	signal(pid int, sig syscall.Signal) error
}

type sysSignaler struct{}

func (sysSignaler) signal(pid int, sig syscall.Signal) error { return syscall.Kill(pid, sig) }

func signalerOrSys(s signaler) signaler {
	if s == nil {
		return sysSignaler{}
	}
	return s
}

// processWaiter reaps an exited child when the incumbent is owned by this
// process. Non-children remain observable only through their probed identity.
type processWaiter interface {
	wait(pid int) (bool, error)
}

type sysProcessWaiter struct{}

func (sysProcessWaiter) wait(pid int) (bool, error) {
	var status syscall.WaitStatus
	waited, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
	if errors.Is(err, syscall.ECHILD) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return waited == pid, nil
}

func processWaiterOrSys(w processWaiter) processWaiter {
	if w == nil {
		return sysProcessWaiter{}
	}
	return w
}
