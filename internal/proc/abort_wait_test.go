package proc

import (
	"errors"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestAbortSpawnRetainsUnprovenWaitFailure(t *testing.T) {
	s, _ := newTestStore(t)
	signals := &funcSignaler{}
	s.signaler = signals
	cause := errors.New("verify refused")
	wait := func(pid int, _ *unix.WaitStatus, _ int, _ *unix.Rusage) (int, error) {
		if pid != 4242 {
			t.Fatalf("wait pid = %d", pid)
		}
		return -1, unix.ECHILD
	}
	err := s.abortSpawnWithWait(4242, nil, cause, wait)
	if !errors.Is(err, ErrUnsettled) || !errors.Is(err, unix.ECHILD) || !errors.Is(err, cause) {
		t.Fatalf("abortSpawnWithWait() = %v", err)
	}
	sent := signals.signals()
	if len(sent) != 1 || sent[0].pid != 4242 || sent[0].sig != syscall.SIGKILL {
		t.Fatalf("abort signals = %v", sent)
	}
}

func TestAbortSpawnRetriesInterruptedWaitAndPreservesProvenFailure(t *testing.T) {
	s, _ := newTestStore(t)
	s.signaler = &funcSignaler{}
	cause := errors.New("verify refused")
	calls := 0
	wait := func(pid int, status *unix.WaitStatus, _ int, _ *unix.Rusage) (int, error) {
		calls++
		if calls == 1 {
			return -1, unix.EINTR
		}
		*status = unix.WaitStatus(syscall.SIGKILL)
		return pid, nil
	}
	err := s.abortSpawnWithWait(4242, nil, cause, wait)
	if !errors.Is(err, cause) || errors.Is(err, ErrUnsettled) || calls != 2 {
		t.Fatalf("abortSpawnWithWait() = %v, wait calls = %d", err, calls)
	}
}
