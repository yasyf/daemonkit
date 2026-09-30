package proc

import (
	"errors"
	"syscall"
)

// errNoProc is the package's one definitive "gone" sentinel, distinct from a
// probe failure, which is undetermined and fails closed.
var errNoProc = errors.New("proc: no such process")

type procInfo struct {
	start   uint64
	comm    string
	group   int
	session int
	zombie  bool
	stopped bool
	exiting bool
}

type groupMember struct {
	pid  int
	info procInfo
}

type prober interface {
	probe(pid int) (procInfo, error)
	groupMembers(sessionID int) ([]groupMember, error)
	boot() (uint64, error)
}

type sysProber struct{}

func (sysProber) probe(pid int) (procInfo, error) { return probeProc(pid) }

func (sysProber) groupMembers(sessionID int) ([]groupMember, error) {
	return probeGroupMembers(sessionID)
}

func (sysProber) boot() (uint64, error) { return bootSession() }

type signaler interface {
	signal(pid int, sig syscall.Signal) error
	hold(pid int) (held, error)
}

// held is one process pinned for signalling. A ladder takes the hold before
// the probe that matches its identity, so every signal it then sends reaches
// the probed instance or nobody: where the kernel offers a handle, a PID
// reused after the probe is ESRCH rather than a stranger.
type held interface {
	signal(sig syscall.Signal) error
	release()
}

type sysSignaler struct{}

func (sysSignaler) signal(pid int, sig syscall.Signal) error { return syscall.Kill(pid, sig) }
