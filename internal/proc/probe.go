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
}

type sysSignaler struct{}

func (sysSignaler) signal(pid int, sig syscall.Signal) error { return syscall.Kill(pid, sig) }
