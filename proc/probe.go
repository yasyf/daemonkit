package proc

import "errors"

// AuditToken is Darwin's kernel-stable process execution identity. Its
// (pid, pidversion) pair survives PID reuse checks and is valid only for the
// process execution that created it.
type AuditToken [32]byte

// IsZero reports whether no audit-token authority is present.
func (t AuditToken) IsZero() bool { return t == AuditToken{} }

// ErrNoAuditToken means the platform cannot bind a process to a Darwin audit
// token. Protected app termination fails closed on this error.
var ErrNoAuditToken = errors.New("proc: no kernel audit-token identity")

// Identity is a live process's revalidation identity. A PID paired with a
// matching Boot and StartTime are the only safe kill authority: a reused PID
// gets a fresh StartTime, and start stamps never cross boot sessions.
type Identity struct {
	PID       int
	StartTime string
	Comm      string
	// Boot is the host boot session the identity was probed in; linux start
	// stamps are only unique within one boot, so a foreign Boot reads as dead.
	Boot string
	// Executable is the kernel-resolved executable path bound to AuditToken.
	Executable string
	// AuditToken is required for protected Darwin peer termination.
	AuditToken AuditToken
}

// ErrNoProcess means a probed PID has no live process — a definitive "gone",
// distinct from a probe failure; a SIGKILL-authority caller branches on it with errors.Is.
var ErrNoProcess = errNoProc

// Probe reads pid's identity from the live process table, returning ErrNoProcess
// when the process is gone; any other error is a genuine probe failure.
func Probe(pid int) (Identity, error) {
	info, err := probeProc(pid)
	if err != nil {
		return Identity{}, err
	}
	boot, err := BootID()
	if err != nil {
		return Identity{}, err
	}
	return Identity{PID: pid, StartTime: info.startTime, Comm: info.comm, Boot: boot}, nil
}
