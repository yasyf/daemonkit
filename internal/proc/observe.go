package proc

import (
	"errors"
	"fmt"
	"net"
)

// ErrNoProcess means a probed PID has no live process — a definitive "gone",
// distinct from a probe failure, which is undetermined and fails closed.
// Callers branch on it with errors.Is.
var ErrNoProcess = errNoProc

// Identity is one exported process-instance pin: a PID beside the kernel start
// stamp and boot session that minted it. Executable is set only by
// ExecutableIdentities, which binds it around the identity snapshot.
type Identity struct {
	PID        int
	Start      uint64
	Boot       uint64
	Executable string
}

// String names one process instance for a refusal to report. A process
// nothing could name still names its pin: "a process remains" tells an
// operator nothing, and the pin is what they can act on.
func (i Identity) String() string {
	if i.Executable == "" {
		return fmt.Sprintf("pid %d (unnameable, start %d, boot %d)", i.PID, i.Start, i.Boot)
	}
	return fmt.Sprintf("pid %d %s", i.PID, i.Executable)
}

// Peer is a connected socket's kernel-authenticated peer credentials, read
// from one getsockopt so the process id and the user id it is judged under
// cannot disagree.
type Peer struct {
	PID int
	UID int
}

// PeerCredentials reads conn's kernel-authenticated peer process and user id
// (LOCAL_PEERPID plus LOCAL_PEERCRED). It answers on both ends of a connected
// socket, so a client reads the identity of the process accepting for it.
func PeerCredentials(conn *net.UnixConn) (Peer, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return Peer{}, fmt.Errorf("proc: syscall conn: %w", err)
	}
	var (
		creds peerCreds
		opErr error
	)
	if err := raw.Control(func(fd uintptr) { creds, opErr = peerCredentials(int(fd)) }); err != nil {
		return Peer{}, fmt.Errorf("proc: control fd: %w", err)
	}
	if opErr != nil {
		return Peer{}, fmt.Errorf("proc: read peer credentials: %w", opErr)
	}
	return Peer{PID: creds.pid, UID: creds.uid}, nil
}

// ProbeIdentity pins pid's current live instance, returning ErrNoProcess when
// the process is gone; any other error is a genuine probe failure.
func ProbeIdentity(pid int) (Identity, error) {
	return probeIdentity(sysProber{}, pid)
}

func probeIdentity(p prober, pid int) (Identity, error) {
	info, err := p.probe(pid)
	if err != nil {
		return Identity{}, err
	}
	boot, err := p.boot()
	if err != nil {
		return Identity{}, fmt.Errorf("proc: load boot identity: %w", err)
	}
	return Identity{PID: pid, Start: info.start, Boot: boot}, nil
}

// Observe classifies one probe of id against the live process table without
// signaling. settled=true carries the anti-PID-reuse absence proof: ReapAbsent
// for an observed-gone or reaped instance, ReapReused for a PID now naming a
// different instance, ReapCrossBoot for a foreign boot session. settled=false
// means the exact instance is still live; an error is undetermined and fails
// closed.
func Observe(id Identity) (Reap, bool, error) {
	return observe(sysProber{}, id)
}

func observe(p prober, id Identity) (Reap, bool, error) {
	boot, err := p.boot()
	if err != nil {
		return reapUndetermined, false, fmt.Errorf("proc: load boot identity: %w", err)
	}
	pin := instance(id)
	if pin.crossBoot(boot) {
		return ReapCrossBoot, true, nil
	}
	info, err := p.probe(pin.pid)
	switch {
	case errors.Is(err, errNoProc):
		return ReapAbsent, true, nil
	case err != nil:
		return reapUndetermined, false, err
	case !pin.matches(identity{pid: pin.pid, start: info.start, boot: boot}):
		return ReapReused, true, nil
	case info.zombie:
		return ReapAbsent, true, nil
	}
	return reapUndetermined, false, nil
}
