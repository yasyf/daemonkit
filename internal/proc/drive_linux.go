//go:build linux && (amd64 || arm64)

package proc

import (
	"errors"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	childExited = 1
	childKilled = 2
	childDumped = 3
)

// childSiginfo is the SIGCHLD arm of the kernel's siginfo_t on a 64-bit ABI,
// where the union after the three header ints is aligned to eight bytes.
type childSiginfo struct {
	Signo  int32
	Errno  int32
	Code   int32
	_      int32
	PID    int32
	UID    uint32
	Status int32
}

// drive learns the child's exit without reaping it and reaps only once the
// driver has settled. Until then the zombie holds the PID and the session id,
// so no signal the driver addresses by number can reach a successor.
func (s *Store) drive(c *Child, id identity, session int) {
	clk := clockOrReal(s.clock)
	exited := make(chan status, 1)
	driven := make(chan struct{})
	go func() {
		terminal := awaitExitUnreaped(c.pid)
		exited <- terminal
		<-driven
		if terminal.err == nil {
			awaitExit(c.pid)
		}
	}()
	s.driveExit(c, id, session, exited, clk)
	close(driven)
}

func awaitExitUnreaped(pid int) status {
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return status{code: -1, err: err}
		}
		break
	}
	child := (*childSiginfo)(unsafe.Pointer(&info)) //nolint:gosec // the kernel's siginfo_t read as its SIGCHLD arm
	switch child.Code {
	case childExited:
		return status{code: int(child.Status)}
	case childKilled, childDumped:
		return status{code: -1, signal: syscall.Signal(child.Status)}
	}
	return status{code: -1}
}
