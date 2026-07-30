package proc

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const handoffFD = 3

func socketpairFiles() (parent, child *os.File, err error) {
	syscall.ForkLock.Lock()
	defer syscall.ForkLock.Unlock()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("proc: create handoff socketpair: %w", err)
	}
	for _, fd := range fds {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
			_ = unix.Close(fds[0])
			_ = unix.Close(fds[1])
			return nil, nil, fmt.Errorf("proc: secure handoff socketpair: %w", err)
		}
	}
	return os.NewFile(uintptr(fds[0]), "daemonkit-handoff-parent"),
		os.NewFile(uintptr(fds[1]), "daemonkit-handoff-child"), nil
}

// ClaimHandoff proves and returns the inherited handoff end inside a spawned
// child: dup CLOEXEC, verify AF_UNIX SOCK_STREAM and that the socket peer is
// the exact parent, then close the original — K16's dup-then-prove, one
// function. A refusal leaves the original descriptor unchanged; a child that
// cannot prove its handoff exits, it does not negotiate.
func ClaimHandoff() (*os.File, error) {
	dup, err := unix.FcntlInt(handoffFD, unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("proc: dup handoff fd %d: %w", handoffFD, err)
	}
	if err := proveHandoff(dup); err != nil {
		_ = unix.Close(dup)
		return nil, err
	}
	_ = unix.Close(handoffFD)
	return os.NewFile(uintptr(dup), "daemonkit-handoff"), nil
}

func proveHandoff(fd int) error {
	kind, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil {
		return fmt.Errorf("proc: handoff fd is not a socket: %w", err)
	}
	if kind != unix.SOCK_STREAM {
		return fmt.Errorf("proc: handoff socket type is %d, want SOCK_STREAM", kind)
	}
	name, err := unix.Getsockname(fd)
	if err != nil {
		return fmt.Errorf("proc: read handoff socket name: %w", err)
	}
	if _, ok := name.(*unix.SockaddrUnix); !ok {
		return fmt.Errorf("proc: handoff socket family is %T, want AF_UNIX", name)
	}
	creds, err := peerCredentials(fd)
	if err != nil {
		return fmt.Errorf("proc: read handoff peer credentials: %w", err)
	}
	if creds.pid != os.Getppid() || creds.uid != os.Getuid() {
		return fmt.Errorf(
			"proc: handoff peer pid %d uid %d is not the parent pid %d uid %d",
			creds.pid, creds.uid, os.Getppid(), os.Getuid(),
		)
	}
	return nil
}
