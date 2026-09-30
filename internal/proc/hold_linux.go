package proc

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

const noPidfd = -1

// pidfdHold pins one process through its pidfd. The descriptor names the
// process it was opened on for as long as it is held, so a signal sent through
// it after that process was reaped is ESRCH and never reaches whatever the PID
// names next.
type pidfdHold struct{ fd int }

func (sysSignaler) hold(pid int) (held, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if errors.Is(err, unix.ESRCH) {
		return pidfdHold{fd: noPidfd}, nil
	}
	if err != nil {
		return nil, err
	}
	return pidfdHold{fd: fd}, nil
}

func (h pidfdHold) signal(sig syscall.Signal) error {
	if h.fd == noPidfd {
		return syscall.ESRCH
	}
	return unix.PidfdSendSignal(h.fd, sig, nil, 0)
}

func (h pidfdHold) release() {
	if h.fd != noPidfd {
		_ = unix.Close(h.fd)
	}
}
