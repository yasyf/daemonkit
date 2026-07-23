//go:build !darwin

package peer

import (
	"fmt"

	"github.com/yasyf/daemonkit/proc"
	"golang.org/x/sys/unix"
)

func bindProcess(identity Identity) (proc.Identity, error) {
	before, err := proc.ExecutablePath(identity.PID)
	if err != nil {
		return proc.Identity{}, err
	}
	process, err := proc.Probe(identity.PID)
	if err != nil {
		return proc.Identity{}, err
	}
	after, err := proc.ExecutablePath(identity.PID)
	if err != nil {
		return proc.Identity{}, err
	}
	if before != after {
		return proc.Identity{}, proc.ErrIdentityChanged
	}
	process.Executable = after
	return process, nil
}

func fromFD(fd int) (Identity, error) {
	cred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return Identity{}, fmt.Errorf("peer: getsockopt SO_PEERCRED: %w", err)
	}
	return Identity{PID: int(cred.Pid), UID: int(cred.Uid)}, nil
}
