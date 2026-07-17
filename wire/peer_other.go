//go:build !darwin

package wire

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func peerFromFD(fd int) (Peer, error) {
	cred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return Peer{}, fmt.Errorf("wire: getsockopt SO_PEERCRED: %w", err)
	}
	return Peer{PID: int(cred.Pid), UID: int(cred.Uid)}, nil
}
