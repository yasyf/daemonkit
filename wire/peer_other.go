//go:build !darwin

package wire

import (
	"fmt"

	"github.com/yasyf/daemonkit/proc"
	"golang.org/x/sys/unix"
)

func bindPeerIdentity(peer Peer) (proc.Identity, error) {
	before, err := proc.ExecutablePath(peer.PID)
	if err != nil {
		return proc.Identity{}, err
	}
	identity, err := proc.Probe(peer.PID)
	if err != nil {
		return proc.Identity{}, err
	}
	after, err := proc.ExecutablePath(peer.PID)
	if err != nil {
		return proc.Identity{}, err
	}
	if before != after {
		return proc.Identity{}, proc.ErrIdentityChanged
	}
	identity.Executable = after
	return identity, nil
}

func peerFromFD(fd int) (Peer, error) {
	cred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return Peer{}, fmt.Errorf("wire: getsockopt SO_PEERCRED: %w", err)
	}
	return Peer{PID: int(cred.Pid), UID: int(cred.Uid)}, nil
}
