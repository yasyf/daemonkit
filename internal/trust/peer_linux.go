package trust

import (
	"fmt"

	"github.com/yasyf/daemonkit/internal/proc"
	"golang.org/x/sys/unix"
)

// SO_PEERCRED answers with the credentials the peer held when it connected,
// read from one getsockopt so the uid and the pid cannot disagree.
func peerFromFD(fd int) (Peer, error) {
	credentials, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return Peer{}, fmt.Errorf("trust: getsockopt SO_PEERCRED: %w", err)
	}
	return Peer{UID: int(credentials.Uid), Token: proc.PeerToken(int(credentials.Pid))}, nil
}
