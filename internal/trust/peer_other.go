//go:build !darwin

package trust

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Token is left zero: no platform but darwin binds an execution generation to
// an accepted socket, and Verify's fail-closed verifier denies every
// configured Requirement here anyway.
func peerFromFD(fd int) (Peer, error) {
	credentials, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return Peer{}, fmt.Errorf("trust: getsockopt SO_PEERCRED: %w", err)
	}
	return Peer{UID: int(credentials.Uid)}, nil
}
