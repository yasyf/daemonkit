//go:build linux

package proc

import "golang.org/x/sys/unix"

type spawnedSessionPeerCredentialsValue struct {
	PID int
	UID int
}

func spawnedSessionPeerCredentials(fd int) (spawnedSessionPeerCredentialsValue, error) {
	credentials, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return spawnedSessionPeerCredentialsValue{}, err
	}
	return spawnedSessionPeerCredentialsValue{PID: int(credentials.Pid), UID: int(credentials.Uid)}, nil
}
