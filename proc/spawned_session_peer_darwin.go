//go:build darwin

package proc

import "golang.org/x/sys/unix"

type spawnedSessionPeerCredentialsValue struct {
	PID int
	UID int
}

func spawnedSessionPeerCredentials(fd int) (spawnedSessionPeerCredentialsValue, error) {
	pid, err := unix.GetsockoptInt(fd, unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	if err != nil {
		return spawnedSessionPeerCredentialsValue{}, err
	}
	credentials, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return spawnedSessionPeerCredentialsValue{}, err
	}
	return spawnedSessionPeerCredentialsValue{PID: pid, UID: int(credentials.Uid)}, nil
}
