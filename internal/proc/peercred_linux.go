package proc

import "golang.org/x/sys/unix"

func peerCredentials(fd int) (peerCreds, error) {
	creds, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return peerCreds{}, err
	}
	return peerCreds{pid: int(creds.Pid), uid: int(creds.Uid)}, nil
}
