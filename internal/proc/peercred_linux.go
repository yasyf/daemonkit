package proc

import "golang.org/x/sys/unix"

type peerCreds struct {
	pid int
	uid int
}

func peerCredentials(fd int) (peerCreds, error) {
	creds, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return peerCreds{}, err
	}
	return peerCreds{pid: int(creds.Pid), uid: int(creds.Uid)}, nil
}
