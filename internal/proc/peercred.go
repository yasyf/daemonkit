package proc

import "golang.org/x/sys/unix"

type peerCreds struct {
	pid int
	uid int
}

func peerCredentials(fd int) (peerCreds, error) {
	pid, err := unix.GetsockoptInt(fd, unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	if err != nil {
		return peerCreds{}, err
	}
	creds, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return peerCreds{}, err
	}
	return peerCreds{pid: pid, uid: int(creds.Uid)}, nil
}
