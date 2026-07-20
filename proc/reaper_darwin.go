//go:build darwin

package proc

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

const darwinZombieState = 5

func probeProc(pid int) (procInfo, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return procInfo{}, fmt.Errorf("sysctl kern.proc.pid %d: %w", pid, err)
	}
	if len(procs) == 0 {
		return procInfo{}, errNoProc
	}
	kp := procs[0]
	sid, err := unix.Getsid(pid)
	if errors.Is(err, unix.ESRCH) {
		return procInfo{}, errNoProc
	}
	if err != nil {
		return procInfo{}, fmt.Errorf("getsid %d: %w", pid, err)
	}
	return procInfoFromKinfo(kp, sid), nil
}

func probeGroupMembers(_ int, sessionID int) ([]groupMember, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, fmt.Errorf("sysctl kern.proc.all: %w", err)
	}
	members := make([]groupMember, 0)
	for _, kp := range procs {
		pid := int(kp.Proc.P_pid)
		if pid <= 1 {
			continue
		}
		sid, err := unix.Getsid(pid)
		if errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("getsid %d while enumerating session %d: %w", pid, sessionID, err)
		}
		if sid != sessionID {
			continue
		}
		members = append(members, groupMember{pid: pid, info: procInfoFromKinfo(kp, sid)})
	}
	return members, nil
}

func procInfoFromKinfo(kp unix.KinfoProc, sessionID int) procInfo {
	st := kp.Proc.P_starttime
	comm := string(kp.Proc.P_comm[:])
	if i := strings.IndexByte(comm, 0); i >= 0 {
		comm = comm[:i]
	}
	return procInfo{
		startTime: fmt.Sprintf("%d.%06d", st.Sec, st.Usec),
		comm:      comm,
		groupID:   int(kp.Eproc.Pgid),
		sessionID: sessionID,
		zombie:    kp.Proc.P_stat == darwinZombieState,
	}
}
