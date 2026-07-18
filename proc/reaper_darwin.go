//go:build darwin

package proc

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func probeProc(pid int) (procInfo, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return procInfo{}, fmt.Errorf("sysctl kern.proc.pid %d: %w", pid, err)
	}
	if len(procs) == 0 {
		return procInfo{}, errNoProc
	}
	kp := procs[0]
	st := kp.Proc.P_starttime
	comm := string(kp.Proc.P_comm[:])
	if i := strings.IndexByte(comm, 0); i >= 0 {
		comm = comm[:i]
	}
	return procInfo{startTime: fmt.Sprintf("%d.%06d", st.Sec, st.Usec), comm: comm}, nil
}
