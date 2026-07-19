//go:build darwin

package proc

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func processIDs() ([]int, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, fmt.Errorf("enumerate process table: %w", err)
	}
	pids := make([]int, 0, len(procs))
	for _, kp := range procs {
		if pid := int(kp.Proc.P_pid); pid > 1 && kp.Proc.P_stat != darwinZombieState {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}
