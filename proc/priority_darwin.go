//go:build darwin

package proc

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Nice lowers the calling process's scheduling priority to n — classic
// nice(2), inherited by children, deliberately NOT the Darwin background band
// (its I/O tier starves a data-plane server; a self-set band cannot be
// cleared from outside). One-way for unprivileged processes: set once.
func Nice(n int) error {
	if err := unix.Setpriority(unix.PRIO_PROCESS, 0, n); err != nil {
		return fmt.Errorf("set nice %d: %w", n, err)
	}
	return nil
}
