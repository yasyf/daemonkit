package proc

import (
	"fmt"
	"os"
	"runtime"
	"strconv"

	"golang.org/x/sys/unix"
)

// CloseInheritedFDs closes every inherited non-CLOEXEC descriptor ≥3.
// Children spawned via Spawn call it FIRST in main, before opening anything
// non-CLOEXEC — an inherited lease fd would stay pinned for the child's life.
func CloseInheritedFDs() error {
	dir := "/dev/fd"
	if runtime.GOOS == "linux" {
		dir = "/proc/self/fd"
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list open fds: %w", err)
	}
	for _, e := range entries {
		fd, err := strconv.Atoi(e.Name())
		if err != nil || fd < 3 {
			continue
		}
		// ReadDir's own transient fd reads EBADF here and is skipped.
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC != 0 {
			continue
		}
		_ = unix.Close(fd)
	}
	return nil
}
