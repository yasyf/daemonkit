package proc

import (
	"errors"
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
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("list open fds: %w", err)
	}
	names, readErr := directory.Readdirnames(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		return fmt.Errorf("list open fds: %w", errors.Join(readErr, closeErr))
	}
	for _, name := range names {
		fd, err := strconv.Atoi(name)
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
