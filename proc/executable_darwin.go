//go:build darwin

package proc

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// ExecutablePath returns the absolute exec path captured by the kernel for pid.
func ExecutablePath(pid int) (string, error) {
	auditProcOnce.Do(loadAuditProcessAPI)
	if auditProcErr != nil {
		return "", auditProcErr
	}
	buf := make([]byte, auditPathBufferSize)
	r1 := pidPath(int32(pid), &buf[0], uint32(auditPathBufferSize)) //nolint:gosec // kernel PIDs fit pid_t
	if r1 <= 0 {
		err := currentErrno()
		if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EINVAL) {
			return "", ErrNoProcess
		}
		return "", fmt.Errorf("read executable path for pid %d: %w", pid, err)
	}
	path := buf[:r1]
	if len(path) > 0 && path[len(path)-1] == 0 {
		path = path[:len(path)-1]
	}
	if len(path) == 0 {
		return "", fmt.Errorf("read executable path for pid %d: empty path", pid)
	}
	return string(path), nil
}
