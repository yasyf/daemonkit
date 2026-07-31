//go:build darwin

package proc

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

const auditPathBufferSize = 4096

var (
	auditProcOnce sync.Once
	auditProcErr  error
	pidPath       func(int32, *byte, uint32) int32
	errnoLocation func() *int32
)

func loadAuditProcessAPI() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		auditProcErr = fmt.Errorf("proc: dlopen libSystem: %w", err)
		return
	}
	purego.RegisterLibFunc(&pidPath, lib, "proc_pidpath")
	purego.RegisterLibFunc(&errnoLocation, lib, "__error")
}

func currentErrno() unix.Errno {
	if errnoLocation == nil || errnoLocation() == nil {
		return unix.EIO
	}
	value := *errnoLocation()
	if value < 0 {
		return unix.EIO
	}
	return unix.Errno(uint32(value))
}

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
