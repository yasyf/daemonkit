//go:build darwin

package proc

import (
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

const auditPathBufferSize = 4096

var (
	auditProcOnce   sync.Once
	auditProcErr    error
	pidPathAudit    func(*AuditToken, *byte, uint32) int32
	pidPath         func(int32, *byte, uint32) int32
	procSignalAudit func(*AuditToken, int32) int32
	errnoLocation   func() *int32
)

func loadAuditProcessAPI() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		auditProcErr = fmt.Errorf("proc: dlopen libSystem: %w", err)
		return
	}
	purego.RegisterLibFunc(&pidPathAudit, lib, "proc_pidpath_audittoken")
	purego.RegisterLibFunc(&pidPath, lib, "proc_pidpath")
	purego.RegisterLibFunc(&procSignalAudit, lib, "proc_signal_with_audittoken")
	purego.RegisterLibFunc(&errnoLocation, lib, "__error")
}

// ExecutablePathAuditToken resolves the executable belonging to the exact
// audit-token execution, never whichever process currently holds its PID.
func ExecutablePathAuditToken(token AuditToken) (string, error) {
	if !token.Valid() {
		return "", ErrNoAuditToken
	}
	auditProcOnce.Do(loadAuditProcessAPI)
	if auditProcErr != nil {
		return "", auditProcErr
	}
	buf := make([]byte, auditPathBufferSize)
	r1 := pidPathAudit(&token, &buf[0], uint32(auditPathBufferSize))
	if r1 <= 0 {
		errno := currentErrno()
		if errors.Is(errno, unix.ESRCH) || errors.Is(errno, unix.ENOENT) {
			return "", ErrNoProcess
		}
		return "", fmt.Errorf("proc_pidpath_audittoken pid %d version %d: %w", token.PID(), token.PIDVersion(), errno)
	}
	path := buf[:r1]
	if len(path) > 0 && path[len(path)-1] == 0 {
		path = path[:len(path)-1]
	}
	if len(path) == 0 {
		return "", errors.New("proc_pidpath_audittoken returned an empty path")
	}
	return string(path), nil
}

func signalAuditToken(token AuditToken, sig syscall.Signal) (bool, error) {
	if !token.Valid() {
		return false, ErrNoAuditToken
	}
	auditProcOnce.Do(loadAuditProcessAPI)
	if auditProcErr != nil {
		return false, auditProcErr
	}
	r1 := procSignalAudit(&token, int32(sig)) //nolint:gosec // Darwin signals are small positive C ints
	if r1 == -1 {
		errno := currentErrno()
		if errors.Is(errno, unix.ESRCH) {
			return true, nil
		}
		return false, fmt.Errorf("proc_signal_with_audittoken pid %d version %d: %w", token.PID(), token.PIDVersion(), errno)
	}
	return false, nil
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
