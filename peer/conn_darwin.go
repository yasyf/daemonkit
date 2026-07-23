//go:build darwin

package peer

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/yasyf/daemonkit/proc"
	"golang.org/x/sys/unix"
)

func bindProcess(identity Identity) (proc.Identity, error) {
	token, err := proc.AuditTokenFromBytes(identity.Audit)
	if err != nil {
		return proc.Identity{}, err
	}
	return proc.BindAuditTokenIdentity(token, identity.PID)
}

const auditTokenLen = 32

var (
	getsockoptOnce sync.Once
	getsockoptSym  uintptr
	getsockoptErr  error
)

func loadGetsockopt() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		getsockoptErr = fmt.Errorf("peer: dlopen libSystem: %w", err)
		return
	}
	getsockoptSym, getsockoptErr = purego.Dlsym(lib, "getsockopt")
}

func fromFD(fd int) (Identity, error) {
	xu, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return Identity{}, fmt.Errorf("peer: getsockopt LOCAL_PEERCRED: %w", err)
	}
	pid, err := unix.GetsockoptInt(fd, unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	if err != nil {
		return Identity{}, fmt.Errorf("peer: getsockopt LOCAL_PEERPID: %w", err)
	}
	audit, err := auditToken(fd)
	if err != nil {
		return Identity{}, err
	}
	return Identity{PID: pid, UID: int(xu.Uid), Audit: audit}, nil
}

func auditToken(fd int) ([]byte, error) {
	getsockoptOnce.Do(loadGetsockopt)
	if getsockoptErr != nil {
		return nil, getsockoptErr
	}
	token := make([]byte, auditTokenLen)
	vallen := uint32(auditTokenLen)
	r1, _, errno := purego.SyscallN(getsockoptSym,
		uintptr(fd), uintptr(unix.SOL_LOCAL), uintptr(unix.LOCAL_PEERTOKEN),
		uintptr(unsafe.Pointer(&token[0])), //nolint:gosec // FFI requires the pinned buffer pointer.
		uintptr(unsafe.Pointer(&vallen)),   //nolint:gosec // FFI requires the pinned length pointer.
		0,
	)
	if r1 != 0 {
		return nil, fmt.Errorf("peer: getsockopt LOCAL_PEERTOKEN: %w", unix.Errno(errno))
	}
	if vallen != auditTokenLen {
		return nil, fmt.Errorf("peer: LOCAL_PEERTOKEN returned %d bytes, want %d", vallen, auditTokenLen)
	}
	return token, nil
}
