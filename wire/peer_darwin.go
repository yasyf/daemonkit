//go:build darwin

package wire

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

// auditTokenLen is the byte size of darwin's audit_token_t (8 uint32).
const auditTokenLen = 32

var (
	getsockoptOnce sync.Once
	getsockoptSym  uintptr
	getsockoptErr  error
)

func loadGetsockopt() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		getsockoptErr = fmt.Errorf("wire: dlopen libSystem: %w", err)
		return
	}
	sym, err := purego.Dlsym(lib, "getsockopt")
	if err != nil {
		getsockoptErr = fmt.Errorf("wire: dlsym getsockopt: %w", err)
		return
	}
	getsockoptSym = sym
}

func peerFromFD(fd int) (Peer, error) {
	xu, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return Peer{}, fmt.Errorf("wire: getsockopt LOCAL_PEERCRED: %w", err)
	}
	pid, err := unix.GetsockoptInt(fd, unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	if err != nil {
		return Peer{}, fmt.Errorf("wire: getsockopt LOCAL_PEERPID: %w", err)
	}
	audit, err := peerAuditToken(fd)
	if err != nil {
		return Peer{}, err
	}
	return Peer{PID: pid, UID: int(xu.Uid), Audit: audit}, nil
}

// A raw getsockopt read: GetsockoptString truncates at the first NUL, and the
// token routinely holds NULs. Query-time binding — see trust.Policy.Check.
func peerAuditToken(fd int) ([]byte, error) {
	getsockoptOnce.Do(loadGetsockopt)
	if getsockoptErr != nil {
		return nil, getsockoptErr
	}
	token := make([]byte, auditTokenLen)
	vallen := uint32(auditTokenLen)
	r1, _, errno := purego.SyscallN(getsockoptSym,
		uintptr(fd),
		uintptr(unix.SOL_LOCAL),
		uintptr(unix.LOCAL_PEERTOKEN),
		uintptr(unsafe.Pointer(&token[0])), //nolint:gosec // G103: FFI getsockopt needs the raw buffer pointer; purego.SyscallN is go:uintptrescapes so the arg stays pinned across the call
		uintptr(unsafe.Pointer(&vallen)),   //nolint:gosec // G103: FFI getsockopt needs the raw len pointer; purego.SyscallN is go:uintptrescapes so the arg stays pinned across the call
		0,
	)
	if r1 != 0 {
		return nil, fmt.Errorf("wire: getsockopt LOCAL_PEERTOKEN: %w", unix.Errno(errno))
	}
	if vallen != auditTokenLen {
		return nil, fmt.Errorf("wire: LOCAL_PEERTOKEN returned %d bytes, want %d", vallen, auditTokenLen)
	}
	return token, nil
}
