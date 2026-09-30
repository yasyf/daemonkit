package trust

import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/yasyf/daemonkit/internal/proc"
	"golang.org/x/sys/unix"
)

const auditTokenLength = 32

var (
	getsockoptOnce sync.Once
	getsockoptSym  uintptr
	getsockoptErr  error
)

func loadGetsockopt() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		getsockoptErr = fmt.Errorf("trust: dlopen libSystem: %w", err)
		return
	}
	getsockoptSym, getsockoptErr = purego.Dlsym(lib, "getsockopt")
}

// LOCAL_PEERPID is deliberately not read: the audit token already carries the
// pid, and a separately fetched one is a value that can disagree with it.
func peerFromFD(fd int) (Peer, error) {
	credentials, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return Peer{}, fmt.Errorf("trust: getsockopt LOCAL_PEERCRED: %w", err)
	}
	raw, err := peerToken(fd)
	if err != nil {
		return Peer{}, err
	}
	token, err := proc.AuditTokenFromBytes(raw)
	if err != nil {
		return Peer{}, fmt.Errorf("trust: peer audit token: %w", err)
	}
	return Peer{UID: int(credentials.Uid), Token: token}, nil
}

func peerToken(fd int) ([]byte, error) {
	getsockoptOnce.Do(loadGetsockopt)
	if getsockoptErr != nil {
		return nil, getsockoptErr
	}
	token := make([]byte, auditTokenLength)
	length := uint32(auditTokenLength)
	var pinner runtime.Pinner
	pinner.Pin(&token[0])
	pinner.Pin(&length)
	defer pinner.Unpin()
	result, _, errno := purego.SyscallN(
		getsockoptSym,
		uintptr(fd), uintptr(unix.SOL_LOCAL), uintptr(unix.LOCAL_PEERTOKEN),
		uintptr(unsafe.Pointer(&token[0])), //nolint:gosec // FFI requires the pinned buffer pointer.
		uintptr(unsafe.Pointer(&length)),   //nolint:gosec // FFI requires the pinned length pointer.
		0,
	)
	if int32(result) != 0 { //nolint:gosec // getsockopt returns a C int in the low half of the register.
		return nil, fmt.Errorf("trust: getsockopt LOCAL_PEERTOKEN: %w",
			unix.Errno(uint32(errno))) //nolint:gosec // The errno is a C int; the upper half is noise.
	}
	if length != auditTokenLength {
		return nil, fmt.Errorf("trust: LOCAL_PEERTOKEN returned %d bytes, want %d", length, auditTokenLength)
	}
	return token, nil
}
