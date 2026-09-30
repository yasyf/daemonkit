//go:build !daemonkit_unsigned

package trust

import (
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/yasyf/daemonkit/internal/proc"
)

var (
	csopsOnce sync.Once
	csopsSym  uintptr
	csopsErr  error
)

func loadCsops() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		csopsErr = fmt.Errorf("trust: dlopen libSystem: %w", err)
		return
	}
	symbol, err := purego.Dlsym(lib, "csops_audittoken")
	if err != nil {
		csopsErr = fmt.Errorf("trust: dlsym csops_audittoken: %w", err)
		return
	}
	csopsSym = symbol
}

// kernelReads binds the csops_audittoken seam to one execution generation:
//
//	int csops_audittoken(pid_t pid, uint32_t ops, void *useraddr,
//	                     size_t usersize, audit_token_t *token);
//
// The token, not the pid, is the subject. A pid names a slot in the process
// table; the token's pidversion names the execution that opened the socket and
// is never reused, so a peer that exited between accept and this call is
// ESRCH rather than someone else's process.
func kernelReads(token proc.AuditToken) csopsRead {
	return func(op uint32, buf []byte) syscall.Errno {
		var pinner runtime.Pinner
		pinner.Pin(&buf[0])
		pinner.Pin(&token)
		result, _, errno := purego.SyscallN(
			csopsSym,
			uintptr(token.PID()), uintptr(op),
			uintptr(unsafe.Pointer(&buf[0])), //nolint:gosec // FFI requires the pinned buffer pointer.
			uintptr(len(buf)),
			uintptr(unsafe.Pointer(&token)), //nolint:gosec // FFI requires the pinned token pointer.
		)
		pinner.Unpin()
		if int32(result) == 0 { //nolint:gosec // csops returns a C int in the low half of the register.
			return 0
		}
		// purego's darwin trampoline loads the 4-byte C errno with an 8-byte
		// move, so the upper half is adjacent thread-local noise.
		return syscall.Errno(uint32(errno)) //nolint:gosec // The errno is a C int; the upper half is noise.
	}
}
