package trust

import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/yasyf/daemonkit/internal/proc"
)

const (
	taskAuditTokenFlavor = 15
	taskAuditTokenCount  = auditTokenLength / 4
)

var (
	machOnce sync.Once
	machErr  error

	machSelf       uint32
	taskSelfTrap   uintptr
	taskNameForPID uintptr
	taskInfo       uintptr
	portDeallocate uintptr
)

func loadMach() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		machErr = fmt.Errorf("trust: dlopen libSystem: %w", err)
		return
	}
	for _, binding := range []struct {
		name string
		into *uintptr
	}{
		{"task_self_trap", &taskSelfTrap},
		{"task_name_for_pid", &taskNameForPID},
		{"task_info", &taskInfo},
		{"mach_port_deallocate", &portDeallocate},
	} {
		symbol, err := purego.Dlsym(lib, binding.name)
		if err != nil {
			machErr = fmt.Errorf("trust: dlsym %s: %w", binding.name, err)
			return
		}
		*binding.into = symbol
	}
	// The trap is called once and its name cached, the way libSystem caches it
	// into mach_task_self_: each call mints a user reference on the task's own
	// port, and a per-verify call would leak one.
	self, _, _ := purego.SyscallN(taskSelfTrap)
	machSelf = uint32(self) //nolint:gosec // task_self_trap returns a mach_port_name_t in the low half of the register.
}

// ProcessToken mints pid's kernel-held audit token from the task name port,
// the identity every csops_audittoken read is judged against. It answers for a
// child suspended at its entry point: the kernel establishes the CodeDirectory
// at exec, before the first instruction, so an exec-posture gate can read the
// exact image that is about to run.
//
// The whole mach family reports through kern_return_t in the return register,
// so nothing here reads the thread-local errno and no second call can hand
// back another thread's value.
func ProcessToken(pid int) (proc.AuditToken, error) {
	machOnce.Do(loadMach)
	if machErr != nil {
		return proc.AuditToken{}, fmt.Errorf("%w: %w", ErrNoVerifier, machErr)
	}
	var port uint32
	var portPinner runtime.Pinner
	portPinner.Pin(&port)
	result, _, _ := purego.SyscallN(
		taskNameForPID,
		uintptr(machSelf), uintptr(pid),
		uintptr(unsafe.Pointer(&port)), //nolint:gosec // FFI requires the pinned port pointer.
	)
	portPinner.Unpin()
	if kr := int32(result); kr != 0 { //nolint:gosec // kern_return_t is a C int in the low half of the register.
		return proc.AuditToken{}, fmt.Errorf("%w: task_name_for_pid %d: kern_return %d", ErrPeerGone, pid, kr)
	}
	defer purego.SyscallN(portDeallocate, uintptr(machSelf), uintptr(port)) //nolint:errcheck // a port name we minted always deallocates.

	raw := make([]byte, auditTokenLength)
	count := uint32(taskAuditTokenCount)
	var tokenPinner runtime.Pinner
	tokenPinner.Pin(&raw[0])
	tokenPinner.Pin(&count)
	result, _, _ = purego.SyscallN(
		taskInfo,
		uintptr(port), uintptr(taskAuditTokenFlavor),
		uintptr(unsafe.Pointer(&raw[0])), //nolint:gosec // FFI requires the pinned buffer pointer.
		uintptr(unsafe.Pointer(&count)),  //nolint:gosec // FFI requires the pinned count pointer.
	)
	tokenPinner.Unpin()
	if kr := int32(result); kr != 0 { //nolint:gosec // kern_return_t is a C int in the low half of the register.
		return proc.AuditToken{}, fmt.Errorf("%w: task_info TASK_AUDIT_TOKEN for pid %d: kern_return %d", ErrPeerGone, pid, kr)
	}
	if count != taskAuditTokenCount {
		return proc.AuditToken{}, fmt.Errorf("%w: task_info returned %d words, want %d", ErrNoVerifier, count, taskAuditTokenCount)
	}
	token, err := proc.AuditTokenFromBytes(raw)
	if err != nil {
		return proc.AuditToken{}, fmt.Errorf("trust: process audit token: %w", err)
	}
	if token.PID() != pid {
		return proc.AuditToken{}, fmt.Errorf("%w: task_info token names pid %d, want %d", ErrPeerGone, token.PID(), pid)
	}
	return token, nil
}

// VerifyProcess enforces req against the kernel-held code identity of a live
// process, addressed by the audit token its task name port mints rather than
// by pid: the token's pidversion binds the verdict to one execution, so a pid
// reused between the mint and the reads is ErrPeerGone rather than someone
// else's verdict. Every failure denies, with the same three classes Verify
// reports.
func VerifyProcess(pid int, req Requirement) error {
	if err := req.Validate(); err != nil {
		return err
	}
	token, err := ProcessToken(pid)
	if err != nil {
		return err
	}
	return verifyRequirement(token, req)
}
