//go:build darwin

package proc

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/ebitengine/purego"
)

const (
	taskAuditToken      = 15
	taskAuditTokenCount = 8
)

var (
	currentIdentityOnce sync.Once
	currentIdentityErr  error
	taskInfo            func(uint32, int32, *AuditToken, *uint32) int32
	machTaskSelf        uint32
)

func readUint32(address uintptr) uint32

func loadCurrentIdentityAPI() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		currentIdentityErr = fmt.Errorf("proc: dlopen libSystem for current audit token: %w", err)
		return
	}
	purego.RegisterLibFunc(&taskInfo, lib, "task_info")
	self, err := purego.Dlsym(lib, "mach_task_self_")
	if err != nil {
		currentIdentityErr = fmt.Errorf("proc: resolve mach_task_self_: %w", err)
		return
	}
	machTaskSelf = readUint32(self)
	if machTaskSelf == 0 {
		currentIdentityErr = errors.New("proc: mach_task_self_ is zero")
	}
}

// CurrentIdentity binds the current Darwin process to its kernel audit token,
// exact executable, boot, and start stamp.
func CurrentIdentity() (Identity, error) {
	currentIdentityOnce.Do(loadCurrentIdentityAPI)
	if currentIdentityErr != nil {
		return Identity{}, currentIdentityErr
	}
	var token AuditToken
	count := uint32(taskAuditTokenCount)
	if status := taskInfo(machTaskSelf, taskAuditToken, &token, &count); status != 0 {
		return Identity{}, fmt.Errorf("proc: task_info TASK_AUDIT_TOKEN: kern_return_t %d", status)
	}
	if count != taskAuditTokenCount {
		return Identity{}, fmt.Errorf("proc: TASK_AUDIT_TOKEN returned %d words, want %d", count, taskAuditTokenCount)
	}
	return BindAuditTokenIdentity(token, os.Getpid())
}
