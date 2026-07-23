// Package proc holds exact durable process identity, ownership, and reaping.
package proc

import "errors"

// ErrLockBusy means FileLockSpec.TryAcquire found the lock held by another owner;
// consumers alias it and match with errors.Is.
var ErrLockBusy = errors.New("proc: lock held by another owner")

// ErrInvalidFileLock means a file-lock specification is incomplete or unsafe.
var ErrInvalidFileLock = errors.New("proc: invalid file lock")

// ErrUnsafeLockFile means an existing lock path cannot safely identify one
// advisory-lock inode.
var ErrUnsafeLockFile = errors.New("proc: unsafe lock file")
