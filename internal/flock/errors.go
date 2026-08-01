package flock

import "errors"

// ErrLockBusy means Spec.TryAcquire found the lock held by another owner;
// consumers alias it and match with errors.Is.
var ErrLockBusy = errors.New("durable: lock held by another owner")

// ErrInvalidFileLock means a file-lock specification is incomplete or unsafe.
var ErrInvalidFileLock = errors.New("daemonkit: invalid file lock")

// ErrUnsafeLockFile means an existing lock path cannot safely identify one
// advisory-lock inode.
var ErrUnsafeLockFile = errors.New("daemonkit: unsafe lock file")
