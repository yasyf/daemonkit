package flock

import "errors"

// ErrLockBusy means an attempt found the lock held by another owner: the one
// attempt Spec.TryAcquire makes, or an attempt within a bounded Acquire whose
// budget then ended, where it is joined with the context error. Consumers
// alias it and match with errors.Is.
var ErrLockBusy = errors.New("durable: lock held by another owner")

// ErrInvalidFileLock means a file-lock specification is incomplete or unsafe.
var ErrInvalidFileLock = errors.New("daemonkit: invalid file lock")

// ErrUnsafeLockFile means an existing lock path cannot safely identify one
// advisory-lock inode.
var ErrUnsafeLockFile = errors.New("daemonkit: unsafe lock file")
