// Package proc holds exact durable process identity, ownership, and reaping.
package proc

import "github.com/yasyf/daemonkit/internal/flock"

// ErrLockBusy means flock.Spec.TryAcquire found the lock held by another owner;
// consumers alias it and match with errors.Is.
var ErrLockBusy = flock.ErrLockBusy

// ErrInvalidFileLock means a file-lock specification is incomplete or unsafe.
var ErrInvalidFileLock = flock.ErrInvalidFileLock

// ErrUnsafeLockFile means an existing lock path cannot safely identify one
// advisory-lock inode.
var ErrUnsafeLockFile = flock.ErrUnsafeLockFile
