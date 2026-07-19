// Package proc holds consumer-agnostic process primitives: a flock
// single-entrant socket bind, a detached-child spawn, exponential backoff,
// and sliding-window strike/ladder breakers.
package proc

import "errors"

// ErrChildUnavailable means a child process could not be reached or started;
// an availability condition, never a domain verdict — drivers retry.
var ErrChildUnavailable = errors.New("child process not running")

// ErrSkipSpawn is the benign spawn refusal a CanHost returns to mean there is
// nothing for a child to serve; callers treat it as a no-op, never a failure.
var ErrSkipSpawn = errors.New("nothing for the child to serve; spawn skipped")

// ErrPeerStarting means the ".lock" file is held but its owner does not answer
// the Evict probe yet (mid-start: post-flock, pre-bind); callers retry.
var ErrPeerStarting = errors.New("a peer owns the socket lock but is not answering yet")

// ErrLockStillHeld means Evict made way but the contended lock was not
// released within the post-evict poll deadline.
var ErrLockStillHeld = errors.New("socket lock still held after eviction")

// ErrLockBusy means FileLockSpec.TryAcquire found the lock held by another owner;
// consumers alias it and match with errors.Is.
var ErrLockBusy = errors.New("proc: lock held by another owner")

// ErrInvalidFileLock means a file-lock specification is incomplete or unsafe.
var ErrInvalidFileLock = errors.New("proc: invalid file lock")

// ErrUnsafeLockFile means an existing lock path cannot safely identify one
// advisory-lock inode.
var ErrUnsafeLockFile = errors.New("proc: unsafe lock file")
