package daemon

import "errors"

// ErrRefuseVictim means a takeover refused to signal a PID it must never target:
// init (pid <= 1) or the successor's own process. A PID alone is never kill
// authority.
var ErrRefuseVictim = errors.New("daemon: refusing to signal pid <= 1 or self")

// ErrReleaseTimeout means a takeover waited out its release deadline while the
// incumbent still held the socket (or had not exited, for PIDExit).
var ErrReleaseTimeout = errors.New("daemon: incumbent did not release before the deadline")

// ErrEnsureTimeout means EnsureCurrent's deadline elapsed before the peer
// answering Health reported the target version.
var ErrEnsureTimeout = errors.New("daemon: peer did not reach the target version before the deadline")

// ErrUnknownContract means a Takeover config carried an unrecognized Contract.
var ErrUnknownContract = errors.New("daemon: unknown takeover contract")

// ErrUnknownWaitMode means a Takeover config carried an unrecognized WaitMode.
var ErrUnknownWaitMode = errors.New("daemon: unknown takeover wait mode")
