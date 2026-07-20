package daemon

import "errors"

// ErrNoPeer means the lifecycle endpoint has no listening peer, returned only
// when connection setup proves the endpoint absent.
var ErrNoPeer = errors.New("daemon: no lifecycle peer")

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

// ErrProtocolMismatch refuses takeover across an incompatible lifecycle protocol.
var ErrProtocolMismatch = errors.New("daemon: lifecycle protocol mismatch")

// ErrRuntimeStarted refuses a second Run on the same Runtime.
var ErrRuntimeStarted = errors.New("daemon: runtime already started")

// ErrRuntimeNotRunning refuses lifecycle requests before Run starts.
var ErrRuntimeNotRunning = errors.New("daemon: runtime is not running")

// ErrRuntimeClosed refuses lifecycle requests after Run finishes.
var ErrRuntimeClosed = errors.New("daemon: runtime is closed")

// ErrRuntimeNotReady means the runtime has not reached or no longer retains
// healthy serving readiness.
var ErrRuntimeNotReady = errors.New("daemon: runtime is not ready")

// ErrRuntimeReady means a session server published readiness more than once.
var ErrRuntimeReady = errors.New("daemon: runtime readiness already published")

// ErrSessionServerStopped reports a session server that returned without a shutdown request.
var ErrSessionServerStopped = errors.New("daemon: session server stopped unexpectedly")

// ErrEmbeddedProcessStarted refuses a second Start on an EmbeddedProcess.
var ErrEmbeddedProcessStarted = errors.New("daemon: embedded process already started")

// ErrEmbeddedProcessNotStarted refuses lifecycle operations before Start.
var ErrEmbeddedProcessNotStarted = errors.New("daemon: embedded process is not started")
