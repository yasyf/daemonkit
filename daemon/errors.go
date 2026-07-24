package daemon

import "errors"

// ErrRuntimeStarted refuses a second Run on the same Runtime.
var ErrRuntimeStarted = errors.New("daemon: runtime already started")

// ErrRuntimeNotRunning refuses runtime operations before Run starts.
var ErrRuntimeNotRunning = errors.New("daemon: runtime is not running")

// ErrRuntimeClosed refuses runtime operations after Run finishes.
var ErrRuntimeClosed = errors.New("daemon: runtime is closed")

// ErrRuntimeNotReady means the runtime has not reached or no longer retains
// healthy serving readiness.
var ErrRuntimeNotReady = errors.New("daemon: runtime is not ready")

// ErrDraining means runtime intake has closed.
var ErrDraining = errors.New("daemon: runtime is draining")

// ErrSequenceExhausted means a monotonic runtime sequence cannot advance
// without consuming state reserved for terminal settlement.
var ErrSequenceExhausted = errors.New("daemon: sequence exhausted")

// ErrSessionServerStopped reports a session server that returned without a shutdown request.
var ErrSessionServerStopped = errors.New("daemon: session server stopped unexpectedly")

// ErrTrustVerifierProbe means the serve-time self-probe could not complete one
// verifier child exchange, so every peer would be silently rejected as
// untrusted. The daemon executable must dispatch trust.RunVerifierChild at the
// top of main, before argument parsing.
var ErrTrustVerifierProbe = errors.New("daemon: trust verifier self-probe failed; the daemon executable must dispatch trust.RunVerifierChild")

// ErrShutdownIncomplete requires process exit with retained owned state.
var ErrShutdownIncomplete = errors.New("daemon: shutdown incomplete")

// ErrProductSettlementUnavailable means this activation cannot issue a settlement claim.
var ErrProductSettlementUnavailable = errors.New("daemon: product settlement is unavailable")

// ErrProductSettlementActive means completion was attempted before generation cancellation.
var ErrProductSettlementActive = errors.New("daemon: product settlement generation is active")

// ErrProductSettlementStale means a settlement claim is expired, completed, or foreign.
var ErrProductSettlementStale = errors.New("daemon: product settlement is stale")
