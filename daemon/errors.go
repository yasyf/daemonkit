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

// ErrShutdownIncomplete requires process exit with retained owned state.
var ErrShutdownIncomplete = errors.New("daemon: shutdown incomplete")
