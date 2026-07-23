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

// ErrRuntimeReady means a session server published readiness more than once.
var ErrRuntimeReady = errors.New("daemon: runtime readiness already published")

// ErrSessionServerStopped reports a session server that returned without a shutdown request.
var ErrSessionServerStopped = errors.New("daemon: session server stopped unexpectedly")

// ErrEmbeddedProcessStarted refuses a second Start on an EmbeddedProcess.
var ErrEmbeddedProcessStarted = errors.New("daemon: embedded process already started")

// ErrEmbeddedProcessNotStarted refuses embedded-process operations before Start.
var ErrEmbeddedProcessNotStarted = errors.New("daemon: embedded process is not started")
