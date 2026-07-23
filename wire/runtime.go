package wire

import (
	"errors"
	"io"
	"os"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/internal/runtimeauth"
)

// RuntimeConfig composes the sole product session server with daemonkit's
// process runtime, readiness barrier, and receipt-authenticated stop route.
type RuntimeConfig struct {
	Socket                    string
	RuntimeBuild              string
	RuntimeProtocol           int
	Wire                      *Server
	Classifier                ProtectedSessionClassifier
	ReservedProtectedSessions int
	StopVerifier              StopControlVerifier
	Observations              []ObservationRoute
	Readiness                 ReadinessBarrier
	BootstrapRoutes           []BootstrapRoute

	ListenerWait time.Duration
	Admission    daemon.Admission
	Workers      daemon.Workers
	State        io.Closer
	Resources    daemon.Resources
	Activate     func(daemon.Activation) error
	Busy         func() bool
	HealthState  func() daemon.State

	ShutdownTimeout time.Duration
	Signals         <-chan os.Signal
}

// NewRuntime constructs the sole daemon process coordinator. Products stop
// and settle an installed generation before starting another.
func NewRuntime(config RuntimeConfig) (*daemon.Runtime, error) {
	if config.Wire == nil {
		return nil, errors.New("wire: runtime server is required")
	}
	if config.Wire.WireBuild == "" {
		return nil, errors.New("wire: runtime server build is required")
	}
	composed, err := runtimeauth.Build(daemon.RuntimeConfig{
		Socket: config.Socket, RuntimeBuild: config.RuntimeBuild, RuntimeProtocol: config.RuntimeProtocol,
		ListenerWait: config.ListenerWait, Admission: config.Admission, Server: config.Wire,
		Workers: config.Workers, State: config.State, Resources: config.Resources,
		Activate: config.Activate, Busy: config.Busy, HealthState: config.HealthState,
		ShutdownTimeout: config.ShutdownTimeout, Signals: config.Signals,
	})
	if err != nil {
		return nil, err
	}
	runtime, ok := composed.(*daemon.Runtime)
	if !ok {
		return nil, errors.New("wire: private runtime constructor returned an invalid runtime")
	}
	if err := config.Wire.bindRuntime(
		config.RuntimeBuild,
		config.Classifier,
		config.ReservedProtectedSessions,
		runtime,
		config.StopVerifier,
		config.Observations,
		config.Readiness,
		config.BootstrapRoutes,
	); err != nil {
		return nil, err
	}
	return runtime, nil
}
