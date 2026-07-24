package wire

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/internal/runtimeauth"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

// RuntimeConfig composes the sole product session server with daemonkit's
// process runtime and receipt-authenticated stop route.
type RuntimeConfig struct {
	Socket           string
	RuntimeBuild     string
	RuntimeProtocol  int
	Wire             *Server
	TrustPolicy      trust.TrustPolicy
	StopControlStore *proc.FileStore
	Observations     []ObservationRoute

	ListenerWait    time.Duration
	Workers         *worker.Pool
	Children        *proc.Manager
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
	if err := config.TrustPolicy.Validate(); err != nil {
		return nil, fmt.Errorf("wire: runtime trust policy: %w", err)
	}
	composed, err := runtimeauth.Build(runtimeauth.Composition{
		RuntimeConfig: daemon.RuntimeConfig{
			Socket: config.Socket, RuntimeBuild: config.RuntimeBuild, RuntimeProtocol: config.RuntimeProtocol,
			ListenerWait: config.ListenerWait, Workers: config.Workers, Children: config.Children,
			ShutdownTimeout: config.ShutdownTimeout, Signals: config.Signals,
		},
		TrustPolicy: config.TrustPolicy,
		Server:      runtimeServerAdapter{server: config.Wire, trustPolicy: config.TrustPolicy},
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
		runtime,
		config.StopControlStore,
		config.Observations,
		config.TrustPolicy,
	); err != nil {
		return nil, err
	}
	return runtime, nil
}
