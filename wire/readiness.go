package wire

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

// MaxReadinessDetailBytes bounds opaque product progress on the wire.
const MaxReadinessDetailBytes = daemon.MaxLifecycleDetailBytes

// RuntimeReadinessState is one exact lifecycle publication state.
type RuntimeReadinessState = daemon.LifecycleState

const (
	// RuntimeStarting means consumer preparation has not committed readiness.
	RuntimeStarting = daemon.LifecycleStarting
	// RuntimeReady means the exact consumer publication is live.
	RuntimeReady = daemon.LifecycleReady
	// RuntimeFailed means preparation or serving failed terminally.
	RuntimeFailed = daemon.LifecycleFailed
	// RuntimeDraining means new Ready-only admission is closed.
	RuntimeDraining = daemon.LifecycleDraining
)

var (
	// ErrReadinessProgress means a lifecycle publication is structurally invalid.
	ErrReadinessProgress = errors.New("wire: invalid runtime readiness progress")
	// ErrReadinessNoProgress means a runtime did not advance before its progress bound.
	ErrReadinessNoProgress = errors.New("wire: runtime readiness made no progress")
	// ErrRuntimeFailed means the exact runtime failed before readiness.
	ErrRuntimeFailed = errors.New("wire: runtime failed before readiness")
	// ErrRuntimeBuildMismatch means the connected runtime build is not exact.
	ErrRuntimeBuildMismatch = errors.New("wire: runtime build mismatch")
	// ErrProcessGenerationMismatch means the connected process generation is not exact.
	ErrProcessGenerationMismatch = errors.New("wire: runtime process generation mismatch")
	// ErrRuntimeReceiptUnavailable means no authenticated runtime receipt was published.
	ErrRuntimeReceiptUnavailable = errors.New("wire: runtime receipt is unavailable")
)

// RuntimeIdentity identifies one exact product runtime process.
type RuntimeIdentity struct {
	RuntimeBuild      string               `json:"runtime_build"`
	ProcessGeneration proc.OwnerGeneration `json:"process_generation"`
}

// RuntimeReceipt proves the exact runtime process returned by an authenticated
// runtime control session.
type RuntimeReceipt struct {
	identity RuntimeIdentity
}

// Identity returns the immutable runtime process identity carried by the receipt.
func (r RuntimeReceipt) Identity() RuntimeIdentity { return r.identity }

// ReadinessProgress is daemonkit's atomic lifecycle progress snapshot.
type ReadinessProgress = daemon.LifecycleProgress

// RuntimeClientConfig configures lifecycle discovery without pinning a runtime.
type RuntimeClientConfig struct {
	Client            ClientConfig
	NoProgressTimeout time.Duration
}

func (s *Server) authorizePreReady(
	_ context.Context,
	entry entry,
	_ Request,
) error {
	if s.staticOrdinary {
		if entry.route != routeBusiness {
			return ErrPermissionDenied
		}
		return nil
	}
	lifecycle := s.lifecycle
	if lifecycle == nil {
		return ErrNotReady
	}
	progress := lifecycle.Snapshot()
	if progress.State == RuntimeDraining {
		return ErrDraining
	}
	if entry.route == routeLifecycle {
		return nil
	}
	if progress.State != RuntimeReady {
		return ErrNotReady
	}
	return nil
}
