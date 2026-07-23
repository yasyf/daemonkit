package daemon

import (
	"bytes"
	"errors"
	"fmt"
	"math"
)

// HealthStatus is copied product health published through a generation-bound reporter.
type HealthStatus struct {
	State  State
	Detail []byte
}

// StatusReporter publishes product health and owns explicit background activity.
type StatusReporter struct {
	runtime    *Runtime
	generation uint64
}

type activityState struct{ alive bool }

// ActivityLease is one idempotently released product background activity.
type ActivityLease struct {
	runtime    *Runtime
	generation uint64
	id         uint64
	state      *activityState
}

// StatusReporter returns this activation's generation-bound status capability.
func (a Activation) StatusReporter() StatusReporter {
	return StatusReporter{runtime: a.runtime, generation: a.generation}
}

// Update publishes one copied, exact-idempotent health status.
func (s StatusReporter) Update(status HealthStatus) error {
	if len(status.Detail) > MaxLifecycleDetailBytes {
		return fmt.Errorf("daemon: health detail bytes=%d exceeds %d", len(status.Detail), MaxLifecycleDetailBytes)
	}
	if status.State != StateHealthy && status.State != StateDegraded && status.State != StateFailed {
		return errors.New("daemon: invalid health state")
	}
	if s.runtime == nil {
		return ErrPublicationStale
	}
	r := s.runtime
	r.mu.Lock()
	r.lifecycle.mu.Lock()
	defer r.lifecycle.mu.Unlock()
	defer r.mu.Unlock()
	if s.generation != r.controllerGeneration || r.finished ||
		(r.lifecycle.progress.State != LifecycleStarting && r.lifecycle.progress.State != LifecycleReady) {
		return ErrPublicationStale
	}
	if r.lifecycle.fatal != nil {
		return r.lifecycle.fatal
	}
	if r.lifecycle.health.State == status.State && bytes.Equal(r.lifecycle.health.Detail, status.Detail) {
		return nil
	}
	r.lifecycle.health = HealthStatus{State: status.State, Detail: append([]byte{}, status.Detail...)}
	return nil
}

// BeginActivity acquires one explicit product background activity lease.
func (s StatusReporter) BeginActivity() (*ActivityLease, error) {
	if s.runtime == nil {
		return nil, ErrPublicationStale
	}
	r := s.runtime
	r.mu.Lock()
	r.lifecycle.mu.Lock()
	defer r.lifecycle.mu.Unlock()
	defer r.mu.Unlock()
	if s.generation != r.controllerGeneration || r.finished || r.lifecycle.progress.State != LifecycleReady {
		return nil, ErrPublicationStale
	}
	if r.lifecycle.fatal != nil {
		return nil, r.lifecycle.fatal
	}
	if r.lifecycle.nextActivity == math.MaxUint64 {
		return nil, errors.New("daemon: activity sequence overflow")
	}
	r.lifecycle.nextActivity++
	state := &activityState{alive: true}
	id := r.lifecycle.nextActivity
	r.lifecycle.activities[id] = state
	return &ActivityLease{runtime: r, generation: s.generation, id: id, state: state}, nil
}

// Release idempotently settles this activity lease.
func (l *ActivityLease) Release() error {
	if l == nil || l.runtime == nil || l.state == nil {
		return ErrPublicationStale
	}
	r := l.runtime
	r.mu.Lock()
	r.lifecycle.mu.Lock()
	defer r.lifecycle.mu.Unlock()
	defer r.mu.Unlock()
	if !l.state.alive {
		return nil
	}
	if l.generation != r.controllerGeneration {
		return ErrPublicationStale
	}
	l.state.alive = false
	delete(r.lifecycle.activities, l.id)
	return nil
}

func (l *Lifecycle) invalidateActivitiesLocked() {
	for id, activity := range l.activities {
		activity.alive = false
		delete(l.activities, id)
	}
}
