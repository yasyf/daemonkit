package daemon

import (
	"context"
	"fmt"
	"math"
	"sync"
)

// MaxLifecycleDetailBytes bounds opaque product readiness progress.
const MaxLifecycleDetailBytes = 4096

// LifecycleState is one exact runtime publication phase.
type LifecycleState string

const (
	// LifecycleStarting precedes product publication.
	LifecycleStarting LifecycleState = "runtime_starting"
	// LifecycleReady admits ordinary product work.
	LifecycleReady LifecycleState = "runtime_ready"
	// LifecycleFailed terminates failed product publication.
	LifecycleFailed LifecycleState = "runtime_failed"
	// LifecycleDraining rejects new ordinary product work.
	LifecycleDraining LifecycleState = "runtime_draining"
)

// LifecycleProgress is the singular daemon runtime publication snapshot.
type LifecycleProgress struct {
	Sequence uint64         `json:"sequence"`
	State    LifecycleState `json:"state"`
	Detail   []byte         `json:"detail"`
}

// LifecycleView exposes immutable runtime lifecycle observation.
type LifecycleView interface {
	Snapshot() LifecycleProgress
	WaitChange(context.Context, uint64) (LifecycleProgress, error)
}

// Lifecycle owns runtime phase, monotonic progress, and readiness wakeup.
type Lifecycle struct {
	mu           sync.Mutex
	progress     LifecycleProgress
	changed      chan struct{}
	fatal        error
	publication  *publicationCore
	inflight     int
	settled      chan struct{}
	health       HealthStatus
	nextActivity uint64
	activities   map[uint64]*activityState
}

func newLifecycle() *Lifecycle {
	return &Lifecycle{
		progress:   LifecycleProgress{Sequence: 1, State: LifecycleStarting, Detail: []byte{}},
		changed:    make(chan struct{}),
		health:     HealthStatus{State: StateHealthy, Detail: []byte{}},
		activities: make(map[uint64]*activityState),
	}
}

// Snapshot returns one copied atomic lifecycle snapshot.
func (l *Lifecycle) Snapshot() LifecycleProgress {
	l.mu.Lock()
	defer l.mu.Unlock()
	return cloneLifecycleProgress(l.progress)
}

func (l *Lifecycle) fatalError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fatal
}

// WaitChange returns immediately when the sequence differs, otherwise waits
// for one later atomic lifecycle snapshot.
func (l *Lifecycle) WaitChange(ctx context.Context, sequence uint64) (LifecycleProgress, error) {
	for {
		l.mu.Lock()
		current := cloneLifecycleProgress(l.progress)
		if l.fatal != nil {
			err := l.fatal
			l.mu.Unlock()
			return current, err
		}
		if current.Sequence != sequence {
			l.mu.Unlock()
			return current, nil
		}
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return LifecycleProgress{}, ctx.Err()
		case <-changed:
		}
	}
}

func (l *Lifecycle) fail() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.progress.State == LifecycleFailed {
		return nil
	}
	if l.progress.State != LifecycleStarting {
		return fmt.Errorf("daemon: invalid lifecycle transition %s to %s", l.progress.State, LifecycleFailed)
	}
	if l.fatal != nil {
		return l.fatal
	}
	if l.progress.Sequence == math.MaxUint64 {
		l.setFatalLocked(ErrSequenceExhausted)
		return ErrSequenceExhausted
	}
	if l.publication != nil {
		l.publication.invalidateStagedLocked()
	}
	l.invalidateActivitiesLocked()
	if err := l.advanceTerminalLocked(LifecycleFailed, l.progress.Detail); err != nil {
		panic("daemon: lifecycle failure violated terminal sequence preflight")
	}
	return nil
}

func (l *Lifecycle) advanceLocked(state LifecycleState, detail []byte) error {
	if l.progress.Sequence == math.MaxUint64 {
		return ErrSequenceExhausted
	}
	l.publishLocked(l.progress.Sequence+1, state, detail)
	return nil
}

func (l *Lifecycle) advanceStartingProgressLocked(detail []byte) error {
	if l.progress.Sequence >= math.MaxUint64-2 {
		return ErrSequenceExhausted
	}
	return l.advanceLocked(LifecycleStarting, detail)
}

func (l *Lifecycle) advanceTerminalLocked(state LifecycleState, detail []byte) error {
	return l.advanceLocked(state, detail)
}

func (l *Lifecycle) setFatalLocked(cause error) {
	if cause == nil || l.fatal != nil {
		return
	}
	l.fatal = cause
	close(l.changed)
	l.changed = make(chan struct{})
}

func (l *Lifecycle) publishLocked(sequence uint64, state LifecycleState, detail []byte) {
	l.progress.Sequence = sequence
	l.progress.State = state
	l.progress.Detail = append([]byte{}, detail...)
	close(l.changed)
	l.changed = make(chan struct{})
}

func cloneLifecycleProgress(progress LifecycleProgress) LifecycleProgress {
	return LifecycleProgress{
		Sequence: progress.Sequence,
		State:    progress.State,
		Detail:   append([]byte{}, progress.Detail...),
	}
}
