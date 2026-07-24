package daemon

import (
	"errors"
)

var (
	// ErrPublicationUnavailable means the runtime is not exactly Ready.
	ErrPublicationUnavailable = errors.New("daemon: publication is unavailable")
	// ErrPublicationStale means a publication or activation belongs to another generation.
	ErrPublicationStale = errors.New("daemon: publication is stale")
)

// Publication is one opaque, generation-fenced staged value.
type Publication struct {
	core       *publicationCore
	token      *publicationToken
	generation uint64
	stage      uint64
	lease      *publicationLease
	value      any
}

type (
	publicationToken        struct{ marker byte }
	publicationValue[T any] struct{ value T }
)

type publicationLease struct{ alive bool }

type publicationCore struct {
	runtime        *Runtime
	token          *publicationToken
	lifecycle      *Lifecycle
	generation     uint64
	nextStage      uint64
	staged         any
	stagedSet      bool
	published      any
	publishedSet   bool
	publishedStage uint64
	poisoned       bool
}

// PublicationSlot is the typed view of one Runtime publication.
type PublicationSlot[T any] struct {
	core  *publicationCore
	token *publicationToken
}

// NewPublicationSlot binds the Runtime's singular publication to a typed slot.
// It must be called exactly once before Begin.
func NewPublicationSlot[T any](runtime *Runtime) *PublicationSlot[T] {
	if runtime == nil {
		panic("daemon: publication runtime is required")
	}
	token := &publicationToken{marker: 1}
	core, err := runtime.bindPublication(token)
	if err != nil {
		panic(err)
	}
	return &PublicationSlot[T]{core: core, token: token}
}

// Stage stores value invisibly for one generation-bound activation.
func (s *PublicationSlot[T]) Stage(activation Activation, value T) (Publication, error) {
	if s == nil || s.core == nil {
		return Publication{}, errors.New("daemon: publication slot is required")
	}
	core := s.core
	core.runtime.mu.Lock()
	lifecycle := core.lifecycle
	lifecycle.mu.Lock()
	if activation.runtime != core.runtime || activation.generation != core.generation || core.runtime.finished {
		lifecycle.mu.Unlock()
		core.runtime.mu.Unlock()
		return Publication{}, ErrPublicationStale
	}
	if lifecycle.progress.State == LifecycleDraining {
		lifecycle.mu.Unlock()
		core.runtime.mu.Unlock()
		return Publication{}, ErrDraining
	}
	if lifecycle.fatal != nil {
		cause := lifecycle.fatal
		lifecycle.mu.Unlock()
		core.runtime.mu.Unlock()
		return Publication{}, cause
	}
	if core.poisoned || lifecycle.progress.State != LifecycleStarting || core.stagedSet || core.publishedSet {
		lifecycle.mu.Unlock()
		core.runtime.mu.Unlock()
		return Publication{}, ErrPublicationUnavailable
	}
	if core.nextStage == ^uint64(0) {
		cause := errors.New("daemon: publication stage sequence overflow")
		if lifecycle.progress.Sequence == ^uint64(0) {
			cause = ErrSequenceExhausted
			lifecycle.setFatalLocked(cause)
			cancel := core.runtime.activationCancel
			lifecycle.mu.Unlock()
			core.runtime.mu.Unlock()
			if cancel != nil {
				cancel()
			}
			core.runtime.signalStop()
			return Publication{}, cause
		}
		core.staged = nil
		core.stagedSet = false
		core.poisoned = true
		core.runtime.startupFailure = cause
		cancel := core.runtime.activationCancel
		if lifecycle.progress.State == LifecycleStarting {
			lifecycle.invalidateActivitiesLocked()
			if err := lifecycle.advanceTerminalLocked(LifecycleFailed, lifecycle.progress.Detail); err != nil {
				panic("daemon: publication overflow violated sequence preflight")
			}
		}
		lifecycle.mu.Unlock()
		core.runtime.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		core.runtime.signalStop()
		return Publication{}, cause
	}
	core.nextStage++
	core.staged = publicationValue[T]{value: value}
	core.stagedSet = true
	publication := Publication{core: core, token: s.token, generation: core.generation, stage: core.nextStage}
	lifecycle.mu.Unlock()
	core.runtime.mu.Unlock()
	return publication, nil
}

// Value returns the exact value pinned by an already-admitted publication.
// The caller's admission lease must remain live for the entire use of value.
func (s *PublicationSlot[T]) Value(publication Publication) (T, error) {
	var zero T
	if s == nil || s.core == nil {
		return zero, ErrPublicationUnavailable
	}
	core := s.core
	core.lifecycle.mu.Lock()
	defer core.lifecycle.mu.Unlock()
	if publication.core != core || publication.token != s.token || publication.generation != core.generation ||
		publication.stage != core.publishedStage || publication.lease == nil || !publication.lease.alive ||
		publication.value == nil {
		return zero, ErrPublicationStale
	}
	boxed := publication.value.(publicationValue[T])
	return boxed.value, nil
}

// Acquire pins the committed value in the Runtime admission lane until release.
func (s *PublicationSlot[T]) Acquire() (T, func(), error) {
	var zero T
	if s == nil || s.core == nil {
		return zero, nil, ErrPublicationUnavailable
	}
	core := s.core
	publication, release, err := core.runtime.admitReady()
	if err != nil {
		return zero, nil, err
	}
	core.lifecycle.mu.Lock()
	if publication.core != core || publication.token != s.token || publication.generation != core.generation ||
		publication.stage != core.publishedStage || publication.lease == nil || !publication.lease.alive ||
		!core.publishedSet {
		core.lifecycle.mu.Unlock()
		release()
		panic("daemon: admitted publication changed before acquisition")
	}
	boxed := core.published.(publicationValue[T])
	core.lifecycle.mu.Unlock()
	return boxed.value, release, nil
}

func (p *publicationCore) invalidateStagedLocked() {
	p.staged = nil
	p.stagedSet = false
	if p.nextStage < ^uint64(0) {
		p.nextStage++
	} else {
		p.poisoned = true
	}
}

func (p *publicationCore) clearLocked() {
	p.invalidateStagedLocked()
	p.published = nil
	p.publishedSet = false
	p.publishedStage = 0
}
