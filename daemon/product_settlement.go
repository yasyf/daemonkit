package daemon

import (
	"context"
)

type productSettlementToken struct{ marker byte }

type productSettlementState struct {
	runtime    *Runtime
	generation uint64
	ctx        context.Context
	token      *productSettlementToken
	done       chan struct{}
	completed  bool
	expired    bool
}

// ProductSettlement is one sealed proof that a product graph closed and joined.
type ProductSettlement struct {
	state *productSettlementState
	token *productSettlementToken
}

// ClaimProductSettlement issues this activation's singular Starting-only proof.
func (a Activation) ClaimProductSettlement() (ProductSettlement, error) {
	if a.runtime == nil || a.ctx == nil {
		return ProductSettlement{}, ErrProductSettlementUnavailable
	}
	runtime := a.runtime
	runtime.mu.Lock()
	lifecycle := runtime.lifecycle
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	defer runtime.mu.Unlock()
	if a.generation != runtime.controllerGeneration || runtime.finished || runtime.stopping ||
		lifecycle.fatal != nil || lifecycle.progress.State != LifecycleStarting ||
		runtime.productSettlement != nil {
		return ProductSettlement{}, ErrProductSettlementUnavailable
	}
	token := &productSettlementToken{marker: 1}
	state := &productSettlementState{
		runtime: runtime, generation: a.generation, ctx: a.ctx,
		token: token, done: make(chan struct{}),
	}
	runtime.productSettlement = state
	return ProductSettlement{state: state, token: token}, nil
}

// Complete terminally proves product close and join after generation cancellation.
func (s ProductSettlement) Complete() error {
	if s.state == nil || s.token == nil {
		return ErrProductSettlementStale
	}
	state := s.state
	runtime := state.runtime
	if runtime == nil {
		return ErrProductSettlementStale
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.productSettlement != state || state.token != s.token ||
		state.generation != runtime.controllerGeneration || runtime.finished ||
		state.completed || state.expired {
		return ErrProductSettlementStale
	}
	select {
	case <-state.ctx.Done():
	default:
		return ErrProductSettlementActive
	}
	state.completed = true
	close(state.done)
	return nil
}

func (r *Runtime) settleProduct(ctx context.Context) error {
	r.mu.Lock()
	state := r.productSettlement
	if state == nil || state.completed {
		r.mu.Unlock()
		return nil
	}
	done := state.done
	r.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if state.completed {
		return nil
	}
	state.expired = true
	return ctx.Err()
}
