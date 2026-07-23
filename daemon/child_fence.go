package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/internal/fenceauth"
	"github.com/yasyf/daemonkit/internal/runtimeauth"
	peeridentity "github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
)

const childFenceHandshakeTimeout = 10 * time.Second

var (
	// ErrUnfenceable means a receipt cannot establish an exact signed-peer fence.
	ErrUnfenceable = errors.New("daemon: child receipt is unfenceable")
	// ErrFenceMismatch means the exact child connected with the wrong executable or signature.
	ErrFenceMismatch = errors.New("daemon: child peer fence mismatch")
	// ErrFenceConsumed means an exact child already has a live authenticated session.
	ErrFenceConsumed = errors.New("daemon: child peer fence consumed")
	// ErrFenceClosed means runtime drain or failed dispatch revoked the child fence.
	ErrFenceClosed = errors.New("daemon: child peer fence closed")
)

type childFenceKey struct {
	pid         int
	start, boot string
	generation  string
}

type childFenceState struct {
	receipt     proc.ProcessReceipt
	executable  string
	signature   proc.SignatureDigest
	role        trust.PeerRole
	requirement trust.Requirement
	child       *proc.PreparedChild
	authDone    chan struct{}
	authErr     error
	trusted     *TrustedChild
	dispatching bool
	verifying   bool
	sessionLive bool
	childDone   bool
	closed      bool
}

// ReadyOnlyListener is the runtime-bound authority that arms signed children.
type ReadyOnlyListener struct {
	runtime    *Runtime
	generation uint64
}

// ChildFence is one exact receipt-bound dispatch and authentication authority.
type ChildFence struct {
	listener ReadyOnlyListener
	key      childFenceKey
	state    *childFenceState
}

// TrustedChild is an exact authenticated child in one Ready generation.
type TrustedChild struct {
	identity      proc.Identity
	requestDigest proc.SpawnRequestDigest
	executable    string
	signature     proc.SignatureDigest
	role          trust.PeerRole
	generation    uint64
	child         *proc.PreparedChild
}

// ProcessIdentity returns the exact prepared process identity.
func (c *TrustedChild) ProcessIdentity() proc.Identity { return c.identity }

// RequestDigest returns the immutable spawn-request identity.
func (c *TrustedChild) RequestDigest() proc.SpawnRequestDigest { return c.requestDigest }

// Executable returns the authenticated target executable.
func (c *TrustedChild) Executable() string { return c.executable }

// SignatureDigest returns the authenticated signed-target policy identity.
func (c *TrustedChild) SignatureDigest() proc.SignatureDigest { return c.signature }

// Role returns the exact authenticated trust-policy role.
func (c *TrustedChild) Role() trust.PeerRole { return c.role }

// Done closes after the exact managed process is reaped and untracked.
func (c *TrustedChild) Done() <-chan struct{} { return c.child.Done() }

// ReadyOnlyListener returns this runtime generation's sealed child-fence authority.
func (r *Runtime) ReadyOnlyListener() ReadyOnlyListener {
	if r == nil {
		return ReadyOnlyListener{}
	}
	return ReadyOnlyListener{runtime: r, generation: r.controllerGeneration}
}

// ArmChild reserves one exact Ready-only peer fence without dispatching the child.
func (l ReadyOnlyListener) ArmChild(receipt proc.ProcessReceipt) (*ChildFence, error) {
	r := l.runtime
	if r == nil || !receipt.Prepared() || !receipt.RequiresPeerFence() || !r.cfg.Children.OwnsReceipt(receipt) {
		return nil, ErrUnfenceable
	}
	signature, ok := receipt.ExpectedSignature()
	if !ok || signature == (proc.SignatureDigest{}) || receipt.RequestDigest() == (proc.SpawnRequestDigest{}) {
		return nil, ErrUnfenceable
	}
	role, requirement, ok := r.fenceRequirement(signature)
	if !ok {
		return nil, ErrUnfenceable
	}
	identity := receipt.ProcessIdentity()
	if identity.PID <= 0 || identity.StartTime == "" || identity.Boot == "" || receipt.OwnerGeneration() == "" || receipt.ExpectedExecutable() == "" {
		return nil, ErrUnfenceable
	}
	key := childFenceKey{pid: identity.PID, start: identity.StartTime, boot: identity.Boot, generation: receipt.OwnerGeneration()}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lifecycle.mu.Lock()
	defer r.lifecycle.mu.Unlock()
	if l.generation != r.controllerGeneration || r.finished || r.serverTerminal || !r.serverLive || r.lifecycle.fatal != nil || r.lifecycle.progress.State != LifecycleReady {
		return nil, ErrRuntimeNotReady
	}
	if prior := r.childFences[key]; prior != nil {
		if prior.sessionLive || prior.verifying {
			return nil, ErrFenceConsumed
		}
		return nil, ErrFenceClosed
	}
	state := &childFenceState{
		receipt: receipt, executable: receipt.ExpectedExecutable(), signature: signature,
		role: role, requirement: requirement, authDone: make(chan struct{}),
	}
	r.childFences[key] = state
	return &ChildFence{listener: l, key: key, state: state}, nil
}

func (r *Runtime) fenceRequirement(signature proc.SignatureDigest) (trust.PeerRole, trust.Requirement, bool) {
	var matched trust.PeerRole
	var requirement trust.Requirement
	for _, role := range r.trustPolicy.RoleNames() {
		digest, ok := r.trustPolicy.SignatureDigest(role)
		if !ok || digest != signature {
			continue
		}
		if matched != "" {
			return "", trust.Requirement{}, false
		}
		candidate, ok := r.trustPolicy.Requirement(role)
		if !ok {
			return "", trust.Requirement{}, false
		}
		matched, requirement = role, candidate
	}
	if matched == "" {
		return "", trust.Requirement{}, false
	}
	controlOnly := r.trustPolicy.AllowsStop(matched) || r.trustPolicy.AllowsReceipt(matched) || r.trustPolicy.AllowsReadiness(matched)
	if controlOnly && !r.trustPolicy.AllowsHandoff(matched) {
		return "", trust.Requirement{}, false
	}
	return matched, requirement, true
}

// Start releases the exact prepared child and waits for its first exact authenticated session.
func (f *ChildFence) Start(ctx context.Context, child *proc.PreparedChild) (*TrustedChild, error) {
	if f == nil || f.listener.runtime == nil || child == nil {
		return nil, ErrUnfenceable
	}
	r := f.listener.runtime
	r.mu.Lock()
	r.lifecycle.mu.Lock()
	state := r.childFences[f.key]
	if f.listener.generation != r.controllerGeneration || state != f.state || state == nil || state.closed || r.finished ||
		r.lifecycle.fatal != nil || r.lifecycle.progress.State != LifecycleReady {
		r.lifecycle.mu.Unlock()
		r.mu.Unlock()
		return nil, ErrFenceClosed
	}
	if state.dispatching {
		r.lifecycle.mu.Unlock()
		r.mu.Unlock()
		return nil, ErrFenceConsumed
	}
	state.child = child
	state.dispatching = true
	authDone := state.authDone
	r.lifecycle.mu.Unlock()
	r.mu.Unlock()

	if err := child.StartFenced(ctx, state.receipt, fenceauth.New()); err != nil {
		return nil, r.revokeAndStopFence(ctx, f.key, state, err)
	}
	go r.observeFencedChild(f.key, state, child)
	timeout := r.childFenceTimeout
	if timeout <= 0 {
		timeout = childFenceHandshakeTimeout
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, timeout)
	defer waitCancel()
	select {
	case <-waitCtx.Done():
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.shutdownTimeout())
		defer cancel()
		return nil, r.revokeAndStopFence(stopCtx, f.key, state, waitCtx.Err())
	case <-authDone:
		r.mu.Lock()
		trusted, authErr := state.trusted, state.authErr
		r.mu.Unlock()
		if authErr != nil {
			return nil, authErr
		}
		if trusted == nil {
			return nil, ErrFenceClosed
		}
		return trusted, nil
	}
}

func (r *Runtime) matchChildFence(ctx context.Context, peer peeridentity.Identity) (*runtimeauth.PeerFencePermit, error) {
	identity := peer.ProcessIdentity()
	r.mu.Lock()
	key, state := r.lookupChildFenceLocked(identity)
	if state == nil {
		r.mu.Unlock()
		return nil, nil
	}
	if state.closed || state.childDone {
		r.mu.Unlock()
		return nil, ErrFenceClosed
	}
	if !state.dispatching || identity.Executable != state.executable {
		r.mu.Unlock()
		return nil, r.revokeAndStopFence(ctx, key, state, ErrFenceMismatch)
	}
	if state.verifying || state.sessionLive {
		r.mu.Unlock()
		return nil, ErrFenceConsumed
	}
	state.verifying = true
	requirement := state.requirement
	r.mu.Unlock()

	if err := r.verifyChildFencePeer(ctx, peer, requirement); err != nil {
		return nil, r.revokeAndStopFence(ctx, key, state, errors.Join(ErrFenceMismatch, err))
	}

	r.mu.Lock()
	current := r.childFences[key]
	if current != state || state.closed || state.childDone {
		state.verifying = false
		r.mu.Unlock()
		return nil, ErrFenceClosed
	}
	r.mu.Unlock()
	var permitMu sync.Mutex
	settled := false
	permit := &runtimeauth.PeerFencePermit{}
	permit.Commit = func() (func(), error) {
		permitMu.Lock()
		defer permitMu.Unlock()
		if settled {
			return nil, ErrFenceClosed
		}
		settled = true
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.childFences[key] != state || state.closed || state.childDone || !state.verifying || state.sessionLive {
			state.verifying = false
			return nil, ErrFenceClosed
		}
		state.verifying = false
		state.sessionLive = true
		if state.trusted == nil {
			state.trusted = &TrustedChild{
				identity: identity, requestDigest: state.receipt.RequestDigest(), executable: state.executable,
				signature: state.signature, role: state.role, generation: r.controllerGeneration, child: state.child,
			}
			close(state.authDone)
		}
		return sync.OnceFunc(func() { r.settleChildSession(key, state) }), nil
	}
	permit.Rollback = func() {
		permitMu.Lock()
		defer permitMu.Unlock()
		if settled {
			return
		}
		settled = true
		r.mu.Lock()
		if r.childFences[key] == state {
			state.verifying = false
		}
		r.mu.Unlock()
	}
	return permit, nil
}

func (r *Runtime) verifyChildFencePeer(ctx context.Context, peer peeridentity.Identity, requirement trust.Requirement) error {
	if r.childFenceVerifier != nil {
		return r.childFenceVerifier(ctx, peer, requirement)
	}
	verifier := trust.ProcessVerifier{
		Runner: r.trustWorkers, Executable: r.trustExecutable,
		Policy: trust.Policy{Requirement: &requirement},
	}
	return verifier.Check(ctx, peer)
}

func (r *Runtime) lookupChildFenceLocked(identity proc.Identity) (childFenceKey, *childFenceState) {
	for key, state := range r.childFences {
		if key.pid == identity.PID && key.start == identity.StartTime && key.boot == identity.Boot {
			return key, state
		}
	}
	return childFenceKey{}, nil
}

func (r *Runtime) settleChildSession(key childFenceKey, state *childFenceState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.childFences[key] != state {
		return
	}
	state.sessionLive = false
	if state.childDone || state.closed {
		delete(r.childFences, key)
	}
}

func (r *Runtime) observeFencedChild(key childFenceKey, state *childFenceState, child *proc.PreparedChild) {
	<-child.Done()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.childFences[key] != state {
		return
	}
	state.childDone = true
	state.closed = true
	if state.trusted == nil && state.authErr == nil {
		state.authErr = ErrFenceClosed
		close(state.authDone)
	}
	if !state.sessionLive {
		delete(r.childFences, key)
	}
}

func (r *Runtime) revokeAndStopFence(ctx context.Context, key childFenceKey, state *childFenceState, cause error) error {
	r.mu.Lock()
	if r.childFences[key] == state {
		state.closed = true
		state.verifying = false
	}
	child := state.child
	r.mu.Unlock()
	var stopErr error
	if child != nil {
		stopErr = child.Stop(ctx)
	}
	result := errors.Join(cause, stopErr)
	r.mu.Lock()
	if state.trusted == nil && state.authErr == nil {
		state.authErr = result
		close(state.authDone)
	}
	r.mu.Unlock()
	if stopErr != nil {
		r.fatalFenceSettlement(result)
	}
	return result
}

func (r *Runtime) fatalFenceSettlement(cause error) {
	r.mu.Lock()
	r.lifecycle.mu.Lock()
	r.lifecycle.setFatalLocked(cause)
	cancel := r.activationCancel
	r.lifecycle.mu.Unlock()
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if r.server != nil {
		_ = r.server.CloseRuntimeIntake()
	}
	r.signalStop()
}

func (r *Runtime) revokeChildFencesLocked() []*proc.PreparedChild {
	children := make([]*proc.PreparedChild, 0, len(r.childFences))
	for _, state := range r.childFences {
		state.closed = true
		if state.trusted == nil && state.authErr == nil {
			state.authErr = ErrFenceClosed
			close(state.authDone)
		}
		if state.child != nil {
			children = append(children, state.child)
		}
	}
	return children
}

func stopFencedChildren(ctx context.Context, children []*proc.PreparedChild) error {
	var errs []error
	for _, child := range children {
		if err := child.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("daemon: stop fenced child: %w", err))
		}
	}
	return errors.Join(errs...)
}
