package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

const (
	runtimeReadinessSubscribeOp = Op("daemon.control.readiness.subscribe")
	runtimeReceiptOp            = Op("daemon.control.runtime.receipt")
)

var runtimeReadinessBackoff = proc.Backoff{Base: 10 * time.Millisecond, Cap: 250 * time.Millisecond}

var errRuntimePeerStale = errors.New("wire: runtime readiness peer ended")

type runtimeReadinessSubscribeRequest struct {
	Protocol uint16 `json:"protocol"`
}

type runtimeReadinessSubscribeResponse struct {
	Protocol uint16 `json:"protocol"`
}

type runtimeReceiptRequest struct {
	Protocol uint16 `json:"protocol"`
}

type runtimeReceiptResponse struct {
	Protocol        uint16          `json:"protocol"`
	RuntimeIdentity RuntimeIdentity `json:"runtime_identity"`
}

type runtimeReadinessEvent struct {
	Protocol        uint16            `json:"protocol"`
	WireBuild       string            `json:"wire_build"`
	RuntimeIdentity RuntimeIdentity   `json:"runtime_identity"`
	Progress        ReadinessProgress `json:"progress"`
}

// ReadinessNoProgressError reports the last immutable runtime identity and progress.
type ReadinessNoProgressError struct {
	Identity RuntimeIdentity
	Last     ReadinessProgress
}

func (e *ReadinessNoProgressError) Error() string {
	return fmt.Sprintf("%s: sequence=%d state=%s", ErrReadinessNoProgress, e.Last.Sequence, e.Last.State)
}

func (*ReadinessNoProgressError) Unwrap() error { return ErrReadinessNoProgress }

// RuntimeFailedError reports the terminal immutable runtime identity and progress.
type RuntimeFailedError struct {
	Identity RuntimeIdentity
	Last     ReadinessProgress
}

func (e *RuntimeFailedError) Error() string {
	return fmt.Sprintf("%s: sequence=%d", ErrRuntimeFailed, e.Last.Sequence)
}

func (*RuntimeFailedError) Unwrap() error { return ErrRuntimeFailed }

type readinessWaiter func(context.Context, int) error

type readinessClock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realReadinessClock struct{}

func (realReadinessClock) Now() time.Time                                { return time.Now() }
func (realReadinessClock) After(duration time.Duration) <-chan time.Time { return time.After(duration) }

// AcquireReadyRuntime authenticates one runtime identity and waits for that
// exact process to become ready within one no-progress budget.
func AcquireReadyRuntime(
	ctx context.Context,
	config RuntimeClientConfig,
	expectedRuntimeBuild string,
) (RuntimeReceipt, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeReceipt{}, err
	}
	if err := validateRuntimeClientConfig(config); err != nil {
		return RuntimeReceipt{}, err
	}
	if expectedRuntimeBuild == "" {
		return RuntimeReceipt{}, errors.New("wire: expected runtime build is required")
	}
	progress := newRuntimeProgressTracker(config, nil, realReadinessClock{})
	return acquireReadyRuntimeTracked(
		ctx, config, expectedRuntimeBuild, progress, waitReadinessRetry,
	)
}

func acquireReadyRuntimeTracked(
	ctx context.Context,
	config RuntimeClientConfig,
	expectedRuntimeBuild string,
	progress *runtimeProgressTracker,
	wait readinessWaiter,
) (RuntimeReceipt, error) {
	receipt, err := acquireRuntimeReceiptTracked(
		ctx, config.Client, expectedRuntimeBuild, progress, wait,
	)
	if err != nil {
		return RuntimeReceipt{}, err
	}
	identity := receipt.Identity()
	client, err := waitRuntimeReadyTracked(
		ctx, config, &identity, wait, progress, false,
	)
	if err != nil {
		return RuntimeReceipt{}, err
	}
	if err := client.Abort(ErrClientAbort); err != nil {
		return RuntimeReceipt{}, fmt.Errorf("wire: settle ready runtime session: %w", err)
	}
	return receipt, nil
}

func acquireRuntimeReceiptTracked(
	ctx context.Context,
	config ClientConfig,
	expectedRuntimeBuild string,
	progress *runtimeProgressTracker,
	wait readinessWaiter,
) (RuntimeReceipt, error) {
	payload, err := json.Marshal(runtimeReceiptRequest{Protocol: ProtocolVersion})
	if err != nil {
		return RuntimeReceipt{}, err
	}
	var lastErr error
	for failures := 0; ; failures++ {
		client, err := withinProgress(ctx, progress, func(attemptCtx context.Context) (*Client, error) {
			return NewClient(attemptCtx, config)
		})
		if err != nil {
			if !provesNoListener(err) && !errors.Is(err, ErrSessionCapacity) {
				return RuntimeReceipt{}, errors.Join(ErrRuntimeReceiptUnavailable, err)
			}
			lastErr = err
			if waitErr := progress.wait(ctx, failures+1, wait); waitErr != nil {
				return RuntimeReceipt{}, errors.Join(waitErr, lastErr)
			}
			continue
		}

		result, callErr := withinProgress(ctx, progress, func(callCtx context.Context) (Result, error) {
			return client.Call(callCtx, runtimeReceiptOp, "", payload)
		})
		if callErr != nil {
			_ = client.Abort(callErr)
			if isReadinessPeerEnd(callErr) {
				lastErr = callErr
				if waitErr := progress.wait(ctx, failures+1, wait); waitErr != nil {
					return RuntimeReceipt{}, errors.Join(waitErr, lastErr)
				}
				continue
			}
			return RuntimeReceipt{}, errors.Join(ErrRuntimeReceiptUnavailable, callErr)
		}
		if result.Outcome != Delivered || result.Response.Rejected {
			_ = client.Abort(ErrRuntimeReceiptUnavailable)
			if rejection := result.Rejection(); rejection != nil {
				return RuntimeReceipt{}, errors.Join(ErrRuntimeReceiptUnavailable, rejection)
			}
			return RuntimeReceipt{}, fmt.Errorf(
				"%w: receipt outcome %s", ErrRuntimeReceiptUnavailable, result.Outcome,
			)
		}
		if result.Response.Err != "" {
			_ = client.Abort(ErrRuntimeReceiptUnavailable)
			return RuntimeReceipt{}, fmt.Errorf("%w: %s", ErrRuntimeReceiptUnavailable, result.Response.Err)
		}
		var response runtimeReceiptResponse
		if err := decodeStrict(result.Response.Payload, &response); err != nil {
			_ = client.Abort(err)
			return RuntimeReceipt{}, fmt.Errorf("%w: decode: %w", ErrRuntimeReceiptUnavailable, err)
		}
		if response.Protocol != ProtocolVersion {
			_ = client.Abort(ErrProtocolVersion)
			return RuntimeReceipt{}, fmt.Errorf(
				"%w: %w: runtime receipt protocol=%d",
				ErrRuntimeReceiptUnavailable, ErrProtocolVersion, response.Protocol,
			)
		}
		if err := validateRuntimeIdentity(response.RuntimeIdentity); err != nil {
			_ = client.Abort(err)
			return RuntimeReceipt{}, fmt.Errorf("%w: %w", ErrRuntimeReceiptUnavailable, err)
		}
		if response.RuntimeIdentity.RuntimeBuild != expectedRuntimeBuild {
			_ = client.Abort(ErrRuntimeBuildMismatch)
			return RuntimeReceipt{}, errors.Join(
				ErrRuntimeReceiptUnavailable,
				fmt.Errorf(
					"%w: got %q, want %q",
					ErrRuntimeBuildMismatch, response.RuntimeIdentity.RuntimeBuild, expectedRuntimeBuild,
				),
			)
		}
		if err := progress.pinIdentity(response.RuntimeIdentity); err != nil {
			_ = client.Abort(err)
			return RuntimeReceipt{}, err
		}
		if err := client.Abort(ErrClientAbort); err != nil {
			return RuntimeReceipt{}, fmt.Errorf("wire: settle runtime receipt session: %w", err)
		}
		return RuntimeReceipt{identity: response.RuntimeIdentity}, nil
	}
}

type runtimeProgressTracker struct {
	mu       sync.Mutex
	clock    readinessClock
	timeout  time.Duration
	expected *RuntimeIdentity
	identity RuntimeIdentity
	progress ReadinessProgress
	deadline time.Time
	observed bool
	notify   func(RuntimeIdentity, ReadinessProgress)
}

// WaitRuntimeReady opens an exact-build session, subscribes to its lifecycle,
// and waits for one exact runtime identity to publish Ready.
func WaitRuntimeReady(
	ctx context.Context,
	config RuntimeClientConfig,
	expected RuntimeIdentity,
) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRuntimeClientConfig(config); err != nil {
		return nil, err
	}
	if err := validateRuntimeIdentity(expected); err != nil {
		return nil, err
	}
	return waitRuntimeReadyTracked(
		ctx, config, &expected, waitReadinessRetry,
		newRuntimeProgressTracker(config, &expected, realReadinessClock{}), false,
	)
}

func waitRuntimeReadyTracked(
	ctx context.Context,
	config RuntimeClientConfig,
	expected *RuntimeIdentity,
	wait readinessWaiter,
	progress *runtimeProgressTracker,
	allowSuccessor bool,
) (*Client, error) {
	var lastErr error
	failures := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, err := withinProgress(ctx, progress, func(attemptCtx context.Context) (*Client, error) {
			return newLifecycleClient(
				attemptCtx,
				config.Client,
				newRuntimeLifecycleValidator(config.Client.WireBuild, expected),
			)
		})
		if err != nil {
			if provesNoListener(err) || errors.Is(err, ErrSessionCapacity) {
				lastErr = err
				failures++
				if waitErr := progress.wait(ctx, failures, wait); waitErr != nil {
					return nil, errors.Join(waitErr, lastErr)
				}
				continue
			}
			return nil, err
		}
		if err := withinProgressError(ctx, progress, func(subscribeCtx context.Context) error {
			return subscribeRuntimeReadiness(subscribeCtx, client)
		}); err != nil {
			_ = client.Abort(err)
			if isReadinessPeerEnd(err) {
				continue
			}
			return nil, err
		}
		for {
			event, eventErr := withinProgress(ctx, progress, func(eventCtx context.Context) (runtimeReadinessEvent, error) {
				return nextRuntimeReadiness(eventCtx, client, config.Client.WireBuild)
			})
			if eventErr != nil {
				_ = client.Abort(eventErr)
				if event.Progress.State == RuntimeFailed || event.Progress.State == RuntimeDraining {
					_, progressErr := progress.observe(event, allowSuccessor)
					if errors.Is(progressErr, ErrDraining) && allowSuccessor && expected == nil {
						break
					}
					if progressErr != nil {
						return nil, errors.Join(progressErr, eventErr)
					}
				}
				if isReadinessPeerEnd(eventErr) {
					break
				}
				return nil, eventErr
			}
			ready, progressErr := progress.observe(event, allowSuccessor)
			if progressErr == nil && ready {
				return client, nil
			}
			if errors.Is(progressErr, ErrDraining) && allowSuccessor && expected == nil {
				_ = client.Abort(progressErr)
				break
			}
			if progressErr != nil {
				return nil, errors.Join(progressErr, client.Abort(progressErr))
			}
		}
	}
}

func subscribeRuntimeReadiness(ctx context.Context, client *Client) error {
	payload, err := json.Marshal(runtimeReadinessSubscribeRequest{Protocol: ProtocolVersion})
	if err != nil {
		return err
	}
	result, err := client.Call(ctx, runtimeReadinessSubscribeOp, "", payload)
	if err != nil {
		return err
	}
	if result.Outcome != Delivered || result.Response.Rejected {
		if rejection := result.Rejection(); rejection != nil {
			return rejection
		}
		return fmt.Errorf("wire: runtime readiness subscription outcome %s", result.Outcome)
	}
	if result.Response.Err != "" {
		return errors.New(result.Response.Err)
	}
	var response runtimeReadinessSubscribeResponse
	if err := decodeStrict(result.Response.Payload, &response); err != nil {
		return fmt.Errorf("wire: decode runtime readiness subscription: %w", err)
	}
	if response.Protocol != ProtocolVersion {
		return fmt.Errorf("%w: readiness subscription protocol=%d", ErrProtocolVersion, response.Protocol)
	}
	return nil
}

func nextRuntimeReadiness(
	ctx context.Context,
	client *Client,
	wireBuild string,
) (runtimeReadinessEvent, error) {
	payload, transportErr := client.nextLifecycle(ctx)
	if len(payload) == 0 {
		if transportErr != nil {
			return runtimeReadinessEvent{}, runtimeLifecycleTransportError(transportErr)
		}
		return runtimeReadinessEvent{}, fmt.Errorf("%w: empty lifecycle payload", ErrReadinessProgress)
	}
	event, decodeErr := decodeRuntimeReadiness(payload, wireBuild)
	if decodeErr != nil {
		return runtimeReadinessEvent{}, decodeErr
	}
	if transportErr != nil {
		return event, runtimeLifecycleTransportError(transportErr)
	}
	return event, nil
}

func tryRuntimeReadiness(client *Client, wireBuild string) (runtimeReadinessEvent, bool, error) {
	payload, ok, transportErr := client.tryLifecycle()
	if !ok {
		return runtimeReadinessEvent{}, false, nil
	}
	if len(payload) == 0 {
		if transportErr != nil {
			return runtimeReadinessEvent{}, true, runtimeLifecycleTransportError(transportErr)
		}
		return runtimeReadinessEvent{}, true, fmt.Errorf("%w: empty lifecycle payload", ErrReadinessProgress)
	}
	event, decodeErr := decodeRuntimeReadiness(payload, wireBuild)
	if decodeErr != nil {
		return runtimeReadinessEvent{}, true, decodeErr
	}
	if transportErr != nil {
		return event, true, runtimeLifecycleTransportError(transportErr)
	}
	return event, true, nil
}

func runtimeLifecycleTransportError(err error) error {
	if isReadinessPeerEnd(err) {
		return fmt.Errorf("%w: %w", errRuntimePeerStale, err)
	}
	return err
}

func newRuntimeLifecycleValidator(
	wireBuild string,
	expected *RuntimeIdentity,
) func([]byte) (bool, error) {
	tracker := &runtimeProgressTracker{
		clock: realReadinessClock{}, expected: cloneRuntimeIdentity(expected),
		progress: ReadinessProgress{Detail: []byte{}},
	}
	return func(payload []byte) (bool, error) {
		event, err := decodeRuntimeReadiness(payload, wireBuild)
		if err != nil {
			return false, err
		}
		_, err = tracker.observe(event, false)
		switch {
		case errors.Is(err, ErrRuntimeFailed), errors.Is(err, ErrDraining):
			return true, nil
		case err != nil:
			return false, err
		default:
			return false, nil
		}
	}
}

func decodeRuntimeReadiness(payload []byte, wireBuild string) (runtimeReadinessEvent, error) {
	var event runtimeReadinessEvent
	if err := decodeStrict(payload, &event); err != nil {
		return runtimeReadinessEvent{}, fmt.Errorf("%w: decode runtime readiness event: %w", ErrReadinessProgress, err)
	}
	if event.Protocol != ProtocolVersion {
		return runtimeReadinessEvent{}, fmt.Errorf(
			"%w: readiness protocol=%d want=%d", ErrProtocolVersion, event.Protocol, ProtocolVersion,
		)
	}
	if event.WireBuild != wireBuild {
		return runtimeReadinessEvent{}, fmt.Errorf(
			"%w: readiness server=%q client=%q", ErrBuildMismatch, event.WireBuild, wireBuild,
		)
	}
	return event, nil
}

func validateRuntimeClientConfig(config RuntimeClientConfig) error {
	if _, err := validateClientConfig(config.Client); err != nil {
		return err
	}
	if config.NoProgressTimeout <= 0 {
		return errors.New("wire: positive no-progress timeout is required")
	}
	return nil
}

func validateRuntimeIdentity(identity RuntimeIdentity) error {
	if identity.RuntimeBuild == "" || identity.ProcessGeneration == "" {
		return errors.New("wire: exact runtime build and process generation are required")
	}
	return nil
}

func newRuntimeProgressTracker(
	config RuntimeClientConfig,
	expected *RuntimeIdentity,
	clock readinessClock,
) *runtimeProgressTracker {
	tracker := &runtimeProgressTracker{
		clock: clock, timeout: config.NoProgressTimeout,
		expected: cloneRuntimeIdentity(expected),
		progress: ReadinessProgress{Detail: []byte{}},
	}
	tracker.deadline = clock.Now().Add(config.NoProgressTimeout)
	return tracker
}

func (t *runtimeProgressTracker) observe(event runtimeReadinessEvent, allowSuccessor bool) (bool, error) {
	identity := event.RuntimeIdentity
	progress := cloneReadinessProgress(event.Progress)
	if identity.RuntimeBuild == "" || identity.ProcessGeneration == "" {
		return false, fmt.Errorf("%w: empty runtime identity", ErrReadinessProgress)
	}
	if progress.Sequence == 0 || len(progress.Detail) > MaxReadinessDetailBytes {
		return false, fmt.Errorf("%w: invalid progress sequence/detail", ErrReadinessProgress)
	}
	switch progress.State {
	case RuntimeStarting, RuntimeReady, RuntimeFailed, RuntimeDraining:
	default:
		return false, fmt.Errorf("%w: state %q", ErrReadinessProgress, progress.State)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock.Now()
	if t.timeout > 0 && !now.Before(t.deadline) {
		return false, t.noProgressLocked()
	}
	if t.expected != nil && identity != *t.expected {
		return false, identityMismatch(identity, *t.expected)
	}
	newGeneration := t.identity.RuntimeBuild != "" && t.identity != identity
	if newGeneration && !allowSuccessor {
		return false, identityMismatch(identity, t.identity)
	}
	if t.identity.RuntimeBuild == "" || newGeneration {
		t.identity = identity
		t.progress = progress
		t.observed = true
		if t.notify != nil {
			t.notify(identity, cloneReadinessProgress(progress))
		}
	} else if !t.observed {
		t.progress = progress
		t.observed = true
		if t.notify != nil {
			t.notify(identity, cloneReadinessProgress(progress))
		}
	} else {
		switch {
		case progress.Sequence < t.progress.Sequence:
			return false, fmt.Errorf(
				"%w: sequence regressed from %d to %d",
				ErrReadinessProgress, t.progress.Sequence, progress.Sequence,
			)
		case progress.Sequence == t.progress.Sequence:
			if progress.State != t.progress.State || !bytes.Equal(progress.Detail, t.progress.Detail) {
				return false, fmt.Errorf("%w: sequence %d mutated", ErrReadinessProgress, progress.Sequence)
			}
		default:
			if !validReadinessTransition(t.progress.State, progress.State) {
				return false, fmt.Errorf(
					"%w: invalid transition %s to %s",
					ErrReadinessProgress, t.progress.State, progress.State,
				)
			}
			t.progress = progress
			if t.timeout > 0 {
				t.deadline = now.Add(t.timeout)
			}
			if t.notify != nil {
				t.notify(identity, cloneReadinessProgress(progress))
			}
		}
	}

	switch progress.State {
	case RuntimeReady:
		return true, nil
	case RuntimeFailed:
		return false, &RuntimeFailedError{Identity: identity, Last: progress}
	case RuntimeDraining:
		return false, ErrDraining
	default:
		return false, nil
	}
}

func validReadinessTransition(from, to RuntimeReadinessState) bool {
	switch from {
	case RuntimeStarting:
		return to == RuntimeStarting || to == RuntimeReady || to == RuntimeFailed || to == RuntimeDraining
	case RuntimeReady:
		return to == RuntimeFailed || to == RuntimeDraining
	default:
		return false
	}
}

func (t *runtimeProgressTracker) adopt(identity RuntimeIdentity, progress ReadinessProgress) error {
	_, err := t.observe(
		runtimeReadinessEvent{RuntimeIdentity: identity, Progress: progress},
		t.expected == nil,
	)
	return err
}

func (t *runtimeProgressTracker) pinIdentity(identity RuntimeIdentity) error {
	if err := validateRuntimeIdentity(identity); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timeout > 0 && !t.clock.Now().Before(t.deadline) {
		return t.noProgressLocked()
	}
	if t.expected != nil && identity != *t.expected {
		return identityMismatch(identity, *t.expected)
	}
	if t.identity.RuntimeBuild != "" && t.identity != identity {
		return identityMismatch(identity, t.identity)
	}
	t.identity = identity
	return nil
}

func (t *runtimeProgressTracker) current() (RuntimeIdentity, ReadinessProgress) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.identity, cloneReadinessProgress(t.progress)
}

func (t *runtimeProgressTracker) wait(ctx context.Context, failures int, wait readinessWaiter) error {
	_, err := withinProgress(ctx, t, func(waitCtx context.Context) (struct{}, error) {
		return struct{}{}, wait(waitCtx, failures)
	})
	return err
}

func (t *runtimeProgressTracker) noProgressLocked() error {
	return &ReadinessNoProgressError{
		Identity: t.identity,
		Last:     cloneReadinessProgress(t.progress),
	}
}

func withinProgressError(
	ctx context.Context,
	tracker *runtimeProgressTracker,
	operation func(context.Context) error,
) error {
	_, err := withinProgress(ctx, tracker, func(operationCtx context.Context) (struct{}, error) {
		return struct{}{}, operation(operationCtx)
	})
	return err
}

func withinProgress[T any](
	ctx context.Context,
	tracker *runtimeProgressTracker,
	operation func(context.Context) (T, error),
) (T, error) {
	var zero T
	tracker.mu.Lock()
	deadline := tracker.deadline
	clock := tracker.clock
	tracker.mu.Unlock()
	if deadline.IsZero() {
		return operation(ctx)
	}
	remaining := deadline.Sub(clock.Now())
	if remaining <= 0 {
		tracker.mu.Lock()
		err := tracker.noProgressLocked()
		tracker.mu.Unlock()
		return zero, err
	}
	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() {
		value, err := operation(operationCtx)
		done <- result{value: value, err: err}
	}()
	select {
	case outcome := <-done:
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		if !clock.Now().Before(deadline) {
			tracker.mu.Lock()
			err := tracker.noProgressLocked()
			tracker.mu.Unlock()
			return zero, err
		}
		return outcome.value, outcome.err
	case <-ctx.Done():
		cancel()
		<-done
		return zero, ctx.Err()
	case <-clock.After(remaining):
		cancel()
		<-done
		tracker.mu.Lock()
		err := tracker.noProgressLocked()
		tracker.mu.Unlock()
		return zero, err
	}
}

func isReadinessPeerEnd(err error) bool {
	return isDisconnect(err) || errors.Is(err, errRuntimePeerStale) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE)
}

func identityMismatch(got, want RuntimeIdentity) error {
	if got.RuntimeBuild != want.RuntimeBuild {
		return fmt.Errorf(
			"%w: server=%q client=%q", ErrRuntimeBuildMismatch, got.RuntimeBuild, want.RuntimeBuild,
		)
	}
	return fmt.Errorf(
		"%w: server=%q client=%q",
		ErrProcessGenerationMismatch, got.ProcessGeneration, want.ProcessGeneration,
	)
}

func cloneRuntimeIdentity(identity *RuntimeIdentity) *RuntimeIdentity {
	if identity == nil {
		return nil
	}
	clone := *identity
	return &clone
}

func cloneReadinessProgress(progress ReadinessProgress) ReadinessProgress {
	return ReadinessProgress{
		Sequence: progress.Sequence,
		State:    progress.State,
		Detail:   append([]byte{}, progress.Detail...),
	}
}

func waitReadinessRetry(ctx context.Context, failures int) error {
	timer := time.NewTimer(runtimeReadinessBackoff.After(failures))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
