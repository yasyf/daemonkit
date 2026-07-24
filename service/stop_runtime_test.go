package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	bolt "go.etcd.io/bbolt"
)

type fakePreparedStop struct {
	target   wire.RuntimeIdentity
	process  proc.Identity
	protocol int
	dispatch func()
	outcome  wire.Outcome
	err      error
	outcomes []wire.Outcome
	errors   []error

	mu         sync.Mutex
	closed     bool
	dispatches int
}

func (s *fakePreparedStop) Target() wire.RuntimeIdentity { return s.target }
func (s *fakePreparedStop) Process() proc.Identity       { return s.process }
func (s *fakePreparedStop) RuntimeProtocol() int         { return s.protocol }
func (s *fakePreparedStop) StopSession() proc.StopSessionID {
	return proc.StopSessionID{1}
}

func (s *fakePreparedStop) PreparationNonce() proc.StopPreparationNonce {
	return proc.StopPreparationNonce{2}
}

func (s *fakePreparedStop) Dispatch(context.Context, string) (wire.StopResult, wire.Outcome, error) {
	s.mu.Lock()
	index := s.dispatches
	s.dispatches++
	s.mu.Unlock()
	outcome := s.outcome
	dispatchErr := s.err
	if index < len(s.outcomes) {
		outcome = s.outcomes[index]
	}
	if index < len(s.errors) {
		dispatchErr = s.errors[index]
	}
	if outcome == wire.Delivered && dispatchErr == nil && s.dispatch != nil {
		s.dispatch()
	}
	return wire.StopResult{
		Process: s.process, ProcessGeneration: s.target.ProcessGeneration,
		RuntimeBuild: s.target.RuntimeBuild, RuntimeProtocol: s.protocol, Stopped: true,
	}, outcome, dispatchErr
}

func (s *fakePreparedStop) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

type stopRuntimeFixture struct {
	store       *boltControllerStore
	process     proc.Identity
	target      wire.RuntimeIdentity
	gone        bool
	prepares    int
	dispatches  int
	statePath   string
	processPath string
}

func newStopRuntimeFixture(t *testing.T) *stopRuntimeFixture {
	t.Helper()
	dir := t.TempDir()
	store, err := openControllerStore(t.Context(), filepath.Join(dir, "controller.db"))
	if err != nil {
		t.Fatal(err)
	}
	fixture := &stopRuntimeFixture{
		store: store, statePath: filepath.Join(dir, "controller.db"), processPath: filepath.Join(dir, "process.db"),
		process: proc.Identity{
			PID: 4242, StartTime: "runtime-start", Boot: "runtime-boot",
			Comm: "runtime", Executable: "/bin/runtime",
		},
		target: wire.RuntimeIdentity{RuntimeBuild: "runtime-v1", ProcessGeneration: proc.OwnerGeneration{1}},
	}
	t.Cleanup(func() { _ = fixture.store.Close() })
	return fixture
}

func (f *stopRuntimeFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := openControllerStore(t.Context(), f.statePath)
	if err != nil {
		t.Fatal(err)
	}
	f.store = store
}

func (f *stopRuntimeFixture) controller() *Controller {
	controller := &Controller{
		store: f.store,
		stopReaper: &proc.Reaper{
			Store: &proc.FileStore{Path: f.processPath}, Generation: proc.OwnerGeneration{2},
		},
	}
	controller.stopRuntimePrepare = func(
		context.Context, wire.RuntimeClientConfig, trust.PeerRole,
	) (stopRuntimePrepared, error) {
		f.prepares++
		return &fakePreparedStop{
			target: f.target, process: f.process, protocol: 1,
			dispatch: func() { f.dispatches++; f.gone = true },
		}, nil
	}
	controller.stopRuntimeProbe = func(int) (proc.Identity, error) {
		if f.gone {
			return proc.Identity{}, proc.ErrNoProcess
		}
		return f.process, nil
	}
	return controller
}

func stopRuntimeTestRequest() StopRuntimeRequest {
	return StopRuntimeRequest{
		OperationID: "stop-operation-1", ExpectedRuntimeBuild: "runtime-v1", ControlRole: trust.PeerRole("stop"),
		RuntimeClientConfig: wire.RuntimeClientConfig{Client: wire.ClientConfig{
			WireBuild: "suite-v1", Dial: func(context.Context) (net.Conn, error) {
				return nil, errors.New("fake dial must not run")
			},
		}},
	}
}

func TestStopRuntimeCrashMatrixReplaysExactReceipt(t *testing.T) {
	points := []stopRuntimeCrashPoint{
		stopRuntimeCrashAfterIntent,
		stopRuntimeCrashAfterStop,
		stopRuntimeCrashAfterAbsence,
		stopRuntimeCrashBeforeCompletion,
	}
	for _, point := range points {
		t.Run(fmt.Sprintf("point-%d", point), func(t *testing.T) {
			fixture := newStopRuntimeFixture(t)
			controller := fixture.controller()
			crash := errors.New("simulated crash")
			controller.stopRuntimeCrashHook = func(got stopRuntimeCrashPoint) error {
				if got == point {
					return crash
				}
				return nil
			}
			if _, err := controller.StopRuntime(t.Context(), stopRuntimeTestRequest()); !errors.Is(err, crash) {
				t.Fatalf("crash point %d error = %v", point, err)
			}
			prepares := fixture.prepares
			fixture.reopen(t)
			replay := fixture.controller()
			first, err := replay.StopRuntime(t.Context(), stopRuntimeTestRequest())
			if err != nil {
				t.Fatalf("replay: %v", err)
			}
			second, err := replay.StopRuntime(t.Context(), stopRuntimeTestRequest())
			if err != nil || first != second || first.Digest() == (StopReceiptDigest{}) ||
				first.Settlement() != StopSettlementGone {
				t.Fatalf("repeat receipt = %#v, %#v, %v", first, second, err)
			}
			if point != stopRuntimeCrashAfterIntent && fixture.prepares != prepares {
				t.Fatal("replay prepared a successor after stop dispatch")
			}
			durableIntent, durableReceipt, err := fixture.store.LoadStopRuntime(t.Context(), first.OperationID())
			if err != nil || durableIntent != nil || durableReceipt == nil || *durableReceipt != first {
				t.Fatalf("terminal state = %#v, %#v, %v; want receipt-only", durableIntent, durableReceipt, err)
			}
		})
	}
}

func TestStopRuntimeRetriesOneProvenPreSendFailure(t *testing.T) {
	fixture := newStopRuntimeFixture(t)
	controller := fixture.controller()
	prepared := &fakePreparedStop{
		target: fixture.target, process: fixture.process, protocol: 1,
		outcomes: []wire.Outcome{wire.PreSendFailure, wire.Delivered},
		errors:   []error{errors.New("pre-send"), nil},
		dispatch: func() { fixture.dispatches++; fixture.gone = true },
	}
	controller.stopRuntimePrepare = func(
		context.Context, wire.RuntimeClientConfig, trust.PeerRole,
	) (stopRuntimePrepared, error) {
		fixture.prepares++
		return prepared, nil
	}
	receipt, err := controller.StopRuntime(t.Context(), stopRuntimeTestRequest())
	if err != nil || receipt.Settlement() != StopSettlementGone {
		t.Fatalf("StopRuntime = %#v, %v", receipt, err)
	}
	if prepared.dispatches != 2 || fixture.dispatches != 1 || fixture.prepares != 1 {
		t.Fatalf("attempts/deliveries/prepares = %d/%d/%d, want 2/1/1", prepared.dispatches, fixture.dispatches, fixture.prepares)
	}
}

func TestStopRuntimeDoesNotRetryPostSendUnknown(t *testing.T) {
	fixture := newStopRuntimeFixture(t)
	fixture.gone = true
	controller := fixture.controller()
	prepared := &fakePreparedStop{
		target: fixture.target, process: fixture.process, protocol: 1,
		outcomes: []wire.Outcome{wire.DeliveryUnknown},
		errors:   []error{errors.New("delivery unknown")},
	}
	controller.stopRuntimePrepare = func(
		context.Context, wire.RuntimeClientConfig, trust.PeerRole,
	) (stopRuntimePrepared, error) {
		fixture.prepares++
		return prepared, nil
	}
	receipt, err := controller.StopRuntime(t.Context(), stopRuntimeTestRequest())
	if err != nil || receipt.Settlement() != StopSettlementGone {
		t.Fatalf("StopRuntime = %#v, %v", receipt, err)
	}
	if prepared.dispatches != 1 {
		t.Fatalf("post-send dispatch attempts = %d, want 1", prepared.dispatches)
	}
}

func TestStopRuntimeRejectsConflictAndWrongBuild(t *testing.T) {
	fixture := newStopRuntimeFixture(t)
	controller := fixture.controller()
	receipt, err := controller.StopRuntime(t.Context(), stopRuntimeTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	conflict := stopRuntimeTestRequest()
	conflict.ExpectedRuntimeBuild = "runtime-v2"
	if _, err := controller.StopRuntime(t.Context(), conflict); !errors.Is(err, ErrStopRuntimeConflict) {
		t.Fatalf("same ID conflict = %v", err)
	}
	if receipt.Target() != fixture.target {
		t.Fatalf("receipt target = %#v", receipt.Target())
	}

	other := newStopRuntimeFixture(t)
	wrong := stopRuntimeTestRequest()
	wrong.OperationID = "wrong-build"
	wrong.ExpectedRuntimeBuild = "runtime-v2"
	if _, err := other.controller().StopRuntime(t.Context(), wrong); err == nil {
		t.Fatal("wrong runtime build was accepted")
	}
	intent, stored, err := other.store.LoadStopRuntime(t.Context(), wrong.OperationID)
	if err != nil || intent != nil || stored != nil || other.dispatches != 0 {
		t.Fatalf("wrong build residue = %#v, %#v, dispatches=%d, %v", intent, stored, other.dispatches, err)
	}
}

func TestStopRuntimePIDReuseCompletesWithoutStoppingSuccessor(t *testing.T) {
	fixture := newStopRuntimeFixture(t)
	controller := fixture.controller()
	crash := errors.New("after stop")
	controller.stopRuntimeCrashHook = func(point stopRuntimeCrashPoint) error {
		if point == stopRuntimeCrashAfterStop {
			return crash
		}
		return nil
	}
	if _, err := controller.StopRuntime(t.Context(), stopRuntimeTestRequest()); !errors.Is(err, crash) {
		t.Fatalf("first stop = %v", err)
	}
	fixture.gone = false
	successor := fixture.process
	successor.StartTime = "successor-start"
	prepares := fixture.prepares
	replay := fixture.controller()
	replay.stopRuntimeProbe = func(int) (proc.Identity, error) { return successor, nil }
	receipt, err := replay.StopRuntime(t.Context(), stopRuntimeTestRequest())
	if err != nil || receipt.Settlement() != StopSettlementGone {
		t.Fatalf("PID reuse replay = %#v, %v", receipt, err)
	}
	if fixture.prepares != prepares || fixture.dispatches != 1 {
		t.Fatalf("successor was contacted: prepares=%d dispatches=%d", fixture.prepares, fixture.dispatches)
	}
}

func TestStopRuntimeStateRejectsUnknownFields(t *testing.T) {
	fixture := newStopRuntimeFixture(t)
	controller := fixture.controller()
	crash := errors.New("after intent")
	controller.stopRuntimeCrashHook = func(point stopRuntimeCrashPoint) error {
		if point == stopRuntimeCrashAfterIntent {
			return crash
		}
		return nil
	}
	request := stopRuntimeTestRequest()
	if _, err := controller.StopRuntime(t.Context(), request); !errors.Is(err, crash) {
		t.Fatalf("seed intent = %v", err)
	}
	if err := fixture.store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(controllerStopIntentBucket)
		var object map[string]any
		if err := json.Unmarshal(bucket.Get([]byte(request.OperationID)), &object); err != nil {
			return err
		}
		object["legacy"] = true
		value, err := json.Marshal(object)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(request.OperationID), value)
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fixture.store.LoadStopRuntime(t.Context(), request.OperationID); err == nil {
		t.Fatal("unknown stop intent field was accepted")
	}
}
