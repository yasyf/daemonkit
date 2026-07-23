package wire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

type stopTestClassifier struct {
	accepted bool
}

func (stopTestClassifier) Validate() error { return nil }
func (c stopTestClassifier) Classify(context.Context, Peer) (bool, error) {
	return c.accepted, nil
}

type stopTestVerifier struct {
	verify func(context.Context, Peer, string) (proc.Record, error)
}

func (stopTestVerifier) Validate() error { return nil }
func (v stopTestVerifier) VerifyStopControl(
	ctx context.Context,
	peer Peer,
	target string,
) (proc.Record, error) {
	return v.verify(ctx, peer, target)
}

type stopTestControl struct {
	health    daemon.Health
	shutdowns atomic.Int32
}

func (c *stopTestControl) Health(context.Context) (daemon.Health, error) { return c.health, nil }
func (c *stopTestControl) Shutdown(context.Context) error {
	c.shutdowns.Add(1)
	return nil
}

func bindStopTestRuntime(
	t *testing.T,
	control *stopTestControl,
	record func(string) proc.Record,
) (*Server, []byte) {
	t.Helper()
	server := &Server{WireBuild: "target-wire", MaxSessions: 2}
	verifier := stopTestVerifier{verify: func(_ context.Context, _ Peer, target string) (proc.Record, error) {
		return record(target), nil
	}}
	if err := server.bindRuntime(
		"target-runtime", stopTestClassifier{accepted: true}, 1, control, verifier, nil, nil, nil,
	); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(stopControlRequest{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	return server, payload
}

func TestStopHandlerUsesOnlyAuthoritativeConsumedRecord(t *testing.T) {
	control := &stopTestControl{health: daemon.Health{
		RuntimeBuild: "v2.0.0", RuntimeProtocol: 1, PID: os.Getpid(), ProcessGeneration: "test-generation",
	}}
	server, payload := bindStopTestRuntime(t, control, func(target string) proc.Record {
		return proc.Record{
			RuntimeBuild: "v1.0.0", RuntimeProtocol: 1, Intent: string(StopIntentUpgrade), TargetProcessGeneration: target,
		}
	})
	authorized, err := server.authorizeStopControl(t.Context(), Peer{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	value, err := server.handlers[stopControlOp].h(authorized, Request{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	response := value.(stopControlResponse)
	if response.Stopped || control.shutdowns.Load() != 0 {
		t.Fatalf("older upgrade stopped runtime: response=%+v shutdowns=%d", response, control.shutdowns.Load())
	}
}

func TestStopHandlerRestartStopsOnlyCurrentGeneration(t *testing.T) {
	control := &stopTestControl{health: daemon.Health{
		RuntimeBuild: "v2.0.0", RuntimeProtocol: 1, PID: os.Getpid(), ProcessGeneration: "test-generation",
	}}
	server, payload := bindStopTestRuntime(t, control, func(target string) proc.Record {
		return proc.Record{
			RuntimeBuild: "v2.0.0", RuntimeProtocol: 1, Intent: string(StopIntentRestart), TargetProcessGeneration: target,
		}
	})
	authorized, err := server.authorizeStopControl(t.Context(), Peer{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	value, err := server.handlers[stopControlOp].h(authorized, Request{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	response := value.(stopControlResponse)
	if !response.Stopped || control.shutdowns.Load() != 1 {
		t.Fatalf("restart response=%+v shutdowns=%d", response, control.shutdowns.Load())
	}
}

func TestStopAuthorizationRejectsVerifierGenerationSubstitution(t *testing.T) {
	control := &stopTestControl{health: daemon.Health{
		RuntimeBuild: "v2.0.0", RuntimeProtocol: 1, PID: os.Getpid(), ProcessGeneration: "test-generation",
	}}
	server, payload := bindStopTestRuntime(t, control, func(string) proc.Record {
		return proc.Record{
			RuntimeBuild: "v3.0.0", RuntimeProtocol: 1, Intent: string(StopIntentUpgrade), TargetProcessGeneration: "other-runtime",
		}
	})
	if _, err := server.authorizeStopControl(t.Context(), Peer{}, payload); err == nil {
		t.Fatal("authorizeStopControl accepted a substituted runtime generation")
	}
	if control.shutdowns.Load() != 0 {
		t.Fatal("generation substitution reached shutdown")
	}
}

func TestStopAuthorizationRejectsCallerAuthorityFields(t *testing.T) {
	control := &stopTestControl{health: daemon.Health{
		RuntimeBuild: "v2.0.0", RuntimeProtocol: 1, PID: os.Getpid(), ProcessGeneration: "test-generation",
	}}
	var calls atomic.Int32
	server, _ := bindStopTestRuntime(t, control, func(target string) proc.Record {
		calls.Add(1)
		return proc.Record{RuntimeBuild: "v3", RuntimeProtocol: 1, Intent: "upgrade", TargetProcessGeneration: target}
	})
	payload := []byte(`{"version":1,"receipt":{"build":"forged"}}`)
	if _, err := server.authorizeStopControl(t.Context(), Peer{}, payload); err == nil {
		t.Fatal("authorizeStopControl accepted caller authority fields")
	}
	if calls.Load() != 0 || control.shutdowns.Load() != 0 {
		t.Fatal("forged request reached authority or shutdown")
	}
}

func TestStopHandlerRejectsRuntimeIdentityDriftBeforeShutdown(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*daemon.Health)
	}{
		{name: "protocol", mutate: func(health *daemon.Health) { health.RuntimeProtocol++ }},
		{name: "process generation", mutate: func(health *daemon.Health) { health.ProcessGeneration = "replacement-generation" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			control := &stopTestControl{health: daemon.Health{
				RuntimeBuild: "v2.0.0", RuntimeProtocol: 1, PID: os.Getpid(), ProcessGeneration: "test-generation",
			}}
			server, payload := bindStopTestRuntime(t, control, func(target string) proc.Record {
				return proc.Record{
					RuntimeBuild: "v3.0.0", RuntimeProtocol: 1, Intent: string(StopIntentUpgrade), TargetProcessGeneration: target,
				}
			})
			authorized, err := server.authorizeStopControl(t.Context(), Peer{}, payload)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(&control.health)
			if _, err := server.handlers[stopControlOp].h(authorized, Request{Payload: payload}); err == nil {
				t.Fatal("stop handler accepted runtime identity drift")
			}
			if control.shutdowns.Load() != 0 {
				t.Fatal("runtime identity drift reached shutdown")
			}
		})
	}
}

type stopStoreStub struct {
	record proc.Record
	ok     bool
	got    struct {
		identity proc.Identity
		role     string
		target   string
	}
}

func (s *stopStoreStub) ConsumeStopControl(
	_ context.Context,
	identity proc.Identity,
	role, target string,
	_ time.Time,
) (proc.Record, bool, error) {
	s.got.identity, s.got.role, s.got.target = identity, role, target
	return s.record, s.ok, nil
}

func TestStopVerifierKeysAuthorityFromAcceptedPeerAndFixedRole(t *testing.T) {
	peer := Peer{PID: 42, StartTime: "start", Boot: "boot", Comm: "child", Executable: "/app/daemon"}
	store := &stopStoreStub{ok: true, record: proc.Record{RuntimeBuild: "v2"}}
	verifier := StopVerifier{
		Classifier: stopTestClassifier{accepted: true}, Role: "com.example.stop", Store: store,
	}
	record, err := verifier.VerifyStopControl(t.Context(), peer, "runtime-generation")
	if err != nil {
		t.Fatal(err)
	}
	if record != store.record || store.got.identity != peer.ProcessIdentity() ||
		store.got.role != verifier.Role || store.got.target != "runtime-generation" {
		t.Fatalf("authority lookup = %+v, %+v", record, store.got)
	}
}

func TestStopVerifierRejectsUnclassifiedPeerBeforeStore(t *testing.T) {
	store := &stopStoreStub{ok: true}
	verifier := StopVerifier{
		Classifier: stopTestClassifier{accepted: false}, Role: "com.example.stop", Store: store,
	}
	_, err := verifier.VerifyStopControl(t.Context(), Peer{PID: 42}, "runtime-generation")
	if !errors.Is(err, ErrProtectedSessionRequired) {
		t.Fatalf("VerifyStopControl error = %v, want ErrProtectedSessionRequired", err)
	}
	if store.got.role != "" {
		t.Fatal("unclassified peer reached durable store")
	}
}

func TestStopRequestContextIgnoresProductLadder(t *testing.T) {
	for _, serverDuration := range []time.Duration{time.Millisecond, 10 * time.Minute} {
		ladder, err := NewLadder(
			map[Op]time.Duration{stopControlOp: serverDuration},
			map[Op]time.Duration{stopControlOp: serverDuration + time.Second},
		)
		if err != nil {
			t.Fatal(err)
		}
		server := &Server{Ladder: ladder}
		frameDeadline := time.Now().Add(time.Second).UnixMilli()
		ctx, cancel := server.requestContext(t.Context(), Frame{
			Op: stopControlOp, DeadlineUnixMilli: frameDeadline,
		})
		deadline, ok := ctx.Deadline()
		cancel()
		if !ok || deadline.UnixMilli() != frameDeadline {
			t.Fatalf("stop deadline with server ladder %v = %v, %v; want frame deadline %d", serverDuration, deadline, ok, frameDeadline)
		}
		ctx, cancel = server.requestContext(t.Context(), Frame{Op: stopControlOp})
		_, ok = ctx.Deadline()
		cancel()
		if ok {
			t.Fatalf("stop context inherited product ladder %v without a frame deadline", serverDuration)
		}
	}
}
