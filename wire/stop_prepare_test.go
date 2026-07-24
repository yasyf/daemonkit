package wire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"

	"github.com/yasyf/daemonkit/internal/runtimeauth"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
)

type preparedStopClientStub struct {
	results []Result
	errors  []error
	calls   int
}

func (c *preparedStopClientStub) Call(context.Context, Op, string, []byte) (Result, error) {
	index := c.calls
	c.calls++
	return c.results[index], c.errors[index]
}

func (*preparedStopClientStub) Close() error { return nil }

func (*preparedStopClientStub) PeerWireIdentity() WireIdentity { return WireIdentity{} }

func TestPrepareStopRequiresInternalAuthorityBeforeDial(t *testing.T) {
	dialed := false
	_, err := PrepareStop(t.Context(), RuntimeClientConfig{Client: ClientConfig{
		WireBuild: "suite-v1",
		Dial: func(context.Context) (net.Conn, error) {
			dialed = true
			return nil, errors.New("unexpected dial")
		},
	}}, trust.PeerRole("stop"), runtimeauth.StopControlAuthority{})
	if err == nil || dialed {
		t.Fatalf("PrepareStop = %v, dialed=%v", err, dialed)
	}
}

func TestStopPreparationAuthorizationBindsConcreteRole(t *testing.T) {
	payload, err := json.Marshal(stopControlPrepareRequest{Version: 1, ControlRole: "expected-stop"})
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{}
	if err := server.authorizeStopPreparation("another-stop", payload); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("role substitution error = %v", err)
	}
	for _, malformed := range [][]byte{
		[]byte(`{"version":1,"control_role":"expected-stop","legacy":true}`),
		[]byte(`{"version":2,"control_role":"expected-stop"}`),
	} {
		if err := server.authorizeStopPreparation("expected-stop", malformed); err == nil {
			t.Fatalf("malformed preparation %s was accepted", malformed)
		}
	}
}

func TestSessionStopPreparationRejectsBadRequestWithoutConsuming(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopSession := proc.StopSessionID{1}
	nonce := proc.StopPreparationNonce{2}
	target := proc.Identity{PID: 42, StartTime: "start", Boot: "boot", Executable: "/bin/runtime"}
	runtimeIdentity := RuntimeIdentity{RuntimeBuild: "runtime-v1", ProcessGeneration: "generation-v1"}
	sessionA := &session{ctx: ctx, role: "stop"}
	preparation := sessionStopPreparation{
		role: "stop", stopSession: stopSession, preparationNonce: nonce,
		target: target, runtime: runtimeIdentity, protocol: 1,
	}
	if err := sessionA.installStopPreparation(preparation); err != nil {
		t.Fatal(err)
	}
	request := stopControlRequest{
		Version: 1, OperationID: "stop-operation", StopSession: stopSession[:], PreparationNonce: nonce[:],
		Target:          newStopControlTarget(target, runtimeIdentity.ProcessGeneration),
		RuntimeIdentity: runtimeIdentity, RuntimeProtocol: 1,
	}
	bad := request
	bad.RuntimeProtocol = 2
	consumes := 0
	if err := sessionA.consumeStopPreparation(bad, func(sessionStopPreparation) error {
		consumes++
		return nil
	}); err == nil {
		t.Fatal("bad request consumed the session preparation")
	}
	if consumes != 0 {
		t.Fatalf("bad request consume callbacks = %d, want 0", consumes)
	}
	sessionB := &session{ctx: ctx, role: "stop"}
	if err := sessionB.consumeStopPreparation(request, func(sessionStopPreparation) error {
		consumes++
		return nil
	}); err == nil {
		t.Fatal("sibling session consumed another session's preparation")
	}
	if err := sessionA.consumeStopPreparation(request, func(got sessionStopPreparation) error {
		consumes++
		if got != preparation {
			t.Fatalf("consumed preparation = %#v, want %#v", got, preparation)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if consumes != 1 {
		t.Fatalf("exact consume callbacks = %d, want 1", consumes)
	}
	if err := sessionA.consumeStopPreparation(request, func(sessionStopPreparation) error { return nil }); err == nil {
		t.Fatal("session preparation was consumed twice")
	}
}

func TestPreparedStopPreSendFailureIsRetryable(t *testing.T) {
	target := RuntimeIdentity{RuntimeBuild: "runtime-v1", ProcessGeneration: "generation-v1"}
	process := proc.Identity{PID: 42, StartTime: "start", Boot: "boot", Executable: "/bin/runtime"}
	payload, err := json.Marshal(stopControlResponse{
		Version: 1, Target: newStopControlTarget(process, target.ProcessGeneration),
		RuntimeBuild: target.RuntimeBuild, RuntimeProtocol: 1, Stopped: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := &preparedStopClientStub{
		results: []Result{{Outcome: PreSendFailure}, {Outcome: Delivered, Response: Response{Payload: payload}}},
		errors:  []error{errors.New("pre-send"), nil},
	}
	prepared := &PreparedStop{
		client: client, target: target, process: process, runtimeProtocol: 1,
		stopSession: proc.StopSessionID{1}, preparationNonce: proc.StopPreparationNonce{2},
	}
	if _, outcome, err := prepared.Dispatch(t.Context(), "stop-operation"); outcome != PreSendFailure || err == nil {
		t.Fatalf("first Dispatch = %s, %v; want pre-send failure", outcome, err)
	}
	result, outcome, err := prepared.Dispatch(t.Context(), "stop-operation")
	if err != nil || outcome != Delivered || !result.Stopped || client.calls != 2 {
		t.Fatalf("retry Dispatch = %#v, %s, %v, calls=%d", result, outcome, err, client.calls)
	}
}

func TestPreparedStopDeliveryUnknownSealsHandle(t *testing.T) {
	client := &preparedStopClientStub{
		results: []Result{{Outcome: DeliveryUnknown}},
		errors:  []error{errors.New("partial write")},
	}
	prepared := &PreparedStop{
		client:          client,
		target:          RuntimeIdentity{RuntimeBuild: "runtime-v1", ProcessGeneration: "generation-v1"},
		process:         proc.Identity{PID: 42, StartTime: "start", Boot: "boot", Executable: "/bin/runtime"},
		runtimeProtocol: 1, stopSession: proc.StopSessionID{1}, preparationNonce: proc.StopPreparationNonce{2},
	}
	if _, outcome, err := prepared.Dispatch(t.Context(), "stop-operation"); outcome != DeliveryUnknown || err == nil {
		t.Fatalf("first Dispatch = %s, %v; want delivery unknown", outcome, err)
	}
	if _, _, err := prepared.Dispatch(t.Context(), "stop-operation"); err == nil {
		t.Fatal("delivery-unknown handle was dispatched twice")
	}
	if client.calls != 1 {
		t.Fatalf("client calls = %d, want 1", client.calls)
	}
}

func TestClientCallResponsePrefersReadyTerminalOverCancellation(t *testing.T) {
	ready := make(chan struct{})
	call := &ClientCall{ready: ready, terminal: callResult{result: Result{Outcome: Delivered}}}
	close(ready)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := call.Response(ctx)
	if err != nil || result.Outcome != Delivered {
		t.Fatalf("Response = %#v, %v; want delivered terminal", result, err)
	}
}
