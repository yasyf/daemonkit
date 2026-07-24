package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

var spawnedTestLimits = SessionLimits{
	Workers: 2, Backlog: 2, MaxFrame: 2 << 20,
	InboundQueue: 8, OutboundQueue: 16, StreamQueue: 4, EventQueue: 4,
	HandshakeTimeout: time.Second, WriteTimeout: time.Second,
	CancelSettlementTimeout: time.Second,
}

func spawnedTestLadder(t *testing.T) Ladder {
	t.Helper()
	server := map[Op]time.Duration{
		"test.echo": time.Second, "test.stream": time.Second, "test.cancel": time.Second,
	}
	client := map[Op]time.Duration{
		"test.echo": 2 * time.Second, "test.stream": 2 * time.Second, "test.cancel": 2 * time.Second,
	}
	ladder, err := NewLadder(server, client)
	if err != nil {
		t.Fatal(err)
	}
	return ladder
}

func newSpawnedWireManager(t *testing.T) *proc.Manager {
	t.Helper()
	manager, err := proc.NewManager(1, &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")},
		Generation: "wire-spawned-test", Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ClaimRuntime(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Shutdown(ctx); err != nil {
			t.Errorf("shutdown manager: %v", err)
		}
	})
	return manager
}

func TestSpawnedSessionPublicSeamStreamsEventsCancelsAndJoins(t *testing.T) {
	manager := newSpawnedWireManager(t)
	executable, err := proc.ExecutablePath(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := filepath.Join(t.TempDir(), "helper-error")
	var signature proc.SignatureDigest
	signature[0] = 1
	request, err := proc.NewSpawnRequest(proc.SpawnConfig{
		RecoveryClass: proc.RecoveryTask,
		Executable:    executable,
		Args:          []string{"-test.run=^TestSpawnedWireHelperProcess$", "-test.v"},
		Env: []string{
			"SPAWNED_WIRE_DIAGNOSTIC=" + diagnostic,
			"SPAWNED_WIRE_HELPER=1",
		},
		Stdin: proc.StdioNull, Stdout: proc.StdioNull, Stderr: proc.StdioNull,
		SpawnedSession: true, ExpectedSignature: &signature,
	})
	if err != nil {
		t.Fatal(err)
	}
	child, receipt, err := manager.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	endpoint, err := child.ClaimSpawnedSession(context.Background(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewSpawnedClient(context.Background(), SpawnedClientConfig{
		Endpoint: endpoint, WireBuild: "spawned-test-v1",
		Ladder: spawnedTestLadder(t), Limits: spawnedTestLimits,
	})
	if err != nil {
		diagnosticPayload, _ := os.ReadFile(diagnostic)
		t.Fatalf("NewSpawnedClient: %v; helper=%s", err, diagnosticPayload)
	}

	result, err := client.Call(context.Background(), "test.echo", "tenant-a", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	var echoed string
	if err := json.Unmarshal(result.Response.Payload, &echoed); err != nil {
		t.Fatal(err)
	}
	if echoed != "tenant-a:hello" {
		t.Fatalf("echo response = %q", echoed)
	}

	call, err := client.OpenStream(context.Background(), "test.stream", "tenant-a", []byte("first"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := call.SendChunk(context.Background(), []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := call.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	var responseChunks []string
	for chunk := range call.Chunks() {
		responseChunks = append(responseChunks, string(chunk.Payload))
	}
	result, err = call.Response(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var terminal string
	if err := json.Unmarshal(result.Response.Payload, &terminal); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(responseChunks) != "[first second]" || terminal != "stream-done" {
		t.Fatalf("stream chunks=%v terminal=%q", responseChunks, terminal)
	}
	select {
	case event := <-client.Events():
		if event.Topic != "stream.complete" || string(event.Payload) != "tenant-a" {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("spawned event was not delivered")
	}

	cancelCall, err := client.OpenStream(context.Background(), "test.cancel", "tenant-a", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	cancelCall.Cancel()
	if _, err := cancelCall.Response(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("spawned child did not join after client Close")
	}
	exit, ok := child.Exit()
	if !ok || exit.Code != 0 {
		diagnosticPayload, _ := os.ReadFile(diagnostic)
		t.Fatalf("spawned helper exit=%+v ok=%v diagnostic=%s", exit, ok, diagnosticPayload)
	}
}

func TestSpawnedWireHelperProcess(t *testing.T) {
	if os.Getenv("SPAWNED_WIRE_HELPER") != "1" {
		t.Skip("helper body; runs only re-exec'd")
	}
	fail := func(err error) {
		_ = os.WriteFile(os.Getenv("SPAWNED_WIRE_DIAGNOSTIC"), []byte(err.Error()), 0o600)
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	identity, err := proc.ClaimSpawnedSessionIdentity(ctx)
	if err != nil {
		fail(err)
		return
	}
	if err := proc.CloseInheritedFDs(); err != nil {
		fail(err)
		return
	}
	ladder := spawnedTestLadder(t)
	err = RunSpawnedSession(ctx, SpawnedSessionConfig{
		Identity: identity, WireBuild: "spawned-test-v1", Ladder: ladder, Limits: spawnedTestLimits,
		Handlers: []HandlerSpec{
			{Op: "test.echo", Handler: spawnedEchoHandler},
			{Op: "test.stream", Handler: spawnedStreamHandler, Concurrent: true},
			{Op: "test.cancel", Handler: spawnedCancelHandler, Concurrent: true},
		},
	})
	if err != nil {
		fail(err)
	}
}

func spawnedEchoHandler(_ context.Context, request Request) (any, error) {
	if request.Session == nil || request.Session.Protected() {
		return nil, errors.New("spawned session is not ordinary")
	}
	if request.Peer.PID <= 0 || request.Peer.UID != os.Geteuid() {
		return nil, errors.New("spawned parent identity is incomplete")
	}
	return request.Tenant + ":" + string(request.Payload), nil
}

func spawnedStreamHandler(ctx context.Context, request Request) (any, error) {
	chunks := make(chan []byte, 2)
	chunks <- append([]byte(nil), request.Payload...)
	for chunk := range request.Chunks {
		if chunk.End {
			break
		}
		chunks <- append([]byte(nil), chunk.Payload...)
	}
	close(chunks)
	if err := request.Session.PushEvent(ctx, Event{
		Topic: "stream.complete", Payload: []byte(request.Tenant),
	}); err != nil {
		return nil, err
	}
	return StreamResponse{Chunks: chunks, Value: "stream-done"}, nil
}

func spawnedCancelHandler(ctx context.Context, _ Request) (any, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSpawnedSessionRejectsIncompleteOrPrivateStaticConfig(t *testing.T) {
	ladder := spawnedTestLadder(t)
	valid := SpawnedSessionConfig{
		WireBuild: "spawned-test-v1", Ladder: ladder, Limits: spawnedTestLimits,
		Handlers: []HandlerSpec{{Op: "test.echo", Handler: spawnedEchoHandler}},
	}
	for _, mutate := range []func(*SpawnedSessionConfig){
		func(config *SpawnedSessionConfig) { config.WireBuild = "" },
		func(config *SpawnedSessionConfig) { config.Limits.MaxFrame = 0 },
		func(config *SpawnedSessionConfig) { config.Limits.EventQueue = 0 },
		func(config *SpawnedSessionConfig) { config.Handlers = nil },
		func(config *SpawnedSessionConfig) {
			config.Handlers = []HandlerSpec{{Op: "daemon.private", Handler: spawnedEchoHandler}}
		},
		func(config *SpawnedSessionConfig) {
			config.Handlers = append(config.Handlers, config.Handlers[0])
		},
	} {
		config := valid
		mutate(&config)
		if _, err := compileSpawnedServer(config); err == nil {
			t.Fatalf("invalid static config accepted: %+v", config)
		}
	}
	if err := validateSpawnedClientConfig(SpawnedClientConfig{
		WireBuild: valid.WireBuild, Ladder: valid.Ladder, Limits: SessionLimits{},
	}); err == nil {
		t.Fatal("zero client limits were accepted")
	}
}

func TestSpawnedSessionAbruptParentCloseJoinsChild(t *testing.T) {
	manager := newSpawnedWireManager(t)
	executable, err := proc.ExecutablePath(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := filepath.Join(t.TempDir(), "helper-error")
	var signature proc.SignatureDigest
	signature[0] = 1
	request, err := proc.NewSpawnRequest(proc.SpawnConfig{
		RecoveryClass: proc.RecoveryTask, Executable: executable,
		Args:  []string{"-test.run=^TestSpawnedWireHelperProcess$", "-test.v"},
		Env:   []string{"SPAWNED_WIRE_DIAGNOSTIC=" + diagnostic, "SPAWNED_WIRE_HELPER=1"},
		Stdin: proc.StdioNull, Stdout: proc.StdioNull, Stderr: proc.StdioNull,
		SpawnedSession: true, ExpectedSignature: &signature,
	})
	if err != nil {
		t.Fatal(err)
	}
	child, receipt, err := manager.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	endpoint, err := child.ClaimSpawnedSession(context.Background(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewSpawnedClient(context.Background(), SpawnedClientConfig{
		Endpoint: endpoint, WireBuild: "spawned-test-v1",
		Ladder: spawnedTestLadder(t), Limits: spawnedTestLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Abort(io.ErrUnexpectedEOF); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("spawned child did not observe abrupt parent close")
	}
}
