package wire_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/wire"
)

func TestProtectedSessionsSurviveIdleOrdinarySaturation(t *testing.T) {
	server := &wire.Server{
		Build: "server-test", MaxSessions: 3, ReservedProtectedSessions: 2,
		ProtectedSession: func(_ context.Context, peer wire.Peer) (bool, error) { return peer.PID == os.Getpid(), nil },
	}
	server.RegisterControl("native-bootstrap", func(context.Context, wire.Request) (any, error) {
		return "ready", nil
	})
	server.RegisterLifecycle(protectedTestLifecycle{})
	running := startSessionServer(t, server, admitAll(&atomic.Int32{}))
	helper := startIdleSessionHelper(t, running.path, true)
	defer helper.close(t)

	native, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(running.path), Build: "server-test",
	})
	if err != nil {
		t.Fatalf("protected native bootstrap session: %v", err)
	}
	defer native.Close()
	result, err := native.Call(t.Context(), "native-bootstrap", "", nil)
	if err != nil || result.Outcome != wire.Delivered {
		t.Fatalf("native bootstrap call = %#v, %v", result, err)
	}

	lifecycle := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(running.path), Build: "server-test",
	}}
	defer lifecycle.Close()
	health, err := lifecycle.Health(t.Context())
	if err != nil {
		t.Fatalf("protected lifecycle session: %v", err)
	}
	if health.Build != "holder-v1" {
		t.Fatalf("lifecycle health = %#v", health)
	}
}

func TestUntrustedPeerIsRejectedBeforeProtectedCapacityClassification(t *testing.T) {
	var classifications atomic.Int32
	server := &wire.Server{
		Build: "server-test", MaxSessions: 1, ReservedProtectedSessions: 1,
		Trust: func(_ context.Context, peer wire.Peer) error {
			if peer.PID != os.Getpid() {
				return wire.ErrUntrustedPeer
			}
			return nil
		},
		ProtectedSession: func(context.Context, wire.Peer) (bool, error) {
			classifications.Add(1)
			return true, nil
		},
	}
	server.RegisterControl("probe", func(context.Context, wire.Request) (any, error) { return true, nil })
	running := startSessionServer(t, server, admitAll(&atomic.Int32{}))
	helper := startIdleSessionHelper(t, running.path, false)
	helper.close(t)
	if got := classifications.Load(); got != 0 {
		t.Fatalf("untrusted peer reached protected classifier %d times", got)
	}

	client, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(running.path), Build: "server-test",
	})
	if err != nil {
		t.Fatalf("trusted protected session after rejection: %v", err)
	}
	defer client.Close()
	if got := classifications.Load(); got != 1 {
		t.Fatalf("trusted peer classifications = %d, want 1", got)
	}
}

func TestBlockingPeerVerifierIsTimedOutBeforeNextProtectedAdmission(t *testing.T) {
	entered := make(chan struct{})
	var calls atomic.Int32
	server := &wire.Server{
		Build: "server-test", MaxSessions: 2, ReservedProtectedSessions: 1,
		PeerVerificationTimeout: 25 * time.Millisecond,
		Trust: func(ctx context.Context, _ wire.Peer) error {
			if calls.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
		ProtectedSession: func(context.Context, wire.Peer) (bool, error) { return true, nil },
	}
	server.RegisterControl("probe", func(context.Context, wire.Request) (any, error) { return true, nil })
	running := startSessionServer(t, server, admitAll(&atomic.Int32{}))
	stalled, err := net.Dial("unix", running.path)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking verifier was not entered")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(running.path), Build: "server-test",
	})
	if err != nil {
		t.Fatalf("protected admission after verifier timeout: %v", err)
	}
	defer client.Close()
	if calls.Load() < 2 {
		t.Fatalf("peer verifier calls = %d, want timed-out and protected peers", calls.Load())
	}
}

func TestProtectedSessionCapacityConfigurationIsExact(t *testing.T) {
	for _, test := range []struct {
		name   string
		server *wire.Server
	}{
		{name: "negative", server: &wire.Server{Build: "test", ReservedProtectedSessions: -1}},
		{name: "negative verification timeout", server: &wire.Server{Build: "test", PeerVerificationTimeout: -1}},
		{name: "exceeds maximum", server: &wire.Server{Build: "test", MaxSessions: 1, ReservedProtectedSessions: 2, ProtectedSession: func(context.Context, wire.Peer) (bool, error) { return true, nil }}},
		{name: "missing classifier", server: &wire.Server{Build: "test", ReservedProtectedSessions: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, err := os.MkdirTemp("/tmp", "dk-wire-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(directory) })
			listener, err := net.Listen("unix", filepath.Join(directory, "wire.sock"))
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := test.server.Serve(t.Context(), listener, admitAll(&atomic.Int32{}), admitAll(&atomic.Int32{})); err == nil {
				t.Fatal("invalid protected capacity was accepted")
			}
		})
	}
}

type protectedTestLifecycle struct{}

func (protectedTestLifecycle) Health(context.Context) (daemon.Health, error) {
	return daemon.Health{Build: "holder-v1", Protocol: 1, State: daemon.StateHealthy}, nil
}

func (protectedTestLifecycle) Shutdown(context.Context) error { return nil }
func (protectedTestLifecycle) Handoff(context.Context) error  { return nil }

type idleSessionHelper struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

func startIdleSessionHelper(t *testing.T, socket string, hold bool) *idleSessionHelper {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWireIdleSessionHelper$")
	cmd.Env = append(os.Environ(), "DAEMONKIT_IDLE_SESSION_HELPER=1", "DAEMONKIT_IDLE_SESSION_SOCKET="+socket)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read idle helper status: %v", err)
	}
	line = strings.TrimSpace(line)
	if hold && line != "ready" {
		t.Fatalf("idle helper = %q, want ready", line)
	}
	if !hold && line != "rejected" {
		t.Fatalf("untrusted helper = %q, want rejected", line)
	}
	return &idleSessionHelper{cmd: cmd, stdin: stdin}
}

func (h *idleSessionHelper) close(t *testing.T) {
	t.Helper()
	if h == nil || h.cmd == nil {
		return
	}
	_ = h.stdin.Close()
	if err := h.cmd.Wait(); err != nil {
		t.Fatalf("idle helper: %v", err)
	}
	h.cmd = nil
}

func TestWireIdleSessionHelper(_ *testing.T) {
	if os.Getenv("DAEMONKIT_IDLE_SESSION_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(os.Getenv("DAEMONKIT_IDLE_SESSION_SOCKET")), Build: "server-test",
	})
	if err != nil {
		fmt.Println("rejected")
		return
	}
	defer client.Close()
	fmt.Println("ready")
	_, _ = io.Copy(io.Discard, os.Stdin)
}
