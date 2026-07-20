package wire_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/wire"
)

func TestLifecyclePeerMapsOnlyProvenAbsentListener(t *testing.T) {
	tests := []struct {
		name       string
		dialErr    error
		wantNoPeer bool
	}{
		{name: "missing path", dialErr: &net.OpError{Op: "dial", Net: "unix", Err: &os.PathError{Op: "connect", Path: "/missing", Err: syscall.ENOENT}}, wantNoPeer: true},
		{name: "refused listener", dialErr: &net.OpError{Op: "dial", Net: "unix", Err: syscall.ECONNREFUSED}, wantNoPeer: true},
		{name: "timeout", dialErr: context.DeadlineExceeded},
		{name: "permission", dialErr: syscall.EACCES},
		{name: "cancellation", dialErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
				Build: "client-test", LifecycleBuild: "v2.0.0",
				Dial: func(context.Context) (net.Conn, error) { return nil, test.dialErr },
			}}
			_, err := peer.Health(context.Background())
			if errors.Is(err, daemon.ErrNoPeer) != test.wantNoPeer {
				t.Fatalf("Health error = %v, ErrNoPeer=%v", err, errors.Is(err, daemon.ErrNoPeer))
			}
		})
	}
}

func TestLifecyclePeerDoesNotMapProtocolFailureToNoPeer(t *testing.T) {
	dial := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			if _, err := wire.NewCodec(server).ReadFrame(); err != nil {
				return
			}
			payload := []byte(`{"protocol":1,"build":"old"}`)
			body := make([]byte, 32+len(payload))
			copy(body[:4], []byte("DKS4"))
			binary.BigEndian.PutUint16(body[4:6], wire.ProtocolVersion)
			body[6] = byte(wire.FrameHelloAck)
			body[7] = byte(wire.FlagEnd)
			copy(body[32:], payload)
			packet := make([]byte, 4+len(body))
			binary.BigEndian.PutUint32(packet[:4], uint32(len(body)))
			copy(packet[4:], body)
			_, _ = server.Write(packet)
		}()
		return client, nil
	}
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{Build: "client-test", LifecycleBuild: "v2.0.0", Dial: dial}}
	_, err := peer.Health(context.Background())
	if !errors.Is(err, wire.ErrProtocolVersion) {
		t.Fatalf("Health error = %v, want ErrProtocolVersion", err)
	}
	if errors.Is(err, daemon.ErrNoPeer) {
		t.Fatalf("protocol error mapped to ErrNoPeer: %v", err)
	}
}

type fakeLifecycle struct {
	shutdowns atomic.Int32
	handoffs  atomic.Int32
}

func (f *fakeLifecycle) Health(context.Context) (daemon.Health, error) {
	return daemon.Health{Build: "server-test", Protocol: int(wire.ProtocolVersion), PID: os.Getpid(), State: daemon.StateHealthy}, nil
}

func (f *fakeLifecycle) Shutdown(context.Context) error {
	f.shutdowns.Add(1)
	return nil
}

func (f *fakeLifecycle) Handoff(context.Context) error {
	f.handoffs.Add(1)
	return nil
}

type armedWriteFailureConn struct {
	net.Conn
	armed           atomic.Bool
	failReads       atomic.Bool
	closeAfterWrite bool
}

func (c *armedWriteFailureConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.failReads.Load() {
		return 0, errors.New("injected post-send read failure")
	}
	return n, err
}

func (c *armedWriteFailureConn) Write(p []byte) (int, error) {
	if !c.armed.CompareAndSwap(true, false) {
		return c.Conn.Write(p)
	}
	if !c.closeAfterWrite {
		return 0, errors.New("injected pre-send failure")
	}
	c.failReads.Store(true)
	written := 0
	for written < len(p) {
		n, err := c.Conn.Write(p[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func TestLifecyclePeerReopensOnlyForProvenPreSendFailure(t *testing.T) {
	lifecycle := &fakeLifecycle{}
	server := &wire.Server{Build: "server-test", LifecycleBuild: "v1.0.0"}
	protectAllLifecycleSessions(server)
	server.RegisterLifecycle(lifecycle)
	running := startSessionServer(t, server, func() (func(), error) { return func() {}, nil })

	var dials atomic.Int32
	var first *armedWriteFailureConn
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Build: "client-test", LifecycleBuild: "v2.0.0",
		Dial: func(ctx context.Context) (net.Conn, error) {
			conn, err := wire.UnixDialer(running.path)(ctx)
			if err != nil {
				return nil, err
			}
			if dials.Add(1) == 1 {
				first = &armedWriteFailureConn{Conn: conn}
				return first, nil
			}
			return conn, nil
		},
	}}
	t.Cleanup(func() { _ = peer.Close() })
	if _, err := peer.Health(t.Context()); err != nil {
		t.Fatalf("Health: %v", err)
	}
	first.armed.Store(true)
	if err := peer.Handoff(t.Context()); err != nil {
		t.Fatalf("Handoff after proven pre-send failure: %v", err)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dial count = %d, want 2", got)
	}
	if got := lifecycle.handoffs.Load(); got != 1 {
		t.Fatalf("handoff count = %d, want 1", got)
	}
}

func TestLifecyclePeerDoesNotReplayPostSendHandoff(t *testing.T) {
	lifecycle := &fakeLifecycle{}
	server := &wire.Server{Build: "server-test", LifecycleBuild: "v1.0.0"}
	protectAllLifecycleSessions(server)
	server.RegisterLifecycle(lifecycle)
	running := startSessionServer(t, server, func() (func(), error) { return func() {}, nil })

	var dials atomic.Int32
	var first *armedWriteFailureConn
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Build: "client-test", LifecycleBuild: "v2.0.0",
		Dial: func(ctx context.Context) (net.Conn, error) {
			conn, err := wire.UnixDialer(running.path)(ctx)
			if err != nil {
				return nil, err
			}
			dials.Add(1)
			first = &armedWriteFailureConn{Conn: conn, closeAfterWrite: true}
			return first, nil
		},
	}}
	t.Cleanup(func() { _ = peer.Close() })
	if _, err := peer.Health(t.Context()); err != nil {
		t.Fatalf("Health: %v", err)
	}
	first.armed.Store(true)
	if err := peer.Handoff(t.Context()); err == nil {
		t.Fatal("post-send Handoff unexpectedly succeeded")
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("post-send handoff redialed %d times, want 1", got)
	}
	if got := lifecycle.handoffs.Load(); got > 1 {
		t.Fatalf("post-send handoff replayed: count = %d", got)
	}
}

func TestLifecycleSkipsDrainRejectionButParticipatesInAdmission(t *testing.T) {
	lifecycle := &fakeLifecycle{}
	server := &wire.Server{Build: "server-test", LifecycleBuild: "v1.0.0"}
	protectAllLifecycleSessions(server)
	server.RegisterLifecycle(lifecycle)
	var admissions atomic.Int32
	running := startSessionServer(t, server, func() (func(), error) {
		admissions.Add(1)
		return func() {}, nil
	})
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(running.path), Build: "client-test", LifecycleBuild: "v2.0.0",
	}}
	t.Cleanup(func() { _ = peer.Close() })
	health, err := peer.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if health.Protocol != int(wire.ProtocolVersion) || health.Build != "server-test" {
		t.Fatalf("Health = %#v", health)
	}
	if err := server.CloseIntake(); err != nil {
		t.Fatalf("CloseIntake: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := peer.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if lifecycle.shutdowns.Load() != 1 || admissions.Load() != 2 {
		t.Fatalf("shutdowns=%d admissions=%d", lifecycle.shutdowns.Load(), admissions.Load())
	}
}
