package wire_test

import (
	"context"
	"encoding/binary"
	"errors"
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
				Build: "client-test",
				Dial:  func(context.Context) (net.Conn, error) { return nil, test.dialErr },
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
			copy(body[:4], []byte("DKS2"))
			binary.BigEndian.PutUint16(body[4:6], 1)
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
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{Build: "client-test", Dial: dial}}
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

func TestLifecycleSkipsDrainRejectionButParticipatesInAdmission(t *testing.T) {
	lifecycle := &fakeLifecycle{}
	server := &wire.Server{Build: "server-test"}
	server.RegisterLifecycle(lifecycle)
	var admissions atomic.Int32
	running := startSessionServer(t, server, func() (func(), error) {
		admissions.Add(1)
		return func() {}, nil
	})
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{Dial: wire.UnixDialer(running.path), Build: "client-test"}}
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
