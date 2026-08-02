package wire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/yasyf/daemonkit/internal/trust"
)

func TestTenantIsReservedOnTheLiveCodec(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{"request", Frame{Kind: FrameRequest, Flags: FlagEnd, ID: 1, Op: "mutate", Tenant: "acct-18", Payload: []byte("{}")}},
		{"stream chunk", Frame{Kind: FrameStream, ID: 1, Sequence: 1, Tenant: "acct-18", Payload: []byte("{}")}},
	}
	for _, tt := range tests {
		t.Run(tt.name+" write", func(t *testing.T) {
			clientConn, _ := testPair(t)
			codec := NewCodec(clientConn)
			if err := codec.WriteFrame(tt.frame); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("WriteFrame() = %v, want ErrInvalidFrame", err)
			}
		})
		t.Run(tt.name+" read", func(t *testing.T) {
			clientConn, serverConn := testPair(t)
			packet, err := EncodePacket(tt.frame)
			if err != nil {
				t.Fatalf("EncodePacket() = %v: the frozen layout itself must keep carrying tenant bytes", err)
			}
			if _, err := clientConn.Write(packet); err != nil {
				t.Fatal(err)
			}
			codec := NewCodec(serverConn)
			if _, err := codec.ReadFrame(); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("ReadFrame() = %v, want ErrInvalidFrame", err)
			}
		})
	}
}

func TestAuthorizeIsRequired(t *testing.T) {
	_, err := NewClient(context.Background(), ClientConfig{
		Dial: func(context.Context) (net.Conn, error) {
			t.Fatal("Dial ran before the config was validated")
			return nil, nil
		},
		Lane: LaneControl,
	})
	if err == nil {
		t.Fatal("NewClient() with no Authorize = nil, want a refusal")
	}
}

func TestAuthorizeRefusalPreemptsAForgedDrain(t *testing.T) {
	sock := filepath.Join(testSocketDir(t), "squat")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	received := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write(drainPreamble[:])
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 64)
		n, _ := conn.Read(buf)
		received <- buf[:n]
	}()

	refusal := errors.New("accepting peer failed the deployed-identity requirement")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = NewClient(ctx, ClientConfig{
		Dial:      UnixDialer(sock),
		Authorize: func(net.Conn) error { return refusal },
		Lane:      LaneControl,
	})
	if !errors.Is(err, refusal) {
		t.Fatalf("NewClient() = %v, want the authorize refusal", err)
	}
	if errors.Is(err, ErrDraining) {
		t.Fatal("the squatter's forged preamble was believed over the authorize refusal")
	}
	if wrote := <-received; len(wrote) != 0 {
		t.Fatalf("the client wrote %#x before authorization, want nothing", wrote)
	}
}

func TestMaxFrameStaysBelowThePreambleFloor(t *testing.T) {
	floor := 0x4452 << 16
	if _, err := NewServer(newDrainableRuntime(), Config{Schemas: Schemas{"test.v1"}, MaxFrame: floor}); err == nil {
		t.Fatal("NewServer() admitted a MaxFrame under which a legal frame's length prefix opens with the drain preamble")
	}
	if _, err := NewServer(newDrainableRuntime(), Config{Schemas: Schemas{"test.v1"}, MaxFrame: floor - 1}); err != nil {
		t.Fatalf("NewServer() refused the largest sound MaxFrame: %v", err)
	}
	_, err := NewClient(context.Background(), ClientConfig{
		Dial: func(context.Context) (net.Conn, error) {
			t.Fatal("Dial ran before the config was validated")
			return nil, nil
		},
		Authorize: func(net.Conn) error { return nil },
		Lane:      LaneControl,
		MaxFrame:  floor,
	})
	if err == nil {
		t.Fatal("NewClient() admitted a MaxFrame under which a legal frame's length prefix opens with the drain preamble")
	}
}

type drainableRuntime struct {
	mu       sync.Mutex
	snapshot PhaseSnapshot
	changed  chan struct{}
	drained  chan struct{}
	once     sync.Once
}

func newDrainableRuntime() *drainableRuntime {
	return &drainableRuntime{
		snapshot: PhaseSnapshot{Sequence: 1, Phase: PhaseReady},
		changed:  make(chan struct{}),
		drained:  make(chan struct{}),
	}
}

func (r *drainableRuntime) Handle(context.Context, Request) (any, error) {
	return json.RawMessage("{}"), nil
}

func (r *drainableRuntime) Phase() PhaseSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshot
}

func (r *drainableRuntime) WaitPhase(ctx context.Context, after uint64) (PhaseSnapshot, error) {
	for {
		r.mu.Lock()
		snapshot, changed := r.snapshot, r.changed
		r.mu.Unlock()
		if snapshot.Sequence > after {
			return snapshot, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return PhaseSnapshot{}, ctx.Err()
		}
	}
}

func (r *drainableRuntime) Drain() {
	r.once.Do(func() {
		r.mu.Lock()
		r.snapshot = PhaseSnapshot{Sequence: r.snapshot.Sequence + 1, Phase: PhaseDraining}
		close(r.changed)
		r.mu.Unlock()
		close(r.drained)
	})
}

func TestInboundPreambleDrainsThroughTheTrustGate(t *testing.T) {
	rt := newDrainableRuntime()
	server := mustServer(t, rt, Config{})
	clientConn, serverConn := testPair(t)
	done := make(chan error, 1)
	go func() { done <- server.startConnection(context.Background(), serverConn) }()

	if _, err := clientConn.Write(drainPreamble[:]); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("startConnection() on a trusted preamble = %v, want an admitted drain", err)
	}
	select {
	case <-rt.drained:
	default:
		t.Fatal("the admitted preamble did not drain the runtime")
	}
	answered, err := io.ReadAll(clientConn)
	if err != nil {
		t.Fatalf("read the server's answer: %v", err)
	}
	if len(answered) != 0 {
		t.Fatalf("the draining server answered the preamble with %#x, want nothing before the close", answered)
	}
}

func TestUntrustedPreambleLeavesTheRuntimeServing(t *testing.T) {
	rt := newDrainableRuntime()
	server := mustServer(t, rt, Config{Trust: Trust{Control: &trust.Requirement{
		TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.absent",
	}}})
	clientConn, serverConn := testPair(t)
	done := make(chan error, 1)
	go func() { done <- server.startConnection(context.Background(), serverConn) }()

	if _, err := clientConn.Write(drainPreamble[:]); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, trust.ErrUntrustedPeer) {
		t.Fatalf("startConnection() on an untrusted preamble = %v, want trust.ErrUntrustedPeer", err)
	}
	select {
	case <-rt.drained:
		t.Fatal("an untrusted preamble drained the runtime")
	default:
	}
	answered, err := io.ReadAll(clientConn)
	if err != nil {
		t.Fatalf("read the server's answer: %v", err)
	}
	if len(answered) != 0 {
		t.Fatalf("the refusing server answered the preamble with %#x, want nothing", answered)
	}
}

func TestThePreambleStrandsNoDescriptor(t *testing.T) {
	tests := []struct {
		name  string
		trust Trust
		drain bool
	}{
		{"trusted", Trust{}, true},
		{"untrusted", Trust{Control: &trust.Requirement{
			TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.absent",
		}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := newDrainableRuntime()
			server := mustServer(t, rt, Config{Trust: tt.trust})
			clientConn, serverConn := testPair(t)
			canary := sendWithDescriptor(t, clientConn, drainPreamble[:])
			done := make(chan error, 1)
			go func() { done <- server.startConnection(context.Background(), serverConn) }()
			<-done
			drained := false
			select {
			case <-rt.drained:
				drained = true
			default:
			}
			if drained != tt.drain {
				t.Fatalf("drained = %t, want %t: a descriptor riding the preamble changed what the preamble means", drained, tt.drain)
			}
			assertDescriptorClosed(t, canary)
		})
	}
}

func sendWithDescriptor(t *testing.T, conn *net.UnixConn, payload []byte) *os.File {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	kept := os.NewFile(uintptr(fds[0]), "descriptor-canary")
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var sendErr error
	if err := raw.Write(func(fd uintptr) bool {
		sendErr = unix.Sendmsg(int(fd), payload, unix.UnixRights(fds[1]), nil, 0)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if sendErr != nil {
		t.Fatalf("sendmsg: %v", sendErr)
	}
	if err := unix.Close(fds[1]); err != nil {
		t.Fatalf("close the sender's copy: %v", err)
	}
	return kept
}

func assertDescriptorClosed(t *testing.T, kept *os.File) {
	t.Helper()
	canary, err := net.FileConn(kept)
	if err != nil {
		t.Fatalf("adopt the canary: %v", err)
	}
	if err := kept.Close(); err != nil {
		t.Fatal(err)
	}
	defer canary.Close()
	if err := canary.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := canary.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("canary read = %v, want io.EOF: the descriptor outlived the connection it arrived on", err)
	}
}
