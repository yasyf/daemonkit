//go:build darwin

package wire

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestFrameSidecarReadsFromHelloByteZeroAndAdoptsOnce(t *testing.T) {
	sender, receiver := sidecarUnixPair(t)
	right, peer := sidecarUnixPair(t)
	codec := NewCodec(receiver)
	hello := Frame{Kind: FrameHello, Flags: FlagEnd, Payload: []byte(`{}`)}
	if _, err := sender.Write(sidecarFramePacket(t, hello)); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	want := Frame{
		Kind: FrameRequest, Flags: FlagEnd, ID: 1, Op: brokerHandoffOp,
		Payload: []byte(`{"nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","protocol":1,"runtime_identity":{"process_generation":"process.v1","runtime_build":"app.v1"}}`),
	}
	sendFrameSidecar(t, sender, right, sidecarFramePacket(t, want))
	if err := right.Close(); err != nil {
		t.Fatalf("close original right: %v", err)
	}
	gotHello, err := codec.ReadFrame()
	if err != nil || gotHello.Kind != FrameHello {
		t.Fatalf("read hello = %+v, %v", gotHello, err)
	}
	got, sidecar, err := codec.readFrameWithSidecar()
	if err != nil {
		t.Fatalf("read handoff: %v", err)
	}
	if got.Op != brokerHandoffOp || sidecar == nil {
		t.Fatalf("handoff = %+v, sidecar %v", got, sidecar)
	}
	raw := sidecar.(*scmRightsSidecar)
	flags, err := unix.FcntlInt(raw.file.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("received descriptor flags = %#x, %v", flags, err)
	}
	adopted, err := sidecar.takeUnixConn()
	if err != nil {
		t.Fatalf("adopt sidecar: %v", err)
	}
	t.Cleanup(func() { _ = adopted.Close() })
	if _, err := sidecar.takeUnixConn(); !errors.Is(err, errInvalidFrameSidecar) {
		t.Fatalf("second adoption = %v, want invalid sidecar", err)
	}
	if _, err := adopted.Write([]byte("adopted")); err != nil {
		t.Fatalf("write adopted connection: %v", err)
	}
	assertSidecarRead(t, peer, "adopted")
}

func TestFrameSidecarRejectsMissingMisplacedAndForeignDescriptors(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		sender, receiver := sidecarUnixPair(t)
		if _, err := sender.Write(sidecarFramePacket(t, sidecarHandoffFrame())); err != nil {
			t.Fatalf("write handoff: %v", err)
		}
		if _, _, err := NewCodec(receiver).readFrameWithSidecar(); !errors.Is(err, errInvalidFrameSidecar) {
			t.Fatalf("missing descriptor error = %v", err)
		}
	})

	t.Run("misplaced", func(t *testing.T) {
		sender, receiver := sidecarUnixPair(t)
		right, _ := sidecarUnixPair(t)
		packet := sidecarFramePacket(t, sidecarHandoffFrame())
		if _, err := sender.Write(packet[:1]); err != nil {
			t.Fatalf("write first byte: %v", err)
		}
		sendFrameSidecar(t, sender, right, packet[1:])
		if _, _, err := NewCodec(receiver).readFrameWithSidecar(); !errors.Is(err, errInvalidFrameSidecar) {
			t.Fatalf("misplaced descriptor error = %v", err)
		}
	})

	t.Run("wrong-operation", func(t *testing.T) {
		sender, receiver := sidecarUnixPair(t)
		right, _ := sidecarUnixPair(t)
		frame := Frame{Kind: FrameRequest, Flags: FlagEnd, ID: 1, Op: "ordinary"}
		sendFrameSidecar(t, sender, right, sidecarFramePacket(t, frame))
		if _, _, err := NewCodec(receiver).readFrameWithSidecar(); !errors.Is(err, errInvalidFrameSidecar) {
			t.Fatalf("foreign descriptor error = %v", err)
		}
	})
}

func sidecarHandoffFrame() Frame {
	return Frame{Kind: FrameRequest, Flags: FlagEnd, ID: 1, Op: brokerHandoffOp, Payload: []byte(`{}`)}
}

func sidecarFramePacket(t *testing.T, frame Frame) []byte {
	t.Helper()
	body, err := encodeFrame(frame)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	packet := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(body)))
	copy(packet[4:], body)
	return packet
}

func sidecarUnixPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "dk-scm-")
	if err != nil {
		t.Fatalf("make Unix socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	address := &net.UnixAddr{Name: filepath.Join(directory, "socket"), Net: "unix"}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		t.Fatalf("listen Unix: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan *net.UnixConn, 1)
	go func() {
		conn, _ := listener.AcceptUnix()
		accepted <- conn
	}()
	client, err := net.DialUnix("unix", nil, address)
	if err != nil {
		t.Fatalf("dial Unix: %v", err)
	}
	server := <-accepted
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = server.Close() })
	return client, server
}

func sendFrameSidecar(t *testing.T, carrier *net.UnixConn, right syscall.Conn, packet []byte) {
	t.Helper()
	carrierRaw, err := carrier.SyscallConn()
	if err != nil {
		t.Fatalf("carrier raw connection: %v", err)
	}
	rightRaw, err := right.SyscallConn()
	if err != nil {
		t.Fatalf("right raw connection: %v", err)
	}
	if err := rightRaw.Control(func(rightFD uintptr) {
		var sent int
		var sendErr error
		if err := carrierRaw.Write(func(carrierFD uintptr) bool {
			sent, sendErr = unix.SendmsgN(int(carrierFD), packet, unix.UnixRights(int(rightFD)), nil, 0)
			return !isWouldBlock(sendErr)
		}); err != nil {
			t.Fatalf("wait for sendmsg: %v", err)
		}
		if sendErr != nil {
			t.Fatalf("sendmsg: %v", sendErr)
		}
		if sent < len(packet) {
			if _, err := carrier.Write(packet[sent:]); err != nil {
				t.Fatalf("write frame remainder: %v", err)
			}
		}
	}); err != nil {
		t.Fatalf("pin sidecar descriptor: %v", err)
	}
}

func assertSidecarRead(t *testing.T, conn net.Conn, want string) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read adopted data: %v", err)
	}
	if string(got) != want {
		t.Fatalf("read = %q, want %q", got, want)
	}
}
