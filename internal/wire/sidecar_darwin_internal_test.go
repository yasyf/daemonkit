//go:build darwin

package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func sendPacketWithRights(t *testing.T, conn *net.UnixConn, packet []byte, fd int) {
	t.Helper()
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatalf("syscall conn: %v", err)
	}
	rights := unix.UnixRights(fd)
	var sendErr error
	if err := raw.Control(func(cfd uintptr) {
		sendErr = unix.Sendmsg(int(cfd), packet, rights, nil, 0)
	}); err != nil {
		t.Fatalf("control: %v", err)
	}
	if sendErr != nil {
		t.Fatalf("sendmsg: %v", sendErr)
	}
}

func handoffSocketpair(t *testing.T) (passed int, kept *os.File) {
	t.Helper()
	syscall.ForkLock.Lock()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err == nil {
		for _, fd := range fds {
			_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC)
		}
	}
	syscall.ForkLock.Unlock()
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	kept = os.NewFile(uintptr(fds[1]), "kept-handoff-end")
	t.Cleanup(func() { _ = kept.Close() })
	t.Cleanup(func() { _ = unix.Close(fds[0]) })
	return fds[0], kept
}

func handoffPacket(t *testing.T, id uint64, nonce []byte) []byte {
	t.Helper()
	payload, err := json.Marshal(brokerHandoffEnvelope{Nonce: nonce})
	if err != nil {
		t.Fatalf("marshal handoff envelope: %v", err)
	}
	packet, err := EncodePacket(Frame{Kind: FrameRequest, Flags: FlagEnd, ID: id, Op: brokerHandoffOp, Payload: payload})
	if err != nil {
		t.Fatalf("encode handoff packet: %v", err)
	}
	return packet
}

func TestHandoffFrameArrivesIntactAfterNonDrainingPeek(t *testing.T) {
	clientConn, serverConn := testPair(t)
	passed, kept := handoffSocketpair(t)
	nonce := make([]byte, brokerHandoffNonceBytes)
	nonce[0] = 7
	sendPacketWithRights(t, clientConn, handoffPacket(t, 1, nonce), passed)

	codec := NewCodec(serverConn)
	drain, err := codec.PeekPreamble()
	if err != nil {
		t.Fatalf("PeekPreamble() = %v", err)
	}
	if drain {
		t.Fatal("PeekPreamble() reported drain on a handoff frame")
	}
	frame, sidecar, err := codec.readFrameWithSidecar()
	if err != nil {
		t.Fatalf("readFrameWithSidecar() after peek = %v", err)
	}
	if frame.Op != brokerHandoffOp || frame.ID != 1 {
		t.Fatalf("frame after peek = %+v", frame)
	}
	var envelope brokerHandoffEnvelope
	if err := decodeStrict(frame.Payload, &envelope); err != nil {
		t.Fatalf("decode payload after peek: %v", err)
	}
	if !bytes.Equal(envelope.Nonce, nonce) {
		t.Fatalf("nonce after peek = %x, want %x", envelope.Nonce, nonce)
	}
	if sidecar == nil {
		t.Fatal("sidecar dropped by the peek")
	}
	adopted, err := sidecar.takeUnixConn()
	if err != nil {
		t.Fatalf("takeUnixConn() = %v", err)
	}
	defer adopted.Close()
	if _, err := adopted.Write([]byte("alive")); err != nil {
		t.Fatalf("write through adopted descriptor: %v", err)
	}
	buf := make([]byte, 5)
	if _, err := kept.Read(buf); err != nil || string(buf) != "alive" {
		t.Fatalf("kept end read = %q, %v", buf, err)
	}
}

func readControlResponse(t *testing.T, codec *Codec, id uint64, generation []byte) Response {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("no response before deadline")
		}
		frame, err := codec.ReadFrame()
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if frame.Kind != FrameResponse || frame.ID != id {
			continue
		}
		var response Response
		if err := decodeStrict(frame.Payload, &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Ack {
			if err := codec.WriteFrame(Frame{Kind: FrameAck, Flags: FlagEnd, ID: id, Payload: generation}); err != nil {
				t.Fatalf("write ack: %v", err)
			}
		}
		return response
	}
}

func TestBrokerHandoffAdoptionAndNonceReplayRejection(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{Write: 2 * time.Second})
	sock := startServing(t, server)

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	codec := NewCodec(conn)
	identity, err := clientHandshake(codec, helloIdentity{Protocol: ProtocolVersion, Lane: LaneControl})
	if err != nil {
		t.Fatalf("control handshake: %v", err)
	}
	codec.ReadTimeout = 5 * time.Second

	nonce := make([]byte, brokerHandoffNonceBytes)
	nonce[0] = 9
	passed, kept := handoffSocketpair(t)
	adoptedDone := make(chan error, 1)
	go func() {
		keptConn, err := net.FileConn(kept)
		if err != nil {
			adoptedDone <- err
			return
		}
		defer keptConn.Close()
		adoptedCodec := NewCodec(keptConn)
		_, err = clientHandshake(adoptedCodec, helloIdentity{Protocol: ProtocolVersion, Lane: LaneBusiness, Schema: "test.v1"})
		adoptedDone <- err
	}()
	sendPacketWithRights(t, conn.(*net.UnixConn), handoffPacket(t, 1, nonce), passed)
	response := readControlResponse(t, codec, 1, identity.Session)
	if response.Rejected || response.Err != "" {
		t.Fatalf("handoff response = %+v, want success", response)
	}
	if err := <-adoptedDone; err != nil {
		t.Fatalf("adopted descriptor handshake = %v, want admission as a fresh peer", err)
	}

	replayPassed, _ := handoffSocketpair(t)
	sendPacketWithRights(t, conn.(*net.UnixConn), handoffPacket(t, 2, nonce), replayPassed)
	replay := readControlResponse(t, codec, 2, identity.Session)
	if !replay.Rejected || replay.Code != ResponseCodeHandoffReplay {
		t.Fatalf("replayed nonce response = %+v, want rejection code %q", replay, ResponseCodeHandoffReplay)
	}
	if cause := responseCodeCause(replay.Code); !errors.Is(cause, ErrHandoffReplay) {
		t.Fatalf("replay cause = %v, want ErrHandoffReplay", cause)
	}
}

func TestBusinessLaneCannotInvokeBrokerHandoff(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{Write: 2 * time.Second})
	sock := startServing(t, server)

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	codec := NewCodec(conn)
	identity, err := clientHandshake(codec, helloIdentity{Protocol: ProtocolVersion, Lane: LaneBusiness, Schema: "test.v1"})
	if err != nil {
		t.Fatalf("business handshake: %v", err)
	}
	codec.ReadTimeout = 5 * time.Second

	nonce := make([]byte, brokerHandoffNonceBytes)
	passed, _ := handoffSocketpair(t)
	sendPacketWithRights(t, conn.(*net.UnixConn), handoffPacket(t, 1, nonce), passed)
	response := readControlResponse(t, codec, 1, identity.Session)
	if !response.Rejected || response.Code != ResponseCodePermissionDenied {
		t.Fatalf("business handoff response = %+v, want permission_denied rejection", response)
	}
}
