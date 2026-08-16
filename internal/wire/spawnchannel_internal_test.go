package wire

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/trust"
	"golang.org/x/sys/unix"
)

func testSelfPeer(t *testing.T) trust.Peer {
	t.Helper()
	token, err := trust.ProcessToken(os.Getpid())
	if err != nil {
		t.Fatalf("ProcessToken() = %v", err)
	}
	return trust.Peer{UID: os.Getuid(), Token: token}
}

func testMintNonce(t *testing.T) []byte {
	t.Helper()
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("mint nonce: %v", err)
	}
	return nonce
}

func TestSpawnChannelFrameRoundTripCarriesOneDescriptorAtByteZero(t *testing.T) {
	sender, receiver := testPair(t)
	carried, _ := testPair(t)
	carriedFile, err := carried.File()
	if err != nil {
		t.Fatalf("File() = %v", err)
	}
	defer carriedFile.Close()

	payload := []byte(`{"op":"adopt","nonce":"AAAA"}`)
	if err := WriteSpawnChannelFrame(sender, payload, int(carriedFile.Fd())); err != nil {
		t.Fatalf("WriteSpawnChannelFrame() = %v", err)
	}
	got, fd, err := ReadSpawnChannelFrame(receiver, true)
	if err != nil {
		t.Fatalf("ReadSpawnChannelFrame() = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if fd < 0 {
		t.Fatal("no descriptor arrived with the frame")
	}
	defer func() { _ = unix.Close(fd) }()
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("received descriptor flags = %d (%v), want CLOEXEC", flags, err)
	}

	if err := WriteSpawnChannelFrame(sender, payload, -1); err != nil {
		t.Fatalf("WriteSpawnChannelFrame() = %v", err)
	}
	got, fd, err = ReadSpawnChannelFrame(receiver, true)
	if err != nil || fd != -1 {
		t.Fatalf("bare frame = (%q, %d, %v), want no descriptor", got, fd, err)
	}
}

func TestSpawnChannelFrameRefusesADescriptorWhereNoneIsAllowed(t *testing.T) {
	sender, receiver := testPair(t)
	carried, _ := testPair(t)
	carriedFile, err := carried.File()
	if err != nil {
		t.Fatalf("File() = %v", err)
	}
	defer carriedFile.Close()
	if err := WriteSpawnChannelFrame(sender, []byte(`{}`), int(carriedFile.Fd())); err != nil {
		t.Fatalf("WriteSpawnChannelFrame() = %v", err)
	}
	if _, _, err := ReadSpawnChannelFrame(receiver, false); err == nil {
		t.Fatal("a descriptor outside the rights-bearing verb was admitted")
	}
}

func TestSpawnChannelFrameRefusesNonStreamDescriptors(t *testing.T) {
	tests := []struct {
		name string
		fd   func(t *testing.T) int
	}{
		{"pipe", func(t *testing.T) int {
			t.Helper()
			var fds [2]int
			if err := unix.Pipe(fds[:]); err != nil {
				t.Fatalf("pipe: %v", err)
			}
			t.Cleanup(func() {
				_ = unix.Close(fds[0])
				_ = unix.Close(fds[1])
			})
			return fds[0]
		}},
		{"regular file", func(t *testing.T) int {
			t.Helper()
			file, err := os.CreateTemp(t.TempDir(), "plain")
			if err != nil {
				t.Fatalf("create file: %v", err)
			}
			t.Cleanup(func() { _ = file.Close() })
			return int(file.Fd())
		}},
		{"listening socket", func(t *testing.T) int {
			t.Helper()
			listener, err := net.Listen("unix", testSocketDir(t)+"/l")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			file, err := listener.(*net.UnixListener).File()
			if err != nil {
				t.Fatalf("File() = %v", err)
			}
			t.Cleanup(func() { _ = file.Close() })
			return int(file.Fd())
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, receiver := testPair(t)
			if err := WriteSpawnChannelFrame(sender, []byte(`{}`), tt.fd(t)); err != nil {
				t.Fatalf("WriteSpawnChannelFrame() = %v", err)
			}
			if _, _, err := ReadSpawnChannelFrame(receiver, true); err == nil {
				t.Fatal("a non-stream descriptor was admitted through the spawn channel")
			}
		})
	}
}

func TestSpawnChannelFrameBoundsThePayload(t *testing.T) {
	sender, receiver := testPair(t)
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], SpawnChannelMaxPayload+1)
	if _, err := sender.Write(prefix[:]); err != nil {
		t.Fatalf("write oversize prefix: %v", err)
	}
	if _, _, err := ReadSpawnChannelFrame(receiver, true); !errors.Is(err, ErrSpawnChannelFrame) {
		t.Fatalf("oversize frame error = %v, want ErrSpawnChannelFrame", err)
	}
	if err := WriteSpawnChannelFrame(sender, bytes.Repeat([]byte("x"), SpawnChannelMaxPayload+1), -1); !errors.Is(err, ErrSpawnChannelFrame) {
		t.Fatalf("oversize write error = %v, want ErrSpawnChannelFrame", err)
	}
}

func TestSpawnChannelFrameReportsACleanCloseAsEOF(t *testing.T) {
	sender, receiver := testPair(t)
	_ = sender.Close()
	if _, _, err := ReadSpawnChannelFrame(receiver, true); !errors.Is(err, io.EOF) {
		t.Fatalf("closed channel error = %v, want io.EOF", err)
	}
}

func TestDecodeSpawnChannelRequestValidatesShape(t *testing.T) {
	nonce := base64.StdEncoding.EncodeToString(make([]byte, 32))
	tests := []struct {
		name    string
		payload string
		wantOp  string
		wantErr bool
	}{
		{"mint", `{"op":"mint","nonce":"` + nonce + `"}`, SpawnChannelOpMint, false},
		{"adopt", `{"op":"adopt","nonce":"` + nonce + `"}`, SpawnChannelOpAdopt, false},
		{"unknown op", `{"op":"drain","nonce":"` + nonce + `"}`, "", true},
		{"short nonce", `{"op":"mint","nonce":"AAAA"}`, "", true},
		{"missing nonce", `{"op":"mint"}`, "", true},
		{"unknown field", `{"op":"mint","nonce":"` + nonce + `","extra":1}`, "", true},
		{"trailing value", `{"op":"mint","nonce":"` + nonce + `"}{}`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := DecodeSpawnChannelRequest([]byte(tt.payload))
			if tt.wantErr {
				if !errors.Is(err, ErrSpawnChannelFrame) {
					t.Fatalf("DecodeSpawnChannelRequest() = %v, want ErrSpawnChannelFrame", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeSpawnChannelRequest() = %v", err)
			}
			if request.Op != tt.wantOp || len(request.Nonce) != 32 {
				t.Fatalf("request = %+v, want op %q with a 32-byte nonce", request, tt.wantOp)
			}
		})
	}
}

func TestAdoptMintedAdmitsTheBusinessHelloEchoingTheNonce(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
	startServing(t, server)
	clientEnd, serverEnd := testPair(t)
	nonce := testMintNonce(t)

	adopted := make(chan error, 1)
	go func() { adopted <- server.AdoptMinted(serverEnd, testSelfPeer(t), nonce) }()

	client, err := NewClient(t.Context(), ClientConfig{
		Dial:      func(context.Context) (net.Conn, error) { return clientEnd, nil },
		Authorize: authorizeTestServer,
		Lane:      LaneBusiness,
		Schema:    "test.v1",
		Nonce:     nonce,
	})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = client.Close(closeCtx)
	}()
	if err := <-adopted; err != nil {
		t.Fatalf("AdoptMinted() = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := client.Call(ctx, "echo", []byte(`{}`))
	if err != nil {
		t.Fatalf("Call() = %v", err)
	}
	if result.Outcome != Delivered || result.Response.Rejected {
		t.Fatalf("Call() = %+v, want a delivered response", result)
	}
}

func TestAdoptMintedRefusesEveryOffContractHello(t *testing.T) {
	tests := []struct {
		name  string
		hello helloIdentity
		want  string
	}{
		{
			name:  "wrong nonce",
			hello: helloIdentity{Protocol: ProtocolVersion, Lane: LaneBusiness, Schema: "test.v1", Nonce: make([]byte, 32)},
			want:  "nonce",
		},
		{
			name:  "absent nonce",
			hello: helloIdentity{Protocol: ProtocolVersion, Lane: LaneBusiness, Schema: "test.v1"},
			want:  "nonce",
		},
		{
			name:  "control lane",
			hello: helloIdentity{Protocol: ProtocolVersion, Lane: LaneControl},
			want:  "business lane",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
			startServing(t, server)
			clientEnd, serverEnd := testPair(t)
			nonce := testMintNonce(t)
			adopted := make(chan error, 1)
			go func() { adopted <- server.AdoptMinted(serverEnd, testSelfPeer(t), nonce) }()

			codec := NewCodec(clientEnd)
			_, err := clientHandshake(codec, tt.hello)
			if err == nil {
				t.Fatal("an off-contract hello was admitted")
			}
			adoptErr := <-adopted
			if adoptErr == nil || !strings.Contains(adoptErr.Error(), tt.want) {
				t.Fatalf("AdoptMinted() = %v, want a refusal naming %q", adoptErr, tt.want)
			}
		})
	}
}

func TestAdoptMintedRefusesTheDrainPreambleOutright(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
	startServing(t, server)
	clientEnd, serverEnd := testPair(t)
	adopted := make(chan error, 1)
	go func() { adopted <- server.AdoptMinted(serverEnd, testSelfPeer(t), testMintNonce(t)) }()
	if _, err := clientEnd.Write(drainPreamble[:]); err != nil {
		t.Fatalf("write preamble: %v", err)
	}
	err := <-adopted
	if err == nil || !strings.Contains(err.Error(), "drain preamble") {
		t.Fatalf("AdoptMinted() = %v, want the preamble refused", err)
	}
	if server.rt.Phase().Phase == PhaseDraining {
		t.Fatal("a minted peer's preamble drained the runtime")
	}
}

func TestAdoptMintedRefusesAShortNonceBeforeReading(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
	startServing(t, server)
	_, serverEnd := testPair(t)
	if err := server.AdoptMinted(serverEnd, testSelfPeer(t), []byte("short")); err == nil {
		t.Fatal("a short mint nonce was admitted")
	}
}
