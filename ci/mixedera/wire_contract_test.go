//go:build mixedera

package mixedera

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/ci/mixedera/coverage"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

const contractSchema = "daemonkit.mixedera.contract.v1"

// TestFrozenFixturesMatchRealWire pins the frozen bytes the matrix redeems
// against to internal/wire's exported behaviour, from outside the package: the
// cut frame prefix is what a real packet opens with, the drain preamble is what
// a real draining server emits, and an old-era protocol crosses the real
// handshake in both directions as a bounded typed mismatch.
func TestFrozenFixturesMatchRealWire(t *testing.T) {
	packet, err := wire.EncodePacket(wire.Frame{Kind: wire.FrameRequest, Flags: wire.FlagEnd, ID: 1, Op: "x", Payload: []byte("{}")})
	if err != nil {
		t.Fatalf("EncodePacket: %v", err)
	}
	if got := packet[framePrefixOffset : framePrefixOffset+framePrefixBytes]; !bytes.Equal(got, frozen(t, cutFixture)) {
		t.Errorf("a real wire packet body opens %#x, want the frozen cut prefix %#x", got, frozen(t, cutFixture))
	}

	t.Run("a draining server emits exactly the frozen preamble", func(t *testing.T) {
		sock := contractServer(t, true)
		conn, err := net.Dial("unix", sock)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(refuseBound)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(helloPacket(t, wire.ProtocolVersion)); err != nil {
			t.Fatalf("write hello: %v", err)
		}
		head := make([]byte, 2)
		if _, err := io.ReadFull(conn, head); err != nil {
			t.Fatalf("read preamble: %v", err)
		}
		if !bytes.Equal(head, frozen(t, preambleFixture)) {
			t.Errorf("the draining server emitted %#x, want the frozen preamble %#x", head, frozen(t, preambleFixture))
		}
	})

	t.Run("the server types an old-era hello as a mismatch without hanging", func(t *testing.T) {
		sock := contractServer(t, false)
		conn, err := net.Dial("unix", sock)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(refuseBound)); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		if _, err := conn.Write(helloPacket(t, 1)); err != nil {
			t.Fatalf("write hello: %v", err)
		}
		ack := readContractAck(t, conn)
		if elapsed := time.Since(started); elapsed >= refuseBound {
			t.Errorf("the refusal took %s, at or over the %s bound: a hang, not a typed answer", elapsed, refuseBound)
		}
		if !ack.Rejected {
			t.Fatalf("server ack = %+v, want a rejection", ack)
		}
		want := (&wire.ProtocolMismatchError{Theirs: 1, Ours: wire.ProtocolVersion}).Error()
		if ack.Reason != want {
			t.Errorf("rejection reason = %q, want the typed mismatch %q", ack.Reason, want)
		}
	})

	t.Run("the client types an old-era ack as a mismatch without hanging", func(t *testing.T) {
		sock := oldEraAckServer(t, 1)
		ctx, cancel := context.WithTimeout(context.Background(), refuseBound)
		defer cancel()
		_, err := wire.NewClient(ctx, wire.ClientConfig{
			Dial: wire.UnixDialer(sock), Lane: wire.LaneBusiness, Schema: contractSchema,
		})
		var mismatch *wire.ProtocolMismatchError
		if !errors.As(err, &mismatch) || mismatch.Theirs != 1 || mismatch.Ours != wire.ProtocolVersion {
			t.Fatalf("NewClient() = %v, want ProtocolMismatchError{1, %d}", err, wire.ProtocolVersion)
		}
		if ctx.Err() != nil {
			t.Error("the mismatch consumed the whole deadline: a hang, not a typed answer")
		}
	})
	coverage.Observe(t)
}

func contractServer(t *testing.T, draining bool) string {
	t.Helper()
	runtime := wiretest.NewStubRuntime()
	if draining {
		runtime.Drain()
	}
	server, err := wire.NewServer(runtime, wire.Config{Schemas: wire.Schemas{contractSchema}})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sock := filepath.Join(socketDir(t), "contract.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	return sock
}

// oldEraAckServer answers one hello with a current-frame ack advertising an
// old-era protocol in its payload, so a real wire client decodes the frame and
// then types the version as a mismatch.
func oldEraAckServer(t *testing.T, advertised uint16) string {
	t.Helper()
	sock := filepath.Join(socketDir(t), "oldera.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(refuseBound))
		if !discardPacket(conn) {
			return
		}
		ack := []byte(fmt.Sprintf(`{"protocol":%d,"schema":%q,"phase":"runtime_ready"}`, advertised, contractSchema))
		packet, err := wire.EncodePacket(wire.Frame{Kind: wire.FrameHelloAck, Flags: wire.FlagEnd, Payload: ack})
		if err != nil {
			return
		}
		_, _ = conn.Write(packet)
	}()
	return sock
}

func helloPacket(t *testing.T, advertised uint16) []byte {
	t.Helper()
	payload := []byte(fmt.Sprintf(`{"protocol":%d,"lane":"business","schema":%q}`, advertised, contractSchema))
	packet, err := wire.EncodePacket(wire.Frame{Kind: wire.FrameHello, Flags: wire.FlagEnd, Payload: payload})
	if err != nil {
		t.Fatalf("encode hello: %v", err)
	}
	return packet
}

type contractAck struct {
	Protocol uint16 `json:"protocol"`
	Schema   string `json:"schema"`
	Phase    string `json:"phase"`
	Rejected bool   `json:"rejected"`
	Code     string `json:"code"`
	Reason   string `json:"reason"`
	Session  []byte `json:"session"`
}

func readContractAck(t *testing.T, conn net.Conn) contractAck {
	t.Helper()
	body := readPacketBody(t, conn)
	frame, err := wire.DecodeFrame(body)
	if err != nil {
		t.Fatalf("decode ack frame: %v", err)
	}
	if frame.Kind != wire.FrameHelloAck {
		t.Fatalf("ack kind = %d, want FrameHelloAck", frame.Kind)
	}
	var ack contractAck
	if err := json.Unmarshal(frame.Payload, &ack); err != nil {
		t.Fatalf("unmarshal ack: %v", err)
	}
	return ack
}

func readPacketBody(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	var length [4]byte
	if _, err := io.ReadFull(conn, length[:]); err != nil {
		t.Fatalf("read length prefix: %v", err)
	}
	body := make([]byte, binary.BigEndian.Uint32(length[:]))
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return body
}

func discardPacket(conn net.Conn) bool {
	var length [4]byte
	if _, err := io.ReadFull(conn, length[:]); err != nil {
		return false
	}
	_, err := io.ReadFull(conn, make([]byte, binary.BigEndian.Uint32(length[:])))
	return err == nil
}
