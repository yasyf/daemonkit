package daemonkit

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/paths"
)

func foreignEraDaemon(t *testing.T, label Label, protocol uint16) {
	t.Helper()
	socket, err := paths.Socket(string(label))
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatalf("mkdir socket dir: %v", err)
	}
	listener, err := net.Listen("unix", socket)
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
		var prefix [4]byte
		if _, err := io.ReadFull(conn, prefix[:]); err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint32(prefix[:]))); err != nil {
			return
		}
		packet, err := wire.EncodePacket(wire.Frame{Kind: wire.FrameHelloAck, Flags: wire.FlagEnd, Payload: []byte(`{}`)})
		if err != nil {
			return
		}
		binary.BigEndian.PutUint16(packet[8:10], protocol)
		_, _ = conn.Write(packet)
	}()
}

func TestHealthNamesAForeignEraDaemon(t *testing.T) {
	for _, tt := range []struct {
		name     string
		protocol uint16
		newer    bool
	}{
		{"a newer era", wire.ProtocolVersion + 1, true},
		{"an older era", wire.ProtocolVersion - 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			shortHome(t)
			d := Daemon{Label: "dkforeign", Program: testProgram, Schemas: []Schema{"test.v1"}}
			foreignEraDaemon(t, d.Label, tt.protocol)
			client, err := Open(d)
			if err != nil {
				t.Fatalf("Open() = %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err = client.Health(ctx)
			var foreign *ForeignProtocolError
			if !errors.As(err, &foreign) || !errors.Is(err, ErrProtocolMismatch) {
				t.Fatalf("Health() = %v, want a ForeignProtocolError that is ErrProtocolMismatch", err)
			}
			if foreign.Protocol != tt.protocol || foreign.PID != os.Getpid() || foreign.Newer() != tt.newer {
				t.Fatalf("ForeignProtocolError = {Protocol: %d, PID: %d, Newer: %t}, want {%d, %d, %t}",
					foreign.Protocol, foreign.PID, foreign.Newer(), tt.protocol, os.Getpid(), tt.newer)
			}
		})
	}
}
