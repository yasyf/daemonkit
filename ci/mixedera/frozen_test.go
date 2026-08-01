//go:build mixedera

package mixedera

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/ci/mixedera/coverage"
)

const (
	preambleFixture = "drain-preamble"
	precutFixture   = "frame-prefix-precut"
	cutFixture      = "frame-prefix-cut"

	precutEra = coverage.PrecutEra
	cutEra    = coverage.CutEra

	mechanismFrame            = "frame-v1"
	mechanismGate             = "protocol-gate"
	mechanismSession          = "session"
	mechanismSigterm          = "drain-sigterm"
	mechanismPreamble         = "drain-preamble"
	mechanismPreambleEmitted  = "drain-preamble-emitted"
	mechanismTrustGate        = "drain-preamble-trust-gate"
	mechanismControlTrustGate = "drain-control-trust-gate"

	framePrefixOffset = 4
	framePrefixBytes  = 6
	precutProtocol    = 1
	cutProtocol       = 2

	preambleFrameBody = 1146224640

	intakePoll = 5 * time.Millisecond
)

func frameFixture(era string) string {
	switch era {
	case precutEra:
		return precutFixture
	case cutEra:
		return cutFixture
	}
	panic("no frozen frame prefix for era " + era)
}

func carriesFramePrefix(observed, want []byte) bool {
	return len(observed) >= framePrefixOffset+len(want) &&
		bytes.Equal(observed[framePrefixOffset:framePrefixOffset+len(want)], want)
}

func frozen(t *testing.T, name string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.Join(coverage.FrozenLines(name+".hex"), ""))
	if err != nil {
		t.Fatalf("frozen fixture %s: %v", name, err)
	}
	if len(decoded) == 0 {
		t.Fatalf("frozen fixture %s carries no bytes", name)
	}
	return decoded
}

func TestFrozenFramePrefixes(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		protocol uint16
	}{
		{"pre-cut", precutFixture, precutProtocol},
		{"cut", cutFixture, cutProtocol},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := frozen(t, tt.fixture)
			if len(prefix) != framePrefixBytes {
				t.Fatalf("prefix is %d bytes, want %d", len(prefix), framePrefixBytes)
			}
			if magic := string(prefix[:4]); magic != "DKS1" {
				t.Errorf("magic = %q, want %q", magic, "DKS1")
			}
			if protocol := binary.BigEndian.Uint16(prefix[4:6]); protocol != tt.protocol {
				t.Errorf("protocol = %d, want %d", protocol, tt.protocol)
			}
		})
	}
	if bytes.Equal(frozen(t, precutFixture), frozen(t, cutFixture)) {
		t.Error("the two eras carry one frame identity, so neither can answer the other with a typed mismatch")
	}
	coverage.Observe(t)
}

func TestFrozenPreambleCannotOpenAFrame(t *testing.T) {
	preamble := frozen(t, preambleFixture)
	if len(preamble) != 2 {
		t.Fatalf("preamble is %d bytes, want 2", len(preamble))
	}
	opening := binary.BigEndian.Uint16(preamble)
	if body := int64(opening) << 16; body != preambleFrameBody {
		t.Fatalf("preamble %#04x opens a frame of at least %d bytes, want %d", opening, body, preambleFrameBody)
	}
	tests := []struct {
		name     string
		maxFrame int64
	}{
		{"default 4 MiB", 4 << 20},
		{"synckit 16 MiB", 16 << 20},
		{"cc-interact and captain-hook 64 MiB", 64 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if preambleFrameBody <= tt.maxFrame {
				t.Errorf("preamble %#04x opens a %d-byte body, within a %d-byte frame cap",
					opening, preambleFrameBody, tt.maxFrame)
			}
		})
	}
	coverage.Observe(t)
}

// writePreamble opens the preamble connection through the relay rather than
// straight at the daemon, so what reached that daemon is bytes a third process
// copied instead of a flag this case set on itself.
func writePreamble(t *testing.T, front *relay) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", front.path)
	if err != nil {
		t.Fatalf("dial %s: %v", front.path, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetWriteDeadline(time.Now().Add(drainWait)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(frozen(t, preambleFixture)); err != nil {
		t.Fatalf("write the frozen preamble: %v", err)
	}
	return conn
}

// parked is one handshake held across the drain edge. A cut daemon emits the
// frozen preamble only to a peer whose hello it is still reading when the drain
// begins — it closes its listener as the shutdown ladder's first stage, so no
// connection opened after that edge reaches the wire at all — and holding back
// the hello's last byte is how the harness puts a peer in exactly that state.
type parked struct {
	conn net.Conn
	tail []byte
}

func parkHandshake(t *testing.T, front *relay) *parked {
	t.Helper()
	conn, err := net.Dial("unix", front.path)
	if err != nil {
		t.Fatalf("dial %s: %v", front.path, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(drainWait)); err != nil {
		t.Fatal(err)
	}
	hello := helloPacket(t, cutProtocol)
	if _, err := conn.Write(hello[:len(hello)-1]); err != nil {
		t.Fatalf("park a handshake at %s: %v", front.path, err)
	}
	front.awaitCrossed(t, drainWait)
	return &parked{conn: conn, tail: hello[len(hello)-1:]}
}

// answer completes the parked hello and reads every byte the daemon writes
// before it closes.
func (p *parked) answer(t *testing.T) []byte {
	t.Helper()
	if _, err := p.conn.Write(p.tail); err != nil {
		t.Fatalf("complete the parked handshake: %v", err)
	}
	answered, err := io.ReadAll(p.conn)
	if err != nil {
		t.Fatalf("read the daemon's answer to the parked handshake: %v", err)
	}
	return answered
}

// awaitIntakeClosed waits out the drain edge: the shutdown ladder closes the
// listener before any other stage, so a dial that no longer connects is what
// tells the harness a parked handshake will now be answered as draining.
func awaitIntakeClosed(t *testing.T, socket string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		conn, err := net.Dial("unix", socket)
		if err != nil {
			return
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatalf("the daemon at %s still accepted a connection %s after the drain began", socket, within)
		}
		time.Sleep(intakePoll)
	}
}

// assertPreambleAnswered redeems the preamble against bytes the relay copied:
// the daemon's whole answer to the parked handshake was the frozen preamble and
// nothing else — no frame, no rejection ack, no hang.
func assertPreambleAnswered(t *testing.T, answered []byte, crossings []exchange) {
	t.Helper()
	preamble := frozen(t, preambleFixture)
	if !bytes.Equal(answered, preamble) {
		t.Fatalf("the draining cut daemon answered the parked handshake with %#x, want exactly the frozen preamble %#x",
			answered, preamble)
	}
	if len(crossings) != 1 {
		t.Fatalf("the parked handshake crossed %d connections, want exactly 1", len(crossings))
	}
	if crossed := crossings[0].answered; !bytes.Equal(crossed, preamble) {
		t.Fatalf("the relay copied %#x as the daemon's answer, want exactly the frozen preamble %#x", crossed, preamble)
	}
	coverage.ObservedPresent(t, cutEra, mechanismPreambleEmitted, coverage.FromWire, fmt.Sprintf(
		"the relay copied exactly the frozen drain preamble %#x as the whole answer a draining cut daemon gave the handshake it was still reading when the drain began",
		preamble,
	))
}

func awaitPeerClose(t *testing.T, conn net.Conn, within time.Duration) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatal(err)
	}
	var scratch [1]byte
	switch _, err := conn.Read(scratch[:]); {
	case errors.Is(err, io.EOF):
	case err == nil:
		t.Errorf("the daemon answered the frozen preamble with %#x instead of closing on it", scratch)
	default:
		t.Errorf("the daemon still held the preamble connection %s after the settle: %v", within, err)
	}
}
