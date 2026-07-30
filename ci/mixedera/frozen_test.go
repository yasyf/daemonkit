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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	preambleFixture  = "drain-preamble"
	precutFixture    = "frame-prefix-precut"
	cutFixture       = "frame-prefix-cut"
	mechanismFixture = "mechanisms.txt"

	mechanismFrame     = "frame-v1"
	mechanismGate      = "protocol-gate"
	mechanismSession   = "session"
	mechanismSigterm   = "drain-sigterm"
	mechanismPreamble  = "drain-preamble"
	mechanismTrustGate = "drain-preamble-trust-gate"

	framePrefixOffset = 4
	framePrefixBytes  = 6
	precutProtocol    = 1
	cutProtocol       = 2

	preambleFrameBody = 1146224640
)

func readFrozen(name string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join("testdata", "frozen", name))
	if err != nil {
		return nil, err
	}
	var lines []string
	for line := range strings.Lines(string(raw)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("frozen fixture %s carries nothing", name)
	}
	return lines, nil
}

func frozenLines(t *testing.T, name string) []string {
	t.Helper()
	lines, err := readFrozen(name)
	if err != nil {
		t.Fatal(err)
	}
	return lines
}

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
	decoded, err := hex.DecodeString(strings.Join(frozenLines(t, name+".hex"), ""))
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
	observe(t)
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
	observe(t)
}

func writePreamble(t *testing.T, d *daemonProc) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", d.socket)
	if err != nil {
		t.Fatalf("dial %s: %v", d.socket, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetWriteDeadline(time.Now().Add(drainWait)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(frozen(t, preambleFixture)); err != nil {
		t.Fatalf("write the frozen preamble: %v", err)
	}
	d.preambled = true
	return conn
}

// assertPreambleCrossed redeems nothing on its own: it establishes that the
// bytes the relay copied were exactly the frozen preamble, so a later clean
// exit of d is evidence the preamble drained it.
func assertPreambleCrossed(t *testing.T, d *daemonProc, crossings []exchange) {
	t.Helper()
	if len(crossings) != 1 {
		t.Fatalf("the drain crossed %d connections, want exactly 1", len(crossings))
	}
	preamble := frozen(t, preambleFixture)
	if opening := crossings[0].opened; !bytes.Equal(opening, preamble) {
		t.Fatalf("the drain opened with %#x, want exactly the frozen preamble %#x", opening, preamble)
	}
	d.preambled = true
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
