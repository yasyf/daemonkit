package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func goldenFrame() Frame {
	return Frame{
		Kind:              FrameRequest,
		Flags:             FlagEnd,
		ID:                42,
		DeadlineUnixMilli: 1_700_000_000_123,
		Op:                "mutate",
		Tenant:            "acct-18",
		Payload:           []byte(`{"value":1}`),
	}
}

func goldenPacket(t *testing.T, path string) []byte {
	t.Helper()
	fixture, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var golden struct {
		Hex string `json:"hex"`
	}
	if err := json.Unmarshal(fixture, &golden); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	packet, err := hex.DecodeString(golden.Hex)
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}
	return packet
}

func frozenPrefix(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "ci", "mixedera", "testdata", "frozen", name))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	prefix, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}
	return prefix
}

func TestGoldenV2Packet(t *testing.T) {
	want := goldenPacket(t, filepath.Join("testdata", "frame-v2.json"))
	got, err := EncodePacket(goldenFrame())
	if err != nil {
		t.Fatalf("EncodePacket: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("packet = %x, want %x", got, want)
	}
	decoded, err := DecodePacket(want)
	if err != nil {
		t.Fatalf("DecodePacket: %v", err)
	}
	if !reflect.DeepEqual(decoded, goldenFrame()) {
		t.Fatalf("round trip = %#v, want %#v", decoded, goldenFrame())
	}
}

func TestGoldenV1ShapeSurvivesTheVersionBump(t *testing.T) {
	v1 := goldenPacket(t, filepath.Join("..", "..", "wire", "testdata", "frame-v1.json"))
	v2 := goldenPacket(t, filepath.Join("testdata", "frame-v2.json"))
	if len(v1) != len(v2) {
		t.Fatalf("golden lengths %d and %d differ", len(v1), len(v2))
	}
	versionAt := framePrefixSize + 4
	for i := range v2 {
		if v1[i] != v2[i] && (i < versionAt || i >= versionAt+2) {
			t.Fatalf("goldens differ at packet byte %d, outside body[4:6]", i)
		}
	}
	if binary.BigEndian.Uint16(v1[versionAt:versionAt+2]) != 1 {
		t.Fatalf("v1 golden carries version %x", v1[versionAt:versionAt+2])
	}
	if binary.BigEndian.Uint16(v2[versionAt:versionAt+2]) != 2 {
		t.Fatalf("v2 golden carries version %x", v2[versionAt:versionAt+2])
	}
	packet, err := EncodePacket(goldenFrame())
	if err != nil {
		t.Fatalf("EncodePacket: %v", err)
	}
	binary.BigEndian.PutUint16(packet[versionAt:versionAt+2], 1)
	if !bytes.Equal(packet, v1) {
		t.Fatalf("version-1-forced packet = %x, want the v1 golden %x", packet, v1)
	}
}

func TestFrozenEraPrefixes(t *testing.T) {
	body, err := EncodeFrame(goldenFrame())
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	if cut := frozenPrefix(t, "frame-prefix-cut.hex"); !bytes.Equal(body[:len(cut)], cut) {
		t.Fatalf("v2 body opens %x, want the frozen cut prefix %x", body[:len(cut)], cut)
	}
	v1 := goldenPacket(t, filepath.Join("..", "..", "wire", "testdata", "frame-v1.json"))
	precut := frozenPrefix(t, "frame-prefix-precut.hex")
	if !bytes.Equal(v1[framePrefixSize:framePrefixSize+len(precut)], precut) {
		t.Fatalf("v1 golden body opens %x, want the frozen pre-cut prefix %x",
			v1[framePrefixSize:framePrefixSize+len(precut)], precut)
	}
}

func TestFrameKindRawRange(t *testing.T) {
	if FrameHello != FrameKind(1) || FrameLifecycle != FrameKind(11) {
		t.Fatalf("kind range = %d..%d, want 1..11", FrameHello, FrameLifecycle)
	}
	if FrameKind(0).valid() || FrameKind(12).valid() {
		t.Fatal("kinds outside 1..11 must remain forbidden")
	}
}

func TestEncodeFrameRejects(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{"kind zero", Frame{Kind: 0, Flags: FlagEnd}},
		{"kind beyond lifecycle", Frame{Kind: 12, Flags: FlagEnd}},
		{"foreign flag", Frame{Kind: FrameRequest, Flags: 2, ID: 1, Op: "mutate"}},
		{"negative deadline", Frame{Kind: FrameRequest, Flags: FlagEnd, ID: 1, DeadlineUnixMilli: -1, Op: "mutate"}},
		{"operation too long", Frame{Kind: FrameRequest, Flags: FlagEnd, ID: 1, Op: Op(strings.Repeat("o", 1<<16))}},
		{"tenant too long", Frame{Kind: FrameRequest, Flags: FlagEnd, ID: 1, Op: "mutate", Tenant: strings.Repeat("t", 1<<16)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := EncodeFrame(tt.frame); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("EncodeFrame error = %v, want ErrInvalidFrame", err)
			}
		})
	}
}

func TestDecodeFrameRejects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   error
	}{
		{"short body", func(b []byte) []byte { return b[:frameHeaderSize-1] }, ErrInvalidFrame},
		{"magic", func(b []byte) []byte { b[0] = 'X'; return b }, ErrInvalidFrame},
		{"version 1", func(b []byte) []byte { b[5] = 1; return b }, ErrProtocolVersion},
		{"kind zero", func(b []byte) []byte { b[6] = 0; return b }, ErrInvalidFrame},
		{"kind beyond lifecycle", func(b []byte) []byte { b[6] = 12; return b }, ErrInvalidFrame},
		{"foreign flag", func(b []byte) []byte { b[7] = 2; return b }, ErrInvalidFrame},
		{"deadline exceeds int64", func(b []byte) []byte { b[20] = 0x80; return b }, ErrInvalidFrame},
		{"routing lengths", func(b []byte) []byte { b[28] = 0xff; return b }, ErrInvalidFrame},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := EncodeFrame(goldenFrame())
			if err != nil {
				t.Fatalf("EncodeFrame: %v", err)
			}
			if _, err := DecodeFrame(tt.mutate(body)); !errors.Is(err, tt.want) {
				t.Fatalf("DecodeFrame error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDecodePacketRejects(t *testing.T) {
	golden := goldenPacket(t, filepath.Join("testdata", "frame-v2.json"))
	oversized := make([]byte, framePrefixSize)
	binary.BigEndian.PutUint32(oversized, DefaultMaxFrame+1)
	tests := []struct {
		name   string
		packet []byte
		want   error
	}{
		{"short prefix", golden[:framePrefixSize-1], ErrFrameTruncated},
		{"truncated body", golden[:len(golden)-1], ErrFrameTruncated},
		{"trailing bytes", append(append([]byte(nil), golden...), 0), ErrInvalidFrame},
		{"oversized declared", oversized, ErrFrameTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodePacket(tt.packet); !errors.Is(err, tt.want) {
				t.Fatalf("DecodePacket error = %v, want %v", err, tt.want)
			}
		})
	}
}
