package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

func TestFrameV3CrossLanguageGolden(t *testing.T) {
	fixture, err := os.ReadFile("testdata/frame-v3.json")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var golden struct {
		Hex string `json:"hex"`
	}
	if err := json.Unmarshal(fixture, &golden); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want, err := hex.DecodeString(golden.Hex)
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}
	body, err := encodeFrame(Frame{
		Kind:              FrameRequest,
		Flags:             FlagEnd,
		ID:                42,
		Sequence:          0,
		DeadlineUnixMilli: 1_700_000_000_123,
		Op:                "mutate",
		Tenant:            "acct-18",
		Payload:           []byte(`{"value":1}`),
	})
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}
	got := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(got[:4], uint32(len(body)))
	copy(got[4:], body)
	if !bytes.Equal(got, want) {
		t.Fatalf("packet = %x, want %x", got, want)
	}
}

func TestCodecRoundTrip(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	want := Frame{
		Kind:              FrameRequest,
		Flags:             FlagEnd,
		ID:                42,
		Sequence:          0,
		DeadlineUnixMilli: 1_700_000_000_123,
		Op:                "mutate",
		Tenant:            "acct-18",
		Payload:           []byte(`{"value":1}`),
	}
	errCh := make(chan error, 1)
	go func() { errCh <- NewCodec(client).WriteFrame(want) }()
	got, err := NewCodec(server).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if got.Kind != want.Kind || got.Flags != want.Flags || got.ID != want.ID || got.Sequence != want.Sequence ||
		got.DeadlineUnixMilli != want.DeadlineUnixMilli || got.Op != want.Op || got.Tenant != want.Tenant ||
		!bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}

func TestCodecRejectsOversizedBeforeBodyRead(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	codec := NewCodec(server)
	codec.MaxFrame = 32
	go func() {
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], 33)
		_, _ = client.Write(prefix[:])
	}()
	_, err := codec.ReadFrame()
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadFrame error = %v, want ErrFrameTooLarge", err)
	}
}

func TestCodecRejectsPartialAndLegacyLFFrames(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "partial prefix", data: []byte{0, 0}, want: ErrFrameTruncated},
		{name: "partial body", data: append([]byte{0, 0, 0, 32}, []byte("short")...), want: ErrFrameTruncated},
		{name: "legacy LF JSON", data: []byte("{\"op\":\"health\"}\n"), want: ErrFrameTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			go func() {
				_, _ = client.Write(test.data)
				_ = client.Close()
			}()
			_, err := NewCodec(server).ReadFrame()
			if !errors.Is(err, test.want) {
				t.Fatalf("ReadFrame error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestCodecRejectsForeignVersion(t *testing.T) {
	body, err := encodeFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}
	binary.BigEndian.PutUint16(body[4:6], ProtocolVersion-1)
	_, err = decodeFrame(body)
	if !errors.Is(err, ErrProtocolVersion) {
		t.Fatalf("decodeFrame error = %v, want ErrProtocolVersion", err)
	}
}

func TestCodecReadDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	codec := NewCodec(server)
	codec.ReadTimeout = 10 * time.Millisecond
	_, err := codec.ReadFrame()
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("ReadFrame error = %v, want timeout", err)
	}
}

func TestFrameKindStructuralValidation(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{name: "cancel sequence", frame: Frame{Kind: FrameCancel, Flags: FlagEnd, ID: 1, Sequence: 1}},
		{name: "cancel deadline", frame: Frame{Kind: FrameCancel, Flags: FlagEnd, ID: 1, DeadlineUnixMilli: 1}},
		{name: "cancel payload", frame: Frame{Kind: FrameCancel, Flags: FlagEnd, ID: 1, Payload: []byte("x")}},
		{name: "go-away routing", frame: Frame{Kind: FrameGoAway, Flags: FlagEnd, Op: "x"}},
		{name: "go-away payload", frame: Frame{Kind: FrameGoAway, Flags: FlagEnd, Payload: []byte("x")}},
		{name: "event sequence", frame: Frame{Kind: FrameEvent, Flags: FlagEnd, Sequence: 1, Op: "changed"}},
		{name: "event deadline", frame: Frame{Kind: FrameEvent, Flags: FlagEnd, DeadlineUnixMilli: 1, Op: "changed"}},
		{name: "event tenant", frame: Frame{Kind: FrameEvent, Flags: FlagEnd, Op: "changed", Tenant: "acct-18"}},
		{name: "request negative deadline", frame: Frame{Kind: FrameRequest, Flags: FlagEnd, ID: 1, DeadlineUnixMilli: -1, Op: "mutate"}},
		{name: "response routing", frame: Frame{Kind: FrameResponse, Flags: FlagEnd, ID: 1, Op: "mutate", Payload: []byte(`{}`)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := encodeFrame(test.frame); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("encodeFrame error = %v, want ErrInvalidFrame", err)
			}
		})
	}
	if _, err := encodeFrame(Frame{Kind: FrameEvent, Flags: FlagEnd, Op: "changed", Payload: []byte("payload")}); err != nil {
		t.Fatalf("valid event payload: %v", err)
	}
}
