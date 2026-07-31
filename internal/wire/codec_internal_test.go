package wire

import (
	"bytes"
	"testing"
)

func TestPlainPathPeekPrependsToNextFrame(t *testing.T) {
	clientConn, serverConn := testPair(t)
	want := Frame{Kind: FrameEvent, Flags: FlagEnd, Op: "topic.v1", Payload: []byte("payload")}
	packet, err := EncodePacket(want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientConn.Write(packet); err != nil {
		t.Fatal(err)
	}

	codec := NewCodec(serverConn)
	codec.rights = nil
	drain, err := codec.PeekPreamble()
	if err != nil || drain {
		t.Fatalf("PeekPreamble() = (%v, %v), want (false, nil)", drain, err)
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() after peek = %v", err)
	}
	if frame.Kind != want.Kind || frame.Op != want.Op || !bytes.Equal(frame.Payload, want.Payload) {
		t.Fatalf("frame after peek = %+v, want %+v", frame, want)
	}
}
