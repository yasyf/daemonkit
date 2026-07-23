package wire

import (
	"errors"
	"net"
	"os"
	"reflect"
	"testing"
	"time"
)

const frameDeadlineTestTimeout = 40 * time.Millisecond

func TestCodecClearsDuplexReadDeadlineAfterFrame(t *testing.T) {
	leftConn, rightConn := newFrameDeadlineDuplexPair(t)
	left := NewCodec(leftConn)
	right := NewCodec(rightConn)
	right.ReadTimeout = frameDeadlineTestTimeout

	exchangeDeadlineTestFrame(t, left, right, Frame{
		Kind: FrameHello, Flags: FlagEnd, Payload: []byte(`{}`),
	})
	time.Sleep(4 * frameDeadlineTestTimeout)
	exchangeDeadlineTestFrame(t, left, right, Frame{
		Kind: FrameRequest, Flags: FlagEnd, ID: 1, Op: "after-idle",
	})
}

func TestCodecClearsDuplexWriteDeadlineAfterFrame(t *testing.T) {
	leftConn, rightConn := newFrameDeadlineDuplexPair(t)
	left := NewCodec(leftConn)
	right := NewCodec(rightConn)
	left.WriteTimeout = frameDeadlineTestTimeout

	exchangeDeadlineTestFrame(t, left, right, Frame{
		Kind: FrameHelloAck, Flags: FlagEnd, Payload: []byte(`{}`),
	})
	time.Sleep(4 * frameDeadlineTestTimeout)
	exchangeDeadlineTestFrame(t, left, right, Frame{
		Kind: FrameResponse, Flags: FlagEnd, ID: 1, Payload: []byte(`{"ok":true}`),
	})
}

func TestCodecReadFrameReturnsClearDeadlineFailure(t *testing.T) {
	clearErr := errors.New("clear read deadline")
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	codec := NewCodec(&deadlineFaultConn{Conn: server, clearReadErr: clearErr})
	codec.ReadTimeout = time.Second
	want := Frame{Kind: FrameHello, Flags: FlagEnd, Payload: []byte(`{}`)}
	writeDone := make(chan error, 1)
	go func() { writeDone <- NewCodec(client).WriteFrame(want) }()

	got, err := codec.ReadFrame()
	if !errors.Is(err, clearErr) {
		t.Fatalf("ReadFrame error = %v, want clear failure", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadFrame frame = %#v, want %#v", got, want)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("peer WriteFrame: %v", err)
	}
}

func TestCodecReadFrameJoinsReadAndClearDeadlineFailures(t *testing.T) {
	readErr := errors.New("read frame")
	clearErr := errors.New("clear read deadline")
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	codec := NewCodec(&deadlineFaultConn{
		Conn:         server,
		readErr:      readErr,
		clearReadErr: clearErr,
	})
	codec.ReadTimeout = time.Second

	_, err := codec.ReadFrame()
	if !errors.Is(err, readErr) || !errors.Is(err, clearErr) {
		t.Fatalf("ReadFrame error = %v, want joined read and clear failures", err)
	}
}

func TestCodecWriteFrameJoinsWriteAndClearDeadlineFailures(t *testing.T) {
	writeErr := errors.New("write frame")
	clearErr := errors.New("clear write deadline")
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	codec := NewCodec(&deadlineFaultConn{
		Conn:          client,
		writeErr:      writeErr,
		clearWriteErr: clearErr,
	})
	codec.WriteTimeout = time.Second

	complete, err := codec.writeFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: []byte(`{}`)})
	if complete {
		t.Fatal("writeFrame complete = true, want false")
	}
	if !errors.Is(err, writeErr) || !errors.Is(err, clearErr) {
		t.Fatalf("writeFrame error = %v, want joined write and clear failures", err)
	}
}

func TestCodecWriteFramePreservesCompleteOnClearDeadlineFailure(t *testing.T) {
	clearErr := errors.New("clear write deadline")
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	codec := NewCodec(&deadlineFaultConn{Conn: client, clearWriteErr: clearErr})
	codec.WriteTimeout = time.Second
	want := Frame{Kind: FrameHello, Flags: FlagEnd, Payload: []byte(`{}`)}
	readDone := make(chan error, 1)
	go func() {
		got, err := NewCodec(server).ReadFrame()
		if err == nil && !reflect.DeepEqual(got, want) {
			err = errors.New("peer received the wrong frame")
		}
		readDone <- err
	}()

	complete, err := codec.writeFrame(want)
	if !complete {
		t.Fatal("writeFrame complete = false, want true")
	}
	if !errors.Is(err, clearErr) {
		t.Fatalf("writeFrame error = %v, want clear failure", err)
	}
	if err := <-readDone; err != nil {
		t.Fatalf("peer ReadFrame: %v", err)
	}
}

func newFrameDeadlineDuplexPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	leftToRightReader, leftToRightWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("left-to-right pipe: %v", err)
	}
	rightToLeftReader, rightToLeftWriter, err := os.Pipe()
	if err != nil {
		_ = leftToRightReader.Close()
		_ = leftToRightWriter.Close()
		t.Fatalf("right-to-left pipe: %v", err)
	}
	left, err := NewDuplexConn(rightToLeftReader, leftToRightWriter)
	if err != nil {
		_ = leftToRightReader.Close()
		_ = leftToRightWriter.Close()
		_ = rightToLeftReader.Close()
		_ = rightToLeftWriter.Close()
		t.Fatalf("left NewDuplexConn: %v", err)
	}
	right, err := NewDuplexConn(leftToRightReader, rightToLeftWriter)
	if err != nil {
		_ = left.Close()
		_ = leftToRightReader.Close()
		_ = rightToLeftWriter.Close()
		t.Fatalf("right NewDuplexConn: %v", err)
	}
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})
	return left, right
}

func exchangeDeadlineTestFrame(t *testing.T, writer, reader *Codec, want Frame) {
	t.Helper()
	writeDone := make(chan error, 1)
	go func() { writeDone <- writer.WriteFrame(want) }()
	got, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("frame = %#v, want %#v", got, want)
	}
}

type deadlineFaultConn struct {
	net.Conn
	readErr       error
	writeErr      error
	clearReadErr  error
	clearWriteErr error
}

func (c *deadlineFaultConn) Read(payload []byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	return c.Conn.Read(payload)
}

func (c *deadlineFaultConn) Write(payload []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return c.Conn.Write(payload)
}

func (c *deadlineFaultConn) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() && c.clearReadErr != nil {
		return c.clearReadErr
	}
	return c.Conn.SetReadDeadline(deadline)
}

func (c *deadlineFaultConn) SetWriteDeadline(deadline time.Time) error {
	if deadline.IsZero() && c.clearWriteErr != nil {
		return c.clearWriteErr
	}
	return c.Conn.SetWriteDeadline(deadline)
}
