package wire

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type readCloser struct{ io.Reader }

func (readCloser) Close() error { return nil }

type writeCloser struct{ io.Writer }

func (writeCloser) Close() error { return nil }

type fragmentReader struct {
	reader io.Reader
	limit  int
}

func (r fragmentReader) Read(payload []byte) (int, error) {
	if len(payload) > r.limit {
		payload = payload[:r.limit]
	}
	return r.reader.Read(payload)
}

func TestDuplexConnPreservesFragmentedFramesAndEOF(t *testing.T) {
	var encoded bytes.Buffer
	encoderConn, err := NewDuplexConn(readCloser{bytes.NewReader(nil)}, writeCloser{&encoded})
	if err != nil {
		t.Fatalf("NewDuplexConn encoder: %v", err)
	}
	want := Frame{Kind: FrameRequest, Flags: FlagEnd, ID: 7, Op: "echo", Payload: []byte("fragmented")}
	if err := NewCodec(encoderConn).WriteFrame(want); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	decoderConn, err := NewDuplexConn(
		readCloser{fragmentReader{reader: bytes.NewReader(encoded.Bytes()), limit: 2}},
		writeCloser{io.Discard},
	)
	if err != nil {
		t.Fatalf("NewDuplexConn decoder: %v", err)
	}
	got, err := NewCodec(decoderConn).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.Kind != want.Kind || got.Flags != want.Flags || got.ID != want.ID || got.Op != want.Op || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("frame = %#v, want %#v", got, want)
	}
	if _, err := NewCodec(decoderConn).ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("second ReadFrame = %v, want EOF", err)
	}
}

func TestDuplexConnWriteDeadlineUnblocksWriterAndCloseIsIdempotent(t *testing.T) {
	reader, writer := io.Pipe()
	conn, err := NewDuplexConn(readCloser{bytes.NewReader(nil)}, writer)
	if err != nil {
		t.Fatalf("NewDuplexConn: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(25 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte("blocked"))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked write succeeded after deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("write deadline did not unblock writer")
	}
	_ = reader.Close()
	if err := conn.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Close: %v", err)
	}
	if err := conn.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("second Close: %v", err)
	}
	if err := conn.SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("SetDeadline after Close = %v, want net.ErrClosed", err)
	}
}

func TestDuplexConnConcurrentCloseJoinsBothStreamsOnce(t *testing.T) {
	reader := &countingReadCloser{Reader: bytes.NewReader(nil)}
	writer := &countingWriteCloser{Writer: io.Discard}
	conn, err := NewDuplexConn(reader, writer)
	if err != nil {
		t.Fatalf("NewDuplexConn: %v", err)
	}
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			_ = conn.Close()
		}()
	}
	group.Wait()
	if reader.closes != 1 || writer.closes != 1 {
		t.Fatalf("close counts = reader %d writer %d, want 1/1", reader.closes, writer.closes)
	}
}

type countingReadCloser struct {
	io.Reader
	closes int
}

func (c *countingReadCloser) Close() error { c.closes++; return nil }

type countingWriteCloser struct {
	io.Writer
	closes int
}

func (c *countingWriteCloser) Close() error { c.closes++; return nil }
