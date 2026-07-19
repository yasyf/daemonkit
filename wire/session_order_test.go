package wire

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

type terminalWriteBarrier struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *terminalWriteBarrier) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Write(p)
}

func TestTerminalIntentPrecedesBlockingWrite(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	barrier := &terminalWriteBarrier{
		Conn: serverConn, entered: make(chan struct{}), release: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	state := &requestState{terminalAck: make(chan struct{}), settled: make(chan struct{})}
	close(state.settled)
	session := &session{
		server: &Server{}, conn: barrier, codec: NewCodec(barrier), ctx: ctx, cancel: cancel,
		generation: make([]byte, sessionGenerationBytes), outbound: make(chan sessionOutbound, 1),
		requestsDone: make(chan struct{}), writerDone: make(chan struct{}),
		active: map[uint64]*requestState{1: state},
	}
	session.writerWG.Add(1)
	go session.writeLoop()
	responseDone := make(chan error, 1)
	go func() {
		responseDone <- session.sendAdmittedResponse(ctx, 1, state, Response{Ack: true})
	}()
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("terminal writer did not reach barrier")
	}
	if err := session.receiveAck(Frame{
		Kind: FrameAck, Flags: FlagEnd, ID: 1, Payload: session.generation,
	}); err != nil {
		t.Fatalf("acknowledgement during terminal write: %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := NewCodec(clientConn).ReadFrame()
		readDone <- err
	}()
	close(barrier.release)
	if err := <-readDone; err != nil {
		t.Fatalf("read terminal response: %v", err)
	}
	if err := <-responseDone; err != nil {
		t.Fatalf("send terminal response: %v", err)
	}
	close(session.requestsDone)
	cancel()
	session.writerWG.Wait()
}
