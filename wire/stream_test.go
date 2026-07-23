package wire

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net"
	"sync"
	"testing"
	"time"
)

func TestStreamSequenceRejectsBeforeWrappingToZero(t *testing.T) {
	sequence := streamSequence{next: math.MaxUint32 - 1}
	first, err := sequence.take()
	if err != nil || first != math.MaxUint32-1 {
		t.Fatalf("first take = %d, %v", first, err)
	}
	last, err := sequence.take()
	if err != nil || last != math.MaxUint32 {
		t.Fatalf("last take = %d, %v", last, err)
	}
	if _, err := sequence.take(); !errors.Is(err, ErrStreamOrder) {
		t.Fatalf("exhausted take error = %v, want ErrStreamOrder", err)
	}
}

func TestLifecycleWriterRefreshesSelectionAtWriteStart(t *testing.T) {
	lane := newLatestWriteLane()
	first, err := lane.offer([]byte("first"), false)
	if err != nil {
		t.Fatal(err)
	}
	selected := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var (
		mu      sync.Mutex
		written []byte
		used    uint64
	)
	go func() {
		defer close(done)
		generation, _, ok, err := lane.tryTake()
		if err != nil || !ok {
			t.Errorf("tryTake = %d, %t, %v", generation, ok, err)
			return
		}
		close(selected)
		<-release
		generation, payload, finish, err := lane.beginWrite(generation)
		if err != nil {
			t.Errorf("beginWrite: %v", err)
			return
		}
		mu.Lock()
		used, written = generation, payload
		mu.Unlock()
		finish(nil)
	}()
	<-selected
	second, err := lane.offer([]byte("second"), false)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	mu.Lock()
	defer mu.Unlock()
	if first == second || used != second || !bytes.Equal(written, []byte("second")) {
		t.Fatalf("selected generations first=%d second=%d used=%d payload=%q", first, second, used, written)
	}
}

func TestLifecycleOfferDoesNotBlockBehindSocketWrite(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	barrier := &terminalWriteBarrier{
		Conn: serverConn, entered: make(chan struct{}), release: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &session{
		server: &Server{}, conn: barrier, codec: NewCodec(barrier), ctx: ctx, cancel: cancel,
		generation: make([]byte, sessionGenerationBytes), outbound: make(chan sessionOutbound, 1),
		lifecycleLane: newLatestWriteLane(), requestsDone: make(chan struct{}), writerDone: make(chan struct{}),
	}
	session.writerWG.Add(1)
	go session.writeLoop()
	if _, err := session.offerLifecycle([]byte("first"), false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("lifecycle writer did not reach socket barrier")
	}
	offered := make(chan error, 1)
	go func() {
		_, err := session.offerLifecycle([]byte("second"), false)
		offered <- err
	}()
	select {
	case err := <-offered:
		if err != nil {
			t.Fatalf("offer during blocked socket write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("offer blocked behind socket write")
	}
	readDone := make(chan error, 1)
	go func() {
		codec := NewCodec(clientConn)
		for _, want := range [][]byte{[]byte("first"), []byte("second")} {
			frame, err := codec.ReadFrame()
			if err != nil {
				readDone <- err
				return
			}
			if frame.Kind != FrameLifecycle || !bytes.Equal(frame.Payload, want) {
				readDone <- errors.New("unexpected lifecycle frame")
				return
			}
		}
		readDone <- nil
	}()
	close(barrier.release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	close(session.requestsDone)
	cancel()
	session.writerWG.Wait()
}
