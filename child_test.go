package daemonkit

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestChildConnIsSingleTakeAndAbsentWithoutAChannel(t *testing.T) {
	owned := ownedScope(t)

	t.Run("handoff conn is single-take", func(t *testing.T) {
		child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
			Path: "/bin/sh",
			Args: []string{"-c", "cat <&3 >/dev/null"},
			Exec: ServingSameUser(),
		}, ChannelHandoff, nil)
		if err != nil {
			t.Fatalf("Spawn() = %v", err)
		}
		conn, err := child.Conn()
		if err != nil {
			t.Fatalf("Conn() = %v", err)
		}
		if _, err := child.Conn(); err == nil {
			t.Fatal("Conn() is not single-take; a second take handed out the same endpoint")
		}
		_ = conn.Close()
		stopCtx := bounded(t, 20*time.Second)
		if _, err := child.Stop(stopCtx); err != nil {
			t.Fatalf("Stop() = %v", err)
		}
	})

	t.Run("channel-less child has none", func(t *testing.T) {
		child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
			Path: "/bin/sleep",
			Args: []string{"0"},
			Exec: ServingSameUser(),
		}, ChannelNone, nil)
		if err != nil {
			t.Fatalf("Spawn() = %v", err)
		}
		if _, err := child.Conn(); err == nil {
			t.Fatal("Conn() returned an endpoint for a ChannelNone child")
		}
		<-child.Done()
	})
}

func TestChildStopIsIdempotentAndConvergesOnOneTerminal(t *testing.T) {
	owned := ownedScope(t)
	child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/sleep",
		Args: []string{"600"},
		Exec: ServingSameUser(),
	}, ChannelNone, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	first, err := child.Stop(bounded(t, 20*time.Second))
	if err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	second, err := child.Stop(bounded(t, 20*time.Second))
	if err != nil {
		t.Fatalf("second Stop() = %v", err)
	}
	if first != second {
		t.Fatalf("Stop() converged on %+v then %+v", first, second)
	}
	if first.Reap != ReapTerminated {
		t.Fatalf("Exit.Reap = %d, want ReapTerminated", first.Reap)
	}
	if first.Code != -1 || first.Signal == 0 {
		t.Fatalf("Exit = %+v, want code -1 beside the fatal signal", first)
	}
	if late := <-child.Done(); late != first {
		t.Fatalf("a subscriber arriving after settlement got %+v, want %+v", late, first)
	}
}

func TestChildStopRequiresADeadline(t *testing.T) {
	owned := ownedScope(t)
	child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/sleep",
		Args: []string{"600"},
		Exec: ServingSameUser(),
	}, ChannelNone, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	t.Cleanup(func() { _, _ = child.Stop(bounded(t, 20*time.Second)) })
	if _, err := child.Stop(t.Context()); err == nil {
		t.Fatal("Stop() accepted a context without a deadline")
	}
}

// TestStderrCopyFailureNeverKillsTheChild is D4: losing diagnostics is not a
// reason to kill a working process, so the failure surfaces on StderrErr and
// the child keeps running to its own exit.
func TestStderrCopyFailureNeverKillsTheChild(t *testing.T) {
	owned := ownedScope(t)
	sink := &failingWriter{}
	child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "echo noise >&2; sleep 0.3; exit 7"},
		Exec: ServingSameUser(),
	}, ChannelNone, sink)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	exit := <-child.Done()
	if exit.Code != 7 {
		t.Fatalf("Exit = %+v, want the child's own exit 7 — the copy failure killed it", exit)
	}
	if err := child.StderrErr(); !errors.Is(err, errSinkClosed) {
		t.Fatalf("StderrErr() = %v, want the copy's failure", err)
	}
}

func TestSpawnedStderrReachesACapture(t *testing.T) {
	owned := ownedScope(t)
	capture := NewCapture(6)
	child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "printf 'abcdefghij' >&2"},
		Exec: ServingSameUser(),
	}, ChannelNone, capture)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	<-child.Done()
	if got := string(capture.Bytes()); got != "abcdef" {
		t.Fatalf("Capture.Bytes() = %q, want the first 6 bytes", got)
	}
	if !capture.Truncated() {
		t.Fatal("Capture.Truncated() = false for a drained overflow")
	}
	if err := child.StderrErr(); err != nil {
		t.Fatalf("StderrErr() = %v; a Capture never fails its child", err)
	}
}

func TestCaptureRetainsThenDrainsWithoutErroring(t *testing.T) {
	capture := NewCapture(4)
	n, err := capture.Write([]byte("ab"))
	if n != 2 || err != nil {
		t.Fatalf("Write() = %d, %v", n, err)
	}
	n, err = capture.Write([]byte("cdefgh"))
	if n != 6 || err != nil {
		t.Fatalf("Write() = %d, %v; a bounded sink must never short-write its child's pipe", n, err)
	}
	if got := string(capture.Bytes()); got != "abcd" {
		t.Fatalf("Bytes() = %q", got)
	}
	if !capture.Truncated() {
		t.Fatal("Truncated() = false after an overflow")
	}
	zero := NewCapture(0)
	if n, err := zero.Write([]byte("x")); n != 1 || err != nil {
		t.Fatalf("zero-limit Write() = %d, %v", n, err)
	}
	if len(zero.Bytes()) != 0 || !zero.Truncated() {
		t.Fatalf("zero-limit Capture retained %q", zero.Bytes())
	}
}

var errSinkClosed = errors.New("sink closed")

type failingWriter struct{}

func (*failingWriter) Write([]byte) (int, error) { return 0, errSinkClosed }

var _ io.Writer = (*failingWriter)(nil)

type lockedWriter struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}
