package proc

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

type blockedStderrWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	failed  error
	value   string
}

func (w *blockedStderrWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	w.value = string(p)
	return len(p), w.failed
}

func (w *blockedStderrWriter) finish() { w.once.Do(func() { close(w.release) }) }

func pendingStderr(t *testing.T, failed error) (*stderrCopy, *blockedStderrWriter) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	sink := &blockedStderrWriter{entered: make(chan struct{}), release: make(chan struct{}), failed: failed}
	drain := startStderrCopy(reader, sink)
	t.Cleanup(func() {
		sink.finish()
		_ = writer.Close()
		_ = reader.Close()
		select {
		case <-drain.done:
		case <-time.After(time.Second):
			t.Error("stderr copy did not finish during cleanup")
		}
	})
	if _, err := writer.Write([]byte("final diagnostics")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("stderr copy did not enter the final write")
	}
	return drain, sink
}

func assertStderrPending(t *testing.T, child *Child) {
	t.Helper()
	select {
	case <-child.StderrDone():
		t.Fatal("stderr completion published while final write was pending")
	default:
	}
	if err := child.StderrErr(); err != nil {
		t.Fatalf("stderr error published before final write returned: %v", err)
	}
}

func TestStderrDoneFollowsFinalWriteAndErrorPublication(t *testing.T) {
	for _, failed := range []error{nil, errors.New("sink failed")} {
		name := "success"
		if failed != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			drain, sink := pendingStderr(t, failed)
			child := &Child{stderr: drain}
			done := child.StderrDone()
			assertStderrPending(t, child)
			sink.finish()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("stderr completion was not published")
			}
			if child.StderrDone() != done || !errors.Is(child.StderrErr(), failed) || sink.value != "final diagnostics" {
				t.Fatalf("stderr error=%v value=%q", child.StderrErr(), sink.value)
			}
			if _, err := drain.reader.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("stderr reader not closed before completion: %v", err)
			}
		})
	}
}

func TestStderrDoneStaysPendingAcrossCancellationAndStoreClose(t *testing.T) {
	drain, sink := pendingStderr(t, errors.New("final write failed"))
	s, _ := newTestStore(t)
	child := &Child{store: s, stderr: drain, settled: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	cancel()
	if _, err := child.WaitNatural(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitNatural() = %v", err)
	}
	assertStderrPending(t, child)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertStderrPending(t, child)
	drain.abort()
	assertStderrPending(t, child)
	sink.finish()
	select {
	case <-child.StderrDone():
	case <-time.After(time.Second):
		t.Fatal("stderr completion was not published after final write returned")
	}
	if !errors.Is(child.StderrErr(), sink.failed) {
		t.Fatalf("final stderr error = %v, want %v", child.StderrErr(), sink.failed)
	}
}

func TestStderrDoneWithoutCopyIsAlreadyClosed(t *testing.T) {
	child := &Child{}
	select {
	case <-child.StderrDone():
	default:
		t.Fatal("absent stderr copy did not report completion")
	}
	if child.StderrErr() != nil {
		t.Fatal("absent stderr copy reported an error")
	}
}
