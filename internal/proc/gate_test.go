package proc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

func TestOpenStoreLockExcludesASecondOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.dkstate")
	first := openTestStore(t, path)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		round, cancelRound := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err := OpenStore(round, path)
		cancelRound()
		if errors.Is(err, durable.ErrLockBusy) {
			break
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second OpenStore() = %v, want durable.ErrLockBusy", err)
		}
		if ctx.Err() != nil {
			t.Fatal("no bounded OpenStore() observed the held store lock within 30s")
		}
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	second := openTestStore(t, path)
	if second.generation == 0 {
		t.Fatal("the released lock did not admit a fresh generation")
	}
}

func TestOpenStoreRequiresADeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.dkstate")
	if _, err := OpenStore(context.Background(), path); err == nil {
		t.Fatal("OpenStore() accepted a context without a deadline")
	}
}

func TestSpawnStdioChannelCarriesBothDirections(t *testing.T) {
	s, _ := newTestStore(t)
	child, err := s.Spawn(t.Context(), Cmd{
		Path:    "/bin/cat",
		Channel: ChannelStdio,
	}, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	conn, err := child.TakeChannel()
	if err != nil {
		t.Fatalf("TakeChannel() = %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetDeadline() = %v; the stdio channel has no working deadlines", err)
	}
	if _, err := conn.Write([]byte("through stdio\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len("through stdio\n"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "through stdio\n" {
		t.Fatalf("read %q back from the stdio channel", buf)
	}
	_ = conn.Close()
	<-child.Done()
}

func TestSpawnStderrReachesTheWriter(t *testing.T) {
	s, _ := newTestStore(t)
	var sink lockedBuffer
	child, err := s.Spawn(t.Context(), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "echo diagnostics >&2"},
	}, &sink)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	<-child.Done()
	if got := strings.TrimSpace(sink.String()); got != "diagnostics" {
		t.Fatalf("stderr sink = %q, want %q", got, "diagnostics")
	}
	if err := child.StderrErr(); err != nil {
		t.Fatalf("StderrErr() = %v for a healthy copy", err)
	}
}

func TestRunRefusesAChannel(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.Run(runContext(t, time.Second), Cmd{Path: "/usr/bin/true", Channel: ChannelStdio}, unheld); err == nil {
		t.Fatal("Run() accepted a channel")
	}
}

func TestSpawnRefusesAnUnestablishedChannel(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.Spawn(t.Context(), Cmd{Path: "/usr/bin/true", Channel: channelLimit}, nil); err == nil {
		t.Fatal("Spawn() accepted a channel outside the established set")
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
