package proc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

func TestVerifyRefusalAbortsBeforeTheChildRuns(t *testing.T) {
	s, path := newTestStore(t)
	marker := filepath.Join(t.TempDir(), "ran")
	refused := errors.New("posture refused")
	var suspended int

	_, err := s.Spawn(t.Context(), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "touch " + marker},
		Verify: func(pid int) error {
			suspended = pid
			info, probeErr := probeProc(pid)
			if probeErr != nil {
				t.Errorf("probe the gated child: %v", probeErr)
			} else if !info.stopped {
				t.Error("the verify gate ran against a released child")
			}
			return refused
		},
	}, nil)
	if !errors.Is(err, refused) {
		t.Fatalf("Spawn() = %v, want the verify refusal", err)
	}
	if suspended == 0 {
		t.Fatal("the verify gate never ran")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("the refused child executed an instruction: %v", err)
	}
	if _, err := probeProc(suspended); !errors.Is(err, errNoProc) {
		t.Fatalf("probe refused pid %d = %v, want it reaped", suspended, err)
	}
	if storeHolds(t, path, identity{pid: suspended}) {
		t.Fatal("the refused child's record survived the abort")
	}
}

func TestOpenStoreLockExcludesASecondOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.dkstate")
	first := openTestStore(t, path)

	busyCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := OpenStore(busyCtx, path); !errors.Is(err, durable.ErrLockBusy) {
		t.Fatalf("second OpenStore() = %v, want durable.ErrLockBusy", err)
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
