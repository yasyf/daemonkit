package supervise

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

const terminalTestTimeout = 8 * time.Second

func newTerminalTestPool(t *testing.T) (*Pool, *proc.FileStore) {
	t.Helper()
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "processes.db")}
	reaper := &proc.Reaper{
		Store: store, Generation: "terminal-test-" + strings.ReplaceAll(t.Name(), "/", "-"),
		Grace: TerminationGrace, Settlement: 2 * time.Second,
	}
	pool, err := NewPool(2, reaper)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Cancel()
		ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
		defer cancel()
		if err := pool.Wait(ctx); err != nil {
			t.Errorf("wait for terminal pool: %v", err)
		}
	})
	return pool, store
}

func startTerminalTest(t *testing.T, pool *Pool, script string, size TerminalSize) *Terminal {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	terminal, err := pool.StartTerminal(ctx, TerminalSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/bin/sh",
		Args:          []string{"-c", script},
		Size:          size,
		AttachTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return terminal
}

func attachTerminalTest(t *testing.T, terminal *Terminal, policy TerminalDisconnectPolicy) *TerminalAttachment {
	t.Helper()
	attachment, err := terminal.Attach(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	return attachment
}

func receiveUntil(t *testing.T, attachment *TerminalAttachment, want string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	var output strings.Builder
	for !strings.Contains(output.String(), want) {
		chunk, err := attachment.Receive(ctx)
		if err != nil {
			t.Fatalf("receive %q after %q: %v", want, output.String(), err)
		}
		output.Write(chunk)
	}
	return output.String()
}

func receiveAll(t *testing.T, attachment *TerminalAttachment) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	var output strings.Builder
	for {
		chunk, err := attachment.Receive(ctx)
		if errors.Is(err, io.EOF) {
			return output.String()
		}
		if err != nil {
			t.Fatalf("receive terminal output: %v", err)
		}
		output.Write(chunk)
	}
}

func waitTerminalTest(t *testing.T, terminal *Terminal) TerminalOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	outcome, err := terminal.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Digest == ([32]byte{}) {
		t.Fatal("terminal outcome digest is empty")
	}
	return outcome
}

func assertTerminalStoreEmpty(t *testing.T, store *proc.FileStore) {
	t.Helper()
	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("durable terminal records remain: %+v", records)
	}
}

func TestTerminalPTYResizeInputEOFAndTypedExit(t *testing.T) {
	pool, store := newTerminalTestPool(t)
	terminal := startTerminalTest(t, pool, `
test -t 0 && test -t 1 && printf 'tty-ok\n'
stty size
IFS= read -r line
stty size
printf 'got:%s\n' "$line"
IFS= read -r rest || printf 'eof-ok\n'
exit 7
`, TerminalSize{Rows: 31, Cols: 91})
	attachment := attachTerminalTest(t, terminal, DetachOnDisconnect)
	output := receiveUntil(t, attachment, "31 91")

	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	if err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputResize, Size: TerminalSize{Rows: 40, Cols: 100}}); err != nil {
		t.Fatal(err)
	}
	if err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputBytes, Data: []byte("hello\n")}); err != nil {
		t.Fatal(err)
	}
	if err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputEOF}); err != nil {
		t.Fatal(err)
	}
	if err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputBytes, Data: []byte("late")}); !errors.Is(err, ErrTerminalInputClosed) {
		t.Fatalf("send after EOF = %v", err)
	}
	output += receiveAll(t, attachment)
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalExited || outcome.ExitCode != 7 {
		t.Fatalf("outcome = %+v", outcome)
	}
	for _, want := range []string{"tty-ok", "31 91", "40 100", "got:hello", "eof-ok"} {
		if !strings.Contains(output, want) {
			t.Errorf("output missing %q: %q", want, output)
		}
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalReportsSignalExactly(t *testing.T) {
	pool, store := newTerminalTestPool(t)
	terminal := startTerminalTest(t, pool, `kill -TERM $$`, TerminalSize{})
	attachment := attachTerminalTest(t, terminal, DetachOnDisconnect)
	_ = receiveAll(t, attachment)
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalSignaled || outcome.Signal != syscall.SIGTERM || outcome.ExitCode != -1 {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalDetachAndReattach(t *testing.T) {
	pool, store := newTerminalTestPool(t)
	terminal := startTerminalTest(t, pool, `printf 'ready\n'; IFS= read -r line; printf 'done:%s\n' "$line"`, TerminalSize{})
	first := attachTerminalTest(t, terminal, DetachOnDisconnect)
	_ = receiveUntil(t, first, "ready")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("wrong\n")}); !errors.Is(err, ErrTerminalDetached) {
		t.Fatalf("detached send = %v", err)
	}
	second := attachTerminalTest(t, terminal, CancelOnDisconnect)
	if err := second.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("right\n")}); err != nil {
		t.Fatal(err)
	}
	output := receiveAll(t, second)
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalExited || outcome.ExitCode != 0 || !strings.Contains(output, "done:right") {
		t.Fatalf("outcome=%+v output=%q", outcome, output)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalLostAttachNeverDispatches(t *testing.T) {
	pool, store := newTerminalTestPool(t)
	marker := filepath.Join(t.TempDir(), "dispatched")
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	terminal, err := pool.StartTerminal(ctx, TerminalSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/bin/sh",
		Args:          []string{"-c", "printf dispatched > " + marker},
		AttachTimeout: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalCanceled {
		t.Fatalf("outcome = %+v", outcome)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("undispatched marker exists or stat failed unexpectedly: %v", err)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalCancelDisconnectSettlesBlockedOutput(t *testing.T) {
	pool, store := newTerminalTestPool(t)
	terminal := startTerminalTest(t, pool, `while :; do printf '01234567890123456789012345678901'; done`, TerminalSize{})
	attachment := attachTerminalTest(t, terminal, CancelOnDisconnect)
	time.Sleep(100 * time.Millisecond)
	owned, err := pool.registry.Owns(terminal.Record())
	if err != nil || !owned {
		t.Fatalf("terminal identity before disconnect: owned=%v err=%v", owned, err)
	}
	if err := attachment.Close(); err != nil {
		t.Fatal(err)
	}
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalCanceled {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalInputBackpressureIsContextAware(t *testing.T) {
	pool, store := newTerminalTestPool(t)
	terminal := startTerminalTest(t, pool, `sleep 60`, TerminalSize{})
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	terminal.write = func([]byte) error {
		select {
		case <-writeStarted:
		default:
			close(writeStarted)
		}
		<-releaseWrite
		return nil
	}
	attachment := attachTerminalTest(t, terminal, CancelOnDisconnect)
	payload := make([]byte, TerminalChunkSize)
	blocked := false
	for range TerminalQueueDepth + 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputBytes, Data: payload})
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			blocked = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !blocked {
		t.Fatal("terminal input did not apply bounded backpressure")
	}
	close(releaseWrite)
	_ = attachment.Close()
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalCanceled {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalCancellationKillsAndReapsDescendant(t *testing.T) {
	pool, store := newTerminalTestPool(t)
	pidPath := filepath.Join(t.TempDir(), "descendant.pid")
	script := fmt.Sprintf(`(trap '' TERM; while :; do sleep 1; done) & child=$!; printf '%%s' "$child" > %q; wait`, pidPath)
	terminal := startTerminalTest(t, pool, script, TerminalSize{})
	attachment := attachTerminalTest(t, terminal, CancelOnDisconnect)

	deadline := time.Now().Add(terminalTestTimeout)
	var pid int
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(pidPath)
		if err == nil {
			pid, err = strconv.Atoi(string(raw))
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("descendant pid was not written")
	}
	_ = attachment.Close()
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalCanceled {
		t.Fatalf("outcome = %+v", outcome)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant %d remains: %v", pid, err)
	}
	assertTerminalStoreEmpty(t, store)
}
