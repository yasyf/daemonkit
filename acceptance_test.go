package daemonkit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestConsumerLongLivedCoprocessOverHandoff is captain-hook's hook engine: a
// co-process spawned once, spoken to over its own channel for its whole life,
// its stderr streamed to a caller-supplied writer, its exit watched
// independently, and its shutdown a synchronous Stop rather than a signal and
// a hope.
func TestConsumerLongLivedCoprocessOverHandoff(t *testing.T) {
	owned := ownedScope(t)
	var diagnostics lockedWriter
	child, err := owned.Spawn(bounded(t, 30*time.Second), childCmd(t, "coprocess"), ChannelHandoff, &diagnostics)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}

	watched := make(chan Exit, 1)
	go func() { watched <- <-child.Done() }()

	conn, err := child.Conn()
	if err != nil {
		t.Fatalf("Conn() = %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("SetDeadline() = %v", err)
	}
	replies := bufio.NewScanner(conn)
	for _, request := range []string{"first", "second", "third"} {
		if _, err := conn.Write([]byte(request + "\n")); err != nil {
			t.Fatalf("write %q: %v", request, err)
		}
		if !replies.Scan() {
			t.Fatalf("no reply to %q: %v (stderr %q)", request, replies.Err(), diagnostics.String())
		}
		if got, want := replies.Text(), "reply:"+request; got != want {
			t.Fatalf("reply = %q, want %q", got, want)
		}
	}

	awaitContains(t, &diagnostics, "coprocess: handling third")
	if err := child.StderrErr(); err != nil {
		t.Fatalf("StderrErr() = %v while the stream was healthy", err)
	}

	exit, err := child.Stop(bounded(t, 20*time.Second))
	if err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if exit.Reap != ReapTerminated {
		t.Fatalf("Exit = %+v, want a proven termination", exit)
	}
	select {
	case watched := <-watched:
		if watched != exit {
			t.Fatalf("the independent watcher saw %+v, Stop saw %+v", watched, exit)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the independent Done subscriber never received the terminal")
	}
	_ = conn.Close()
}

// TestConsumerForeignExecutableOverStdio is synckit's ssh child: a foreign
// executable whose stdio IS the transport, with working deadlines on the conn
// and its stderr bounded by a Capture that can never block it.
func TestConsumerForeignExecutableOverStdio(t *testing.T) {
	owned := ownedScope(t)
	stderr := NewCapture(16)
	child, err := owned.Spawn(bounded(t, 30*time.Second), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", `while :; do printf 'x%.0s' $(seq 1 200) >&2; done & exec /bin/cat`},
		Exec: ServingSameUser(),
	}, ChannelStdio, stderr)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	conn, err := child.Conn()
	if err != nil {
		t.Fatalf("Conn() = %v", err)
	}

	if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("SetDeadline() = %v", err)
	}
	if _, err := conn.Write([]byte("over the transport\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader := bufio.NewScanner(conn)
	if !reader.Scan() {
		t.Fatalf("no echo back: %v", reader.Err())
	}
	if reader.Text() != "over the transport" {
		t.Fatalf("read %q back", reader.Text())
	}

	if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() = %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("the stdio channel honored no read deadline")
	}

	awaitTruncated(t, stderr)
	if len(stderr.Bytes()) != 16 {
		t.Fatalf("Capture retained %d bytes, want the 16-byte cap", len(stderr.Bytes()))
	}
	if _, err := child.Stop(bounded(t, 20*time.Second)); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	_ = conn.Close()
}

// TestConsumerCLIScopeReclaimsAPriorGeneration is synckit's clirunner: a CLI
// with no daemon, no socket, and no wire that still owns its children durably
// — a prior generation's leak is reclaimed at open, and Close is the answer to
// "did everything drain".
func TestConsumerCLIScopeReclaimsAPriorGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.records")
	marker := fmt.Sprintf("dkcli%d", time.Now().UnixNano())

	leaked, err := OwnProcesses(bounded(t, 20*time.Second), path)
	if err != nil {
		t.Fatalf("first OwnProcesses() = %v", err)
	}
	child, err := leaked.Spawn(bounded(t, 20*time.Second), parksAs(marker+"-orphan"), ChannelNone, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	orphan := child.PID()
	// The generation leaves without settling — a killed CLI — so the record
	// stands and the child outlives its owner.
	if err := leaked.store.Close(); err != nil {
		t.Fatalf("release the leaked generation's lock: %v", err)
	}

	next, err := OwnProcesses(bounded(t, 20*time.Second), path)
	if err != nil {
		t.Fatalf("second OwnProcesses() = %v", err)
	}
	reclaimed := next.Reclaimed()
	if len(reclaimed) != 1 || reclaimed[0].PID != orphan {
		t.Fatalf("Reclaimed() = %+v, want the prior generation's orphan %d", reclaimed, orphan)
	}
	if reclaimed[0].Exit.Reap != ReapTerminated && reclaimed[0].Exit.Reap != ReapAbsent {
		t.Fatalf("Reclaimed()[0] = %+v, want a settled outcome the caller can gate on", reclaimed[0])
	}
	if aliveAs(orphan, marker+"-orphan") {
		t.Fatalf("orphan %d survived the next generation's reclaim", orphan)
	}

	live, err := next.Spawn(bounded(t, 20*time.Second), parksAs(marker+"-live"), ChannelNone, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := next.Close(closeCtx); err != nil {
		t.Fatalf("Close() = %v, want everything drained", err)
	}
	if aliveAs(live.PID(), marker+"-live") {
		t.Fatalf("pid %d survived the scope's Close", live.PID())
	}
}

// parksAs is a child that parks forever with the marker in its argv, so
// aliveAs can tell it from whatever inherits its pid — a bare /bin/sleep
// carries nothing to match on. It ignores no signal, so it still dies on the
// first rung of any ladder.
func parksAs(marker string) Cmd {
	return Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "while :; do sleep 60; done # " + marker},
		Exec: ServingSameUser(),
	}
}

// TestConsumerGatedAdoptionOverAPidTheCallerWaits is cc-orchestrate's pty
// host: the caller must own the fork, so the gate keeps the target from
// running an instruction until the record exists, and Tracked never waits —
// two waiters on one pid is a lost wakeup.
func TestConsumerGatedAdoptionOverAPidTheCallerWaits(t *testing.T) {
	owned := ownedScope(t)
	marker := filepath.Join(t.TempDir(), "target-ran")

	t.Run("release runs the target and the caller's own Wait proves the exit", func(t *testing.T) {
		gate, err := NewGate([]string{"/bin/sh", "-c", "touch " + marker + "; exit 0"})
		if err != nil {
			t.Fatalf("NewGate() = %v", err)
		}
		argv := gate.Argv()
		cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // the gate's own argv
		cmd.ExtraFiles = gate.Files()
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			t.Fatalf("Start() = %v", err)
		}

		if err := gate.Ready(bounded(t, 20*time.Second)); err != nil {
			t.Fatalf("Ready() = %v", err)
		}
		if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
			t.Fatalf("the target ran before the gate was released: %v", statErr)
		}
		tracked, err := owned.Adopt(bounded(t, 20*time.Second), cmd.Process.Pid)
		if err != nil {
			t.Fatalf("Adopt() = %v", err)
		}
		if tracked.PID() != cmd.Process.Pid {
			t.Fatalf("Tracked.PID() = %d, want %d", tracked.PID(), cmd.Process.Pid)
		}
		if err := gate.Release(); err != nil {
			t.Fatalf("Release() = %v", err)
		}

		if err := cmd.Wait(); err != nil {
			t.Fatalf("the caller's own Wait = %v", err)
		}
		if _, statErr := os.Stat(marker); statErr != nil {
			t.Fatalf("the released target never ran: %v", statErr)
		}
		if err := tracked.Release(); err != nil {
			t.Fatalf("Tracked.Release() = %v", err)
		}
	})

	t.Run("stop is an observation, not a wait", func(t *testing.T) {
		gate, err := NewGate([]string{"/bin/sleep", "600"})
		if err != nil {
			t.Fatalf("NewGate() = %v", err)
		}
		argv := gate.Argv()
		cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // the gate's own argv
		cmd.ExtraFiles = gate.Files()
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			t.Fatalf("Start() = %v", err)
		}
		if err := gate.Ready(bounded(t, 20*time.Second)); err != nil {
			t.Fatalf("Ready() = %v", err)
		}
		tracked, err := owned.Adopt(bounded(t, 20*time.Second), cmd.Process.Pid)
		if err != nil {
			t.Fatalf("Adopt() = %v", err)
		}
		if err := gate.Release(); err != nil {
			t.Fatalf("Release() = %v", err)
		}

		reap, err := tracked.Stop(bounded(t, 20*time.Second))
		if err != nil {
			t.Fatalf("Stop() = %v", err)
		}
		if reap != ReapTerminated && reap != ReapAbsent {
			t.Fatalf("Stop() = %d, want an observational absence proof", reap)
		}
		waited := make(chan error, 1)
		go func() { waited <- cmd.Wait() }()
		select {
		case <-waited:
		case <-time.After(10 * time.Second):
			t.Fatal("the caller's own Wait never returned; daemonkit took the reap")
		}
	})

	t.Run("close before release never runs the target", func(t *testing.T) {
		aborted := filepath.Join(t.TempDir(), "never")
		gate, err := NewGate([]string{"/bin/sh", "-c", "touch " + aborted})
		if err != nil {
			t.Fatalf("NewGate() = %v", err)
		}
		argv := gate.Argv()
		cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // the gate's own argv
		cmd.ExtraFiles = gate.Files()
		if err := cmd.Start(); err != nil {
			t.Fatalf("Start() = %v", err)
		}
		if err := gate.Ready(bounded(t, 20*time.Second)); err != nil {
			t.Fatalf("Ready() = %v", err)
		}
		if err := gate.Close(); err != nil {
			t.Fatalf("Close() = %v", err)
		}
		if err := gate.Release(); err == nil {
			t.Fatal("Release() reported a release the aborted gate never made")
		}
		err = cmd.Wait()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 125 {
			t.Fatalf("the aborted wrapper exited %v, want 125", err)
		}
		if _, statErr := os.Stat(aborted); !os.IsNotExist(statErr) {
			t.Fatalf("the aborted gate ran its target: %v", statErr)
		}
	})
}

func awaitContains(t *testing.T, sink *lockedWriter, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(sink.String(), want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("streamed stderr never carried %q; it carried %q", want, sink.String())
}

func awaitTruncated(t *testing.T, capture *Capture) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if capture.Truncated() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the bounded stderr never overflowed; the child was not draining")
}
