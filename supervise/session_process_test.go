package supervise

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func TestManagedSessionTracksBeforeReadinessAndRoundTrips(t *testing.T) {
	registry := newFakeRegistry()
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	readyRecord := make(chan proc.Record, 1)
	session, err := pool.StartSession(context.Background(), SessionProcessSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/bin/sh",
		Args:          []string{"-c", `while IFS= read -r line; do printf 'reply:%s\n' "$line"; done`},
		Ready: func(_ context.Context, record proc.Record, conn net.Conn) error {
			if registry.recordCount() != 1 {
				return errors.New("readiness ran before durable tracking")
			}
			readyRecord <- record
			if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
				return err
			}
			if _, err := conn.Write([]byte("ready\n")); err != nil {
				return err
			}
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				return err
			}
			if line != "reply:ready\n" {
				return fmt.Errorf("ready reply = %q", line)
			}
			return conn.SetDeadline(time.Time{})
		},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if record := <-readyRecord; record != session.Record() {
		t.Fatalf("ready record = %#v, want %#v", record, session.Record())
	}
	if _, err := session.Conn().Write([]byte("event\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	line, err := bufio.NewReader(session.Conn()).ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	if line != "reply:event\n" {
		t.Fatalf("reply = %q, want reply:event", line)
	}
	if err := session.Conn().Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := session.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedSessionReadinessFailureClosesAndReaps(t *testing.T) {
	registry := newFakeRegistry()
	registry.trackStarted = make(chan int, 1)
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	refused := errors.New("product handshake rejected")
	_, err = pool.StartSession(context.Background(), SessionProcessSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/bin/sh",
		Args:          []string{"-c", "sleep 60"},
		Ready: func(context.Context, proc.Record, net.Conn) error {
			return refused
		},
	})
	if !errors.Is(err, refused) {
		t.Fatalf("StartSession error = %v, want %v", err, refused)
	}
	pid := <-registry.trackStarted
	assertPIDGone(t, pid)
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedSessionStopEscalatesAndClosesConnection(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	registry := newFakeRegistry()
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	spec := managedProcessSpec(t, marker)
	session, err := pool.StartSession(context.Background(), SessionProcessSpec{
		RecoveryClass: spec.RecoveryClass,
		Path:          spec.Path,
		Args:          spec.Args,
		Env:           spec.Env,
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	pid := readPIDFile(t, marker)
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := session.Wait(context.Background()); !errors.Is(err, ErrProcessStopped) {
		t.Fatalf("Wait error = %v, want ErrProcessStopped", err)
	}
	if _, err := session.Conn().Write([]byte("closed")); err == nil {
		t.Fatal("session connection remained writable after Stop")
	}
	assertPIDGone(t, pid)
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedSessionExitClosesConnection(t *testing.T) {
	registry := newFakeRegistry()
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	session, err := pool.StartSession(context.Background(), SessionProcessSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/usr/bin/true",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if err := session.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	buffer := make([]byte, 1)
	if _, err := session.Conn().Read(buffer); err == nil {
		t.Fatal("session connection remained readable after child exit")
	}
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedSessionLeaderExitSettlesBackgroundedDescendant(t *testing.T) {
	registry := newFakeRegistry()
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	pool.grace = 40 * time.Millisecond
	marker := filepath.Join(t.TempDir(), "descendant.pid")
	session, err := pool.StartSession(context.Background(), SessionProcessSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/bin/sh",
		Args: []string{"-c", `
/bin/sh -c 'trap "" TERM; echo $$ > "$1"; while :; do sleep 10; done' descendant "$1" &
while [ ! -s "$1" ]; do sleep 0.01; done
exit 0
`, "session", marker},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	descendantPID := readPIDFile(t, marker)
	if err := session.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	assertPIDGone(t, descendantPID)
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedSessionWriteDeadlineUnblocksBackpressure(t *testing.T) {
	registry := newFakeRegistry()
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	session, err := pool.StartSession(context.Background(), SessionProcessSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          "/bin/sh",
		Args:          []string{"-c", "sleep 60"},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if err := session.Conn().SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if _, err := session.Conn().Write(make([]byte, 1<<20)); err == nil {
		t.Fatal("backpressured write ignored its deadline")
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}
