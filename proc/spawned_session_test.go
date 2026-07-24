package proc

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/spawnedsession"
	"golang.org/x/sys/unix"
)

func TestSpawnedSessionSocketpairIsUnixStreamAndCloseOnExec(t *testing.T) {
	parent, child, err := newSpawnedSessionFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	defer child.Close()
	for _, file := range []*os.File{parent, child} {
		kind, err := unix.GetsockoptInt(int(file.Fd()), unix.SOL_SOCKET, unix.SO_TYPE)
		if err != nil {
			t.Fatal(err)
		}
		if kind != unix.SOCK_STREAM {
			t.Fatalf("socket type = %d, want SOCK_STREAM", kind)
		}
		flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
		if err != nil {
			t.Fatal(err)
		}
		if flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("descriptor %d lacks FD_CLOEXEC", file.Fd())
		}
	}
}

func TestSpawnedSessionRejectsAndClosesForeignInheritedDescriptor(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	fd := int(read.Fd())
	if _, err := claimSpawnedSessionIdentity(context.Background(), fd); !errors.Is(err, ErrSpawnedSessionIdentity) {
		t.Fatalf("foreign descriptor claim = %v, want identity mismatch", err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("foreign descriptor remained open: %v", err)
	}
}

func TestSpawnedSessionManagerExchangeAndOneShotClaims(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	self, err := spawnedCurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	var signature SignatureDigest
	signature[0] = 1
	diagnostic := filepath.Join(t.TempDir(), "helper-error")
	request, err := NewSpawnRequest(SpawnConfig{
		RecoveryID: RecoveryTaskID,
		Executable: self.Executable,
		Args: []string{
			"-test.run=^TestSpawnedSessionHelperProcess$",
			"-test.v",
		},
		Env: []string{
			"SPAWNED_SESSION_DIAGNOSTIC=" + diagnostic,
			"SPAWNED_SESSION_HELPER=1",
		},
		Stdin: StdioNull, Stdout: StdioNull, Stderr: StdioPipe,
		SpawnedSession: true, ExpectedSignature: &signature,
	})
	if err != nil {
		t.Fatalf("NewSpawnRequest executable %q: %v", self.Executable, err)
	}
	child, receipt, err := manager.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.HasSpawnedSession() {
		t.Fatal("receipt did not retain spawned-session policy")
	}
	stderr, err := child.TakeStderr()
	if err != nil {
		t.Fatal(err)
	}
	defer stderr.Close()
	if _, err := child.ClaimSpawnedSession(context.Background(), receipt); !errors.Is(err, ErrSpawnedSessionUnavailable) {
		t.Fatalf("pre-Start claim = %v, want unavailable", err)
	}
	if err := child.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	endpoint, err := child.ClaimSpawnedSession(context.Background(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child.ClaimSpawnedSession(context.Background(), receipt); !errors.Is(err, ErrSpawnedSessionClaimed) {
		t.Fatalf("second manager claim = %v, want claimed", err)
	}
	opened, err := endpoint.OpenForWire(context.Background(), spawnedsession.WireAuthority())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := endpoint.OpenForWire(context.Background(), spawnedsession.WireAuthority()); !errors.Is(err, ErrSpawnedSessionClaimed) {
		t.Fatalf("second endpoint claim = %v, want claimed", err)
	}
	if _, err := opened.Conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, len("pong"))
	if _, err := io.ReadFull(opened.Conn, response); err != nil {
		failure, _ := io.ReadAll(stderr)
		diagnosticPayload, _ := os.ReadFile(diagnostic)
		exit := waitManagerChild(t, child)
		t.Fatalf(
			"read helper response: %v; exit=%+v stderr=%s diagnostic=%s",
			err, exit, failure, diagnosticPayload,
		)
	}
	if string(response) != "pong" {
		t.Fatalf("response = %q", response)
	}
	if err := opened.Conn.Close(); err != nil {
		t.Fatal(err)
	}
	if exit := waitManagerChild(t, child); exit.Code != 0 {
		t.Fatalf("helper exit = %+v", exit)
	}
}

func TestSpawnedSessionHelperProcess(t *testing.T) {
	if os.Getenv("SPAWNED_SESSION_HELPER") != "1" {
		t.Skip("helper body; runs only re-exec'd")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fail := func(err error) {
		_ = os.WriteFile(os.Getenv("SPAWNED_SESSION_DIAGNOSTIC"), []byte(err.Error()), 0o600)
		t.Fatal(err)
	}
	identity, err := ClaimSpawnedSessionIdentity(ctx)
	if err != nil {
		fail(err)
		return
	}
	if _, err := ClaimSpawnedSessionIdentity(ctx); !errors.Is(err, ErrSpawnedSessionClaimed) {
		fail(errors.New("second child identity claim did not fail closed"))
		return
	}
	if err := CloseInheritedFDs(); err != nil {
		fail(err)
		return
	}
	opened, err := identity.OpenForWire(spawnedsession.WireAuthority())
	if err != nil {
		fail(err)
		return
	}
	request := make([]byte, len("ping"))
	if _, err := io.ReadFull(opened.Conn, request); err != nil {
		fail(err)
		return
	}
	if string(request) != "ping" {
		t.Fatalf("request = %q", request)
	}
	if _, err := opened.Conn.Write([]byte("pong")); err != nil {
		fail(err)
		return
	}
	if err := opened.Conn.Close(); err != nil {
		fail(err)
	}
}

func TestSpawnedSessionSpawnConfigIsExact(t *testing.T) {
	var signature SignatureDigest
	signature[0] = 1
	base := SpawnConfig{
		RecoveryID: RecoveryTaskID, Executable: "/bin/sh",
		Stdin: StdioNull, Stdout: StdioNull, Stderr: StdioNull,
		SpawnedSession: true, ExpectedSignature: &signature,
	}
	for _, mutate := range []func(*SpawnConfig){
		func(config *SpawnConfig) { config.ExpectedSignature = nil },
		func(config *SpawnConfig) { config.RequiresPeerFence = true },
		func(config *SpawnConfig) { config.Stdin = StdioPipe },
		func(config *SpawnConfig) { config.Stdout = StdioPipe },
	} {
		config := base
		mutate(&config)
		if _, err := NewSpawnRequest(config); err == nil {
			t.Fatalf("invalid spawned-session config accepted: %+v", config)
		}
	}
	first, err := NewSpawnRequest(base)
	if err != nil {
		t.Fatal(err)
	}
	base.SpawnedSession = false
	second, err := NewSpawnRequest(base)
	if err != nil {
		t.Fatal(err)
	}
	if first.digest == second.digest {
		t.Fatal("spawned-session policy is absent from immutable request digest")
	}
}
