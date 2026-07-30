package proc

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSpawnHandoffReachesChildAtFD3(t *testing.T) {
	s, _ := newTestStore(t)
	marker := filepath.Join(t.TempDir(), "received")
	child, err := s.Spawn(Cmd{
		Path:    "/bin/sh",
		Args:    []string{"-c", "cat <&3 > " + marker},
		Handoff: true,
	})
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	parent, err := child.Handoff()
	if err != nil {
		t.Fatalf("Handoff() = %v", err)
	}
	if _, err := parent.Write([]byte("over fd 3")); err != nil {
		t.Fatalf("write handoff: %v", err)
	}
	if err := parent.Close(); err != nil {
		t.Fatalf("close handoff: %v", err)
	}
	var exit Exit
	select {
	case exit = <-child.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("handoff child never settled")
	}
	if exit.Code != 0 {
		t.Fatalf("Exit.Code = %d, want 0", exit.Code)
	}
	got, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if string(got) != "over fd 3" {
		t.Fatalf("child read %q from fd 3", got)
	}
	if _, err := child.Handoff(); err == nil {
		t.Fatal("Handoff() is not single-take")
	}
}

func TestHandoffAbsentWithoutCmdHandoff(t *testing.T) {
	s, _ := newTestStore(t)
	child, err := s.Spawn(Cmd{Path: "/bin/sleep", Args: []string{"0"}})
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	if _, err := child.Handoff(); err == nil {
		t.Fatal("Handoff() returned an endpoint for a handoff-less spawn")
	}
	<-child.Done()
}

func TestProveHandoffRefusalsLeaveDescriptorUnchanged(t *testing.T) {
	t.Run("not a socket", func(t *testing.T) {
		f, err := os.Open(os.DevNull)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		fd := int(f.Fd())
		if err := proveHandoff(fd); err == nil {
			t.Fatal("proveHandoff accepted a non-socket")
		}
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
			t.Fatalf("refused descriptor was mutated: %v", err)
		}
	})
	t.Run("wrong socket type", func(t *testing.T) {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(fds[0])
		defer unix.Close(fds[1])
		if err := proveHandoff(fds[0]); err == nil {
			t.Fatal("proveHandoff accepted a SOCK_DGRAM endpoint")
		}
	})
	t.Run("peer is not the parent", func(t *testing.T) {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(fds[0])
		defer unix.Close(fds[1])
		if err := proveHandoff(fds[0]); err == nil {
			t.Fatal("proveHandoff accepted a self-peered socket as the parent's")
		}
		if _, err := unix.FcntlInt(uintptr(fds[0]), unix.F_GETFD, 0); err != nil {
			t.Fatalf("refused descriptor was mutated: %v", err)
		}
	})
}
