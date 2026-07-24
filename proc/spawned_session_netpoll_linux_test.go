//go:build linux

package proc

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestSpawnedSessionIdentityFailurePreservesLiveNetpollDescriptor(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	netpollFD := -1
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		target, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err == nil && strings.Contains(target, "eventpoll") {
			netpollFD = fd
			break
		}
	}
	if netpollFD < 0 {
		t.Fatal("live Go netpoll descriptor not found")
	}
	flags, err := unix.FcntlInt(uintptr(netpollFD), unix.F_GETFD, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claimSpawnedSessionIdentity(context.Background(), netpollFD); !errors.Is(err, ErrSpawnedSessionIdentity) {
		t.Fatalf("netpoll descriptor claim = %v, want identity mismatch", err)
	}
	after, err := unix.FcntlInt(uintptr(netpollFD), unix.F_GETFD, 0)
	if err != nil {
		t.Fatalf("netpoll descriptor was closed: %v", err)
	}
	if after != flags {
		t.Fatalf("netpoll descriptor flags = %#x, want unchanged %#x", after, flags)
	}
	accepted := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			err = connection.Close()
		}
		accepted <- err
	}()
	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
}
