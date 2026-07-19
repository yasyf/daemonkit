// Package wiretest is the in-process harness for wire's transport and peer
// tests: short-path socket dirs, a real client/server pair, an injectable
// peer, and a manually-advanced clock mirroring proc's seam.
package wiretest

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

// SocketDir returns a fresh directory short enough for a unix socket path
// (macOS caps sun_path at 104 bytes; t.TempDir routinely exceeds it), removed
// on t's cleanup.
func SocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", fmt.Sprintf("dk-%d-", os.Getpid()))
	if err != nil {
		t.Fatalf("wiretest: mkdir socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// Pair returns a connected client/server unix-socket pair. Both ends live in this
// process, so PeerFromConn on either reports this process's own uid and pid.
func Pair(t *testing.T) (client, server *net.UnixConn) {
	t.Helper()
	sock := filepath.Join(SocketDir(t), "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("wiretest: listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	type accepted struct {
		conn net.Conn
		err  error
	}
	ch := make(chan accepted, 1)
	go func() {
		conn, err := ln.Accept()
		ch <- accepted{conn, err}
	}()

	dialed, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("wiretest: dial: %v", err)
	}
	a := <-ch
	if a.err != nil {
		t.Fatalf("wiretest: accept: %v", a.err)
	}
	client = dialed.(*net.UnixConn)
	server = a.conn.(*net.UnixConn)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

type peerKey struct{}

// WithPeer returns a context carrying p for handler tests that need a peer
// identity. Production peers come from wire.PeerFromConn; this seam is test-only.
func WithPeer(ctx context.Context, p wire.Peer) context.Context {
	return context.WithValue(ctx, peerKey{}, p)
}

// PeerFrom returns the peer WithPeer stored on ctx.
func PeerFrom(ctx context.Context) (wire.Peer, bool) {
	p, ok := ctx.Value(peerKey{}).(wire.Peer)
	return p, ok
}

// Clock is the time seam mirrored from proc: real wall time in production, a
// FakeClock in tests.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// FakeClock is a manually-advanced Clock: Now moves only on Advance, which fires
// every waiter whose deadline has arrived.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
}

type fakeWaiter struct {
	at time.Time
	ch chan time.Time
}

var _ Clock = (*FakeClock)(nil)

// NewFakeClock returns a FakeClock reading start until the first Advance.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that fires once the fake clock reaches now+d. A
// non-positive d fires immediately.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	at := c.now.Add(d)
	if !at.After(c.now) {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, fakeWaiter{at: at, ch: ch})
	return ch
}

// Advance moves the clock forward by d and fires every waiter now due.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if w.at.After(c.now) {
			kept = append(kept, w)
			continue
		}
		w.ch <- c.now
	}
	c.waiters = kept
}
