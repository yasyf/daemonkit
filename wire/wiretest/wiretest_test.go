package wiretest_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/wiretest"
)

func TestFakeClock(t *testing.T) {
	start := time.Unix(0, 0)
	c := wiretest.NewFakeClock(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Now = %v, want %v", c.Now(), start)
	}

	ch := c.After(10 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("After fired before any Advance")
	default:
	}

	c.Advance(5 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("After fired at half the deadline")
	default:
	}

	c.Advance(5 * time.Millisecond)
	want := start.Add(10 * time.Millisecond)
	select {
	case got := <-ch:
		if !got.Equal(want) {
			t.Errorf("After fired at %v, want %v", got, want)
		}
	default:
		t.Fatal("After did not fire once the deadline arrived")
	}
	if !c.Now().Equal(want) {
		t.Errorf("Now = %v, want %v", c.Now(), want)
	}
}

func TestFakeClockImmediate(t *testing.T) {
	c := wiretest.NewFakeClock(time.Unix(0, 0))
	select {
	case <-c.After(0):
	default:
		t.Fatal("After(0) did not fire immediately")
	}
}

func TestWithPeer(t *testing.T) {
	ctx := context.Background()
	if _, ok := wiretest.PeerFrom(ctx); ok {
		t.Fatal("PeerFrom on a bare context ok = true, want false")
	}
	want := wire.Peer{PID: 42, UID: 501, Audit: []byte{1, 2, 3}}
	got, ok := wiretest.PeerFrom(wiretest.WithPeer(ctx, want))
	if !ok {
		t.Fatal("PeerFrom after WithPeer ok = false, want true")
	}
	if got.PID != want.PID || got.UID != want.UID || !bytes.Equal(got.Audit, want.Audit) {
		t.Errorf("PeerFrom = %+v, want %+v", got, want)
	}
}

func TestPairIsConnected(t *testing.T) {
	client, server := wiretest.Pair(t)
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := server.Read(buf); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if string(buf) != "ping" {
		t.Errorf("server read = %q, want %q", buf, "ping")
	}
}
