package daemonkit

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestControlRefusesAnUntrustedServer(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dktrust", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	child := startControlChild(t, string(d.Label))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	warmup := awaitControl(ctx, t, openClient(t, d))
	if err := warmup.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	pinned := d
	pinned.Trust.Serving = ServingSigned(Requirement{
		TeamID:            "SXKCTF23Q2",
		SigningIdentifier: "com.yasyf.daemonkit.not-this-binary",
	})
	pinnedClient, err := Open(pinned)
	if err != nil {
		t.Fatalf("Open(pinned) = %v", err)
	}
	control, err := pinnedClient.Control(ctx)
	if !errors.Is(err, ErrUntrusted) {
		if err == nil {
			_ = control.Close(ctx)
		}
		t.Fatalf("Control() = %v, want ErrUntrusted for a daemon that cannot prove the deployed identity", err)
	}

	unpinned := awaitControl(ctx, t, openClient(t, d))
	if _, err := unpinned.Drain(ctx, Expect{}); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	if err := child.Wait(); err != nil {
		t.Fatalf("child exit = %v", err)
	}
}
