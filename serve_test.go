package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/paths"
)

type stubProduct struct {
	drained atomic.Bool
	closed  atomic.Bool
}

func (p *stubProduct) Handle(context.Context, Request) (Reply, error) {
	return Reply{}, errors.New("unused")
}

func (p *stubProduct) Drain(Budget) error {
	p.drained.Store(true)
	return nil
}

func (p *stubProduct) Close(Budget) error {
	p.closed.Store(true)
	return nil
}

func shortHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", fmt.Sprintf("dk-%d-", os.Getpid()))
	if err != nil {
		t.Fatalf("mkdir home: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv(realhome.EnvOverride, home)
	return home
}

func TestServeDrainVerbDrivesReturn(t *testing.T) {
	shortHome(t)
	d := Daemon{
		Label:    "dkt",
		Schemas:  []Schema{"test.v1"},
		Shutdown: Grace(5 * time.Second),
	}
	product := &stubProduct{}
	type outcome struct {
		drained Drained
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		drained, err := Serve(context.Background(), d, func(Ctx) (Product, error) { return product, nil })
		done <- outcome{drained, err}
	}()

	socket, err := paths.Socket("dkt")
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	report, err := session.Health(ctx)
	if err != nil {
		t.Fatalf("Health() = %v", err)
	}
	if report.PID != os.Getpid() || report.Generation == 0 || report.Build == "" {
		t.Fatalf("Health() = %+v, want this process's identity", report)
	}
	result, err := session.Drain(ctx)
	if err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	if result.Outcome != wire.Delivered || result.Response.Rejected {
		t.Fatalf("Drain() = %+v, want a delivered ack", result)
	}

	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("Serve() = %v", out.err)
		}
		if len(out.drained.Abandoned) != 0 {
			t.Fatalf("Abandoned = %v, want none", out.drained.Abandoned)
		}
		wantSettled := []Stage{StageIntake, StageRequests, StageProductDrain, StageProductClose, StageChildren}
		if len(out.drained.Settled) != len(wantSettled) {
			t.Fatalf("Settled = %v, want %v", out.drained.Settled, wantSettled)
		}
		for i, stage := range wantSettled {
			if out.drained.Settled[i] != stage {
				t.Fatalf("Settled = %v, want %v", out.drained.Settled, wantSettled)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after the drain verb")
	}
	if !product.drained.Load() || !product.closed.Load() {
		t.Fatalf("product drained=%t closed=%t, want both", product.drained.Load(), product.closed.Load())
	}

	lock, err := flock.Spec{Path: socket + ".lock", Mode: flock.Exclusive, Deadline: time.Second}.TryAcquire()
	if err != nil {
		t.Fatalf("flock after Serve returned = %v, want released", err)
	}
	_ = lock.Close()
}

func TestServeRefusesLiveIncumbent(t *testing.T) {
	shortHome(t)
	d := Daemon{
		Label:    "dkt",
		Schemas:  []Schema{"test.v1"},
		Shutdown: Grace(5 * time.Second),
	}
	product := &stubProduct{}
	done := make(chan error, 1)
	go func() {
		_, err := Serve(context.Background(), d, func(Ctx) (Product, error) { return product, nil })
		done <- err
	}()
	socket, err := paths.Socket("dkt")
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)

	if _, err := Serve(context.Background(), d, func(Ctx) (Product, error) { return product, nil }); !errors.Is(err, ErrBusy) {
		t.Fatalf("second Serve() = %v, want ErrBusy", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := session.Drain(ctx); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after the drain verb")
	}
}

func awaitControlSession(t *testing.T, socket string) *wire.Client {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		session, err := wire.NewClient(ctx, wire.ClientConfig{
			Dial: wire.UnixDialer(socket), Lane: wire.LaneControl,
		})
		cancel()
		if err == nil {
			t.Cleanup(func() { _ = session.Abort(nil) })
			return session
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial control: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
