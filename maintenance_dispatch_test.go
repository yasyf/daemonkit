package daemonkit

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

type evidenceProduct struct {
	*preparationProduct
	ordinary atomic.Int32
	evidence atomic.Int32
}

func (p *evidenceProduct) Handle(context.Context, Request) (Reply, error) {
	p.ordinary.Add(1)
	return Reply{}, nil
}

func (p *evidenceProduct) HandleMaintenance(_ context.Context, req Request) (Reply, error) {
	if req.Op != "evidence" {
		return Reply{}, ErrMaintenance
	}
	p.evidence.Add(1)
	return Reply{Body: []byte("evidence")}, nil
}

func maintenanceBusiness(t *testing.T, r *serveRuntime) *wire.Client {
	t.Helper()
	server, err := wire.NewServer(r, wire.Config{Schemas: wire.Schemas{"test.v1"}, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(wiretest.SocketDir(t), "maintenance")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	client, err := wire.NewClient(drainContext(t), wire.ClientConfig{Dial: wire.UnixDialer(socket), Authorize: wiretest.AuthorizeTestServer, Lane: wire.LaneBusiness, Schema: "test.v1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Abort(nil) })
	return client
}

func TestMaintenanceDispatchNeverFallsBackToOrdinaryHandle(t *testing.T) {
	product := &evidenceProduct{preparationProduct: &preparationProduct{prepare: func(Budget) (DrainPreparation, error) { return nil, ErrDrainBusy }}}
	runtime := preservingRuntime(t, product)
	client := maintenanceBusiness(t, runtime)
	runtime.publish(wire.PhaseMaintenance)
	allowed, err := client.Call(drainContext(t), "evidence", nil)
	if err != nil || allowed.Rejection() != nil || allowed.Terminal() != nil {
		t.Fatalf("evidence=%+v err=%v", allowed, err)
	}
	denied, err := client.Call(drainContext(t), "new-job", nil)
	if err != nil || !errors.Is(denied.Rejection(), ErrMaintenance) {
		t.Fatalf("new-job=%+v err=%v", denied, err)
	}
	if product.ordinary.Load() != 0 || product.evidence.Load() != 1 {
		t.Fatalf("ordinary=%d evidence=%d", product.ordinary.Load(), product.evidence.Load())
	}
	requireNotStopped(t, runtime)
}

func TestMaintenanceWithoutExplicitHandlerRejectsAllBusiness(t *testing.T) {
	product := &preparationProduct{prepare: func(Budget) (DrainPreparation, error) { return nil, ErrDrainBusy }}
	runtime := preservingRuntime(t, product)
	client := maintenanceBusiness(t, runtime)
	runtime.publish(wire.PhaseMaintenance)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := client.Call(ctx, "evidence", nil)
	if err != nil || !errors.Is(result.Rejection(), ErrMaintenance) {
		t.Fatalf("Call=%+v err=%v", result, err)
	}
}
