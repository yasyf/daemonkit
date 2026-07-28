package wire

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newDispatchPool(t *testing.T, workers, backlog int) *Server {
	t.Helper()
	server := &Server{
		queue: make(chan job, backlog),
		slots: make(chan struct{}, workers+backlog),
	}
	server.startWorkers(workers)
	return server
}

func awaitAdmitted(t *testing.T, server *Server, admitted int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(server.slots) == admitted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("admitted requests = %d, want %d", len(server.slots), admitted)
}

// TestDispatchRunsAdmittedRequestWithExpiredDeadline pins the half of the
// concurrent-pool contract that capacity accounting does not cover: slot
// acquisition is the only rejection point, so past it the handler always runs.
// An expired client DeadlineUnixMilli reaches dispatch with an already-done
// context and nothing rejects it earlier, so the queue is empty and a
// pre-enqueue context escape would be a coin flip against a ready send.
func TestDispatchRunsAdmittedRequestWithExpiredDeadline(t *testing.T) {
	const requests = 24
	server := newDispatchPool(t, 2, 4)
	defer server.stopWorkers()

	var ran atomic.Int64
	admitted := entry{class: classConcurrent, h: func(context.Context, Request) (any, error) {
		ran.Add(1)
		return "ran", nil
	}}
	expired := Frame{Op: "work", DeadlineUnixMilli: time.Now().Add(-time.Hour).UnixMilli()}

	dropped := 0
	for range requests {
		requestCtx, cancel := server.requestContext(context.Background(), expired)
		if requestCtx.Err() == nil {
			cancel()
			t.Fatal("expired deadline produced a live request context")
		}
		if depth := len(server.queue); depth != 0 {
			cancel()
			t.Fatalf("queue depth before dispatch = %d, want 0", depth)
		}
		value, err := server.dispatch(requestCtx, admitted, Request{Op: "work"})
		cancel()
		switch {
		case err == nil && value == "ran":
		case errors.Is(err, context.DeadlineExceeded):
			dropped++
		default:
			t.Fatalf("dispatch = %v, %v; want the handler result or a pre-enqueue drop", value, err)
		}
	}
	if dropped != 0 || ran.Load() != requests {
		t.Fatalf("admitted requests dropped = %d, handlers run = %d; want 0 dropped and %d run",
			dropped, ran.Load(), requests)
	}
}

// TestShutdownSettlesQueuedWork pins the no-drop half of the same contract:
// capacity admission is decoupled from worker readiness, so work admitted
// before shutdown cancels every request context still drains to its handler and
// the pool joins. The invariant was root-caused twice against a macOS CI hang
// (cc-notes b983140 and 9e4994e) and lost twice with the test that held it.
func TestShutdownSettlesQueuedWork(t *testing.T) {
	const workers, backlog = 1, 3
	const requests = workers + backlog
	server := newDispatchPool(t, workers, backlog)

	gate := make(chan struct{})
	var ran atomic.Int64
	admitted := entry{class: classConcurrent, h: func(context.Context, Request) (any, error) {
		<-gate
		ran.Add(1)
		return "ran", nil
	}}

	cancels := make([]context.CancelFunc, 0, requests)
	settled := make(chan error, requests)
	var wg sync.WaitGroup
	for range requests {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := server.dispatch(ctx, admitted, Request{Op: "work"})
			if err == nil && value != "ran" {
				err = errors.New("handler value lost")
			}
			settled <- err
		}()
	}
	awaitAdmitted(t, server, requests)

	for _, cancel := range cancels {
		cancel()
	}
	close(gate)
	wg.Wait()
	close(settled)

	for err := range settled {
		if err != nil {
			t.Errorf("admitted request settled with %v, want the handler result", err)
		}
	}
	if got := ran.Load(); got != requests {
		t.Fatalf("handlers run = %d, want %d", got, requests)
	}
	server.stopWorkers()
	if depth := len(server.queue); depth != 0 {
		t.Fatalf("queue depth after pool join = %d, want 0", depth)
	}
}
