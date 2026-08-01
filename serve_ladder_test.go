package daemonkit

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/paths"
)

type wedgedProduct struct {
	stubProduct
	release chan struct{}
}

func (p *wedgedProduct) Drain(Budget) error {
	<-p.release
	p.drained.Store(true)
	return nil
}

type bigReplyProduct struct {
	stubProduct
	body []byte
	stop func(error)
}

func (p *bigReplyProduct) Handle(context.Context, Request) (Reply, error) {
	defer p.stop(nil)
	return Reply{Body: p.body}, nil
}

type serveOutcome struct {
	drained Drained
	err     error
}

func serveInBackground(ctx context.Context, t *testing.T, d Daemon, start Start) <-chan serveOutcome {
	t.Helper()
	done := make(chan serveOutcome, 1)
	go func() {
		drained, err := Serve(ctx, d, start)
		done <- serveOutcome{drained, err}
	}()
	return done
}

func TestServeDrainReleasesBlockedStart(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dkblock", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := serveInBackground(ctx, t, d, func(c Ctx) (Product, error) {
		close(started)
		<-c.Context.Done()
		return &stubProduct{}, nil
	})

	<-started
	cancel()
	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("Serve() = %v", out.err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not return: a drain trigger left the activation context alive")
	}
}

// TestServeSettlesLargeReplyBeforeExit proves the ladder settles a terminal
// already in flight: the product initiates the drain from inside Handle, so the
// reply is written while the shutdown ladder is already running.
//
// The body is sized past the socket's own send buffer — 8 KiB on darwin — so
// the write cannot complete into it and has to be flushed against a reading
// peer, which is what a torn settlement would fail. A multi-MiB body proves
// nothing further and races the requests share's deadline for it.
func TestServeSettlesLargeReplyBeforeExit(t *testing.T) {
	shortHome(t)
	d := Daemon{
		Label:    "dkbig",
		Schemas:  []Schema{"test.v1"},
		Shutdown: Grace(5 * time.Second),
	}
	body := make([]byte, 64<<10)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand: %v", err)
	}
	product := &bigReplyProduct{body: body}
	done := serveInBackground(context.Background(), t, d, func(c Ctx) (Product, error) {
		product.stop = c.Stop
		return product, nil
	})

	socket, err := paths.Socket("dkbig")
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session := awaitBusinessSession(t, socket, int(d.MaxFrame))
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	result, err := session.Call(ctx, "product.echo.v1", "", nil)
	if err != nil {
		t.Fatalf("Call() = %v", err)
	}
	if rejection := result.Rejection(); rejection != nil {
		t.Fatalf("Call() rejected: %v", rejection)
	}
	var reply struct{ Body []byte }
	if err := json.Unmarshal(result.Response.Payload, &reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if !bytes.Equal(reply.Body, body) {
		t.Fatalf("reply body = %d bytes, want %d", len(reply.Body), len(body))
	}

	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("Serve() = %v", out.err)
		}
		if len(out.drained.Abandoned) != 0 {
			t.Fatalf("Abandoned = %v, want none", out.drained.Abandoned)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not return after the product-initiated drain")
	}
}

func TestServeAbandonedStageParksUntilSignalled(t *testing.T) {
	shortHome(t)
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	d := Daemon{Label: "dkpark", Schemas: []Schema{"test.v1"}, Shutdown: Grace(time.Second)}
	release := make(chan struct{})
	defer close(release)
	product := &wedgedProduct{release: release}
	done := serveInBackground(context.Background(), t, d, func(Ctx) (Product, error) { return product, nil })

	socket, err := paths.Socket("dkpark")
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	if _, err := session.Drain(ctx); err != nil {
		t.Fatalf("Drain() = %v", err)
	}

	select {
	case out := <-done:
		t.Fatalf("Serve returned %+v with a wedged product; want a parked process", out.drained)
	case <-time.After(3 * time.Second):
	}
	recordLock := proc.LockPath(d.RecordPath())
	if _, err := (flock.Spec{Path: recordLock, Mode: flock.Exclusive, Deadline: time.Second}).TryAcquire(); err == nil {
		t.Fatal("flock was released while parked over abandoned stages")
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signal self: %v", err)
	}
	select {
	case out := <-done:
		if len(out.drained.Abandoned) != 1 || out.drained.Abandoned[0] != StageProductDrain {
			t.Fatalf("Abandoned = %v, want [StageProductDrain]", out.drained.Abandoned)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the parked process did not leave on SIGTERM")
	}
}

func TestRunStage(t *testing.T) {
	tests := []struct {
		name    string
		budget  Budget
		run     func(chan<- struct{}) func(Budget) error
		wantRun bool
		want    bool
	}{
		{
			name:   "in-time success settles",
			budget: Grace(2 * time.Second).mint("t"),
			run: func(ran chan<- struct{}) func(Budget) error {
				return func(Budget) error { close(ran); return nil }
			},
			wantRun: true, want: true,
		},
		{
			name:   "in-time failure settles",
			budget: Grace(2 * time.Second).mint("t"),
			run: func(ran chan<- struct{}) func(Budget) error {
				return func(Budget) error { close(ran); return errors.New("stage failed") }
			},
			wantRun: true, want: true,
		},
		{
			name:   "expiry abandons",
			budget: Grace(100 * time.Millisecond).mint("t"),
			run: func(ran chan<- struct{}) func(Budget) error {
				return func(Budget) error { close(ran); time.Sleep(time.Second); return nil }
			},
			wantRun: true, want: false,
		},
		{
			name:   "an already-spent share never starts",
			budget: Budget{},
			run: func(ran chan<- struct{}) func(Budget) error {
				return func(Budget) error { close(ran); return nil }
			},
			wantRun: false, want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ran := make(chan struct{})
			got := runStage(tt.budget, tt.run(ran))
			if got != tt.want {
				t.Fatalf("runStage() = %t, want %t", got, tt.want)
			}
			started := false
			select {
			case <-ran:
				started = true
			case <-time.After(500 * time.Millisecond):
			}
			if started != tt.wantRun {
				t.Fatalf("stage started = %t, want %t", started, tt.wantRun)
			}
		})
	}
}

func TestRunStageClassifiesAgainstTheAbsoluteDeadline(t *testing.T) {
	tests := []struct {
		name string
		run  func(Budget) error
		want bool
	}{
		{
			name: "work that spends its whole share is abandoned",
			run: func(b Budget) error {
				ctx, cancel := b.Context(context.Background())
				defer cancel()
				<-ctx.Done()
				return ctx.Err()
			},
			want: false,
		},
		{
			name: "work that returns inside its share is settled",
			run:  func(Budget) error { return nil },
			want: true,
		},
		{
			name: "an in-share failure is still settled",
			run:  func(Budget) error { return errors.New("stage failed") },
			want: true,
		},
	}
	const iterations = 100
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := range iterations {
				if got := runStage(Grace(20*time.Millisecond).mint("t"), tt.run); got != tt.want {
					t.Fatalf("iteration %d: runStage() = %t, want %t", i, got, tt.want)
				}
			}
		})
	}
}

func awaitBusinessSession(t *testing.T, socket string, maxFrame int) *wire.Client {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		session, err := wire.NewClient(ctx, wire.ClientConfig{
			Dial: wire.UnixDialer(socket), Lane: wire.LaneBusiness, Schema: "test.v1", MaxFrame: maxFrame,
		})
		cancel()
		if err == nil {
			t.Cleanup(func() { _ = session.Abort(nil) })
			return session
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial business: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
