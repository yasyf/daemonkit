package daemonkit

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
)

// settleFixture stands the owner record up where a Daemon's own Label puts it:
// Settle derives the record path past the Label rule, so a fixture that named
// its own file would exercise a path no verb takes.
func settleFixture(t *testing.T) (Daemon, proc.Owner) {
	t.Helper()
	shortHome(t)
	d := Daemon{Label: "com.example.settle"}
	el, err := d.Label.element()
	if err != nil {
		t.Fatalf("element() error = %v", err)
	}
	if err := el.state().EnsureStateDir(); err != nil {
		t.Fatalf("EnsureStateDir: %v", err)
	}
	openCtx, cancelOpen := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelOpen()
	store, err := proc.OpenStore(openCtx, el.record())
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	owner, err := store.RecordOwner("b1")
	if err != nil {
		t.Fatalf("RecordOwner: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return d, owner
}

func TestClientSettle(t *testing.T) {
	absent := func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapAbsent, true, nil }
	reused := func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapReused, true, nil }
	live := func(proc.Identity) (proc.Reap, bool, error) { return 0, false, nil }
	tests := []struct {
		name     string
		missing  bool
		expect   Expect
		observe  func(proc.Identity) (proc.Reap, bool, error)
		wantReap Reap
		wantErr  error
	}{
		{name: "no owner record", missing: true, observe: absent, wantErr: ErrUnrecorded},
		{name: "expect build mismatch", expect: Expect{Build: "b2"}, observe: absent, wantErr: ErrWrongIncumbent},
		{name: "expect generation mismatch", expect: Expect{Generation: 1}, observe: absent, wantErr: ErrWrongIncumbent},
		{name: "settles absent", observe: absent, wantReap: ReapAbsent},
		{name: "settles reused", observe: reused, wantReap: ReapReused},
		{name: "unsettled at ctx end", observe: live, wantErr: ErrUnsettled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, owner := settleFixture(t)
			if tt.missing {
				d.Label = "com.example.settle.unrecorded"
			}
			expect := tt.expect
			if expect == (Expect{}) && !tt.missing && tt.wantErr == nil {
				expect = Expect{Build: owner.Build, Generation: owner.Generation}
			}
			client := &Client{
				daemon: d,
				observe: func(id proc.Identity) (proc.Reap, bool, error) {
					if !tt.missing && id != owner.Identity() {
						t.Fatalf("observed %+v, want the recorded owner core %+v", id, owner.Identity())
					}
					return tt.observe(id)
				},
				readOwner: proc.ReadOwner,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			stopped, err := client.Settle(ctx, expect)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Settle() error = %v, want %v", err, tt.wantErr)
				}
				if errors.Is(tt.wantErr, ErrUnsettled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Settle() error = %v, want joined ctx.Err()", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Settle() = %v", err)
			}
			if stopped.Reap != tt.wantReap {
				t.Fatalf("Reap = %d, want %d", stopped.Reap, tt.wantReap)
			}
			before := stopped.Before
			if before.PID != os.Getpid() || before.Generation != owner.Generation || before.Build != "b1" {
				t.Fatalf("Before = %+v, want the synthesized owner core", before)
			}
			if before.Phase != phaseInvalid || before.Protocol != 0 || before.Detail != nil {
				t.Fatalf("Before = %+v, want zero Phase, Protocol, and Detail", before)
			}
		})
	}
}

func TestSettleRequiresDeadline(t *testing.T) {
	client := &Client{readOwner: proc.ReadOwner}
	if _, err := client.Settle(context.Background(), Expect{}); err == nil {
		t.Fatal("Settle() without a deadline succeeded")
	}
}
