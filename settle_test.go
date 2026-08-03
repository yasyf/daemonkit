package daemonkit

import (
	"context"
	"errors"
	"os"
	"os/exec"
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

// liveOwnerFixture stands a real daemon child up and waits for the owner record
// Serve writes before it binds. That is the only way a record names a live
// process other than this one: a real store records only the process that opens
// it, so the in-process fixture can never name anything but this one.
func liveOwnerFixture(t *testing.T, label Label) (Daemon, *exec.Cmd, proc.Owner) {
	t.Helper()
	shortHome(t)
	d := Daemon{Label: label, Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	child := startControlChild(t, string(d.Label))
	record := d.RecordPath()
	deadline := time.Now().Add(20 * time.Second)
	for {
		owner, ok, err := proc.ReadOwner(record)
		if err != nil {
			t.Fatalf("ReadOwner(%q) = %v", record, err)
		}
		if ok && owner.PID == child.Process.Pid {
			return d, child, owner
		}
		if time.Now().After(deadline) {
			t.Fatalf("the daemon child never recorded itself at %q", record)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// departedOwnerFixture is liveOwnerFixture's incumbent reaped, so the record it
// leaves behind names a process the kernel no longer answers for.
func departedOwnerFixture(t *testing.T) (Daemon, proc.Owner) {
	t.Helper()
	d, child, owner := liveOwnerFixture(t, "dkgone")
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill the recorded daemon: %v", err)
	}
	_ = child.Wait()
	if _, err := proc.ProbeIdentity(owner.PID); !errors.Is(err, proc.ErrNoProcess) {
		t.Fatalf("ProbeIdentity(%d) = %v after the child was reaped, want %v", owner.PID, err, proc.ErrNoProcess)
	}
	return d, owner
}

func TestClientSettle(t *testing.T) {
	tests := []struct {
		name    string
		missing bool
		expect  Expect
		wantErr error
	}{
		{name: "no owner record", missing: true, wantErr: ErrUnrecorded},
		{name: "expect build mismatch", expect: Expect{Build: "b2"}, wantErr: ErrWrongIncumbent},
		{name: "expect generation mismatch", expect: Expect{Generation: 1}, wantErr: ErrWrongIncumbent},
		{name: "unsettled at ctx end", wantErr: ErrUnsettled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, owner := settleFixture(t)
			if owner.PID != os.Getpid() {
				t.Fatalf("recorded owner PID = %d, want this process %d", owner.PID, os.Getpid())
			}
			if tt.missing {
				d.Label = "com.example.settle.unrecorded"
			}
			client := &Client{daemon: d}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_, err := client.Settle(ctx, tt.expect)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Settle() error = %v, want %v", err, tt.wantErr)
			}
			if errors.Is(tt.wantErr, ErrUnsettled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Settle() error = %v, want joined ctx.Err()", err)
			}
		})
	}
}

// TestClientSettleSynthesizesTheRecordedCore pins what a settled proof carries.
// The recorded incumbent has to be a process that departs, so it is a real
// child that opened the store, recorded itself, and exited — the only way a
// record names anything but the process reading it.
func TestClientSettleSynthesizesTheRecordedCore(t *testing.T) {
	d, owner := departedOwnerFixture(t)
	client := &Client{daemon: d}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped, err := client.Settle(ctx, Expect{Build: owner.Build, Generation: owner.Generation})
	if err != nil {
		t.Fatalf("Settle() = %v", err)
	}
	if stopped.Reap != ReapAbsent {
		t.Fatalf("Reap = %d, want ReapAbsent", stopped.Reap)
	}
	before := stopped.Before
	if before.PID != owner.PID || before.Generation != owner.Generation || before.Build != owner.Build {
		t.Fatalf("Before = %+v, want the synthesized owner core %+v", before, owner)
	}
	if before.Phase != phaseInvalid || before.Protocol != 0 || before.Detail != nil {
		t.Fatalf("Before = %+v, want zero Phase, Protocol, and Detail", before)
	}
}

func TestSettleRequiresDeadline(t *testing.T) {
	client := &Client{}
	if _, err := client.Settle(context.Background(), Expect{}); err == nil {
		t.Fatal("Settle() without a deadline succeeded")
	}
}
