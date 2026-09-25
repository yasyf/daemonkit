package daemonkit

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

func preservingOwned(t *testing.T) (*Owned, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owned.dkstate")
	o, err := OwnProcessesWithPolicy(bounded(t, 5*time.Second), path, PreserveOwned)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		o.mu.Lock()
		drain := o.drain
		o.mu.Unlock()
		if drain != nil && !drain.committed {
			_ = drain.Abort()
		}
		if err := o.Close(bounded(t, 5*time.Second)); err != nil {
			t.Error(err)
		}
	})
	return o, path
}

func TestNaturalDrainIncludesPendingAdoptionAndKeepsTimedOutLease(t *testing.T) {
	o, _ := preservingOwned(t)
	reservation, err := o.reserve("Adopt")
	if err != nil {
		t.Fatal(err)
	}
	defer o.abandon(reservation)
	drain, err := o.BeginNaturalDrain(bounded(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := o.Observe(bounded(t, time.Second))
	if err != nil || observed.Complete() || !reflect.DeepEqual(observed.Pending, []string{"Adopt"}) || observed.Coverage != RecordedScopeCoverage {
		t.Fatalf("Observe() = %+v, %v", observed, err)
	}
	_, err = drain.Wait(bounded(t, 25*time.Millisecond))
	if !errors.Is(err, ErrDrainPreparationTimeout) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrUnsettled) {
		t.Fatalf("Wait() = %v", err)
	}
	if _, err := o.reserve("Spawn"); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("reserve while paused = %v", err)
	}
	if err := o.Close(bounded(t, time.Second)); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("Close while reversible = %v", err)
	}
	if err := drain.Abort(); err != nil {
		t.Fatal(err)
	}
	next, err := o.reserve("Spawn")
	if err != nil {
		t.Fatalf("admissions did not reopen: %v", err)
	}
	o.abandon(next)
}

func TestNaturalDrainWaitsForAdmittedRegistration(t *testing.T) {
	o, _ := preservingOwned(t)
	reservation, err := o.reserve("Adopt")
	if err != nil {
		t.Fatal(err)
	}
	defer o.abandon(reservation)
	drain, err := o.BeginNaturalDrain(bounded(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		proof, err := drain.Wait(bounded(t, time.Second))
		if err == nil {
			err = drain.Commit(proof)
		}
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("proof completed over pending adoption: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	o.abandon(reservation)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestNaturalDrainCannotDiscardUnprovenAdoption(t *testing.T) {
	o, _ := preservingOwned(t)
	reservation, err := o.reserve("Adopt")
	if err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	reservation.unproven = true
	o.mu.Unlock()
	t.Cleanup(func() {
		o.mu.Lock()
		reservation.unproven = false
		o.mu.Unlock()
		o.abandon(reservation)
	})
	o.abandon(reservation)
	select {
	case <-reservation.done:
		t.Fatal("unproven adoption was discharged")
	default:
	}
	observed, err := o.Observe(bounded(t, time.Second))
	if err != nil || !observed.Uncertain || observed.Complete() || !reflect.DeepEqual(observed.Pending, []string{"Adopt (registration unproven)"}) {
		t.Fatalf("Observe() = %+v, %v", observed, err)
	}
	drain, err := o.BeginNaturalDrain(bounded(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := drain.Wait(bounded(t, 25*time.Millisecond)); !errors.Is(err, ErrDrainPreparationTimeout) {
		t.Fatalf("Wait() = %v", err)
	}
	if err := drain.Abort(); err != nil {
		t.Fatal(err)
	}
	observed, err = o.Observe(bounded(t, time.Second))
	if err != nil || observed.Complete() || !observed.Uncertain {
		t.Fatalf("Abort discarded uncertainty: %+v, %v", observed, err)
	}
}

func TestNaturalProofCannotCrossLeasesOrOwners(t *testing.T) {
	o, _ := preservingOwned(t)
	first, err := o.BeginNaturalDrain(bounded(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	old, err := first.Wait(bounded(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Abort(); err != nil {
		t.Fatal(err)
	}
	second, err := o.BeginNaturalDrain(bounded(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(old); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("old lease proof = %v", err)
	}
	if err := first.Commit(old); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("aborted lease proof = %v", err)
	}
	other, _ := preservingOwned(t)
	foreign, err := other.BeginNaturalDrain(bounded(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := second.Wait(bounded(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := foreign.Commit(proof); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("foreign proof = %v", err)
	}
	if err := second.Commit(NaturalProof{}); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("zero proof = %v", err)
	}
	newProof, err := second.Wait(bounded(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(proof); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("superseded proof = %v", err)
	}
	if err := second.Commit(newProof); err != nil {
		t.Fatal(err)
	}
	if err := second.Abort(); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("committed lease reopened: %v", err)
	}
	if _, err := o.reserve("Adopt"); !errors.Is(err, errScopeSettling) {
		t.Fatalf("committed admissions = %v", err)
	}
}

func TestNaturalDrainRejectsForeignProducer(t *testing.T) {
	o, _ := preservingOwned(t)
	if _, err := o.BeginNaturalDrain(bounded(t, time.Second), []*Child{{}}); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("foreign producer = %v", err)
	}
	reservation, err := o.reserve("Adopt")
	if err != nil {
		t.Fatalf("invalid producer held admission: %v", err)
	}
	o.abandon(reservation)
}

func TestPreserveOwnedRefusesDisposableRun(t *testing.T) {
	o, _ := preservingOwned(t)
	_, err := o.Run(bounded(t, time.Second), Cmd{Path: "/bin/cat", Exec: ServingSameUser()})
	if err == nil || !strings.Contains(err.Error(), "Spawn and WaitNatural") {
		t.Fatalf("Run() = %v", err)
	}
	observed, err := o.Observe(bounded(t, time.Second))
	if err != nil || !observed.Complete() || len(observed.Scopes) != 0 {
		t.Fatalf("refused Run admitted work: %+v, %v", observed, err)
	}
}

func TestPreserveProducerStaysAliveThroughProof(t *testing.T) {
	o, _ := preservingOwned(t)
	child, err := o.Spawn(bounded(t, 5*time.Second), Cmd{Path: "/bin/cat", Exec: ServingSameUser(), Session: true}, ChannelStdio, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := child.Conn()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_, _ = child.Stop(bounded(t, 5*time.Second))
	})
	drain, err := o.Ctx(t.Context()).BeginNaturalDrain(bounded(t, time.Second), []*Child{child})
	if err != nil {
		t.Fatal(err)
	}
	proof, err := drain.Wait(bounded(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	observed, err := child.Observe(bounded(t, time.Second))
	if err != nil || observed.Quiet() || len(observed.Members) != 1 {
		t.Fatalf("producer did not survive proof: %+v, %v", observed, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := child.WaitNatural(bounded(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := drain.Commit(proof); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(bounded(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestPreserveCloseTimeoutRetainsOwnershipLock(t *testing.T) {
	o, path := preservingOwned(t)
	child, err := o.Spawn(bounded(t, 5*time.Second), Cmd{Path: "/bin/cat", Exec: ServingSameUser(), Session: true}, ChannelStdio, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := child.Conn()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_, _ = child.Stop(bounded(t, 5*time.Second))
	})
	if err := o.Close(bounded(t, 25*time.Millisecond)); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("Close() = %v", err)
	}
	if _, err := OwnProcesses(bounded(t, 25*time.Millisecond), path); !errors.Is(err, durable.ErrLockBusy) {
		t.Fatalf("timed out preservation released ownership: %v", err)
	}
	observation, err := child.Observe(bounded(t, time.Second))
	if err != nil || observation.Quiet() {
		t.Fatalf("Close disturbed producer: %+v, %v", observation, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(bounded(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.Done():
	default:
		if _, err := child.WaitNatural(bounded(t, time.Second)); err != nil {
			t.Fatalf("Close did not join child publication: %v", err)
		}
	}
}
