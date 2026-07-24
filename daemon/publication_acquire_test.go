package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPublicationSlotAcquireFollowsReadyLifecycle(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, 0, nil, nil)
	activation := trig.begin(t)
	publication, err := trig.slot.Stage(activation, "ready")
	if err != nil {
		t.Fatal(err)
	}
	if _, release, err := trig.slot.Acquire(); !errors.Is(err, ErrRuntimeNotReady) || release != nil {
		t.Fatalf("Acquire while Starting = release %v err %v", release != nil, err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	value, release, err := trig.slot.Acquire()
	if err != nil || release == nil || value != "ready" {
		t.Fatalf("Acquire while Ready = value %q release %v err %v", value, release != nil, err)
	}
	trig.runtime.lifecycle.mu.Lock()
	inflight := trig.runtime.lifecycle.inflight
	trig.runtime.lifecycle.mu.Unlock()
	if inflight != 1 {
		t.Fatalf("Ready admission count = %d, want 1", inflight)
	}
	release()
	release()
	trig.runtime.lifecycle.mu.Lock()
	inflight = trig.runtime.lifecycle.inflight
	trig.runtime.lifecycle.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("idempotent release admission count = %d, want 0", inflight)
	}
	if err := trig.runtime.Drain(); err != nil {
		t.Fatal(err)
	}
	if _, release, err := trig.slot.Acquire(); !errors.Is(err, ErrDraining) || release != nil {
		t.Fatalf("Acquire while Draining = release %v err %v", release != nil, err)
	}
	if err := closeRuntimeTest(t, trig.runtime); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationSlotAcquirePinsDrainSettlement(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, 0, nil, nil)
	trig.ready(t, "pinned")
	value, release, err := trig.slot.Acquire()
	if err != nil || value != "pinned" {
		t.Fatalf("Acquire = %q, %v", value, err)
	}
	if err := trig.runtime.Drain(); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- trig.runtime.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before publication release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if value != "pinned" {
		t.Fatalf("pinned value during drain = %q", value)
	}
	release()
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(runtimeTestTimeout):
		t.Fatal("Close did not settle after publication release")
	}
}

func TestPublicationSlotAcquireConcurrentDrain(t *testing.T) {
	const count = 64
	trig := newRuntimeTestRig(t, nil, 0, nil, nil)
	trig.ready(t, "concurrent")
	type acquisition struct {
		value   string
		release func()
		err     error
	}
	acquired := make(chan acquisition, count)
	releaseAll := make(chan struct{})
	var released sync.WaitGroup
	released.Add(count)
	for range count {
		go func() {
			value, release, err := trig.slot.Acquire()
			acquired <- acquisition{value: value, release: release, err: err}
			<-releaseAll
			if release != nil {
				release()
				release()
			}
			released.Done()
		}()
	}
	var acquireFailure string
	for range count {
		result := <-acquired
		if acquireFailure == "" && (result.err != nil || result.release == nil || result.value != "concurrent") {
			acquireFailure = fmt.Sprintf("concurrent Acquire = value %q release %v err %v",
				result.value, result.release != nil, result.err)
		}
	}
	if acquireFailure != "" {
		close(releaseAll)
		released.Wait()
		t.Fatal(acquireFailure)
	}
	trig.runtime.lifecycle.mu.Lock()
	inflight := trig.runtime.lifecycle.inflight
	trig.runtime.lifecycle.mu.Unlock()
	if inflight != count {
		t.Fatalf("concurrent admission count = %d, want %d", inflight, count)
	}
	if err := trig.runtime.Drain(); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- trig.runtime.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned with concurrent pins: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseAll)
	released.Wait()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(runtimeTestTimeout):
		t.Fatal("Close did not settle concurrent publication pins")
	}
}

func TestPublicationSlotAcquirePinsGenerationAcrossReplacement(t *testing.T) {
	old := newRuntimeTestRig(t, nil, 0, nil, nil)
	old.ready(t, "old")
	oldValue, releaseOld, err := old.slot.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if err := old.runtime.Drain(); err != nil {
		t.Fatal(err)
	}

	replacement := newRuntimeTestRig(t, nil, 0, nil, nil)
	replacement.ready(t, "new")
	newValue, releaseNew, err := replacement.slot.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if oldValue != "old" || newValue != "new" {
		t.Fatalf("generation values = old %q new %q", oldValue, newValue)
	}
	releaseNew()
	if err := closeRuntimeTest(t, replacement.runtime); err != nil {
		t.Fatal(err)
	}

	closedOld := make(chan error, 1)
	go func() { closedOld <- old.runtime.Close(context.Background()) }()
	select {
	case err := <-closedOld:
		t.Fatalf("old runtime settled before old generation release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOld()
	select {
	case err := <-closedOld:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(runtimeTestTimeout):
		t.Fatal("old runtime did not settle after old generation release")
	}
}
