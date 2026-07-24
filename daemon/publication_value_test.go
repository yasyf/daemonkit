package daemon

import (
	"errors"
	"testing"
)

func TestPublicationSlotValueUsesLiveAdmissionLease(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, 0, nil, nil)
	trig.ready(t, "admitted")
	publication, release, err := trig.runtime.admitReady()
	if err != nil {
		t.Fatal(err)
	}
	value, err := trig.slot.Value(publication)
	if err != nil || value != "admitted" {
		t.Fatalf("Value = %q, %v", value, err)
	}
	trig.runtime.lifecycle.mu.Lock()
	inflight := trig.runtime.lifecycle.inflight
	trig.runtime.lifecycle.mu.Unlock()
	if inflight != 1 {
		t.Fatalf("Value admission count = %d, want 1", inflight)
	}
	if err := trig.runtime.Drain(); err != nil {
		t.Fatal(err)
	}
	value, err = trig.slot.Value(publication)
	if err != nil || value != "admitted" {
		t.Fatalf("Value during drain = %q, %v", value, err)
	}
	release()
	if _, err := trig.slot.Value(publication); !errors.Is(err, ErrPublicationStale) {
		t.Fatalf("Value after release error = %v, want %v", err, ErrPublicationStale)
	}
	if err := closeRuntimeTest(t, trig.runtime); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationSlotValueRejectsWrongSlot(t *testing.T) {
	first := newRuntimeTestRig(t, nil, 0, nil, nil)
	first.ready(t, "first")
	publication, release, err := first.runtime.admitReady()
	if err != nil {
		t.Fatal(err)
	}
	second := newRuntimeTestRig(t, nil, 0, nil, nil)
	second.ready(t, "second")
	if _, err := second.slot.Value(publication); !errors.Is(err, ErrPublicationStale) {
		t.Fatalf("Value from wrong slot error = %v, want %v", err, ErrPublicationStale)
	}
	release()
	if err := closeRuntimeTest(t, first.runtime); err != nil {
		t.Fatal(err)
	}
	if err := closeRuntimeTest(t, second.runtime); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationSlotValueRejectsUnpublishedStage(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, 0, nil, nil)
	activation := trig.begin(t)
	publication, err := trig.slot.Stage(activation, "staged")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := trig.slot.Value(publication); !errors.Is(err, ErrPublicationStale) {
		t.Fatalf("Value before publication error = %v, want %v", err, ErrPublicationStale)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	if err := closeRuntimeTest(t, trig.runtime); err != nil {
		t.Fatal(err)
	}
}
