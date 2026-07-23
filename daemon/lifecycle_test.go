package daemon

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestLifecycleSnapshotCopiesDetail(t *testing.T) {
	lifecycle := newLifecycle()
	lifecycle.mu.Lock()
	lifecycle.publishLocked(1, LifecycleStarting, []byte("preparing"))
	lifecycle.mu.Unlock()

	first := lifecycle.Snapshot()
	first.Detail[0] = 'X'
	second := lifecycle.Snapshot()
	if string(second.Detail) != "preparing" {
		t.Fatalf("snapshot detail = %q", second.Detail)
	}
}

func TestLifecycleStartingReservesFinalTwoSequences(t *testing.T) {
	lifecycle := newLifecycle()
	lifecycle.mu.Lock()
	lifecycle.progress = LifecycleProgress{Sequence: math.MaxUint64 - 2, State: LifecycleStarting, Detail: []byte("stable")}
	before := cloneLifecycleProgress(lifecycle.progress)
	err := lifecycle.advanceStartingProgressLocked([]byte("new"))
	after := cloneLifecycleProgress(lifecycle.progress)
	lifecycle.mu.Unlock()
	if !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("progress update = %v, want ErrSequenceExhausted", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("progress mutated: before=%+v after=%+v", before, after)
	}
}

func TestLifecycleTerminalMayConsumeFinalSequence(t *testing.T) {
	lifecycle := newLifecycle()
	lifecycle.mu.Lock()
	lifecycle.progress = LifecycleProgress{Sequence: math.MaxUint64 - 1, State: LifecycleReady, Detail: []byte("ready")}
	err := lifecycle.advanceTerminalLocked(LifecycleDraining, lifecycle.progress.Detail)
	after := cloneLifecycleProgress(lifecycle.progress)
	lifecycle.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if after.Sequence != math.MaxUint64 || after.State != LifecycleDraining || string(after.Detail) != "ready" {
		t.Fatalf("terminal progress = %+v", after)
	}
}

func TestLifecycleSequenceFatalPreservesSemanticState(t *testing.T) {
	lifecycle := newLifecycle()
	activity := &activityState{alive: true}
	lifecycle.mu.Lock()
	lifecycle.progress = LifecycleProgress{Sequence: math.MaxUint64, State: LifecycleStarting, Detail: []byte("stable")}
	lifecycle.publication = &publicationCore{staged: "value", stagedSet: true, nextStage: 9}
	lifecycle.activities[4] = activity
	before := cloneLifecycleProgress(lifecycle.progress)
	lifecycle.mu.Unlock()
	err := lifecycle.fail()
	lifecycle.mu.Lock()
	after := cloneLifecycleProgress(lifecycle.progress)
	fatal := lifecycle.fatal
	staged, stagedSet, nextStage := lifecycle.publication.staged, lifecycle.publication.stagedSet, lifecycle.publication.nextStage
	_, activityPresent := lifecycle.activities[4]
	activityAlive := activity.alive
	lifecycle.mu.Unlock()
	if !errors.Is(err, ErrSequenceExhausted) || !errors.Is(fatal, ErrSequenceExhausted) {
		t.Fatalf("fail=%v fatal=%v", err, fatal)
	}
	if !reflect.DeepEqual(after, before) || staged != "value" || !stagedSet || nextStage != 9 || !activityPresent || !activityAlive {
		t.Fatalf("semantic state changed: progress=%+v staged=%v/%v/%d activity=%v/%v", after, staged, stagedSet, nextStage, activityPresent, activityAlive)
	}
}

func TestLifecycleFatalWaitReturnsUnchangedWireSnapshot(t *testing.T) {
	lifecycle := newLifecycle()
	lifecycle.mu.Lock()
	lifecycle.progress = LifecycleProgress{Sequence: math.MaxUint64, State: LifecycleStarting, Detail: []byte("stable")}
	lifecycle.setFatalLocked(ErrSequenceExhausted)
	lifecycle.mu.Unlock()
	progress, err := lifecycle.WaitChange(context.Background(), math.MaxUint64)
	if !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("WaitChange = %v, want ErrSequenceExhausted", err)
	}
	if progress.Sequence != math.MaxUint64 || progress.State != LifecycleStarting || string(progress.Detail) != "stable" {
		t.Fatalf("wire progress = %+v", progress)
	}
}
