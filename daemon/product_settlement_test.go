package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

func TestProductSettlementBlocksLowerRuntimeClosureUntilProductJoin(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, time.Second, nil, nil)
	activation := trig.begin(t)
	settlement, err := activation.ClaimProductSettlement()
	if err != nil {
		t.Fatal(err)
	}
	if err := settlement.Complete(); !errors.Is(err, ErrProductSettlementActive) {
		t.Fatalf("Complete before cancellation = %v, want active", err)
	}
	publication, err := trig.slot.Stage(activation, "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	health, err := trig.runtime.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.Busy {
		t.Fatal("healthy Ready product settlement reported busy")
	}

	cleanupEntered := make(chan struct{})
	allowComplete := make(chan struct{})
	completeResult := make(chan error, 1)
	go func() {
		<-activation.Context().Done()
		close(cleanupEntered)
		<-allowComplete
		completeResult <- settlement.Complete()
	}()
	closeResult := make(chan error, 1)
	go func() { closeResult <- trig.runtime.Close(context.Background()) }()
	select {
	case <-cleanupEntered:
	case <-time.After(runtimeTestTimeout):
		t.Fatal("product cleanup did not observe generation cancellation")
	}
	health, err = trig.runtime.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !health.Busy || !health.Draining {
		t.Fatalf("awaited product settlement health = %+v", health)
	}

	trig.runtime.mu.Lock()
	workerClaimLive := trig.runtime.workerClaim != nil && trig.runtime.workerActivated
	childrenLive := trig.runtime.childrenClaimed
	finished := trig.runtime.finished
	trig.runtime.mu.Unlock()
	trig.runtime.lifecycle.mu.Lock()
	publicationLive := trig.runtime.lifecycle.publication != nil &&
		trig.runtime.lifecycle.publication.publishedSet
	trig.runtime.lifecycle.mu.Unlock()
	if !workerClaimLive || !childrenLive || finished || !publicationLive {
		t.Fatalf(
			"lower ownership before product proof: workers=%v children=%v finished=%v publication=%v",
			workerClaimLive, childrenLive, finished, publicationLive,
		)
	}
	select {
	case err := <-closeResult:
		t.Fatalf("runtime closed before product proof: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(allowComplete)
	if err := <-completeResult; err != nil {
		t.Fatalf("Complete after join: %v", err)
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close after product proof: %v", err)
		}
	case <-time.After(runtimeTestTimeout):
		t.Fatal("runtime did not close after product proof")
	}
	if err := settlement.Complete(); !errors.Is(err, ErrProductSettlementStale) {
		t.Fatalf("duplicate Complete = %v, want stale", err)
	}
	trig.runtime.lifecycle.mu.Lock()
	publicationLive = trig.runtime.lifecycle.publication != nil &&
		trig.runtime.lifecycle.publication.publishedSet
	trig.runtime.lifecycle.mu.Unlock()
	if publicationLive {
		t.Fatal("publication remained after complete lower settlement")
	}
}

func TestProductSettlementTimeoutIsStickyAndRetainsLowerOwnership(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, 25*time.Millisecond, nil, nil)
	activation := trig.begin(t)
	settlement, err := activation.ClaimProductSettlement()
	if err != nil {
		t.Fatal(err)
	}
	publication, err := trig.slot.Stage(activation, "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	if err := closeRuntimeTest(t, trig.runtime); !errors.Is(err, ErrShutdownIncomplete) {
		t.Fatalf("Close = %v, want incomplete", err)
	}
	if err := settlement.Complete(); !errors.Is(err, ErrProductSettlementStale) {
		t.Fatalf("late Complete = %v, want stale", err)
	}
	if err := closeRuntimeTest(t, trig.runtime); !errors.Is(err, ErrShutdownIncomplete) {
		t.Fatalf("second Close = %v, want sticky incomplete", err)
	}
	if fatal := trig.runtime.lifecycle.fatalError(); !errors.Is(fatal, ErrShutdownIncomplete) {
		t.Fatalf("lifecycle fatal = %v, want incomplete", fatal)
	}
	assertRuntimeTestRetained(t, trig.runtime)
	trig.runtime.mu.Lock()
	workerClaim := trig.runtime.workerClaim
	childrenClaimed := trig.runtime.childrenClaimed
	settlementExpired := trig.runtime.productSettlement != nil && trig.runtime.productSettlement.expired
	trig.runtime.mu.Unlock()
	trig.runtime.lifecycle.mu.Lock()
	publicationLive := trig.runtime.lifecycle.publication != nil &&
		trig.runtime.lifecycle.publication.publishedSet
	trig.runtime.lifecycle.mu.Unlock()
	if workerClaim == nil || !childrenClaimed || !settlementExpired || !publicationLive {
		t.Fatalf(
			"retained state: workers=%v children=%v expired=%v publication=%v",
			workerClaim != nil, childrenClaimed, settlementExpired, publicationLive,
		)
	}

	result, err := trig.workers.Run(context.Background(), worker.CommandRequest{
		Path: "/bin/sh", Dir: "/bin", Args: []string{"-c", "exit 0"},
		TotalTimeout: 2 * time.Second,
	})
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("retained worker authority = result %+v err %v", result, err)
	}
	request := managerTestProductRequest(t)
	child, _, err := trig.children.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("retained child authority: %v", err)
	}
	if err := child.Stop(context.Background()); err != nil {
		t.Fatalf("settle retained child probe: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancel()
	if err := workerClaim.Close(ctx); err != nil {
		t.Fatalf("test cleanup workers: %v", err)
	}
	if err := trig.children.Shutdown(ctx); err != nil {
		t.Fatalf("test cleanup children: %v", err)
	}
}

func TestProductSettlementClaimIsSingularStartingAndGenerationFenced(t *testing.T) {
	if _, err := (Activation{}).ClaimProductSettlement(); !errors.Is(err, ErrProductSettlementUnavailable) {
		t.Fatalf("zero activation claim = %v", err)
	}
	trig := newRuntimeTestRig(t, nil, time.Second, nil, nil)
	activation := trig.begin(t)
	settlement, err := activation.ClaimProductSettlement()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := activation.ClaimProductSettlement(); !errors.Is(err, ErrProductSettlementUnavailable) {
		t.Fatalf("second claim = %v, want unavailable", err)
	}
	cause := errors.New("product preparation failed")
	if err := activation.Fail(cause); err != nil {
		t.Fatal(err)
	}
	select {
	case <-activation.Context().Done():
	case <-time.After(runtimeTestTimeout):
		t.Fatal("failed activation was not canceled")
	}
	if err := settlement.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := waitRuntimeTest(t, trig.runtime); !errors.Is(err, cause) {
		t.Fatalf("Wait = %v, want preparation cause", err)
	}
	if _, err := activation.ClaimProductSettlement(); !errors.Is(err, ErrProductSettlementUnavailable) {
		t.Fatalf("finished activation claim = %v, want unavailable", err)
	}

	readyRig := newRuntimeTestRig(t, nil, time.Second, nil, nil)
	readyActivation := readyRig.ready(t, "ready")
	if _, err := readyActivation.ClaimProductSettlement(); !errors.Is(err, ErrProductSettlementUnavailable) {
		t.Fatalf("Ready claim = %v, want unavailable", err)
	}
}

func managerTestProductRequest(t *testing.T) proc.SpawnRequest {
	t.Helper()
	var signature proc.SignatureDigest
	signature[0] = 1
	request, err := proc.NewSpawnRequest(proc.SpawnConfig{
		RecoveryID: proc.RecoveryTaskID,
		Executable: "/bin/sh",
		Args:       []string{"-c", "exit 0"},
		Stdin:      proc.StdioNull, Stdout: proc.StdioNull, Stderr: proc.StdioNull,
		ExpectedSignature: &signature,
	})
	if err != nil {
		t.Fatal(err)
	}
	return request
}
