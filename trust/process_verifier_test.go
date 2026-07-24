package trust

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

func TestRunVerifierChildHardCutsMalformedRequests(t *testing.T) {
	if recognized, err := RunVerifierChild([]string{"consumer-mode"}, os.Stdout); err != nil || recognized {
		t.Fatalf("consumer mode = %t, %v", recognized, err)
	}
	if recognized, err := RunVerifierChild([]string{verifierChildMode}, os.Stdout); err == nil || !recognized {
		t.Fatalf("missing request = %t, %v", recognized, err)
	}
	if recognized, err := RunVerifierChild([]string{verifierChildMode, "%%%"}, os.Stdout); err == nil || !recognized {
		t.Fatalf("invalid request = %t, %v", recognized, err)
	}
}

func TestProcessVerifierCancellationReapsAndReusesDedicatedLane(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "verifier-child")
	script := `#!/bin/sh
if mkdir "$0.state" 2>/dev/null; then
    trap '' TERM
    while :; do sleep 3600 & wait $!; done
fi
printf '{"protocol":1,"result":"trusted"}\n'
`
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	store := &proc.FileStore{Path: filepath.Join(directory, "workers.json")}
	reaper := &proc.Reaper{Store: store, Generation: proc.OwnerGeneration{1}}
	pool, err := worker.NewPool(worker.Config{
		Capacity: 1, QueueCapacity: 1, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: maxVerifierPayload, MaxStdoutBytes: maxVerifierResponse, MaxStderrBytes: maxVerifierResponse,
	}, reaper)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := pool.ClaimRuntime(VerifierWorkerBudgets())
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := claim.Activate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := claim.Close(ctx); err != nil {
			t.Errorf("wait for verifier pool: %v", err)
		}
	})
	verifier := ProcessVerifier{Runner: claim, Executable: executable}
	blockedCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	blocked := make(chan error, 1)
	go func() { blocked <- verifier.Check(blockedCtx, peer.Identity{UID: os.Geteuid()}) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		select {
		case err := <-blocked:
			t.Fatalf("blocking verifier exited before entering: %v", err)
		default:
		}
		if _, err := os.Stat(executable + ".state"); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("blocking verifier child did not enter")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Check = %v, want cancellation", err)
	}

	nextCtx, cancelNext := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelNext()
	if err := verifier.Check(nextCtx, peer.Identity{UID: os.Geteuid()}); err != nil {
		t.Fatalf("next Check after reaping: %v", err)
	}
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("verifier records after settled checks = %#v", records)
	}
}

func TestProcessVerifierSurvivesPathologicalProductPoolBudgets(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "verifier-child")
	script := `#!/bin/sh
printf '{"protocol":1,"result":"trusted"}\n'
`
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	reaper := &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(directory, "workers.json")}, Generation: proc.OwnerGeneration{1},
	}
	pool, err := worker.NewPool(worker.Config{
		Capacity: 1, QueueCapacity: 1, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 0, MaxStdoutBytes: 1, MaxStderrBytes: 1,
	}, reaper)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := pool.ClaimRuntime(VerifierWorkerBudgets())
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := claim.Activate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := claim.Close(ctx); err != nil {
			t.Errorf("wait for verifier pool: %v", err)
		}
	})
	verifier := ProcessVerifier{Runner: claim, Executable: executable}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := verifier.Check(ctx, peer.Identity{UID: os.Geteuid()}); err != nil {
		t.Fatalf("verifier round trip under 1-byte product budget: %v", err)
	}
}
