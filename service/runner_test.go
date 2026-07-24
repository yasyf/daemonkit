package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

func TestRunCombinedCancellationReapsDaemonizedDescendantAndRecord(t *testing.T) {
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.json")}
	reaper := &proc.Reaper{Store: store, Generation: proc.OwnerGeneration{1}}
	runtime, err := newControllerWorkerRuntime(1, reaper)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = runtime.Close(ctx)
	})

	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := runCombined(ctx, runtime, "/bin/sh", "-c",
			`trap '' TERM; (trap '' TERM; while :; do sleep 1; done) & echo $! > "$1"; wait`,
			"service-runner", pidFile)
		result <- err
	}()

	descendant := awaitPIDFile(t, pidFile)
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled disposable service task returned nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled disposable service task did not settle")
	}
	assertProcessGone(t, descendant)
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("durable service worker records after settlement = %+v", records)
	}
}

func TestRunCombinedBoundsOutputWithoutStrandingWorker(t *testing.T) {
	runner := taskRunnerFunc(func(_ context.Context, _ worker.CommandRequest) (worker.CommandResult, error) {
		return worker.CommandResult{Stdout: []byte(strings.Repeat("x", commandOutputLimit+1))}, nil
	})
	output, err := runCombined(t.Context(), runner, "/usr/bin/true")
	if !errors.Is(err, errCommandOutputLimit) {
		t.Fatalf("runCombined oversized output error = %v", err)
	}
	if len(output) != commandOutputLimit {
		t.Fatalf("bounded output length = %d, want %d", len(output), commandOutputLimit)
	}
}

func awaitPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		payload, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(payload)))
			if parseErr == nil && pid > 1 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant pid file %q was not written", path)
	return 0
}

func assertProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant pid %d remains after disposable task settlement", pid)
}
