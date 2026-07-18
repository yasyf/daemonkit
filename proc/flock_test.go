package proc

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func bumpUnderLock(t *testing.T, lockPath, counterPath string) {
	t.Helper()
	h, err := Flock(context.Background(), lockPath)
	if err != nil {
		t.Errorf("acquire: %v", err)
		return
	}
	defer h.Release()
	b, err := os.ReadFile(counterPath)
	if err != nil {
		t.Errorf("read counter: %v", err)
		return
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	runtime.Gosched()
	if err := os.WriteFile(counterPath, []byte(strconv.Itoa(n+1)), 0o600); err != nil {
		t.Errorf("write counter: %v", err)
	}
}

func TestFlockSerializesCriticalSection(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "counter.lock")
	counterPath := filepath.Join(dir, "counter")
	if err := os.WriteFile(counterPath, []byte("0"), 0o600); err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const iterations = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				bumpUnderLock(t, lockPath, counterPath)
			}
		}()
	}
	wg.Wait()

	b, err := os.ReadFile(counterPath)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	if want := goroutines * iterations; got != want {
		t.Fatalf("counter = %d, want %d — lost updates mean the flock did not serialize", got, want)
	}
}

func TestFlockRespectsContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.lock")
	held, err := Flock(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = Flock(ctx, path)
	if err == nil {
		t.Fatal("Flock succeeded while the lock was held; want a ctx error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want errors.Is context.DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("Flock took %v to honor a 50ms deadline", waited)
	}
}

const (
	flockChildLockEnv  = "FUSEKIT_FLOCK_TEST_LOCK"
	flockChildReadyEnv = "FUSEKIT_FLOCK_TEST_READY"
	flockChildHold     = 700 * time.Millisecond
)

func TestFlockChildHolds(t *testing.T) {
	lockPath := os.Getenv(flockChildLockEnv)
	readyPath := os.Getenv(flockChildReadyEnv)
	if lockPath == "" || readyPath == "" {
		t.Skip("child-only helper; driven by TestFlockCrossProcess")
	}
	h, err := Flock(context.Background(), lockPath)
	if err != nil {
		t.Fatalf("child acquire: %v", err)
	}
	if err := os.WriteFile(readyPath, []byte("1"), 0o600); err != nil {
		t.Fatalf("child signal ready: %v", err)
	}
	time.Sleep(flockChildHold)
	h.Release()
}

func TestFlockCrossProcess(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "x.lock")
	readyPath := filepath.Join(dir, "ready")

	child := exec.Command(os.Args[0], "-test.run=^TestFlockChildHolds$", "-test.v")
	child.Env = append(os.Environ(),
		flockChildLockEnv+"="+lockPath,
		flockChildReadyEnv+"="+readyPath)
	var out bytes.Buffer
	child.Stdout, child.Stderr = &out, &out
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			_ = child.Process.Kill()
			_, _ = child.Process.Wait()
			return
		}
		if err := child.Wait(); err != nil {
			t.Errorf("child exit: %v; output:\n%s", err, out.String())
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child never signaled ready; output:\n%s", out.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	h, err := Flock(context.Background(), lockPath)
	if err != nil {
		t.Fatalf("parent acquire: %v; child output:\n%s", err, out.String())
	}
	waited := time.Since(start)
	h.Release()
	if waited < 300*time.Millisecond {
		t.Fatalf("parent acquired in %v without blocking — flock is not excluding across processes; child output:\n%s", waited, out.String())
	}
}

func TestTryLockBusyThenFree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "try.lock")

	h1, err := TryLock(path)
	if err != nil {
		t.Fatalf("first TryLock: %v", err)
	}
	if _, err := TryLock(path); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("second TryLock err = %v, want ErrLockBusy", err)
	}

	h1.Release()
	h2, err := TryLock(path)
	if err != nil {
		t.Fatalf("TryLock after release = %v, want success", err)
	}
	h2.Release()
}

func TestFlockReleaseIsIdempotent(t *testing.T) {
	h, err := Flock(context.Background(), filepath.Join(t.TempDir(), "idempotent.lock"))
	if err != nil {
		t.Fatalf("Flock: %v", err)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("second Release: %v", err)
	}
}

func TestMkdirAllDurableSyncsEveryCreatedLink(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "drain", "g1")
	var synced []string
	if err := mkdirAllDurable(dir, 0o700, func(path string) error {
		synced = append(synced, path)
		return nil
	}); err != nil {
		t.Fatalf("mkdirAllDurable: %v", err)
	}
	want := []string{filepath.Dir(root), root, filepath.Join(root, "drain")}
	if len(synced) != len(want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
	for i := range want {
		if synced[i] != want[i] {
			t.Errorf("synced[%d] = %q, want %q", i, synced[i], want[i])
		}
	}
}

func TestMkdirAllDurableRetriesFailedParentSync(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "drain", "g1")
	syncErr := errors.New("sync failed")
	failed := false
	err := mkdirAllDurable(dir, 0o700, func(path string) error {
		if path == root && !failed {
			failed = true
			return syncErr
		}
		return nil
	})
	if !errors.Is(err, syncErr) {
		t.Fatalf("first mkdirAllDurable err = %v, want sync failure", err)
	}
	var synced []string
	if err := mkdirAllDurable(dir, 0o700, func(path string) error {
		synced = append(synced, path)
		return nil
	}); err != nil {
		t.Fatalf("mkdirAllDurable retry: %v", err)
	}
	if len(synced) == 0 || synced[0] != root {
		t.Fatalf("retry synced directories = %v, want %q first", synced, root)
	}
}
