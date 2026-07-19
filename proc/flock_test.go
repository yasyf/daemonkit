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
	h, err := (FileLockSpec{
		Path:     lockPath,
		Mode:     FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(context.Background())
	if err != nil {
		t.Errorf("acquire: %v", err)
		return
	}
	defer h.Close()
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

func TestFileLockSerializesCriticalSection(t *testing.T) {
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

func TestFileLockRespectsContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.lock")
	held, err := (FileLockSpec{
		Path:     path,
		Mode:     FileLockExclusive,
		Deadline: time.Second,
	}).Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = (FileLockSpec{
		Path:     path,
		Mode:     FileLockExclusive,
		Deadline: time.Second,
	}).Acquire(ctx)
	if err == nil {
		t.Fatal("Acquire succeeded while the lock was held; want a ctx error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want errors.Is context.DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("Acquire took %v to honor a 50ms deadline", waited)
	}
}

const (
	fileLockExclusiveChildEnv = "DAEMONKIT_FILE_LOCK_EXCLUSIVE_TEST_PATH"
	fileLockExclusiveReadyEnv = "DAEMONKIT_FILE_LOCK_EXCLUSIVE_TEST_READY"
	fileLockSharedChildEnv    = "DAEMONKIT_FILE_LOCK_SHARED_TEST_PATH"
	fileLockSharedReadyEnv    = "DAEMONKIT_FILE_LOCK_SHARED_TEST_READY"
	fileLockChildHold         = 700 * time.Millisecond
)

func TestFileLockExclusiveChildHolds(t *testing.T) {
	lockPath := os.Getenv(fileLockExclusiveChildEnv)
	readyPath := os.Getenv(fileLockExclusiveReadyEnv)
	if lockPath == "" || readyPath == "" {
		t.Skip("child-only helper; driven by TestFileLockExclusiveCrossProcess")
	}
	h, err := (FileLockSpec{
		Path:     lockPath,
		Mode:     FileLockExclusive,
		Deadline: time.Second,
	}).Acquire(context.Background())
	if err != nil {
		t.Fatalf("child acquire: %v", err)
	}
	if err := os.WriteFile(readyPath, []byte("1"), 0o600); err != nil {
		t.Fatalf("child signal ready: %v", err)
	}
	time.Sleep(fileLockChildHold)
	h.Close()
}

func TestFileLockExclusiveCrossProcess(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "x.lock")
	readyPath := filepath.Join(dir, "ready")

	child := exec.Command(os.Args[0], "-test.run=^TestFileLockExclusiveChildHolds$", "-test.v")
	child.Env = append(os.Environ(),
		fileLockExclusiveChildEnv+"="+lockPath,
		fileLockExclusiveReadyEnv+"="+readyPath)
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
	h, err := (FileLockSpec{
		Path:     lockPath,
		Mode:     FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(context.Background())
	if err != nil {
		t.Fatalf("parent acquire: %v; child output:\n%s", err, out.String())
	}
	waited := time.Since(start)
	h.Close()
	if waited < 300*time.Millisecond {
		t.Fatalf("parent acquired in %v without blocking — flock is not excluding across processes; child output:\n%s", waited, out.String())
	}
}

func TestFileLockChildHoldsShared(t *testing.T) {
	lockPath := os.Getenv(fileLockSharedChildEnv)
	readyPath := os.Getenv(fileLockSharedReadyEnv)
	if lockPath == "" || readyPath == "" {
		t.Skip("child-only helper; driven by TestFileLockSharedExclusiveCrossProcess")
	}
	h, err := (FileLockSpec{
		Path:     lockPath,
		Mode:     FileLockShared,
		Deadline: time.Second,
	}).Acquire(context.Background())
	if err != nil {
		t.Fatalf("child acquire shared: %v", err)
	}
	defer h.Close()
	if err := os.WriteFile(readyPath, []byte("1"), 0o600); err != nil {
		t.Fatalf("child signal ready: %v", err)
	}
	time.Sleep(fileLockChildHold)
}

func TestFileLockSharedExclusiveCrossProcess(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "lifecycle.lock")
	readyPath := filepath.Join(dir, "ready")

	child := exec.Command(os.Args[0], "-test.run=^TestFileLockChildHoldsShared$", "-test.v")
	child.Env = append(os.Environ(), fileLockSharedChildEnv+"="+lockPath, fileLockSharedReadyEnv+"="+readyPath)
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

	shared, err := (FileLockSpec{
		Path:     lockPath,
		Mode:     FileLockShared,
		Deadline: time.Second,
	}).TryAcquire()
	if err != nil {
		t.Fatalf("second shared owner: %v", err)
	}
	defer shared.Close()

	if _, err := (FileLockSpec{
		Path:     lockPath,
		Mode:     FileLockExclusive,
		Deadline: time.Second,
	}).TryAcquire(); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("exclusive TryAcquire err = %v, want ErrLockBusy", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = (FileLockSpec{
		Path:     lockPath,
		Mode:     FileLockExclusive,
		Deadline: 50 * time.Millisecond,
	}).Acquire(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exclusive Acquire err = %v, want context deadline", err)
	}
}

func TestFileLockSpecValidation(t *testing.T) {
	abs := filepath.Join(t.TempDir(), "valid.lock")
	tests := map[string]FileLockSpec{
		"empty path":        {Mode: FileLockShared, Deadline: time.Second},
		"relative path":     {Path: "relative.lock", Mode: FileLockShared, Deadline: time.Second},
		"unclean path":      {Path: abs + "/../" + filepath.Base(abs), Mode: FileLockShared, Deadline: time.Second},
		"root path":         {Path: string(filepath.Separator), Mode: FileLockShared, Deadline: time.Second},
		"missing mode":      {Path: abs, Deadline: time.Second},
		"unknown mode":      {Path: abs, Mode: FileLockMode(99), Deadline: time.Second},
		"missing deadline":  {Path: abs, Mode: FileLockShared},
		"negative deadline": {Path: abs, Mode: FileLockShared, Deadline: -time.Second},
	}
	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := spec.TryAcquire(); !errors.Is(err, ErrInvalidFileLock) {
				t.Fatalf("TryAcquire err = %v, want ErrInvalidFileLock", err)
			}
		})
	}
}

func TestFileLockAcquireHonorsPreCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "canceled.lock")
	_, err := (FileLockSpec{
		Path:     path,
		Mode:     FileLockExclusive,
		Deadline: time.Second,
	}).Acquire(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire err = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-canceled Acquire created lock file: %v", err)
	}
}

func TestFileLockRejectsUnsafeExistingPaths(t *testing.T) {
	dir := t.TempDir()
	tests := map[string]func(t *testing.T) string{
		"symlink": func(t *testing.T) string {
			target := filepath.Join(dir, "target")
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "symlink.lock")
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
			return path
		},
		"directory": func(t *testing.T) string {
			path := filepath.Join(dir, "directory.lock")
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			return path
		},
		"multiple links": func(t *testing.T) string {
			path := filepath.Join(dir, "linked.lock")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(path, filepath.Join(dir, "alias.lock")); err != nil {
				t.Fatal(err)
			}
			return path
		},
		"group writable": func(t *testing.T) string {
			path := filepath.Join(dir, "writable.lock")
			if err := os.WriteFile(path, nil, 0o620); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o620); err != nil {
				t.Fatal(err)
			}
			return path
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			path := setup(t)
			_, err := (FileLockSpec{Path: path, Mode: FileLockExclusive, Deadline: time.Second}).TryAcquire()
			if !errors.Is(err, ErrUnsafeLockFile) {
				t.Fatalf("TryAcquire err = %v, want ErrUnsafeLockFile", err)
			}
		})
	}
}

func TestFileLockNormalizesSafeExistingModeAndRetainsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mode.lock")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := (FileLockSpec{Path: path, Mode: FileLockExclusive, Deadline: time.Second}).TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("lock file removed on close: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode = %#o, want 0600", got)
	}
}

func TestFileLockTryAcquireBusyThenFree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "try.lock")
	spec := FileLockSpec{Path: path, Mode: FileLockExclusive, Deadline: time.Second}

	h1, err := spec.TryAcquire()
	if err != nil {
		t.Fatalf("first TryAcquire: %v", err)
	}
	if _, err := spec.TryAcquire(); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("second TryAcquire err = %v, want ErrLockBusy", err)
	}

	h1.Close()
	h2, err := spec.TryAcquire()
	if err != nil {
		t.Fatalf("TryAcquire after close = %v, want success", err)
	}
	h2.Close()
}

func TestFileLockCloseIsIdempotent(t *testing.T) {
	h, err := (FileLockSpec{
		Path:     filepath.Join(t.TempDir(), "idempotent.lock"),
		Mode:     FileLockExclusive,
		Deadline: time.Second,
	}).Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
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
