package proc

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestCloseInheritedFDsReleasesParentLease(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sweep bool
	}{
		{name: "sweep releases the inherited lease", sweep: true},
		{name: "no sweep pins it (negative control)", sweep: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			const dir = "/x/mnt"
			h, err := leaseAcquire(root, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()

			cmd := exec.Command(os.Args[0], "-test.run", "^TestFDSweepHelperProcess$", "-test.v")
			cmd.Env = append(
				os.Environ(),
				"FDSWEEP_HELPER=1",
				fmt.Sprintf("FDSWEEP_SWEEP=%v", tc.sweep),
			)
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer devNull.Close()
			cmd.Stderr = devNull
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			var waitOnce sync.Once
			wait := func() { waitOnce.Do(func() { _ = cmd.Wait() }) }
			t.Cleanup(func() {
				_ = stdin.Close()
				_ = cmd.Process.Kill()
				wait()
			})
			waitHelperReady(t, stdout)

			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			f, err := leaseSeize(root, dir)
			if tc.sweep {
				if err != nil {
					t.Fatalf("Seize after child swept = %v, want free (the child must not hold the parent's lease)", err)
				}
				_ = f.Release()
				return
			}
			if !errors.Is(err, errLeaseBusy) {
				t.Fatalf("Seize with unswept child = %v, want errLeaseBusy (the inherited fd must pin — otherwise this test cannot catch the leak)", err)
			}
			_ = stdin.Close()
			wait()
			f, err = leaseSeize(root, dir)
			if err != nil {
				t.Fatalf("Seize after child exit = %v, want free", err)
			}
			_ = f.Release()
		})
	}
}

func waitHelperReady(t *testing.T, stdout io.Reader) {
	t.Helper()
	ready := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if sc.Text() == "fdsweep-ready" {
				ready <- nil
				return
			}
		}
		ready <- fmt.Errorf("helper exited before ready: %v", sc.Err())
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("helper never reported ready")
	}
}

func TestFDSweepHelperProcess(t *testing.T) {
	if os.Getenv("FDSWEEP_HELPER") != "1" {
		t.Skip("helper body; runs only re-exec'd")
	}
	if os.Getenv("FDSWEEP_SWEEP") == "true" {
		if err := CloseInheritedFDs(); err != nil {
			t.Fatalf("CloseInheritedFDs: %v", err)
		}
	}
	fmt.Println("fdsweep-ready")
	_, _ = io.Copy(io.Discard, os.Stdin)
}

var errLeaseBusy = errors.New("lease is held")

type leaseHandle struct{ fd int }

func (h *leaseHandle) Close() error { return syscall.Close(h.fd) }

type leaseFence struct{ f *os.File }

func (f *leaseFence) Release() error { return f.f.Close() }

func leasePath(root, dir string) string {
	sum := sha256.Sum256([]byte(dir))
	return filepath.Join(root, hex.EncodeToString(sum[:])[:16]+".lease")
}

func leaseAcquire(root, dir string) (*leaseHandle, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(leasePath(root, dir), syscall.O_RDWR|syscall.O_CREAT, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(fd, syscall.LOCK_SH); err != nil {
		syscall.Close(fd)
		return nil, err
	}
	return &leaseHandle{fd: fd}, nil
}

func leaseSeize(root, dir string) (*leaseFence, error) {
	f, err := os.OpenFile(leasePath(root, dir), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errLeaseBusy
		}
		return nil, err
	}
	return &leaseFence{f: f}, nil
}
