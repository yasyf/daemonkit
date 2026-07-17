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
	"syscall"
	"testing"
	"time"
)

// TestCloseInheritedFDsReleasesParentLease pins P-11: a detached child
// spawned while the parent holds a session lease inherits the non-CLOEXEC
// lease descriptor; after CloseInheritedFDs the child no longer pins it. The
// no-sweep case is the negative control proving the leak (and this test)
// is real.
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
			cmd.Env = append(os.Environ(),
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
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				stdin.Close()
				_ = cmd.Wait()
			})
			waitHelperReady(t, stdout)

			// The parent's own share is gone; only the child's inherited fd
			// (if any survived the sweep) can keep the lease held.
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
			// The pin dies with the child's descriptor.
			stdin.Close()
			_ = cmd.Wait()
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

// TestFDSweepHelperProcess is the re-exec'd child body, inert unless
// FDSWEEP_HELPER=1: optionally sweep, report ready, then hold all inherited
// state until stdin closes.
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

// errLeaseBusy stands in for the session-lease busy sentinel this test used to
// import from fusekit.
var errLeaseBusy = errors.New("lease is held")

type leaseHandle struct{ fd int }

func (h *leaseHandle) Close() error { return syscall.Close(h.fd) }

type leaseFence struct{ f *os.File }

func (f *leaseFence) Release() error { return f.f.Close() }

func leasePath(root, dir string) string {
	sum := sha256.Sum256([]byte(dir))
	return filepath.Join(root, hex.EncodeToString(sum[:])[:16]+".lease")
}

// leaseAcquire takes a shared lease over a descriptor opened WITHOUT O_CLOEXEC
// (raw syscall.Open — os.OpenFile force-adds O_CLOEXEC on Darwin), so a
// fork+exec child inherits and pins it. That inheritance is exactly what
// CloseInheritedFDs must undo, so it is load-bearing for this test.
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
