package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

const (
	controlChildEnv   = "DAEMONKIT_CONTROL_CHILD"
	controlChildLabel = "DAEMONKIT_CONTROL_CHILD_LABEL"
)

// TestMain branches on the child marker before m.Run so a spawned copy of this
// binary serves one daemon and exits instead of re-entering the suite — the
// re-exec fork-bomb guard scripts/test.sh backstops.
func TestMain(m *testing.M) {
	if os.Getenv(controlChildEnv) == "1" {
		runControlChild()
	}
	os.Exit(m.Run())
}

func runControlChild() {
	d := Daemon{
		Label:    Label(os.Getenv(controlChildLabel)),
		Schemas:  []Schema{"test.v1"},
		Shutdown: Grace(5 * time.Second),
	}
	_, err := Serve(context.Background(), d, func(Ctx) (Product, error) { return &stubProduct{}, nil })
	if err != nil {
		fmt.Fprintf(os.Stderr, "control child: %v\n", err)
		os.Exit(71)
	}
	os.Exit(0)
}

// TestControlAttachPinsTheAcceptingProcess is the pin's only cross-process
// coverage: the peer PID read from the connecting side must name the process
// accepting on the socket, and the health self-attestation must agree with it.
func TestControlAttachPinsTheAcceptingProcess(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dkchild", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	child := startControlChild(t, string(d.Label))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	control := awaitControl(ctx, t, openClient(t, d))
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = control.Close(closeCtx)
	})
	if control.pinned.PID != child.Process.Pid {
		t.Fatalf("pinned PID = %d, want the accepting child %d", control.pinned.PID, child.Process.Pid)
	}
	if control.pinned.Start == 0 || control.pinned.Boot == 0 {
		t.Fatalf("pinned = %+v, want a probed {start, boot}", control.pinned)
	}
	health, err := control.Health(ctx)
	if err != nil {
		t.Fatalf("Health() = %v", err)
	}
	if health.PID != child.Process.Pid || health.Phase != PhaseReady || health.Generation == 0 || health.Build == "" {
		t.Fatalf("Health() = %+v, want the child's ready identity", health)
	}
	if health.Generation != control.generation {
		t.Fatalf("Health().Generation = %d, want the pinned %d", health.Generation, control.generation)
	}

	if _, err := control.Drain(ctx, Expect{Build: health.Build, Generation: health.Generation + 1}); !errors.Is(err, ErrWrongIncumbent) {
		t.Fatalf("Drain() with a wrong generation = %v, want ErrWrongIncumbent", err)
	}
	stopped, err := control.Drain(ctx, Expect{Build: health.Build, Generation: health.Generation})
	if err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	if stopped.Reap != ReapAbsent {
		t.Fatalf("Reap = %d, want ReapAbsent", stopped.Reap)
	}
	if stopped.Before.PID != child.Process.Pid || stopped.Before.Build != health.Build {
		t.Fatalf("Before = %+v, want the drained child's identity echo", stopped.Before)
	}
	if err := child.Wait(); err != nil {
		t.Fatalf("child exit = %v, want a clean exit driven by the drain verb", err)
	}
}

func TestControlRefusesAnUntrustedServer(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dktrust", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	child := startControlChild(t, string(d.Label))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	warmup := awaitControl(ctx, t, openClient(t, d))
	if err := warmup.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	pinned := d
	pinned.Trust.Serving = ServingSigned(Requirement{
		TeamID:            "SXKCTF23Q2",
		SigningIdentifier: "com.yasyf.daemonkit.not-this-binary",
	})
	pinnedClient, err := Open(pinned)
	if err != nil {
		t.Fatalf("Open(pinned) = %v", err)
	}
	control, err := pinnedClient.Control(ctx)
	if !errors.Is(err, ErrUntrusted) {
		if err == nil {
			_ = control.Close(ctx)
		}
		t.Fatalf("Control() = %v, want ErrUntrusted for a daemon that cannot prove the deployed identity", err)
	}

	unpinned := awaitControl(ctx, t, openClient(t, d))
	if _, err := unpinned.Drain(ctx, Expect{}); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	if err := child.Wait(); err != nil {
		t.Fatalf("child exit = %v", err)
	}
}

func startControlChild(t *testing.T, label string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() = %v", err)
	}
	child := exec.Command(executable)
	child.Env = append(os.Environ(), controlChildEnv+"=1", controlChildLabel+"="+label)
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	})
	return child
}

func awaitControl(ctx context.Context, t *testing.T, client *Client) *Control {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		attachCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		control, err := client.Control(attachCtx)
		cancel()
		if err == nil {
			return control
		}
		if time.Now().After(deadline) {
			t.Fatalf("Control() = %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
