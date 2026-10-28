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
	controlChildApp   = "DAEMONKIT_CONTROL_CHILD_APP"
	testBuild         = "test.build"
)

var testProgram = Program{policy: copied{version: testBuild}}

// controlChildProgram is the Program a control child serves: the bundle a
// ladder test versions through its Info.plist, or the fixed test build.
func controlChildProgram() Program {
	if app := os.Getenv(controlChildApp); app != "" {
		return Program{policy: bundled{app: app}}
	}
	return testProgram
}

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
		Program:  controlChildProgram(),
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

// TestDrainPinsTheAcceptingProcess is the pin's only cross-process coverage:
// the peer PID read from the connecting side must name the process accepting
// on the socket, and the health self-attestation must agree with it.
func TestDrainPinsTheAcceptingProcess(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dkchild", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	child := startControlChild(t, string(d.Label))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := openClient(t, d)
	awaitServing(ctx, t, client)
	_, pinned, report, err := client.link.pinned(ctx)
	if err != nil {
		t.Fatalf("pinned() = %v", err)
	}
	if pinned.PID != child.Process.Pid {
		t.Fatalf("pinned PID = %d, want the accepting child %d", pinned.PID, child.Process.Pid)
	}
	if pinned.Start == 0 || pinned.Boot == 0 {
		t.Fatalf("pinned = %+v, want a probed {start, boot}", pinned)
	}
	health := healthFromReport(report)
	if health.PID != child.Process.Pid || health.Phase != PhaseReady || health.Generation == 0 || health.Build != testBuild {
		t.Fatalf("Health() = %+v, want the child's ready identity", health)
	}

	if _, err := client.Drain(ctx, Expect{Build: health.Build, Generation: health.Generation + 1}); !errors.Is(err, ErrWrongIncumbent) {
		t.Fatalf("Drain() with a wrong generation = %v, want ErrWrongIncumbent", err)
	}
	stopped, err := client.Drain(ctx, Expect{Build: health.Build, Generation: health.Generation})
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

// TestBusinessTakesOverTheClientsSession pins the one-connection command: a
// client that asked Health and then Calls rides the session Health opened.
func TestBusinessTakesOverTheClientsSession(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dkshare", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	startControlChild(t, string(d.Label))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := openClient(t, d)
	awaitServing(ctx, t, client)
	opened, _, err := client.link.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire() = %v", err)
	}
	lane := client.Business()
	var product *ProductError
	if _, err := lane.Call(ctx, "echo", []byte("x")); !errors.As(err, &product) {
		t.Fatalf("Call() = %v, want the product's own answer", err)
	}
	if lane.session != opened {
		t.Fatal("the business lane dialed its own session instead of taking over the client's")
	}
	if client.link.session != nil {
		t.Fatal("the client kept a session it handed to the business lane")
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

func awaitServing(ctx context.Context, t *testing.T, client *Client) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		attachCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := client.Health(attachCtx)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Health() = %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
