package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
)

const (
	controlPreservePark       = "DAEMONKIT_CONTROL_PRESERVE_PARK"
	controlPreserveDescendant = "DAEMONKIT_CONTROL_PRESERVE_DESCENDANT"
)

type committedParkProduct struct {
	stubProduct
	directory string
}

func (*committedParkProduct) PrepareDrain(Budget) (DrainPreparation, error) {
	return committedParkPreparation{}, nil
}

type committedParkPreparation struct{}

func (committedParkPreparation) Commit(Budget) error { return nil }
func (committedParkPreparation) Abort(Budget) error  { return nil }

func (p *committedParkProduct) Drain(Budget) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(executable)
	child.Env = append(os.Environ(), controlChildEnv+"=1", controlPreserveDescendant+"="+p.directory)
	if err := child.Start(); err != nil {
		return err
	}
	go func() { _ = child.Wait() }()
	select {}
}

func observePreservationSignals(directory, role string) {
	received := make(chan os.Signal, 8)
	signal.Notify(received, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGCONT)
	go func() {
		for range received {
			if err := os.WriteFile(filepath.Join(directory, role+"-signalled"), []byte("signal"), 0o600); err != nil {
				os.Exit(72)
			}
		}
	}()
}

func runPreservationDescendant(directory string) {
	observePreservationSignals(directory, "child")
	if err := os.WriteFile(filepath.Join(directory, "child-pid"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		os.Exit(73)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "stop-child")); err == nil {
			if err := os.WriteFile(filepath.Join(directory, "child-exited"), nil, 0o600); err != nil {
				os.Exit(74)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPreservingCommittedHostTimeoutNeverSignalsNewDescendant(t *testing.T) {
	ladderHome(t)
	directory := t.TempDir()
	t.Cleanup(func() {
		if err := os.WriteFile(filepath.Join(directory, "stop-child"), nil, 0o600); err != nil {
			t.Error(err)
			return
		}
		deadline := time.Now().Add(2 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(directory, "child-exited")); err == nil {
				return
			}
			if _, err := os.Stat(filepath.Join(directory, "child-pid")); errors.Is(err, os.ErrNotExist) {
				return
			}
			if time.Now().After(deadline) {
				t.Error("fixture descendant did not acknowledge natural exit")
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	d := Daemon{Label: "dkstop-preserving-committed", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second), ShutdownPolicy: PreserveOwned, Program: unrunProgram(t)}
	child := startControlChildEnv(t, string(d.Label), controlPreservePark+"="+directory)
	installedAgentPlist(t, d.Label)
	client := openClient(t, d)
	ready, cancelReady := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancelReady()
	control := awaitControl(ready, t, client)
	before, err := control.Health(ready)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Close(ready); err != nil {
		t.Fatal(err)
	}
	disabled := false
	client.launchctl = func(_ context.Context, _ string, args ...string) (string, int, error) {
		switch args[0] {
		case "print":
			return fmt.Sprintf("gui/%d/%s = {\n\tpid = %d\n\tproperties = keepalive | runatload\n}\n", os.Getuid(), d.Label, child.Process.Pid), 0, nil
		case "print-disabled":
			state := "enabled"
			if disabled {
				state = "disabled"
			}
			return fmt.Sprintf("disabled services = {\n\t\"%s\" => %s\n}\n", d.Label, state), 0, nil
		case "disable":
			disabled = true
			return "", 0, nil
		default:
			t.Errorf("unexpected launchctl operation: %v", args)
			return "", 1, errors.New("unexpected mutation")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err = client.evict(ctx, before, proc.Identity{})
	if !errors.Is(err, ErrDrainPreparationTimeout) || errors.Is(err, ErrUnsettled) || !disabled {
		t.Fatalf("evict=%v disabled=%v", err, disabled)
	}
	pidBytes, err := os.ReadFile(filepath.Join(directory, "child-pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{child.Process.Pid, pid} {
		if _, err := proc.ProbeIdentity(id); err != nil {
			t.Fatalf("preserved process %d disappeared: %v", id, err)
		}
	}
	for _, role := range []string{"host", "child"} {
		if _, err := os.Stat(filepath.Join(directory, role+"-signalled")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s received signal: %v", role, err)
		}
	}
	if control, err = client.Control(ready); !errors.Is(err, ErrDraining) && !errors.Is(err, ErrAbsent) {
		if control != nil {
			_ = control.Close(ready)
		}
		t.Fatalf("postcommit host stopped rejecting intake: %v", err)
	}
}
