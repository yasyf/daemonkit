package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

type failedResumeProduct struct{ stubProduct }

func (failedResumeProduct) PrepareDrain(daemonkit.Budget) (daemonkit.DrainPreparation, error) {
	return failedResumePreparation{}, nil
}

type failedResumePreparation struct{}

func (failedResumePreparation) Commit(daemonkit.Budget) error { return errors.New("commit failed") }
func (failedResumePreparation) Abort(daemonkit.Budget) error  { return errors.New("resume failed") }

func TestPreservingFailedResumeRetainsDisabledJobsAndDurableFence(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprint(remove), func(t *testing.T) {
			f := newFixture(t)
			if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
				t.Fatal(err)
			}
			child := f.startDaemonChildEnv(0, daemonChildPark+"=preserve-abort-failure")
			f.serving(20 * time.Second)
			f.deploy.config.Daemon.ShutdownPolicy = daemonkit.PreserveOwned
			client, err := daemonkit.Open(f.deploy.config.Daemon)
			if err != nil {
				t.Fatal(err)
			}
			f.deploy.client = client
			f.deploy.config.Agents[0].Label = string(f.deploy.config.Daemon.Label)
			agent := f.deploy.config.Agents[0]
			plist, err := agent.Plist()
			if err != nil {
				t.Fatal(err)
			}
			plistPath, err := agent.PlistPath()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(plistPath, plist, 0o600); err != nil {
				t.Fatal(err)
			}
			disabled := false
			f.deploy.run = func(_ context.Context, _ string, args ...string) (string, int, error) {
				f.launchctls = append(f.launchctls, args)
				switch args[0] {
				case "print":
					return fmt.Sprintf("gui/%d/%s = {\n\tpid = %d\n\tproperties = keepalive | runatload\n}\n", os.Getuid(), agent.Label, child.Process.Pid), 0, nil
				case "print-disabled":
					state := "enabled"
					if disabled {
						state = "disabled"
					}
					return fmt.Sprintf("disabled services = {\n\t\"%s\" => %s\n}\n", agent.Label, state), 0, nil
				case "disable":
					disabled = true
					return "", 0, nil
				default:
					t.Errorf("unexpected maintenance mutation: %v", args)
					return "", 1, errors.New("unexpected maintenance mutation")
				}
			}
			called := false
			quiesce := func(context.Context) error { called = true; return nil }
			if remove {
				_, err = f.deploy.Remove(f.ctx(), quiesce)
			} else {
				_, err = f.deploy.Replace(f.ctx(), f.candidate("Second", "2.0", "two"), quiesce)
			}
			if !errors.Is(err, daemonkit.ErrDrainBusy) || !errors.Is(err, ErrMaintenanceIncomplete) {
				t.Fatalf("maintenance=%v", err)
			}
			if called || !disabled || !fileExists(f.deploy.maintenancePath()) || fileExists(f.deploy.layout.swap) {
				t.Fatalf("failed resume crossed fence: called=%v disabled=%v", called, disabled)
			}
			f.wantCanonical("one")
		})
	}
}

func TestPreservingReplaceBusyDoesNotEnterApplicationOrSwap(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
		t.Fatal(err)
	}
	f.startDaemonChild(0)
	f.serving(20 * time.Second)
	f.deploy.config.Daemon.ShutdownPolicy = daemonkit.PreserveOwned
	calls := len(f.launchctls)
	called := false
	_, err := f.deploy.Replace(f.within(10*time.Second), f.candidate("Second", "2.0", "two"), func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, daemonkit.ErrDrainBusy) || errors.Is(err, ErrMaintenanceIncomplete) {
		t.Fatalf("Replace=%v", err)
	}
	if called || len(f.launchctls) != calls || fileExists(f.deploy.layout.swap) || fileExists(f.deploy.maintenancePath()) {
		t.Fatalf("busy replacement mutated state: called=%v launchctl=%v", called, f.launchctls[calls:])
	}
	f.wantCanonical("one")
}

func TestPreservingReplaceFailedApplicationLeavesRecoveryBlocked(t *testing.T) {
	f := newFixture(t)
	f.deploy.config.Daemon.ShutdownPolicy = daemonkit.PreserveOwned
	candidate := f.candidate("First", "1.0", "one")
	failure := errors.New("application preparation failed")
	_, err := f.deploy.Replace(f.ctx(), candidate, func(context.Context) error { return failure })
	if !errors.Is(err, failure) || !errors.Is(err, ErrMaintenanceIncomplete) {
		t.Fatalf("Replace=%v", err)
	}
	if fileExists(f.app) || fileExists(f.deploy.layout.swap) || !fileExists(f.deploy.maintenancePath()) || len(f.launchctls) != 0 {
		t.Fatal("failed preparation advanced or lost its durable intent")
	}
	if _, err := f.deploy.Install(f.ctx(), candidate); !errors.Is(err, ErrMaintenanceIncomplete) {
		t.Fatalf("Install resumed incomplete maintenance: %v", err)
	}
}

func TestPreservingPristineGateIncludesDurableOwnership(t *testing.T) {
	f := newFixture(t)
	path := f.deploy.config.Daemon.RecordPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("unresolved owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.deploy.requirePristine(); !errors.Is(err, daemonkit.ErrDrainBusy) {
		t.Fatalf("pristine=%v", err)
	}
}

func TestVerifyCandidateDoesNotCreateDeploymentState(t *testing.T) {
	f := newFixture(t)
	candidate := f.candidate("First", "1.0", "one")
	if err := f.deploy.VerifyCandidate(f.ctx(), candidate); err != nil {
		t.Fatal(err)
	}
	if fileExists(f.app) || fileExists(f.deploy.layout.metadata) || fileExists(f.deploy.layout.candidate) || len(f.launchctls) != 0 {
		t.Fatal("candidate verification mutated deployment")
	}
}

func TestPreservingFirstInstallMissingParentRefusesWithoutMutation(t *testing.T) {
	f := newFixture(t)
	candidate := f.candidate("First", "1.0", "one")
	f.deploy.config.Daemon.ShutdownPolicy = daemonkit.PreserveOwned
	parent := filepath.Join(f.root, "not-created")
	f.deploy.layout = layoutFor(filepath.Join(parent, "Example.app"))
	f.deploy.config.App = f.deploy.layout.canonical
	if err := f.deploy.VerifyCandidate(f.ctx(), candidate); err != nil {
		t.Fatal(err)
	}
	_, err := f.deploy.Replace(f.ctx(), candidate, func(context.Context) error { t.Error("application callback invoked"); return nil })
	if !errors.Is(err, ErrFirstInstallUnavailable) {
		t.Fatalf("Replace=%v", err)
	}
	if _, err := os.Stat(parent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("installation parent was mutated: %v", err)
	}
}

func TestVerifyCandidateIgnoresPathCodesign(t *testing.T) {
	f := newFixture(t)
	candidate := f.candidate("First", "1.0", "one")
	directory := t.TempDir()
	marker := filepath.Join(directory, "invoked")
	program := "#!/bin/sh\nprintf invoked > '" + marker + "'\nexit 1\n"
	if err := os.WriteFile(filepath.Join(directory, "codesign"), []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)
	if err := f.deploy.VerifyCandidate(f.ctx(), candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("untrusted codesign was invoked: %v", err)
	}
}

func TestReplaceReattestsPreviouslyVerifiedCandidate(t *testing.T) {
	f := newFixture(t)
	f.deploy.config.Daemon.ShutdownPolicy = daemonkit.PreserveOwned
	candidate := f.candidate("First", "1.0", "one")
	if err := f.deploy.VerifyCandidate(f.ctx(), candidate); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(candidate.Source, bundleBodyRel), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	called := false
	_, err := f.deploy.Replace(f.ctx(), candidate, func(context.Context) error { called = true; return nil })
	if !errors.Is(err, ErrUntrusted) || called || fileExists(f.deploy.maintenancePath()) || len(f.launchctls) != 0 {
		t.Fatalf("changed candidate crossed preparation: err=%v called=%v", err, called)
	}
}

func TestPreservingSupersedeRefusesBeforeRemovalOrSwap(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
		t.Fatal(err)
	}
	f.startDaemonChild(0)
	f.serving(20 * time.Second)
	f.deploy.config.Daemon.ShutdownPolicy = daemonkit.PreserveOwned
	client, err := daemonkit.Open(f.deploy.config.Daemon)
	if err != nil {
		t.Fatal(err)
	}
	f.deploy.client = client
	calls := len(f.launchctls)
	_, err = f.deploy.Supersede(f.within(10*time.Second), f.candidate("Second", "2.0", "two"))
	if !errors.Is(err, daemonkit.ErrDrainBusy) {
		t.Fatalf("Supersede=%v", err)
	}
	if len(f.launchctls) != calls {
		t.Fatalf("services changed: %v", f.launchctls[calls:])
	}
	if fileExists(f.deploy.layout.swap) || fileExists(f.deploy.layout.prior) {
		t.Fatal("preservation refusal committed swap writes")
	}
	f.wantCanonical("one")
	control, err := client.Control(f.within(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = control.Close(f.within(time.Second)) }()
	health, err := control.Health(f.within(time.Second))
	if err != nil || health.Phase != daemonkit.PhaseReady {
		t.Fatalf("incumbent=%+v err=%v", health, err)
	}
}

func TestPreservingQuiesceDoesNotInferQuietFromAbsentHost(t *testing.T) {
	f := newFixture(t)
	f.deploy.config.Daemon.ShutdownPolicy = daemonkit.PreserveOwned
	client, err := daemonkit.Open(f.deploy.config.Daemon)
	if err != nil {
		t.Fatal(err)
	}
	f.deploy.client = client
	_, err = f.deploy.Quiesce(f.within(time.Second))
	if !errors.Is(err, daemonkit.ErrDrainBusy) {
		t.Fatalf("Quiesce=%v", err)
	}
	if len(f.launchctls) != 0 {
		t.Fatalf("launchctl=%v", f.launchctls)
	}
}
