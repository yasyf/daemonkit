package deploy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

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
