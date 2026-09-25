package deploy

import (
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

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
