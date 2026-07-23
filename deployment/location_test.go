package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

func runtimeAppFixture(t *testing.T) (string, stateLocation) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	app := filepath.Join(dir, "Helper.app")
	executable := filepath.Join(app, "Contents", "MacOS", "Helper")
	if err := os.MkdirAll(filepath.Dir(executable), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("helper"), 0o700); err != nil {
		t.Fatal(err)
	}
	return executable, stateLocation{Dir: dir, AppName: "Helper"}
}

func TestRuntimeStopControlStoreDerivesExactAppWithoutCreatingState(t *testing.T) {
	executable, location := runtimeAppFixture(t)
	prior := runtimeExecutable
	runtimeExecutable = func() (string, error) { return executable, nil }
	t.Cleanup(func() { runtimeExecutable = prior })
	store, err := RuntimeStopControlStore()
	if err != nil {
		t.Fatal(err)
	}
	fileStore, ok := store.(*proc.FileStore)
	if !ok || fileStore.Path != deploymentPathsForLocation(location).serviceProcess {
		t.Fatalf("store = %#v", store)
	}
	if _, err := os.Lstat(filepath.Join(location.Dir, ".daemonkit-deployment")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("store lookup created state: %v", err)
	}
}

func TestRuntimeStopControlStoreRejectsWrongShapeAndSymlinkedApp(t *testing.T) {
	prior := runtimeExecutable
	t.Cleanup(func() { runtimeExecutable = prior })
	runtimeExecutable = func() (string, error) { return filepath.Join(t.TempDir(), "helper"), nil }
	if _, err := RuntimeStopControlStore(); err == nil {
		t.Fatal("wrong executable shape accepted")
	}
	executable, _ := runtimeAppFixture(t)
	realApp := filepath.Dir(filepath.Dir(filepath.Dir(executable)))
	link := filepath.Join(filepath.Dir(realApp), "Linked.app")
	if err := os.Symlink(realApp, link); err != nil {
		t.Fatal(err)
	}
	runtimeExecutable = func() (string, error) {
		return filepath.Join(link, "Contents", "MacOS", "Helper"), nil
	}
	if _, err := RuntimeStopControlStore(); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("symlinked app error = %v", err)
	}
}

func TestRuntimeStopControlStoreConsumesControllerAuthority(t *testing.T) {
	executable, location := runtimeAppFixture(t)
	prior := runtimeExecutable
	runtimeExecutable = func() (string, error) { return executable, nil }
	t.Cleanup(func() { runtimeExecutable = prior })
	runtimeStore, err := RuntimeStopControlStore()
	if err != nil {
		t.Fatal(err)
	}
	controllerStore := &proc.FileStore{Path: deploymentPathsForLocation(location).serviceProcess}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	reaper := &proc.Reaper{Store: controllerStore, Generation: "deployment-controller-test"}
	const role = "com.example.stop"
	const target = "runtime-generation"
	if _, err := reaper.TrackStopControl(
		context.Background(), identity, role, "build", 1, target, string(wire.StopIntentUpgrade), time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	_, consumed, err := runtimeStore.ConsumeStopControl(t.Context(), identity, role, target, time.Now())
	if err != nil || !consumed {
		t.Fatalf("ConsumeStopControl = %v, %v", consumed, err)
	}
}
