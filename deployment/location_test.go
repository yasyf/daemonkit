package deployment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
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
	_ = store
	if store.Path != deploymentPathsForLocation(location).serviceProcess {
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
