package service

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestControllerUpgradeSimulationKeepsStableProgramPath(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := stableTestRoot(t)
	v1Bytes := "#!/bin/sh\n# release one\n"
	v2Bytes := "#!/bin/sh\n# release two\n"
	v1 := fakeExecutable(t, "daemon", v1Bytes)
	v2 := fakeExecutable(t, "daemon", v2Bytes)

	stableV1, err := stableProgram(root, "daemon", "v1.0.0", v1)
	if err != nil {
		t.Fatal(err)
	}
	if got := stableBytes(t, stableV1); got != v1Bytes {
		t.Fatalf("stable program bytes = %q, want %q", got, v1Bytes)
	}
	agent := controllerAgent(t, "com.example.upgrade")
	agent.Program = stableV1
	run := launchctlStub(func(args []string) (string, error) {
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		return "", nil
	})
	controller, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, run, nil)

	if err := controller.Converge(t.Context(), []Agent{agent}); err != nil {
		t.Fatalf("Converge(v1) = %v", err)
	}
	assertUpgradeStoredProgram(t, store, agent.Label, stableV1)
	plistPath, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	plistV1, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatal(err)
	}
	wantPlist, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plistV1, wantPlist) {
		t.Fatalf("v1 plist differs from agent plist:\n%s", plistV1)
	}

	if err := os.RemoveAll(filepath.Dir(v1)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(v1); !os.IsNotExist(err) {
		t.Fatalf("v1 source still exists after GC: %v", err)
	}
	stableV2, err := stableProgram(root, "daemon", "v2.0.0", v2)
	if err != nil {
		t.Fatal(err)
	}
	if stableV2 != stableV1 {
		t.Fatalf("stable program changed across upgrade: %q != %q", stableV2, stableV1)
	}
	if err := controller.Converge(t.Context(), []Agent{agent}); err != nil {
		t.Fatalf("Converge(v2) = %v", err)
	}
	assertUpgradeStoredProgram(t, store, agent.Label, stableV1)
	plistV2, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plistV2, plistV1) {
		t.Fatalf("plist changed across stable-program upgrade:\n--- v1 ---\n%s\n--- v2 ---\n%s", plistV1, plistV2)
	}
	if got := stableBytes(t, stableV2); got != v2Bytes {
		t.Fatalf("stable program bytes = %q, want %q", got, v2Bytes)
	}
}

func TestUpgradeSimulationStableProgramSurvivesContentAddressedCacheGC(t *testing.T) {
	root := stableTestRoot(t)
	cacheRoot := stableTestRoot(t)
	cacheEntry := filepath.Join(
		cacheRoot,
		"28af1e73c26b160c7f459b92c4c5b94d85c4c6129bf27f13f765cae785def6e8",
	)
	if err := os.MkdirAll(cacheEntry, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(cacheEntry, "daemon")
	sourceBytes := []byte("#!/bin/sh\n# cached release\n")
	if err := os.WriteFile(source, sourceBytes, 0o755); err != nil {
		t.Fatal(err)
	}
	stable, err := stableProgram(root, "daemon", "v1.0.0", source)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(cacheEntry); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("cache source still exists after GC: %v", err)
	}
	if err := validateProgramTree(Agent{Program: stable}); err != nil {
		t.Fatalf("stable path failed validation after cache GC: %v", err)
	}
	if got := stableBytes(t, stable); got != string(sourceBytes) {
		t.Fatalf("stable program bytes = %q, want %q", got, sourceBytes)
	}
}

func assertUpgradeStoredProgram(
	t *testing.T,
	store *controllerStoreStub,
	label string,
	program string,
) {
	t.Helper()
	for _, state := range []struct {
		name   string
		agents map[string]Agent
	}{
		{name: "desired", agents: store.state.Desired},
		{name: "applied", agents: store.state.Applied},
	} {
		if len(state.agents) != 1 {
			t.Fatalf("%s agents = %d, want one label", state.name, len(state.agents))
		}
		agent, ok := state.agents[label]
		if !ok {
			t.Fatalf("%s agents do not contain label %q", state.name, label)
		}
		if agent.Program != program {
			t.Fatalf("%s program = %q, want stable path %q", state.name, agent.Program, program)
		}
	}
}
