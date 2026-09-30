package artifact

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func buildSignedFixture(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	sources := map[string]string{
		"go.mod":  "module fixture\n\ngo 1.22\n",
		"main.go": "package main\n\nimport (\n\t\"fmt\"\n\t\"os\"\n)\n\nfunc main() { fmt.Println(os.Args[1]) }\n",
	}
	for name, body := range sources {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	bin := filepath.Join(dir, "fixture")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = dir
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off", "GOTOOLCHAIN=local")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build fixture: %v\n%s", err, out)
	}
	if out, err := exec.Command("codesign", "--force", "--sign", "-", bin).CombinedOutput(); err != nil {
		t.Fatalf("codesign fixture: %v\n%s", err, out)
	}
	data, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestResolveSignedAppCopyExecRelocatesSignedMachO(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	source := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
	if err := os.WriteFile(source, buildSignedFixture(t), 0o755); err != nil {
		t.Fatal(err)
	}
	store := Store{Root: t.TempDir()}

	path, err := store.Resolve(context.Background(), copyExecDescriptor(dir, "12.15.3"))
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if out, err := exec.Command("codesign", "--verify", "--strict", "--verbose=2", path).CombinedOutput(); err != nil {
		t.Fatalf("codesign --verify %q: %v\n%s", path, err, out)
	}
	out, err := exec.Command(path, "relocated").Output()
	if err != nil {
		t.Fatalf("run %q: %v", path, err)
	}
	if string(out) != "relocated\n" {
		t.Fatalf("output = %q, want relocated", out)
	}
}
