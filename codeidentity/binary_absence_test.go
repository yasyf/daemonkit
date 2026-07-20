package codeidentity_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var signedOnlyMarkers = [][]byte{
	[]byte("com.apple.security.application-groups"),
	[]byte("com.apple.security.cs.disable-library-validation"),
	[]byte("com.apple.security.cs.allow-dyld-environment-variables"),
	[]byte("com.apple.security.cs.allow-unsigned-executable-memory"),
	[]byte("com.apple.security.cs.allow-jit"),
	[]byte("com.apple.security.cs.disable-executable-page-protection"),
	[]byte("com.apple.security.get-task-allow"),
	[]byte("group.com.example.daemonkit.signed-only-marker"),
	[]byte("com.example.daemonkit.signed-only-marker"),
	[]byte("daemonkit-signed-only-marker-value"),
}

func TestDaemonFacingBinaryExcludesSignedOnlyPackagesAndPolicyLiterals(t *testing.T) {
	root := moduleRoot(t)
	dependencies := runGo(t, root, "list", "-deps", "-f", "{{.ImportPath}}", "./codeidentity/testdata/daemoncli")
	for _, forbidden := range []string{
		"github.com/yasyf/daemonkit/trust",
		"github.com/yasyf/daemonkit/appgroup",
	} {
		for _, dependency := range strings.Fields(dependencies) {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden+"/") {
				t.Fatalf("daemon-facing dependency graph includes signed-only package %q", forbidden)
			}
		}
	}

	temporary := t.TempDir()
	daemonBinary := filepath.Join(temporary, "daemoncli")
	signedBinary := filepath.Join(temporary, "signedcli")
	runGo(t, root, "build", "-trimpath", "-o", daemonBinary, "./codeidentity/testdata/daemoncli")
	runGo(t, root, "build", "-trimpath", "-o", signedBinary, "./codeidentity/testdata/signedcli")
	daemonBytes, err := os.ReadFile(daemonBinary)
	if err != nil {
		t.Fatalf("read daemon-facing fixture: %v", err)
	}
	signedBytes, err := os.ReadFile(signedBinary)
	if err != nil {
		t.Fatalf("read signed-only fixture: %v", err)
	}
	for _, marker := range signedOnlyMarkers {
		if bytes.Contains(daemonBytes, marker) {
			t.Errorf("daemon-facing fixture contains signed-only marker %q", marker)
		}
		if !bytes.Contains(signedBytes, marker) {
			t.Errorf("signed-only control does not contain marker %q", marker)
		}
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	root := filepath.Dir(workingDirectory)
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("locate module root from %q: %v", workingDirectory, err)
	}
	return root
}

func runGo(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("go", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return string(output)
}
