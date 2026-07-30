package trust_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Only the consumer-supplied half of the old marker list survives. The six
// injection entitlement identifiers and com.apple.security.application-groups
// are unconditional constants in the one verifier now, so every daemonkit
// binary contains them; that half of the invariant is given up by the
// kernel-only decision and recorded in the CHANGELOG.
var signedOnlyMarkers = [][]byte{
	[]byte("group.com.example.daemonkit.signed-only-marker"),
	[]byte("com.example.daemonkit.signed-only-marker"),
	[]byte("daemonkit-signed-only-marker-value"),
}

func TestDaemonFacingBinaryExcludesConsumerSuppliedPolicyValues(t *testing.T) {
	root := moduleRoot(t)
	temporary := t.TempDir()
	daemonBinary := filepath.Join(temporary, "daemoncli")
	signedBinary := filepath.Join(temporary, "signedcli")
	runGo(t, root, "build", "-trimpath", "-o", daemonBinary, "./internal/trust/testdata/daemoncli")
	runGo(t, root, "build", "-trimpath", "-o", signedBinary, "./internal/trust/testdata/signedcli")
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
	root := filepath.Dir(filepath.Dir(workingDirectory))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("locate module root from %q: %v", workingDirectory, err)
	}
	return root
}

func runGo(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	command := exec.Command("go", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
}
