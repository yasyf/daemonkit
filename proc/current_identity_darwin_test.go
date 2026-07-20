//go:build darwin

package proc

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCurrentIdentityBindsExactAuditTokenExecutableAndProcess(t *testing.T) {
	identity, err := CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	if identity.PID != os.Getpid() || !identity.AuditToken.Valid() ||
		identity.AuditToken.PID() != os.Getpid() ||
		identity.Executable != executable ||
		identity.StartTime == "" || identity.Boot == "" {
		t.Fatalf("CurrentIdentity = %+v, want exact current execution %q", identity, executable)
	}
}
