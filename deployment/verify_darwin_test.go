//go:build darwin

package deployment

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/codeidentity"
)

// TestCodesignVerifierRejectsUnsigned exercises the real production seam:
// codesign must fail on an unsigned bundle, so New wires a verifier that
// rejects untrusted code rather than a no-op.
func TestCodesignVerifierRejectsUnsigned(t *testing.T) {
	app := filepath.Join(t.TempDir(), "Unsigned.app", "Contents", "MacOS")
	if err := os.MkdirAll(app, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(app, "Unsigned"), []byte("bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	appPath := filepath.Dir(filepath.Dir(app))

	dr, err := (codeidentity.CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.Unsigned"}).DRString()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New().verifier.Verify(context.Background(), appPath, dr); err == nil {
		t.Fatal("codesign verifier accepted an unsigned bundle")
	}
}
