//go:build darwin

package fetch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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

	dr, err := testIdentity.DRString()
	if err != nil {
		t.Fatal(err)
	}
	if err := New().Verifier.Verify(context.Background(), appPath, dr); err == nil {
		t.Fatal("codesign verifier accepted an unsigned bundle")
	}
}
