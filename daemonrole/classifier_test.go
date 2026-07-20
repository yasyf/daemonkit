package daemonrole

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

func TestClassifierFollowsExactAtomicRoleRetargetAcrossUpgrade(t *testing.T) {
	root := t.TempDir()
	oldExecutable := executableFixture(t, filepath.Join(root, "Cellar", "product", "1.0", "bin", "product"))
	newExecutable := executableFixture(t, filepath.Join(root, "Cellar", "product", "2.0", "bin", "product"))
	unrelated := executableFixture(t, filepath.Join(root, "Cellar", "other", "2.0", "bin", "other"))
	rolePath := filepath.Join(root, "bin", "product")
	if err := os.MkdirAll(filepath.Dir(rolePath), 0o700); err != nil {
		t.Fatal(err)
	}
	retargetRole(t, rolePath, oldExecutable)

	classifier := Classifier{RoleID: "homebrew.mxcl.product", RolePath: rolePath}
	peer := func(executable string) wire.Peer {
		return wire.Peer{PID: 42, UID: os.Geteuid(), StartTime: "start", Boot: "boot", Executable: executable}
	}
	if accepted, err := classifier.Classify(t.Context(), peer(oldExecutable)); err != nil || !accepted {
		t.Fatalf("old role target classification = %t, %v", accepted, err)
	}

	retargetRole(t, rolePath, newExecutable)
	if accepted, err := classifier.Classify(t.Context(), peer(newExecutable)); err != nil || !accepted {
		t.Fatalf("new role target classification = %t, %v", accepted, err)
	}
	for _, rejected := range []string{oldExecutable, unrelated} {
		if accepted, err := classifier.Classify(t.Context(), peer(rejected)); err != nil || accepted {
			t.Fatalf("unrelated target %q classification = %t, %v", rejected, accepted, err)
		}
	}
	if !classifier.AuthorizeBuild("v1.0.0", "v2.0.0") || classifier.AuthorizeBuild("v2.0.0", "v1.0.0") {
		t.Fatal("request-daemon same/newer build admission is not exact")
	}
}

func TestClassifierRejectsMissingRoleAndIncompleteOrForeignPeer(t *testing.T) {
	executable := executableFixture(t, filepath.Join(t.TempDir(), "product"))
	classifier := Classifier{RoleID: "com.example.product", RolePath: executable}
	for _, peer := range []wire.Peer{
		{PID: 42, UID: os.Geteuid() + 1, StartTime: "start", Boot: "boot", Executable: executable},
		{PID: 1, UID: os.Geteuid(), StartTime: "start", Boot: "boot", Executable: executable},
	} {
		accepted, err := classifier.Classify(t.Context(), peer)
		if accepted || (peer.PID > 1 && err != nil) || (peer.PID <= 1 && err == nil) {
			t.Fatalf("invalid peer classification = %t, %v", accepted, err)
		}
	}
	for _, roleID := range []string{"", "product", ".product", "com..product", "com.example/product"} {
		if err := (Classifier{RoleID: roleID, RolePath: executable}).Validate(); err == nil {
			t.Fatalf("role id %q validated", roleID)
		}
	}
}

func executableFixture(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func retargetRole(t *testing.T, rolePath, target string) {
	t.Helper()
	temporary := rolePath + ".new"
	_ = os.Remove(temporary)
	if err := os.Symlink(target, temporary); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, rolePath); err != nil {
		t.Fatal(err)
	}
}
