package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fakeUV(t *testing.T, marker string) string {
	t.Helper()
	script := "#!/bin/sh\n" +
		"echo \"$@\" >> \"" + marker + "\"\n" +
		"real=\"$UV_TOOL_DIR/env/bin\"\n" +
		"mkdir -p \"$real\" \"$UV_TOOL_BIN_DIR\"\n" +
		"printf '#!/bin/sh\\necho tool\\n' > \"$real/capt-hookd\"\n" +
		"chmod +x \"$real/capt-hookd\"\n" +
		"ln -sf \"$real/capt-hookd\" \"$UV_TOOL_BIN_DIR/capt-hookd\"\n"
	path := filepath.Join(t.TempDir(), "uv")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestResolvePythonToolInstallsAndIsIdempotent(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "uv.calls")
	uv := fakeUV(t, marker)
	store := Store{Root: t.TempDir()}
	desc := &Descriptor{
		Schema: 1, Name: "capt-hook", Kind: PythonTool,
		Version: VersionSource{Static: "1.2.3"},
		Tool:    &ToolSpec{Dist: "capt-hook", Entrypoint: "capt-hookd"},
	}

	path, err := store.Resolve(context.Background(), desc, WithUV(uv))
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if !strings.Contains(path, filepath.Join("tools", "capt-hook", "1.2.3")) {
		t.Fatalf("path %q is not under the version store", path)
	}
	if filepath.Base(path) != "capt-hookd" {
		t.Fatalf("entrypoint base = %q, want capt-hookd", filepath.Base(path))
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("Resolve returned a symlink, want the real entrypoint (%v)", info.Mode())
	}
	calls, _ := os.ReadFile(marker)
	if string(calls) != "tool install --force --compile-bytecode --refresh-package capt-hook capt-hook==1.2.3\n" {
		t.Fatalf("uv args = %q, want the pinned install spec, bytecode compiled, with a refreshed index for its dist", calls)
	}

	path2, err := store.Resolve(context.Background(), desc, WithUV(uv))
	if err != nil || path2 != path {
		t.Fatalf("second Resolve = %q, %v; want %q, nil", path2, err, path)
	}
	calls, _ = os.ReadFile(marker)
	if got := strings.Count(string(calls), "install"); got != 1 {
		t.Fatalf("uv invoked %d times, want 1 (env already materialized)", got)
	}
}

func TestPythonToolDurabilityFailureCannotPublishOrReuseTheEnvironment(t *testing.T) {
	calls := filepath.Join(t.TempDir(), "uv.calls")
	uv := fakeUV(t, calls)
	store := Store{Root: t.TempDir()}
	toolDir := filepath.Join(store.ToolsDir(), "capt-hook", "1.2.3")
	binDir := filepath.Join(toolDir, "bin")
	marker := filepath.Join(toolDir, installedMarker)
	wantErr := errors.New("durability failure")
	err := installPythonTool(context.Background(), uv, "capt-hook", "1.2.3", toolDir, binDir, filepath.Join(binDir, "capt-hookd"), marker, func(root string) error {
		if root != toolDir {
			t.Fatalf("sync root = %s, want %s", root, toolDir)
		}
		if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("marker before durability = %v, want absent", err)
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("install = %v, want durability failure", err)
	}
	if _, err := os.Stat(toolDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed environment = %v, want removed", err)
	}
	desc := &Descriptor{
		Schema: 1, Name: "capt-hook", Kind: PythonTool,
		Version: VersionSource{Static: "1.2.3"},
		Tool:    &ToolSpec{Dist: "capt-hook", Entrypoint: "capt-hookd"},
	}
	if _, err := store.Resolve(context.Background(), desc, WithUV(uv)); err != nil {
		t.Fatalf("fresh Resolve = %v", err)
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), "install"); got != 2 {
		t.Fatalf("uv installs = %d, want 2", got)
	}
}
