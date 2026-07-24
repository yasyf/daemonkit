package artifact

import (
	"context"
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
	if calls, _ := os.ReadFile(marker); !strings.Contains(string(calls), "tool install --force capt-hook==1.2.3") {
		t.Fatalf("uv args = %q, want the pinned install spec", calls)
	}

	path2, err := store.Resolve(context.Background(), desc, WithUV(uv))
	if err != nil || path2 != path {
		t.Fatalf("second Resolve = %q, %v; want %q, nil", path2, err, path)
	}
	calls, _ := os.ReadFile(marker)
	if got := strings.Count(string(calls), "install"); got != 1 {
		t.Fatalf("uv invoked %d times, want 1 (env already materialized)", got)
	}
}
