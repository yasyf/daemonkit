package artifact

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/daemon"
)

func (s Store) resolvePythonTool(ctx context.Context, desc *Descriptor, version string, o options) (string, error) {
	entrypoint := desc.Tool.Entrypoint
	if entrypoint == "" {
		entrypoint = desc.Name
	}
	// Contain dist, version, and the entrypoint name: all three flow from the
	// descriptor (version from arbitrary command output for a dynamic tool) and
	// must not escape the tool store.
	toolDir, err := safeJoin(s.ToolsDir(), filepath.Join(desc.Tool.Dist, version))
	if err != nil {
		return "", err
	}
	binDir := filepath.Join(toolDir, "bin")
	link, err := safeJoin(binDir, entrypoint)
	if err != nil {
		return "", err
	}
	marker := filepath.Join(toolDir, ".installed")

	if regular(marker) {
		return realEntrypoint(link)
	}
	if err := s.withLock(ctx, "tool:"+desc.Tool.Dist+"@"+version, func() error {
		if regular(marker) {
			return nil
		}
		return installPythonTool(ctx, o.uvExecutable(), desc.Tool.Dist, version, toolDir, binDir, link, marker)
	}); err != nil {
		return "", err
	}
	return realEntrypoint(link)
}

// installPythonTool materializes a tool env and writes .installed only after uv
// succeeds and the entrypoint is present, so a failed or interrupted install is
// never reused: any prior partial is cleared first, and a failure removes the env.
func installPythonTool(ctx context.Context, uv, dist, version, toolDir, binDir, link, marker string) error {
	if err := os.RemoveAll(toolDir); err != nil {
		return fmt.Errorf("artifact: clear prior tool env: %w", err)
	}
	if err := installTool(ctx, uv, dist, version, toolDir, binDir); err != nil {
		_ = os.RemoveAll(toolDir)
		return err
	}
	if _, err := realEntrypoint(link); err != nil {
		_ = os.RemoveAll(toolDir)
		return fmt.Errorf("artifact: uv tool install %s did not produce its entrypoint: %w", dist, err)
	}
	// Fsync the env's file data before the marker, so a crash cannot leave a
	// durable .installed pointing at a truncated env (the release path SyncDirs
	// its stage before the rename for the same reason).
	if err := syncTree(toolDir); err != nil {
		_ = os.RemoveAll(toolDir)
		return fmt.Errorf("artifact: sync tool env: %w", err)
	}
	if err := daemon.WriteFileDurable(marker, nil, 0o600); err != nil {
		_ = os.RemoveAll(toolDir)
		return fmt.Errorf("artifact: mark tool env installed: %w", err)
	}
	return nil
}

func realEntrypoint(link string) (string, error) {
	info, err := os.Lstat(link)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return filepath.EvalSymlinks(link)
	}
	return link, nil
}

func installTool(ctx context.Context, uv, dist, version, toolDir, binDir string) error {
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		return fmt.Errorf("artifact: create tool directory: %w", err)
	}
	cmd := exec.CommandContext(ctx, uv, "tool", "install", "--force", dist+"=="+version)
	cmd.Env = append(os.Environ(), "UV_TOOL_DIR="+toolDir, "UV_TOOL_BIN_DIR="+binDir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("artifact: uv tool install %s==%s: %w: %s", dist, version, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
