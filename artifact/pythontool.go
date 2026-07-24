package artifact

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func (s Store) resolvePythonTool(ctx context.Context, desc *Descriptor, version string, o options) (string, error) {
	entrypoint := desc.Tool.Entrypoint
	if entrypoint == "" {
		entrypoint = desc.Name
	}
	toolDir := filepath.Join(s.ToolsDir(), desc.Tool.Dist, version)
	binDir := filepath.Join(toolDir, "bin")
	link := filepath.Join(binDir, entrypoint)

	if resolved, err := realEntrypoint(link); err == nil {
		return resolved, nil
	}
	if err := s.withLock(ctx, "tool:"+desc.Tool.Dist+"@"+version, func() error {
		if _, err := realEntrypoint(link); err == nil {
			return nil
		}
		return installTool(ctx, o.uvExecutable(), desc.Tool.Dist, version, toolDir, binDir)
	}); err != nil {
		return "", err
	}
	return realEntrypoint(link)
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
