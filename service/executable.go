package service

import (
	"fmt"
	"os"
	"path/filepath"
)

// CanonicalExecutable returns the resolved regular executable backing the
// current process without searching PATH.
func CanonicalExecutable() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("service: resolve current executable: %w", err)
	}
	return canonicalExecutablePath(executable)
}

func canonicalExecutablePath(executable string) (string, error) {
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return "", fmt.Errorf("service: executable path %q is not exact and absolute", executable)
	}
	resolved, err := filepath.EvalSymlinks(executable)
	if err != nil {
		return "", fmt.Errorf("service: resolve executable path %q: %w", executable, err)
	}
	if !filepath.IsAbs(resolved) || filepath.Clean(resolved) != resolved {
		return "", fmt.Errorf("service: resolved executable path %q is not exact and absolute", resolved)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("service: inspect executable path %q: %w", resolved, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("service: executable path %q is not a regular file", resolved)
	}
	if info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("service: executable path %q is not executable", resolved)
	}
	return resolved, nil
}
