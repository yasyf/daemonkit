//go:build !darwin

package proc

import (
	"errors"
	"fmt"
	"os"
)

// ExecutablePath returns the absolute executable path for pid.
func ExecutablePath(pid int) (string, error) {
	path, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNoProcess
	}
	if err != nil {
		return "", fmt.Errorf("read executable path for pid %d: %w", pid, err)
	}
	return path, nil
}
