//go:build !darwin

package proc

import (
	"os"
	"os/exec"
)

// appLaunchCmd always refuses off darwin: `open` is macOS-only.
func appLaunchCmd(_ Spawn, _ string) (*exec.Cmd, *os.File, error) {
	return nil, nil, ErrAppLaunchUnsupported
}
