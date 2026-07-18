//go:build !darwin

package proc

import (
	"os"
	"os/exec"
)

func appLaunchCmd(_ Spawn, _ string) (*exec.Cmd, *os.File, error) {
	return nil, nil, ErrAppLaunchUnsupported
}
