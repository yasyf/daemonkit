//go:build darwin

package proc

import (
	"fmt"
	"os"
	"os/exec"
)

func appLaunchCmd(s Spawn, app string) (*exec.Cmd, *os.File, error) {
	logFile, err := os.OpenFile(s.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open child log: %w", err)
	}
	cmd := exec.Command("open", "-n", "-g", app)
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = logFile, logFile
	return cmd, logFile, nil
}
