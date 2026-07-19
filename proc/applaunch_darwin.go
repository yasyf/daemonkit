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
	arguments := []string{"-n", "-g", app}
	if len(s.Args) != 0 {
		arguments = append(arguments, "--args")
		arguments = append(arguments, s.Args...)
	}
	cmd := exec.Command("open", arguments...)
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = logFile, logFile
	return cmd, logFile, nil
}
