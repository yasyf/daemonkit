package service

import (
	"errors"
	"fmt"
)

// RestartPolicy defines when launchd restarts a job after it exits.
type RestartPolicy uint8

const (
	restartPolicyUnset RestartPolicy = iota
	// RestartAlways restarts the job after every exit.
	RestartAlways
	// RestartOnFailure restarts the job only after an unsuccessful exit.
	RestartOnFailure
	// NoRestart leaves the job stopped after it exits.
	NoRestart
)

func (p RestartPolicy) plist() (string, error) {
	switch p {
	case RestartAlways:
		return "    <key>KeepAlive</key>\n    <true/>\n", nil
	case RestartOnFailure:
		return "    <key>KeepAlive</key>\n    <dict>\n        <key>SuccessfulExit</key>\n        <false/>\n    </dict>\n", nil
	case NoRestart:
		return "    <key>KeepAlive</key>\n    <false/>\n", nil
	case restartPolicyUnset:
		return "", errors.New("restart policy is required")
	default:
		return "", fmt.Errorf("invalid restart policy %d", p)
	}
}
