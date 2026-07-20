package service

import (
	"fmt"
	"time"
)

// ProcessType is launchd's resource-policy classification for a job.
type ProcessType uint8

const (
	processTypeUnset ProcessType = iota
	// ProcessTypeAdaptive lets launchd move the job between background and
	// interactive policy in response to XPC activity.
	ProcessTypeAdaptive
	// ProcessTypeBackground applies launchd's background resource policy.
	ProcessTypeBackground
	// ProcessTypeInteractive applies launchd's interactive resource policy.
	ProcessTypeInteractive
	// ProcessTypeStandard applies launchd's standard resource policy.
	ProcessTypeStandard
)

func (p ProcessType) plistValue() (string, error) {
	switch p {
	case processTypeUnset:
		return "", nil
	case ProcessTypeAdaptive:
		return "Adaptive", nil
	case ProcessTypeBackground:
		return "Background", nil
	case ProcessTypeInteractive:
		return "Interactive", nil
	case ProcessTypeStandard:
		return "Standard", nil
	default:
		return "", fmt.Errorf("service: invalid process type %d", p)
	}
}

// SessionType is a launchd session in which a job may be loaded.
type SessionType uint8

const (
	sessionTypeUnset SessionType = iota
	// SessionTypeAqua is the graphical login session.
	SessionTypeAqua
	// SessionTypeBackground is the background user session.
	SessionTypeBackground
	// SessionTypeLoginWindow is the login-window session.
	SessionTypeLoginWindow
	// SessionTypeStandardIO is a standard-I/O login session.
	SessionTypeStandardIO
	// SessionTypeSystem is the system session.
	SessionTypeSystem
)

func (s SessionType) plistValue() (string, error) {
	switch s {
	case sessionTypeUnset:
		return "", nil
	case SessionTypeAqua:
		return "Aqua", nil
	case SessionTypeBackground:
		return "Background", nil
	case SessionTypeLoginWindow:
		return "LoginWindow", nil
	case SessionTypeStandardIO:
		return "StandardIO", nil
	case SessionTypeSystem:
		return "System", nil
	default:
		return "", fmt.Errorf("service: invalid session type %d", s)
	}
}

func startIntervalSeconds(interval time.Duration) (int64, error) {
	if interval == 0 {
		return 0, nil
	}
	if interval < time.Second || interval%time.Second != 0 {
		return 0, fmt.Errorf("service: start interval must be a positive whole number of seconds")
	}
	return int64(interval / time.Second), nil
}
