//go:build darwin

package launchd

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
		return "", fmt.Errorf("launchd: invalid process type %d", p)
	}
}

func wholeSeconds(name string, value time.Duration) (int64, error) {
	if value == 0 {
		return 0, nil
	}
	if value < time.Second || value%time.Second != 0 {
		return 0, fmt.Errorf("launchd: %s must be a positive whole number of seconds", name)
	}
	return int64(value / time.Second), nil
}
