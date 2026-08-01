//go:build darwin

package launchd

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
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

// SessionType is a launchd session in which a job may be loaded.
//
// Deprecated: accepted and ignored. launchd refuses any job whose session type
// names a domain other than the bootstrap domain's own (error 134, "Service
// cannot load in requested session"), and daemonkit bootstraps only into
// gui/<uid> — so SessionTypeAqua was always a no-op and every other value was a
// guaranteed permanent refusal. The rendered plist no longer carries the key,
// and these symbols are removed in a future breaking release.
type SessionType uint8

const (
	sessionTypeUnset SessionType = iota
	// SessionTypeAqua is the graphical login session.
	//
	// Deprecated: accepted and ignored; see SessionType.
	SessionTypeAqua
	// SessionTypeBackground is the background user session.
	//
	// Deprecated: accepted and ignored; see SessionType.
	SessionTypeBackground
	// SessionTypeLoginWindow is the login-window session.
	//
	// Deprecated: accepted and ignored; see SessionType.
	SessionTypeLoginWindow
	// SessionTypeStandardIO is a standard-I/O login session.
	//
	// Deprecated: accepted and ignored; see SessionType.
	SessionTypeStandardIO
	// SessionTypeSystem is the system session.
	//
	// Deprecated: accepted and ignored; see SessionType.
	SessionTypeSystem
)

// ParseSessionType parses launchctl's exact typed manager/session name.
//
// Deprecated: the parsed value is accepted and ignored; see SessionType.
func ParseSessionType(value string) (SessionType, error) {
	switch strings.TrimSpace(value) {
	case "Aqua":
		return SessionTypeAqua, nil
	case "Background":
		return SessionTypeBackground, nil
	case "LoginWindow":
		return SessionTypeLoginWindow, nil
	case "StandardIO":
		return SessionTypeStandardIO, nil
	case "System":
		return SessionTypeSystem, nil
	default:
		return sessionTypeUnset, fmt.Errorf("launchd: unknown launchd session type %q", strings.TrimSpace(value))
	}
}

func (s SessionType) name() string {
	switch s {
	case SessionTypeAqua:
		return "Aqua"
	case SessionTypeBackground:
		return "Background"
	case SessionTypeLoginWindow:
		return "LoginWindow"
	case SessionTypeStandardIO:
		return "StandardIO"
	case SessionTypeSystem:
		return "System"
	default:
		return strconv.FormatUint(uint64(s), 10)
	}
}

// Aqua stays silent: it names the very domain daemonkit bootstraps into, so it
// was a no-op before the key was dropped and is a no-op after. Dropping the
// value is what makes the field inert — it is neither rendered nor stored, so a
// value left in memory would split reflect.DeepEqual (plansEqual included)
// between a live agent and that same agent loaded back from the store.
func acceptIgnoredSessionType(agent *Agent) {
	if agent.LimitLoadToSessionType != sessionTypeUnset &&
		agent.LimitLoadToSessionType != SessionTypeAqua {
		slog.Warn(
			"launchd: LimitLoadToSessionType is accepted and ignored; launchd permanently refuses a job "+
				"whose session type names a domain other than gui/<uid>, the only domain daemonkit bootstraps into",
			"label", agent.Label, "session_type", agent.LimitLoadToSessionType.name(),
		)
	}
	agent.LimitLoadToSessionType = sessionTypeUnset
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
