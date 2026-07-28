package service

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/yasyf/daemonkit/worker"
)

const (
	launchctlNotLoadedExit  = 3
	launchctlInProgressExit = 36
	launchctlAlreadyExit    = 37
	launchctlNotFoundExit   = 113
	// launchctlAggregateExit is launchctl's batch status: a bootstrap mixing
	// "108: Invalid path" with a success collapses to it, and an unprivileged
	// refusal reports it as "Bootstrap failed: 5: Input/output error" alongside
	// "Try re-running the command as root for richer errors". It names no
	// condition, so it is never decoded and never retried.
	launchctlAggregateExit = 5
)

// launchctlOutcomeKind is the closed set of launchd verdicts the service
// package recognises. Only launchctlInFlux is retryable.
type launchctlOutcomeKind int

const (
	launchctlLoaded launchctlOutcomeKind = iota
	launchctlNotLoaded
	launchctlRefused
	launchctlInFlux
	launchctlUnknown
)

// launchctlResult is one classified launchctl invocation. Every launchctl call
// in this package returns one, so no call site can classify the boundary itself.
type launchctlResult struct {
	verb   string
	kind   launchctlOutcomeKind
	code   int
	reason string
	out    string
	cause  error
}

// launchdReasonPattern matches launchd's own "<verb> failed: <code>: <reason>"
// line, the machine-readable refusal launchctl prints beside a nonzero status
// (`Boot-out failed: 1: Operation not permitted`).
var launchdReasonPattern = regexp.MustCompile(`(?m)^\S+ failed: (\d+): (.+)$`)

// launchctlOutcome classifies one launchctl invocation from its status and
// combined output, and is the only classifier of this boundary in the package.
// Exit 3 and 113 mean launchd does not know the label. Exit 36 and 37 are
// launchd's own EINPROGRESS and EALREADY — "operation now/already in progress",
// per `launchctl error 36` — the sole positive evidence that the same call can
// later succeed. Exit 5 is the aggregate batch status and is never decoded. Any
// other status whose output carries launchd's own reason for that same status is
// a refusal; everything else is unknown to daemonkit.
func launchctlOutcome(verb, out string, err error) launchctlResult {
	result := launchctlResult{verb: verb, code: launchctlExitCode(err), out: out, cause: err}
	if err == nil {
		result.kind = launchctlLoaded
		return result
	}
	switch result.code {
	case launchctlNotLoadedExit, launchctlNotFoundExit:
		result.kind = launchctlNotLoaded
	case launchctlInProgressExit, launchctlAlreadyExit:
		result.kind = launchctlInFlux
	case launchctlAggregateExit:
		result.kind = launchctlUnknown
	default:
		result.kind = launchctlUnknown
		result.reason = launchdReason(out, result.code)
		if result.reason != "" {
			result.kind = launchctlRefused
		}
	}
	return result
}

func launchctlExitCode(err error) int {
	var exitErr *worker.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode
	}
	return -1
}

func launchdReason(out string, code int) string {
	want := strconv.Itoa(code)
	for _, match := range launchdReasonPattern.FindAllStringSubmatch(out, -1) {
		if match[1] == want {
			return strings.TrimSpace(match[2])
		}
	}
	return ""
}

// settled reports an outcome needing no further action: launchd either did the
// work or does not know the label. It answers for bootout, not for a verb whose
// success requires launchd to know the label.
func (r launchctlResult) settled() bool {
	return r.kind == launchctlLoaded || r.kind == launchctlNotLoaded
}

// fail reports the classified failure, or nil when launchctl succeeded. A
// refusal carries launchd's own reason; an unknown status carries the two ways
// to decode it offline, since launchctl's own log subsystem redacts the reason
// to <private> and only launchd[1] logs it in the clear.
func (r launchctlResult) fail() error {
	out := strings.TrimSpace(r.out)
	switch r.kind {
	case launchctlLoaded:
		return nil
	case launchctlRefused:
		return fmt.Errorf("launchctl %s: %w: %s (launchd refused: %s)", r.verb, r.cause, out, r.reason)
	case launchctlUnknown:
		if r.code < 0 {
			return fmt.Errorf("launchctl %s: %w: %s", r.verb, r.cause, out)
		}
		return fmt.Errorf(
			"launchctl %s: %w: %s (unclassified launchctl status %d; decode it with `launchctl error %d`"+
				` and read launchd's own "failed (<code>: <reason>)" from`+
				" `log show --predicate 'subsystem == \"com.apple.xpc.launchd\" AND processID == 1' --last 5m`)",
			r.verb, r.cause, out, r.code, r.code,
		)
	}
	return fmt.Errorf("launchctl %s: %w: %s", r.verb, r.cause, out)
}
