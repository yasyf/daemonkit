package launchd

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
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

// launchctlOutcomeKind is the closed set of launchd verdicts the launchd package
// recognises. Only launchctlInFlux is retryable.
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
func launchctlOutcome(verb, out string, code int, err error) launchctlResult {
	result := launchctlResult{verb: verb, code: code, out: out, cause: err}
	if err == nil && code == 0 {
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
//
// Only a positive status is one launchctl exited with, and only such a status
// has a decoding to prescribe. A zero or negative code reaches here from a
// [Runner] that could not run launchctl at all, or from one the kernel killed
// by signal — neither produced a status, and telling their caller to decode one
// is the misdiagnosis this reports without.
func (r launchctlResult) fail() error {
	out := strings.TrimSpace(r.out)
	switch r.kind {
	case launchctlLoaded:
		return nil
	case launchctlRefused:
		return r.errorf("%s (launchd refused: %s)", out, r.reason)
	case launchctlUnknown:
		if r.code <= 0 {
			return r.errorf("%s", out)
		}
		return r.errorf(
			"%s (unclassified launchctl status %d; decode it with `launchctl error %d`"+
				` and read launchd's own "failed (<code>: <reason>)" from`+
				" `log show --predicate 'subsystem == \"com.apple.xpc.launchd\" AND processID == 1' --last 5m`)",
			out, r.code, r.code,
		)
	}
	return r.errorf("%s", out)
}

// errorf renders one classified failure, wrapping the cause only when there is
// one. Every runner in the fleet answers an exit status with a nil error —
// launchd refusing is an answer, not a failure to run launchctl — so formatting
// the cause unconditionally printed %!w(<nil>) where the diagnosis belonged.
func (r launchctlResult) errorf(format string, args ...any) error {
	failure := fmt.Sprintf("launchctl %s: %s", r.verb, fmt.Sprintf(format, args...))
	if r.cause == nil {
		return errors.New(failure)
	}
	return fmt.Errorf("%s: %w", failure, r.cause)
}
