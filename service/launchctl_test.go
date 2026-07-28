package service

import (
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/worker"
)

// The producer feeding every classifier call site is worker.Pool, whose failure
// value is *worker.ExitError — a struct with an ExitCode field, not a method.
var _ = launchctlOutcome("bootout", "", &worker.ExitError{ExitCode: launchctlNotLoadedExit})

func TestLaunchctlOutcomeClassifiesTheProducersErrorType(t *testing.T) {
	errRunner := errors.New("service: disposable task runner is required")
	tests := []struct {
		name       string
		out        string
		err        error
		wantKind   launchctlOutcomeKind
		wantCode   int
		wantReason string
	}{
		{name: "success", wantKind: launchctlLoaded, wantCode: -1},
		{
			name: "bootout no such process", out: "Boot-out failed: 3: No such process",
			err: launchctlExit(launchctlNotLoadedExit), wantKind: launchctlNotLoaded, wantCode: 3,
		},
		{
			name: "print could not find service", out: `Could not find service "x" in domain for user gui: 502`,
			err: launchctlExit(launchctlNotFoundExit), wantKind: launchctlNotLoaded, wantCode: 113,
		},
		{
			name: "operation now in progress", out: "Boot-out failed: 36: Operation now in progress",
			err: launchctlExit(launchctlInProgressExit), wantKind: launchctlInFlux, wantCode: 36,
		},
		{
			name: "operation already in progress", out: "Bootstrap failed: 37: Operation already in progress",
			err: launchctlExit(launchctlAlreadyExit), wantKind: launchctlInFlux, wantCode: 37,
		},
		{
			name: "aggregate status is never decoded", out: "Bootstrap failed: 5: Input/output error",
			err: launchctlExit(launchctlAggregateExit), wantKind: launchctlUnknown, wantCode: 5,
		},
		{
			name: "aggregate status never adopts a batch member's reason",
			out:  "Bootstrap failed: 108: Invalid path\nBootstrap failed: 5: Input/output error",
			err:  launchctlExit(launchctlAggregateExit), wantKind: launchctlUnknown, wantCode: 5,
		},
		{
			name: "refusal carries launchd's own reason", out: "Boot-out failed: 1: Operation not permitted",
			err: launchctlExit(1), wantKind: launchctlRefused, wantCode: 1, wantReason: "Operation not permitted",
		},
		{
			name: "refusal ignores a reason line for another status",
			out:  "Bootstrap failed: 108: Invalid path", err: launchctlExit(77),
			wantKind: launchctlUnknown, wantCode: 77,
		},
		{name: "status without a reason line", out: "denied", err: launchctlExit(77), wantKind: launchctlUnknown, wantCode: 77},
		{name: "failure that never reached launchd", err: errRunner, wantKind: launchctlUnknown, wantCode: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := launchctlOutcome("bootout", test.out, test.err)
			if got.kind != test.wantKind {
				t.Errorf("kind = %d, want %d", got.kind, test.wantKind)
			}
			if got.code != test.wantCode {
				t.Errorf("code = %d, want %d", got.code, test.wantCode)
			}
			if got.reason != test.wantReason {
				t.Errorf("reason = %q, want %q", got.reason, test.wantReason)
			}
			if (got.fail() == nil) != (test.wantKind == launchctlLoaded) {
				t.Errorf("fail() = %v for kind %d", got.fail(), got.kind)
			}
			if settled := got.settled(); settled != (test.wantKind == launchctlLoaded || test.wantKind == launchctlNotLoaded) {
				t.Errorf("settled() = %v for kind %d", settled, got.kind)
			}
		})
	}
}

func TestLaunchctlOutcomeFailNamesTheOfflineDecodings(t *testing.T) {
	refused := launchctlOutcome("bootout", "Boot-out failed: 1: Operation not permitted", launchctlExit(1))
	if got := refused.fail().Error(); !strings.Contains(got, "launchd refused: Operation not permitted") {
		t.Errorf("refusal error = %q, want launchd's own reason", got)
	}

	unknown := launchctlOutcome("bootstrap", "Bootstrap failed: 5: Input/output error", launchctlExit(launchctlAggregateExit))
	got := unknown.fail().Error()
	for _, want := range []string{
		"unclassified launchctl status 5",
		"launchctl error 5",
		`subsystem == "com.apple.xpc.launchd" AND processID == 1`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("unknown-status error = %q, want it to name %q", got, want)
		}
	}
	if strings.Contains(got, "session") {
		t.Errorf("unknown-status error = %q, must prescribe nothing about session types", got)
	}

	unreached := launchctlOutcome("print", "", errors.New("runner gone"))
	if got := unreached.fail().Error(); strings.Contains(got, "launchctl error") {
		t.Errorf("non-launchd failure error = %q, must not prescribe decoding a status", got)
	}
}
