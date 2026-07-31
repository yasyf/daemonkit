package launchd

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func launchctlErr(code int) error {
	return fmt.Errorf("launchctl exited %d", code)
}

func TestLaunchctlOutcomeClassifies(t *testing.T) {
	tests := []struct {
		name       string
		out        string
		code       int
		err        error
		wantKind   launchctlOutcomeKind
		wantReason string
		wantFail   bool
		wantSettle bool
	}{
		{name: "success", code: 0, wantKind: launchctlLoaded, wantSettle: true},
		{
			name: "bootout no such process", out: "Boot-out failed: 3: No such process",
			code: launchctlNotLoadedExit, err: launchctlErr(3),
			wantKind: launchctlNotLoaded, wantFail: true, wantSettle: true,
		},
		{
			name: "print could not find service", out: `Could not find service "x" in domain for user gui: 502`,
			code: launchctlNotFoundExit, err: launchctlErr(113),
			wantKind: launchctlNotLoaded, wantFail: true, wantSettle: true,
		},
		{
			name: "operation now in progress", out: "Boot-out failed: 36: Operation now in progress",
			code: launchctlInProgressExit, err: launchctlErr(36),
			wantKind: launchctlInFlux, wantFail: true,
		},
		{
			name: "operation already in progress", out: "Bootstrap failed: 37: Operation already in progress",
			code: launchctlAlreadyExit, err: launchctlErr(37),
			wantKind: launchctlInFlux, wantFail: true,
		},
		{
			name: "aggregate status is never decoded", out: "Bootstrap failed: 5: Input/output error",
			code: launchctlAggregateExit, err: launchctlErr(5),
			wantKind: launchctlUnknown, wantFail: true,
		},
		{
			name: "aggregate status never adopts a batch member's reason",
			out:  "Bootstrap failed: 108: Invalid path\nBootstrap failed: 5: Input/output error",
			code: launchctlAggregateExit, err: launchctlErr(5),
			wantKind: launchctlUnknown, wantFail: true,
		},
		{
			name: "refusal carries launchd's own reason", out: "Boot-out failed: 1: Operation not permitted",
			code: 1, err: launchctlErr(1),
			wantKind: launchctlRefused, wantReason: "Operation not permitted", wantFail: true,
		},
		{
			name: "refusal ignores a reason line for another status",
			out:  "Bootstrap failed: 108: Invalid path", code: 77, err: launchctlErr(77),
			wantKind: launchctlUnknown, wantFail: true,
		},
		{
			name: "status without a reason line", out: "denied", code: 77, err: launchctlErr(77),
			wantKind: launchctlUnknown, wantFail: true,
		},
		{
			name: "failure that never reached launchd", code: -1, err: errors.New("runner gone"),
			wantKind: launchctlUnknown, wantFail: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := launchctlOutcome("bootout", test.out, test.code, test.err)
			if got.kind != test.wantKind {
				t.Errorf("kind = %d, want %d", got.kind, test.wantKind)
			}
			if got.code != test.code {
				t.Errorf("code = %d, want %d", got.code, test.code)
			}
			if got.reason != test.wantReason {
				t.Errorf("reason = %q, want %q", got.reason, test.wantReason)
			}
			if (got.fail() != nil) != test.wantFail {
				t.Errorf("fail() = %v, want failure=%t", got.fail(), test.wantFail)
			}
			if got.settled() != test.wantSettle {
				t.Errorf("settled() = %v, want %t", got.settled(), test.wantSettle)
			}
		})
	}
}

func TestLaunchctlOutcomeFailNamesTheOfflineDecodings(t *testing.T) {
	refused := launchctlOutcome("bootout", "Boot-out failed: 1: Operation not permitted", 1, launchctlErr(1))
	if got := refused.fail().Error(); !strings.Contains(got, "launchd refused: Operation not permitted") {
		t.Errorf("refusal error = %q, want launchd's own reason", got)
	}

	unknown := launchctlOutcome("bootstrap", "Bootstrap failed: 5: Input/output error", launchctlAggregateExit, launchctlErr(5))
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

	unreached := launchctlOutcome("print", "", -1, errors.New("runner gone"))
	if got := unreached.fail().Error(); strings.Contains(got, "launchctl error") {
		t.Errorf("non-launchd failure error = %q, must not prescribe decoding a status", got)
	}
}
