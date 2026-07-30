package csposture_test

import (
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/internal/csposture"
	"github.com/yasyf/daemonkit/internal/csposture/csposturetest"
)

func TestCheckMatchesCorpus(t *testing.T) {
	for _, tt := range csposturetest.Cases() {
		t.Run(tt.Name, func(t *testing.T) {
			err := csposture.Check(tt.Status)
			if tt.Denies {
				if err == nil {
					t.Fatalf("Check(0x%x) = nil, want a denial", tt.Status)
				}
				if !strings.Contains(err.Error(), "status 0x") {
					t.Errorf("Check(0x%x) = %q, want the status word in the reason", tt.Status, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Check(0x%x) = %v, want nil", tt.Status, err)
			}
		})
	}
}

func TestCheckDeniesEachClauseIndependently(t *testing.T) {
	const admitted = csposture.Valid | csposture.Runtime | csposture.Hard |
		csposture.Enforcement | csposture.RequireLV
	if err := csposture.Check(admitted); err != nil {
		t.Fatalf("Check(0x%x) = %v, want nil", admitted, err)
	}
	tests := []struct {
		name   string
		status int64
		reason string
	}{
		{"CS_VALID clear", admitted &^ csposture.Valid, "CS_VALID clear"},
		{"CS_RUNTIME clear", admitted &^ csposture.Runtime, "Hardened Runtime"},
		{"CS_HARD clear", admitted &^ csposture.Hard, "CS_HARD clear"},
		{"CS_ENFORCEMENT clear", admitted &^ csposture.Enforcement, "CS_ENFORCEMENT clear"},
		{"both LV bits clear", admitted &^ csposture.RequireLV, "library validation"},
		{"CS_GET_TASK_ALLOW set", admitted | csposture.GetTaskAllow, "CS_GET_TASK_ALLOW"},
		{"CS_DEBUGGED set", admitted | csposture.Debugged, "CS_DEBUGGED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := csposture.Check(tt.status)
			if err == nil {
				t.Fatalf("Check(0x%x) = nil, want a denial", tt.status)
			}
			if !strings.Contains(err.Error(), tt.reason) {
				t.Errorf("Check(0x%x) = %q, want the reason to name %q", tt.status, err, tt.reason)
			}
		})
	}
}
