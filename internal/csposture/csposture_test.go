package csposture_test

import (
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/internal/csposture"
	"github.com/yasyf/daemonkit/internal/csposture/csposturetest"
)

func TestCheckMatchesCorpusUnderBothLibraryValidationPolicies(t *testing.T) {
	policies := []struct {
		name   string
		policy csposture.LibraryValidation
		denies func(csposturetest.Case) bool
	}{
		{"require", csposture.RequireLibraryValidation, func(c csposturetest.Case) bool { return c.RequireLVDenies }},
		{"by entitlement", csposture.LibraryValidationByEntitlement, func(c csposturetest.Case) bool { return c.EntitlementLVDenies }},
	}
	for _, policy := range policies {
		for _, tt := range csposturetest.Cases() {
			t.Run(policy.name+"/"+tt.Name, func(t *testing.T) {
				err := csposture.Check(tt.Status, policy.policy)
				if policy.denies(tt) {
					if err == nil {
						t.Fatalf("Check(0x%x, %v) = nil, want a denial", tt.Status, policy.policy)
					}
					if !strings.Contains(err.Error(), "status 0x") {
						t.Errorf("Check(0x%x, %v) = %q, want the status word in the reason", tt.Status, policy.policy, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Check(0x%x, %v) = %v, want nil", tt.Status, policy.policy, err)
				}
			})
		}
	}
}

func TestLibraryValidationIsTheOnlyPolicyDifference(t *testing.T) {
	for _, tt := range csposturetest.Cases() {
		if tt.EntitlementLVDenies && !tt.RequireLVDenies {
			t.Fatalf("corpus case %q denies under the weaker policy only", tt.Name)
		}
		if tt.RequireLVDenies == tt.EntitlementLVDenies {
			continue
		}
		if csposture.LibraryValidationEnforced(tt.Status) {
			t.Errorf("case %q (0x%x) diverges though the status word proves library validation", tt.Name, tt.Status)
		}
		err := csposture.Check(tt.Status, csposture.RequireLibraryValidation)
		if err == nil {
			t.Errorf("case %q (0x%x) diverges yet the stricter policy allows it", tt.Name, tt.Status)
			continue
		}
		if !strings.Contains(err.Error(), "library validation") {
			t.Errorf("case %q (0x%x) diverges on a clause other than library validation: %v", tt.Name, tt.Status, err)
		}
	}
}

func TestLibraryValidationEnforced(t *testing.T) {
	tests := []struct {
		name   string
		status int64
		want   bool
	}{
		{"neither bit", csposture.Runtime, false},
		{"CS_REQUIRE_LV", csposture.Runtime | csposture.RequireLV, true},
		{"CS_FORCED_LV", csposture.Runtime | csposture.ForcedLV, true},
		{"both bits", csposture.Runtime | csposture.RequireLV | csposture.ForcedLV, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := csposture.LibraryValidationEnforced(tt.status); got != tt.want {
				t.Errorf("LibraryValidationEnforced(0x%x) = %t, want %t", tt.status, got, tt.want)
			}
		})
	}
}
