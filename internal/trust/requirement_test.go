package trust

import (
	"testing"
)

func TestRequirementDRString(t *testing.T) {
	req := Requirement{TeamID: testTeam, SigningIdentifier: "com.yasyf.daemonkit.holder"}
	got, err := req.DRString()
	if err != nil {
		t.Fatalf("DRString: %v", err)
	}
	want := `identifier "com.yasyf.daemonkit.holder" and anchor apple generic and ` +
		`certificate leaf[subject.OU] = "SXKCTF23Q2" ` +
		`and certificate 1[field.1.2.840.113635.100.6.2.6] exists ` +
		`and certificate leaf[field.1.2.840.113635.100.6.1.13] exists`
	if got != want {
		t.Errorf("DRString mismatch:\n got %q\nwant %q", got, want)
	}
}

func TestRequirementValidation(t *testing.T) {
	tests := []struct {
		name string
		req  Requirement
	}{
		{"no team", Requirement{SigningIdentifier: "com.yasyf.x"}},
		{"no identifier", Requirement{TeamID: testTeam}},
		{"quoted team", Requirement{TeamID: `SX"Q2`, SigningIdentifier: "com.yasyf.x"}},
		{"backslash identifier", Requirement{TeamID: testTeam, SigningIdentifier: `com\yasyf`}},
		{"duplicate app group", Requirement{
			TeamID: testTeam, SigningIdentifier: "com.yasyf.x", RequiredAppGroup: "group.x",
			RequiredEntitlements: map[string]EntitlementRequirement{
				appGroupsEntitlement: {Match: EntitlementStringArrayContains, String: "group.other"},
			},
		}},
		{"unknown match", Requirement{
			TeamID: testTeam, SigningIdentifier: "com.yasyf.x",
			RequiredEntitlements: map[string]EntitlementRequirement{"com.yasyf.role": {Match: 99}},
		}},
		{"empty required entitlement key", Requirement{
			TeamID: testTeam, SigningIdentifier: "com.yasyf.x",
			RequiredEntitlements: map[string]EntitlementRequirement{" ": {Match: EntitlementBoolean}},
		}},
		{"empty required string value", Requirement{
			TeamID: testTeam, SigningIdentifier: "com.yasyf.x",
			RequiredEntitlements: map[string]EntitlementRequirement{"com.yasyf.role": {Match: EntitlementString}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.req.Validate(); err == nil {
				t.Errorf("Validate(%+v) = nil, want a rejection", tt.req)
			}
			if _, err := tt.req.DRString(); err == nil {
				t.Errorf("DRString(%+v) = nil error, want a rejection", tt.req)
			}
			if _, err := tt.req.ValidationDigest(); err == nil {
				t.Errorf("ValidationDigest(%+v) = nil error, want a rejection", tt.req)
			}
		})
	}
}

func TestRequirementEntitlementRequirementsFoldInTheAppGroup(t *testing.T) {
	bare := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
	if got := bare.entitlementRequirements(); len(got) != 0 {
		t.Fatalf("entitlementRequirements = %v, want empty", got)
	}
	req := Requirement{
		TeamID: testTeam, SigningIdentifier: testIdentifier, RequiredAppGroup: testGroup,
		RequiredEntitlements: map[string]EntitlementRequirement{
			"com.yasyf.role":    {Match: EntitlementString, String: "broker"},
			"com.yasyf.enabled": {Match: EntitlementBoolean, Boolean: true},
		},
	}
	got := req.entitlementRequirements()
	wantGroup := EntitlementRequirement{Match: EntitlementStringArrayContains, String: testGroup}
	if got[appGroupsEntitlement] != wantGroup {
		t.Fatalf("application-groups requirement = %+v, want %+v", got[appGroupsEntitlement], wantGroup)
	}
	for key, want := range req.RequiredEntitlements {
		if got[key] != want {
			t.Fatalf("typed extra %q = %+v, want %+v", key, got[key], want)
		}
	}
}

func TestValidationDigestCoversEveryPredicate(t *testing.T) {
	base := Requirement{
		TeamID: testTeam, SigningIdentifier: testIdentifier, RequiredAppGroup: testGroup,
		RequiredEntitlements: map[string]EntitlementRequirement{
			"com.yasyf.role":    {Match: EntitlementString, String: "broker"},
			"com.yasyf.enabled": {Match: EntitlementBoolean, Boolean: true},
		},
	}
	digest := func(t *testing.T, req Requirement) PolicyDigest {
		t.Helper()
		got, err := req.ValidationDigest()
		if err != nil {
			t.Fatalf("ValidationDigest: %v", err)
		}
		return got
	}
	want := digest(t, base)
	if want == (PolicyDigest{}) {
		t.Fatal("ValidationDigest returned the zero digest")
	}

	reordered := base
	reordered.RequiredEntitlements = map[string]EntitlementRequirement{
		"com.yasyf.enabled": base.RequiredEntitlements["com.yasyf.enabled"],
		"com.yasyf.role":    base.RequiredEntitlements["com.yasyf.role"],
	}
	if digest(t, reordered) != want {
		t.Fatal("map iteration order changed the validation digest")
	}

	tests := []struct {
		name   string
		mutate func(*Requirement)
	}{
		{"team", func(r *Requirement) { r.TeamID = "ZZ0FAKE9TX" }},
		{"signing identifier", func(r *Requirement) { r.SigningIdentifier = "com.yasyf.other" }},
		{"app group", func(r *Requirement) { r.RequiredAppGroup = "group.com.yasyf.other" }},
		{"required boolean", func(r *Requirement) {
			r.RequiredEntitlements = map[string]EntitlementRequirement{
				"com.yasyf.role":    r.RequiredEntitlements["com.yasyf.role"],
				"com.yasyf.enabled": {Match: EntitlementBoolean, Boolean: false},
			}
		}},
		{"required string", func(r *Requirement) {
			r.RequiredEntitlements = map[string]EntitlementRequirement{
				"com.yasyf.role":    {Match: EntitlementString, String: "other"},
				"com.yasyf.enabled": r.RequiredEntitlements["com.yasyf.enabled"],
			}
		}},
		{"AllowJIT relaxation", func(r *Requirement) { r.AllowJIT = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := base
			tt.mutate(&changed)
			if digest(t, changed) == want {
				t.Errorf("%s did not affect the validation digest", tt.name)
			}
		})
	}
}
