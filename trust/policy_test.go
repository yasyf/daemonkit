package trust

import (
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

func TestRequirementDRString(t *testing.T) {
	req := Requirement{
		TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.holder",
	}
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
		{"no identifier", Requirement{TeamID: "SXKCTF23Q2"}},
		{"quoted team", Requirement{TeamID: `SX"Q2`, SigningIdentifier: "com.yasyf.x"}},
		{"backslash identifier", Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: `com\yasyf`}},
		{"duplicate app group", Requirement{
			TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.x", RequiredAppGroup: "group.x",
			RequiredEntitlements: map[string]EntitlementRequirement{appGroupsEntitlement: {Match: EntitlementStringArrayContains, String: "group.other"}},
		}},
		{"unknown match", Requirement{
			TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.x",
			RequiredEntitlements: map[string]EntitlementRequirement{"com.yasyf.role": {Match: 99}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.req.DRString(); err == nil {
				t.Errorf("DRString(%+v) = nil error, want rejection", tt.req)
			}
		})
	}
}

func TestCheckUIDFloorRejectsForeignUID(t *testing.T) {
	p := Policy{}
	peer := wire.Peer{UID: os.Getuid() + 1}
	err := p.Check(peer)
	if !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(foreign uid) = %v, want ErrUntrustedPeer", err)
	}
}

func TestCheckUIDOnlyPassesSameUID(t *testing.T) {
	p := Policy{}
	if err := p.Check(wire.Peer{UID: os.Getuid()}); err != nil {
		t.Errorf("Check(same uid, no requirement) = %v, want nil", err)
	}
}

func TestCheckConfiguredRequirementValidatesFields(t *testing.T) {
	p := Policy{Requirement: &Requirement{TeamID: "SXKCTF23Q2"}}
	err := p.Check(wire.Peer{UID: os.Getuid()})
	if err == nil {
		t.Fatal("Check with an invalid Requirement = nil, want an error")
	}
	if errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("invalid-requirement error should be a config error, not %v", ErrUntrustedPeer)
	}
}

func TestCheckFailsClosedWithoutVerifier(t *testing.T) {
	p := Policy{Requirement: &Requirement{
		TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.x",
	}}
	err := p.Check(wire.Peer{UID: os.Getuid()})
	if !errors.Is(err, ErrNoVerifier) {
		t.Errorf("Check(no verifier) = %v, want ErrNoVerifier (fail closed)", err)
	}
}

func TestSignedRequirementWithoutAppGroupIsValid(t *testing.T) {
	req := Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.x"}
	if err := req.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got := req.entitlementRequirements(); len(got) != 0 {
		t.Fatalf("entitlementRequirements = %v, want empty", got)
	}
}

func TestRequirementExplicitlyIncludesAppGroupAndTypedExtras(t *testing.T) {
	req := Requirement{
		TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.x",
		RequiredAppGroup: "group.com.yasyf.daemonkit",
		RequiredEntitlements: map[string]EntitlementRequirement{
			"com.yasyf.role":    {Match: EntitlementString, String: "broker"},
			"com.yasyf.enabled": {Match: EntitlementBoolean, Boolean: true},
		},
	}
	if err := req.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	got := req.entitlementRequirements()
	wantGroup := EntitlementRequirement{Match: EntitlementStringArrayContains, String: req.RequiredAppGroup}
	if got[appGroupsEntitlement] != wantGroup {
		t.Fatalf("application-groups requirement = %+v, want %+v", got[appGroupsEntitlement], wantGroup)
	}
	if got["com.yasyf.role"] != req.RequiredEntitlements["com.yasyf.role"] || got["com.yasyf.enabled"] != req.RequiredEntitlements["com.yasyf.enabled"] {
		t.Fatalf("typed extras changed: %v", got)
	}
}
