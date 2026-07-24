package trust

import (
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/peer"
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
	peer := peer.Identity{UID: os.Getuid() + 1}
	err := p.Check(peer)
	if !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(foreign uid) = %v, want ErrUntrustedPeer", err)
	}
}

func TestCheckUIDOnlyPassesSameUID(t *testing.T) {
	p := Policy{}
	if err := p.Check(peer.Identity{UID: os.Getuid()}); err != nil {
		t.Errorf("Check(same uid, no requirement) = %v, want nil", err)
	}
}

func TestCheckConfiguredRequirementValidatesFields(t *testing.T) {
	p := Policy{Requirement: &Requirement{TeamID: "SXKCTF23Q2"}}
	err := p.Check(peer.Identity{UID: os.Getuid()})
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
	err := p.Check(peer.Identity{UID: os.Getuid()})
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

func TestRequirementValidationDigestIsCanonicalAndComplete(t *testing.T) {
	first := Requirement{
		TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.x",
		RequiredAppGroup: "group.com.yasyf.daemonkit",
		RequiredEntitlements: map[string]EntitlementRequirement{
			"com.yasyf.role":    {Match: EntitlementString, String: "broker"},
			"com.yasyf.enabled": {Match: EntitlementBoolean, Boolean: true},
		},
	}
	second := Requirement{
		TeamID: first.TeamID, SigningIdentifier: first.SigningIdentifier, RequiredAppGroup: first.RequiredAppGroup,
		RequiredEntitlements: map[string]EntitlementRequirement{
			"com.yasyf.enabled": first.RequiredEntitlements["com.yasyf.enabled"],
			"com.yasyf.role":    first.RequiredEntitlements["com.yasyf.role"],
		},
	}
	firstDigest, err := first.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := second.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest || firstDigest == ([32]byte{}) {
		t.Fatalf("canonical digests = %x / %x", firstDigest, secondDigest)
	}
	second.RequiredAppGroup = "group.com.yasyf.daemonkit.other"
	changed, err := second.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if changed == firstDigest {
		t.Fatal("App Group requirement did not affect opaque validation digest")
	}
}

func trustPolicyRequirement(identifier string) Requirement {
	return Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: identifier}
}

func trustPolicyConfig() TrustPolicyConfig {
	return TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
		Roles: map[PeerRole]Requirement{
			"stop":      trustPolicyRequirement("com.yasyf.daemonkit.stop"),
			"lifecycle": trustPolicyRequirement("com.yasyf.daemonkit.lifecycle"),
			"broker":    trustPolicyRequirement("com.yasyf.daemonkit.broker"),
		},
		StopRoles: []PeerRole{"stop"}, ReceiptRoles: []PeerRole{"lifecycle"},
		ReadinessRoles: []PeerRole{"lifecycle"}, HandoffRoles: []PeerRole{"broker"},
	}
}

func TestTrustPolicyAllowsEquivalentRequirementsAcrossExplicitRoles(t *testing.T) {
	config := trustPolicyConfig()
	config.Roles["readiness-2"] = config.Roles["lifecycle"]
	config.ReadinessRoles = []PeerRole{"lifecycle", "readiness-2"}
	policy, err := NewTrustPolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := policy.Requirement("lifecycle")
	if !ok {
		t.Fatal("lifecycle role is absent")
	}
	second, ok := policy.Requirement("readiness-2")
	if !ok {
		t.Fatal("second readiness role is absent")
	}
	firstDigest, err := first.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := second.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("equivalent role requirements differ: %x != %x", firstDigest, secondDigest)
	}
	if !policy.AllowsReadiness("lifecycle") || !policy.AllowsReadiness("readiness-2") {
		t.Fatal("explicit readiness roles lost authority")
	}
}

func TestTrustPolicySealsUnprotectedRoleAndDigest(t *testing.T) {
	config := trustPolicyConfig()
	allowed, err := NewTrustPolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	if !allowed.AllowsUnprotected() {
		t.Fatal("configured unprotected role was not allowed")
	}
	if allowed.AllowsStop(UnprotectedRole) || allowed.AllowsReceipt(UnprotectedRole) ||
		allowed.AllowsReadiness(UnprotectedRole) || allowed.AllowsHandoff(UnprotectedRole) {
		t.Fatal("unprotected role gained private authority")
	}
	allowedDigest, err := allowed.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	config.AllowUnprotected = false
	denied, err := NewTrustPolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	deniedDigest, err := denied.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if allowedDigest == deniedDigest {
		t.Fatal("unprotected-session policy did not affect validation digest")
	}
	config = trustPolicyConfig()
	config.Roles[UnprotectedRole] = trustPolicyRequirement("com.yasyf.daemonkit.fake-unprotected")
	if _, err := NewTrustPolicy(config); err == nil {
		t.Fatal("consumer-defined unprotected role succeeded")
	}
}

func TestTrustPolicyAllowsExplicitUnprotectedOnlyMode(t *testing.T) {
	policy, err := NewTrustPolicy(TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	if !policy.AllowsUnprotected() || len(policy.RoleNames()) != 0 {
		t.Fatalf("unprotected-only policy = allow:%t roles:%v", policy.AllowsUnprotected(), policy.RoleNames())
	}
	if policy.AllowsStop(UnprotectedRole) || policy.AllowsReceipt(UnprotectedRole) ||
		policy.AllowsReadiness(UnprotectedRole) || policy.AllowsHandoff(UnprotectedRole) {
		t.Fatal("unprotected-only policy gained protected authority")
	}
	if digest, err := policy.ValidationDigest(); err != nil || digest == (PolicyDigest{}) {
		t.Fatalf("validation digest = %x, %v", digest, err)
	}
	for name, mutate := range map[string]func(*TrustPolicyConfig){
		"stop":      func(c *TrustPolicyConfig) { c.StopRoles = []PeerRole{"fake"} },
		"receipt":   func(c *TrustPolicyConfig) { c.ReceiptRoles = []PeerRole{"fake"} },
		"readiness": func(c *TrustPolicyConfig) { c.ReadinessRoles = []PeerRole{"fake"} },
		"handoff":   func(c *TrustPolicyConfig) { c.HandoffRoles = []PeerRole{"fake"} },
	} {
		t.Run(name, func(t *testing.T) {
			config := TrustPolicyConfig{ExpectedUID: os.Geteuid(), AllowUnprotected: true}
			mutate(&config)
			if _, err := NewTrustPolicy(config); err == nil {
				t.Fatal("unprotected-only policy accepted protected authority")
			}
		})
	}
}

func TestTrustPolicyIsImmutableAndCanonical(t *testing.T) {
	config := trustPolicyConfig()
	policy, err := NewTrustPolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	first, err := policy.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	config.Roles["broker"] = trustPolicyRequirement("com.yasyf.changed")
	config.HandoffRoles[0] = "stop"
	names := policy.RoleNames()
	names[0] = "changed"
	second, err := policy.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if first != second || !policy.AllowsHandoff("broker") || policy.AllowsHandoff("stop") {
		t.Fatalf("policy mutated: first=%x second=%x names=%v", first, second, policy.RoleNames())
	}
}

func TestTrustPolicyAllowsDisabledHandoff(t *testing.T) {
	config := trustPolicyConfig()
	config.HandoffRoles = nil
	policy, err := NewTrustPolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	if policy.AllowsHandoff("broker") {
		t.Fatal("disabled handoff granted authority")
	}
}

func TestTrustPolicyRejectsCrossBucketOverlap(t *testing.T) {
	for name, mutate := range map[string]func(*TrustPolicyConfig){
		"stop lifecycle": func(c *TrustPolicyConfig) { c.StopRoles = []PeerRole{"lifecycle"} },
		"stop handoff":   func(c *TrustPolicyConfig) { c.StopRoles = []PeerRole{"broker"} },
		"lifecycle handoff": func(c *TrustPolicyConfig) {
			c.HandoffRoles = []PeerRole{"lifecycle"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := trustPolicyConfig()
			mutate(&config)
			if _, err := NewTrustPolicy(config); err == nil {
				t.Fatal("invalid authority layout succeeded")
			}
		})
	}
}
