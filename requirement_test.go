package daemonkit

import (
	"testing"

	"github.com/yasyf/daemonkit/internal/trust"
)

func TestRequirementDigest(t *testing.T) {
	tests := []struct {
		name string
		req  Requirement
		want PolicyDigest
	}{
		{
			"holder",
			Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.holder"},
			"ab9ca22bbabb4ef6b8f17b36418e4f40d8c89f516e8c6b233e3c87130a342e83",
		},
		{
			"zero",
			Requirement{},
			"7f9c9e31ac8256ca2f258583df262dbc7d6f68f2a03043d5c99a4ae5a7396ce9",
		},
		{
			"field boundary AB/C",
			Requirement{TeamID: "AB", SigningIdentifier: "C"},
			"77019b9018c753fa6b2ca0ca3286156069a46b70fd35a130134c082c1f41eefb",
		},
		{
			"field boundary A/BC",
			Requirement{TeamID: "A", SigningIdentifier: "BC"},
			"c722345d983e6747ece65212151b393cf0eac2b66e1d279dfb9a99f0f4ada4b1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.req.Digest(); got != tt.want {
				t.Errorf("Digest() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRequirementDigestSeparatesEveryClause(t *testing.T) {
	base := Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.holder"}
	variants := []struct {
		name string
		req  Requirement
	}{
		{"base", base},
		{"app group", Requirement{
			TeamID: base.TeamID, SigningIdentifier: base.SigningIdentifier,
			RequiredAppGroup: "SXKCTF23Q2.com.yasyf.daemonkit",
		}},
		{"entitlement key", Requirement{
			TeamID: base.TeamID, SigningIdentifier: base.SigningIdentifier,
			RequiredEntitlements: map[string]EntitlementRequirement{
				"com.yasyf.role": {Match: EntitlementString, String: "daemon"},
			},
		}},
		{"entitlement value", Requirement{
			TeamID: base.TeamID, SigningIdentifier: base.SigningIdentifier,
			RequiredEntitlements: map[string]EntitlementRequirement{
				"com.yasyf.role": {Match: EntitlementString, String: "launcher"},
			},
		}},
		{"entitlement match", Requirement{
			TeamID: base.TeamID, SigningIdentifier: base.SigningIdentifier,
			RequiredEntitlements: map[string]EntitlementRequirement{
				"com.yasyf.role": {Match: EntitlementStringArrayContains, String: "daemon"},
			},
		}},
		{"allow jit", Requirement{
			TeamID: base.TeamID, SigningIdentifier: base.SigningIdentifier, AllowJIT: true,
		}},
	}
	seen := make(map[PolicyDigest]string, len(variants))
	for _, variant := range variants {
		digest := variant.req.Digest()
		if other, collided := seen[digest]; collided {
			t.Fatalf("%s and %s share digest %q", variant.name, other, digest)
		}
		seen[digest] = variant.name
	}
}

func TestWireRequirementCarriesEveryClause(t *testing.T) {
	req := &Requirement{
		TeamID:            "SXKCTF23Q2",
		SigningIdentifier: "com.yasyf.daemonkit.holder",
		RequiredAppGroup:  "SXKCTF23Q2.com.yasyf.daemonkit",
		RequiredEntitlements: map[string]EntitlementRequirement{
			"com.yasyf.role":    {Match: EntitlementString, String: "daemon"},
			"com.yasyf.enabled": {Match: EntitlementBoolean, Boolean: true},
		},
		AllowJIT: true,
	}
	wired := wireRequirement(req)
	if wired.TeamID != req.TeamID || wired.SigningIdentifier != req.SigningIdentifier {
		t.Fatalf("wired identity = %q/%q", wired.TeamID, wired.SigningIdentifier)
	}
	if wired.RequiredAppGroup != req.RequiredAppGroup {
		t.Fatalf("RequiredAppGroup = %q, want %q", wired.RequiredAppGroup, req.RequiredAppGroup)
	}
	if !wired.AllowJIT {
		t.Fatal("AllowJIT was dropped")
	}
	if len(wired.RequiredEntitlements) != len(req.RequiredEntitlements) {
		t.Fatalf("RequiredEntitlements = %v, want %d clauses", wired.RequiredEntitlements, len(req.RequiredEntitlements))
	}
	role := wired.RequiredEntitlements["com.yasyf.role"]
	if role.Match != trust.EntitlementString || role.String != "daemon" {
		t.Fatalf("role clause = %+v", role)
	}
	enabled := wired.RequiredEntitlements["com.yasyf.enabled"]
	if enabled.Match != trust.EntitlementBoolean || !enabled.Boolean {
		t.Fatalf("enabled clause = %+v", enabled)
	}
	if err := wired.Validate(); err != nil {
		t.Fatalf("wired requirement does not validate: %v", err)
	}
	if wireRequirement(nil) != nil {
		t.Fatal("a nil requirement wired to a non-nil trust requirement")
	}
}

// TestEntitlementMatchAgreesWithTrustNumbering pins the numeric cast
// wireRequirement makes across the package boundary: a member reordered in
// either enum silently rewrites a consumer's policy on the wire.
func TestEntitlementMatchAgreesWithTrustNumbering(t *testing.T) {
	tests := []struct {
		name  string
		match EntitlementMatch
		want  trust.EntitlementMatch
	}{
		{"boolean", EntitlementBoolean, trust.EntitlementBoolean},
		{"string", EntitlementString, trust.EntitlementString},
		{"string array contains", EntitlementStringArrayContains, trust.EntitlementStringArrayContains},
	}
	if members := int(entitlementMatchLimit) - 1; members != len(tests) {
		t.Fatalf("EntitlementMatch has %d members, the table covers %d", members, len(tests))
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := trust.EntitlementMatch(tt.match); got != tt.want {
				t.Errorf("trust.EntitlementMatch(%d) = %d, want %d", tt.match, got, tt.want)
			}
		})
	}
}
