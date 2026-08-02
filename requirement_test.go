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

// hostAndExtension is the shape the set exists for: an app and its File
// Provider extension are two genuinely different signed bundles, and an
// extension's entitlements differ from its host's.
func hostAndExtension() (Requirement, Requirement) {
	host := Requirement{
		TeamID:            "SXKCTF23Q2",
		SigningIdentifier: "com.yasyf.daemonkit.host",
		RequiredAppGroup:  "SXKCTF23Q2.com.yasyf.daemonkit",
	}
	extension := Requirement{
		TeamID:            "SXKCTF23Q2",
		SigningIdentifier: "com.yasyf.daemonkit.host.fileprovider",
		RequiredAppGroup:  "SXKCTF23Q2.com.yasyf.daemonkit",
		RequiredEntitlements: map[string]EntitlementRequirement{
			"com.apple.developer.fileprovider.testing-mode": {Match: EntitlementBoolean, Boolean: true},
		},
	}
	return host, extension
}

// TestRequirementsDigestCanonicalizesElementOrder is the set's half of the
// digest contract. Consumers bake these — cc-notes' RequireDaemonkitStopReceipt,
// captain-hook's RequireExactStopReceipt — so an unordered set that produced
// two digests would make one policy read as two.
func TestRequirementsDigestCanonicalizesElementOrder(t *testing.T) {
	host, extension := hostAndExtension()
	tests := []struct {
		name  string
		left  Requirements
		right Requirements
		equal bool
	}{
		{"the same pair in either order", Requirements{host, extension}, Requirements{extension, host}, true},
		{"nil and empty name the same enumeration", nil, Requirements{}, true},
		{"a set is not the member it holds", Requirements{host}, Requirements{host, extension}, false},
		{"a repeated member is its own enumeration", Requirements{host}, Requirements{host, host}, false},
		{"one element differing", Requirements{host, extension}, Requirements{host, host}, false},
		{"the empty set is not a populated one", Requirements{}, Requirements{host}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.left.Digest() == tt.right.Digest(); got != tt.equal {
				t.Fatalf("Digest() equality = %t, want %t (%q vs %q)",
					got, tt.equal, tt.left.Digest(), tt.right.Digest())
			}
		})
	}
	if (Requirements{host}).Digest() == host.Digest() {
		t.Fatal("a one-element set shares its member's own digest")
	}
}

// TestWireRequirementsCarriesEveryDisjunct pins what ANY-of is taken over: the
// set reaches admission as the same enumeration, every clause intact, so a
// disjunct dropped in translation cannot narrow the lane silently.
func TestWireRequirementsCarriesEveryDisjunct(t *testing.T) {
	host, extension := hostAndExtension()
	set := Requirements{host, extension}
	wired := wireRequirements(set)
	if len(wired) != len(set) {
		t.Fatalf("wireRequirements() carried %d disjuncts, want %d", len(wired), len(set))
	}
	for i, want := range set {
		if wired[i].TeamID != want.TeamID || wired[i].SigningIdentifier != want.SigningIdentifier {
			t.Errorf("disjunct %d identity = %q/%q, want %q/%q",
				i, wired[i].TeamID, wired[i].SigningIdentifier, want.TeamID, want.SigningIdentifier)
		}
		if wired[i].RequiredAppGroup != want.RequiredAppGroup {
			t.Errorf("disjunct %d app group = %q, want %q", i, wired[i].RequiredAppGroup, want.RequiredAppGroup)
		}
		if len(wired[i].RequiredEntitlements) != len(want.RequiredEntitlements) {
			t.Errorf("disjunct %d carried %d entitlements, want %d",
				i, len(wired[i].RequiredEntitlements), len(want.RequiredEntitlements))
		}
		if err := wired[i].Validate(); err != nil {
			t.Errorf("disjunct %d does not validate: %v", i, err)
		}
	}
	if wireRequirements(nil) != nil {
		t.Fatal("a nil set wired to a non-nil disjunction")
	}
}
