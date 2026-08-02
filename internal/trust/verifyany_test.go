package trust

import (
	"errors"
	"os"
	"testing"
)

// TestAnyOfIsADisjunctionOverFullRequirements pins the set semantics one lane
// admits on: an app and its File Provider extension are two genuinely
// different signed bundles, so any element admits, while a set every element
// denies denies, and a denial that is not a policy mismatch — an invalid
// requirement, an absent verifier, a peer already gone — denies the whole set
// rather than being tried around.
func TestAnyOfIsADisjunctionOverFullRequirements(t *testing.T) {
	kernel := admittingKernel(t)
	admitting := kernel.requirement()
	extension := Requirement{
		TeamID:            testTeam,
		SigningIdentifier: testIdentifier + ".fileprovider",
		RequiredAppGroup:  testGroup,
	}
	incomplete := Requirement{TeamID: testTeam}

	tests := []struct {
		name      string
		reqs      []Requirement
		admits    bool
		untrusted bool
	}{
		{"the only element matches", []Requirement{admitting}, true, false},
		{"a later element matches", []Requirement{extension, admitting}, true, false},
		{"an earlier element matches", []Requirement{admitting, extension}, true, false},
		{"no element matches", []Requirement{extension, extension}, false, true},
		{"an incomplete element denies the set", []Requirement{extension, incomplete}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := anyOf(tt.reqs, func(req Requirement) error {
				if err := req.Validate(); err != nil {
					return err
				}
				return kernel.verify(req)
			})
			if tt.admits {
				if err != nil {
					t.Fatalf("anyOf() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("anyOf() = nil, want a denial")
			}
			if got := errors.Is(err, ErrUntrustedPeer); got != tt.untrusted {
				t.Fatalf("errors.Is(anyOf(), ErrUntrustedPeer) = %t, want %t (err = %v)", got, tt.untrusted, err)
			}
		})
	}
}

// TestVerifyAnyOverAnEmptySetIsTheFloorAlone keeps the wire lane's unset field
// meaning what a nil Verify requirement means: same-EUID trust, and nothing
// more. The set that is stated but empty never reaches here — ValidateForServe
// refuses it — so this is the unset field's meaning, not an empty policy's.
func TestVerifyAnyOverAnEmptySetIsTheFloorAlone(t *testing.T) {
	peer := Peer{UID: os.Geteuid()}
	if err := VerifyAny(peer, nil); err != nil {
		t.Fatalf("VerifyAny(nil set) = %v, want nil", err)
	}
	if err := VerifyAny(peer, []Requirement{}); err != nil {
		t.Fatalf("VerifyAny(empty set) = %v, want nil", err)
	}
	if err := VerifyAny(Peer{UID: os.Geteuid() + 1}, nil); !errors.Is(err, ErrUntrustedPeer) {
		t.Fatalf("VerifyAny(foreign uid) = %v, want ErrUntrustedPeer", err)
	}
}
