package trust

import (
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

func TestRequirementDRString(t *testing.T) {
	req := Requirement{TeamID: "SXKCTF23Q2", Identifier: "com.yasyf.daemonkit.holder"}
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
		{"no team", Requirement{Identifier: "com.yasyf.x"}},
		{"no identifier", Requirement{TeamID: "SXKCTF23Q2"}},
		{"quoted team", Requirement{TeamID: `SX"Q2`, Identifier: "com.yasyf.x"}},
		{"backslash identifier", Requirement{TeamID: "SXKCTF23Q2", Identifier: `com\yasyf`}},
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
	p := Policy{} // no Requirement → UID-only
	if err := p.Check(wire.Peer{UID: os.Getuid()}); err != nil {
		t.Errorf("Check(same uid, no requirement) = %v, want nil", err)
	}
}

func TestCheckConfiguredRequirementValidatesFields(t *testing.T) {
	p := Policy{Requirement: &Requirement{TeamID: "SXKCTF23Q2"}} // missing Identifier
	err := p.Check(wire.Peer{UID: os.Getuid()})
	if err == nil {
		t.Fatal("Check with an invalid Requirement = nil, want an error")
	}
	if errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("invalid-requirement error should be a config error, not %v", ErrUntrustedPeer)
	}
}

// TestCheckFailsClosedWithoutVerifier: a valid Requirement against a peer with no
// audit token (no verifier can run) is denied with ErrNoVerifier — never a silent
// downgrade to the UID floor. On non-darwin builds no verifier exists at all;
// both paths must fail closed.
func TestCheckFailsClosedWithoutVerifier(t *testing.T) {
	p := Policy{Requirement: &Requirement{TeamID: "SXKCTF23Q2", Identifier: "com.yasyf.daemonkit.x"}}
	err := p.Check(wire.Peer{UID: os.Getuid()}) // same uid, but no Audit token
	if !errors.Is(err, ErrNoVerifier) {
		t.Errorf("Check(no verifier) = %v, want ErrNoVerifier (fail closed)", err)
	}
}
