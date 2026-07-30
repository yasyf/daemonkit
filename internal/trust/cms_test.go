package trust

import (
	"encoding/asn1"
	"strings"
	"testing"
)

// The real Apple CMS: BER, indefinite lengths at four levels, and a
// certificate set ordered intermediate, root, leaf — so "the first cert" and
// "the cert with the matching OU" are both wrong ways to find the signer.
func TestVerifySignerResolvesTheRealDeveloperIDLeaf(t *testing.T) {
	cms := measuredBlob(t, "cms-developerid.der")
	if err := verifySigner(cms, "24VZTF6M5V"); err != nil {
		t.Fatalf("verifySigner(measured Developer ID CMS) = %v, want nil", err)
	}
	err := verifySigner(cms, testTeam)
	assertDenies(t, err, ErrUntrustedPeer)
	if !strings.Contains(err.Error(), "organizational unit does not match") {
		t.Errorf("verifySigner(wrong team) = %q, want the organizational-unit denial", err)
	}
}

func TestVerifySignerNeverPanicsOnATruncatedRealCMS(t *testing.T) {
	full := measuredBlob(t, "cms-developerid.der")
	for length := 0; length < len(full); length += 7 {
		if err := verifySigner(full[:length:length], "24VZTF6M5V"); err == nil {
			t.Fatalf("verifySigner(%d-byte prefix) = nil, want an error", length)
		}
	}
}

// An attacker who can add certificates to the set cannot steer the verdict:
// the leaf is whatever the SignerInfo designates, and its OU is what counts.
func TestVerifySignerIgnoresADecoyCertificateCarryingTheTargetTeam(t *testing.T) {
	signer := newSigningChain(t, certificateSpec{
		commonName:         "Developer ID Application: Attacker (ZZ0FAKE9TX)",
		organizationalUnit: []string{"ZZ0FAKE9TX"},
		markers:            []asn1.ObjectIdentifier{oidDeveloperIDLeaf},
	})
	decoy := newSigningChain(t, certificateSpec{
		commonName:         "Developer ID Application: Decoy (" + testTeam + ")",
		organizationalUnit: []string{testTeam},
		markers:            []asn1.ObjectIdentifier{oidDeveloperIDLeaf},
	})
	cms := buildCMS(t, signer.leaf, decoy.leafDER, decoy.issuerDER, signer.issuerDER, signer.leafDER)
	err := verifySigner(cms, testTeam)
	assertDenies(t, err, ErrUntrustedPeer)
	if !strings.Contains(err.Error(), "organizational unit does not match") {
		t.Errorf("verifySigner(decoy set) = %q, want the organizational-unit denial", err)
	}
}

// The hole T1 exists to close: Apple's verifySignature() skips the team
// comparison entirely when the signing leaf carries no Organizational Unit,
// and the kernel then serves whatever team the CodeDirectory declares.
func TestVerifySignerDeniesAnOrganizationalUnitLessLeaf(t *testing.T) {
	chain := newSigningChain(t, certificateSpec{
		commonName: "Developer ID Application: No OU",
		markers:    []asn1.ObjectIdentifier{oidDeveloperIDLeaf},
	})
	err := verifySigner(buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER), testTeam)
	assertDenies(t, err, ErrUntrustedPeer)
	if !strings.Contains(err.Error(), "0 organizational units") {
		t.Errorf("verifySigner(OU-less leaf) = %q, want the organizational-unit-count denial", err)
	}
}

func TestVerifySignerDeniesTheCMSShape(t *testing.T) {
	chain := devIDChain(t)
	tests := []struct {
		name   string
		cms    func(*testing.T) []byte
		want   error
		reason string
	}{
		{
			"trailing bytes after the ContentInfo",
			func(t *testing.T) []byte {
				return append(buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER), 0x00)
			},
			ErrNoVerifier, "trailing bytes",
		},
		{
			"no certificate answers the SignerInfo",
			func(t *testing.T) []byte { return buildCMS(t, chain.leaf, chain.issuerDER) },
			ErrUntrustedPeer, "no certificate answers",
		},
		{
			"the leaf's issuer is absent",
			func(t *testing.T) []byte { return buildCMS(t, chain.leaf, chain.leafDER) },
			ErrUntrustedPeer, "no Developer ID CA marker",
		},
		{
			"the same certificate appears twice",
			func(t *testing.T) []byte {
				return buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER, chain.leafDER)
			},
			ErrNoVerifier, "two certificates answer",
		},
		{
			"an empty certificate set",
			func(t *testing.T) []byte { return buildCMS(t, chain.leaf) },
			ErrNoVerifier, "0 certificates",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifySigner(tt.cms(t), testTeam)
			assertDenies(t, err, tt.want)
			if !strings.Contains(err.Error(), tt.reason) {
				t.Errorf("verifySigner = %q, want the reason to name %q", err, tt.reason)
			}
		})
	}
}

func TestVerifySignerRequiresBothDeveloperIDMarkers(t *testing.T) {
	noLeafMarker := newSigningChain(t, certificateSpec{
		commonName:         "Developer ID Application: No marker",
		organizationalUnit: []string{testTeam},
	})
	err := verifySigner(buildCMS(t, noLeafMarker.leaf, noLeafMarker.issuerDER, noLeafMarker.leafDER), testTeam)
	assertDenies(t, err, ErrUntrustedPeer)
	if !strings.Contains(err.Error(), "signing leaf carries no Developer ID marker") {
		t.Errorf("verifySigner(leaf without 6.1.13) = %q, want the leaf-marker denial", err)
	}

	noIssuerMarker := newSigningChain(t, certificateSpec{
		commonName:         "Developer ID Application: Test",
		organizationalUnit: []string{testTeam},
		markers:            []asn1.ObjectIdentifier{oidDeveloperIDLeaf},
	}, asn1.ObjectIdentifier{1, 2, 3, 4})
	err = verifySigner(buildCMS(t, noIssuerMarker.leaf, noIssuerMarker.issuerDER, noIssuerMarker.leafDER), testTeam)
	assertDenies(t, err, ErrUntrustedPeer)
	if !strings.Contains(err.Error(), "issuing certificate carries no Developer ID CA marker") {
		t.Errorf("verifySigner(issuer without 6.2.6) = %q, want the CA-marker denial", err)
	}
}

func TestBerParseWalksIndefiniteAndDefiniteLengths(t *testing.T) {
	inner := marshalRaw(t, asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagBoolean, Bytes: []byte{0xff}})
	indefinite := append([]byte{0x30, 0x80}, inner...)
	indefinite = append(indefinite, 0x00, 0x00)
	value, rest, err := berParse(indefinite, 0, &berBudget{})
	if err != nil {
		t.Fatalf("berParse(indefinite) = %v, want nil", err)
	}
	if len(rest) != 0 || value.tag != asn1.TagSequence || !value.compound {
		t.Fatalf("berParse(indefinite) = tag %d compound %t rest %d", value.tag, value.compound, len(rest))
	}
	if string(value.content) != string(inner) {
		t.Errorf("berParse(indefinite) content = %x, want %x", value.content, inner)
	}
}

func TestBerParseDeniesMalformedLengths(t *testing.T) {
	tests := []struct {
		name string
		der  []byte
	}{
		{"element shorter than a header", []byte{0x30}},
		{"definite length past the buffer", []byte{0x30, 0x05, 0x01}},
		{"long form with too many octets", []byte{0x30, 0x85, 0x01, 0x02, 0x03, 0x04, 0x05}},
		{"high tag number form", []byte{0x3f, 0x81, 0x01, 0x00}},
		{"indefinite length on a primitive", []byte{0x04, 0x80, 0x00, 0x00}},
		{"indefinite length with no terminator", []byte{0x30, 0x80, 0x01, 0x01, 0xff}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := berParse(tt.der, 0, &berBudget{}); err == nil {
				t.Fatalf("berParse(%x) = nil, want an error", tt.der)
			}
		})
	}
}

func TestBerParseCapsNestingAndNodes(t *testing.T) {
	deep := []byte{0x01, 0x01, 0xff}
	for depth := 0; depth <= maxBerDepth; depth++ {
		deep = append(append([]byte{0x30, 0x80}, deep...), 0x00, 0x00)
	}
	_, _, err := berParse(deep, 0, &berBudget{})
	assertDenies(t, err, ErrNoVerifier)
	if !strings.Contains(err.Error(), "nests deeper than") && !strings.Contains(err.Error(), "ASN.1 nodes") {
		t.Errorf("berParse(deep) = %q, want a depth or node-budget denial", err)
	}
}
