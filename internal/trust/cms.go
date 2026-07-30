package trust

import (
	"bytes"
	"crypto/x509"
	"encoding/asn1"
	"fmt"
	"math/big"
)

var (
	oidSignedData        = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 2}
	oidDeveloperIDLeaf   = asn1.ObjectIdentifier{1, 2, 840, 113635, 100, 6, 1, 13}
	oidDeveloperIDIssuer = asn1.ObjectIdentifier{1, 2, 840, 113635, 100, 6, 2, 6}
)

const (
	maxBerDepth = 16
	maxBerNodes = 4096
	maxCMSCerts = 32
)

// berValue is one ASN.1 TLV. Apple's CMS blob wrapper is BER, not DER — the
// ContentInfo, its [0] content, the SignedData and the encapContentInfo all
// use indefinite lengths (measured), which encoding/asn1 refuses outright — so
// the envelope is walked here and only definite-length DER substructures
// (certificates, SignerInfos) are handed to encoding/asn1 and crypto/x509.
type berValue struct {
	class    int
	tag      int
	compound bool
	content  []byte
	full     []byte
}

type berBudget struct{ nodes int }

func (b *berBudget) node() error {
	b.nodes++
	if b.nodes > maxBerNodes {
		return fmt.Errorf("%w: CMS carries more than %d ASN.1 nodes", ErrNoVerifier, maxBerNodes)
	}
	return nil
}

func berParse(der []byte, depth int, budget *berBudget) (berValue, []byte, error) {
	if depth > maxBerDepth {
		return berValue{}, nil, fmt.Errorf("%w: CMS nests deeper than %d", ErrNoVerifier, maxBerDepth)
	}
	if err := budget.node(); err != nil {
		return berValue{}, nil, err
	}
	if len(der) < 2 {
		return berValue{}, nil, fmt.Errorf("%w: CMS element is %d bytes, want at least 2", ErrNoVerifier, len(der))
	}
	class := int(der[0] >> 6)
	compound := der[0]&0x20 != 0
	tag := int(der[0] & 0x1f)
	if tag == 0x1f {
		return berValue{}, nil, fmt.Errorf("%w: CMS uses a high-tag-number form", ErrNoVerifier)
	}
	cursor := 1
	length := int(der[cursor])
	cursor++
	switch {
	case length == 0x80:
		if !compound {
			return berValue{}, nil, fmt.Errorf("%w: CMS primitive element has an indefinite length", ErrNoVerifier)
		}
		start := cursor
		for {
			if cursor+1 < len(der) && der[cursor] == 0 && der[cursor+1] == 0 {
				return berValue{class, tag, compound, der[start:cursor], der[:cursor+2]}, der[cursor+2:], nil
			}
			_, rest, err := berParse(der[cursor:], depth+1, budget)
			if err != nil {
				return berValue{}, nil, err
			}
			cursor = len(der) - len(rest)
		}
	case length&0x80 != 0:
		octets := length & 0x7f
		if octets > 4 || cursor+octets > len(der) {
			return berValue{}, nil, fmt.Errorf("%w: CMS element declares a %d-octet length", ErrNoVerifier, octets)
		}
		length = 0
		for index := 0; index < octets; index++ {
			length = length<<8 | int(der[cursor+index])
		}
		cursor += octets
	}
	if length < 0 || cursor+length > len(der) {
		return berValue{}, nil, fmt.Errorf("%w: CMS element declares %d bytes past its %d-byte buffer", ErrNoVerifier, length, len(der))
	}
	return berValue{class, tag, compound, der[cursor : cursor+length], der[:cursor+length]}, der[cursor+length:], nil
}

func berChildren(content []byte, depth int, budget *berBudget) ([]berValue, error) {
	var values []berValue
	for len(content) > 0 {
		value, rest, err := berParse(content, depth, budget)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		content = rest
	}
	return values, nil
}

func berOnly(content []byte, depth int, budget *berBudget) (berValue, error) {
	values, err := berChildren(content, depth, budget)
	if err != nil {
		return berValue{}, err
	}
	if len(values) != 1 {
		return berValue{}, fmt.Errorf("%w: CMS element holds %d children, want exactly 1", ErrNoVerifier, len(values))
	}
	return values[0], nil
}

// verifySigner resolves the signing leaf through the CMS SignerInfo and
// asserts it vouches for teamID. The leaf is never chosen by looking for a
// certificate whose Organizational Unit matches: the certificate set is
// attacker-stuffable and is not even leaf-ordered in practice (measured order:
// intermediate, root, leaf).
func verifySigner(cms []byte, teamID string) error {
	budget := &berBudget{}
	contentInfo, rest, err := berParse(cms, 0, budget)
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return fmt.Errorf("%w: CMS carries %d trailing bytes", ErrNoVerifier, len(rest))
	}
	if contentInfo.class != asn1.ClassUniversal || contentInfo.tag != asn1.TagSequence || !contentInfo.compound {
		return fmt.Errorf("%w: CMS ContentInfo is class %d tag %d, want SEQUENCE", ErrNoVerifier, contentInfo.class, contentInfo.tag)
	}
	fields, err := berChildren(contentInfo.content, 1, budget)
	if err != nil {
		return err
	}
	if len(fields) != 2 {
		return fmt.Errorf("%w: CMS ContentInfo holds %d fields, want 2", ErrNoVerifier, len(fields))
	}
	var contentType asn1.ObjectIdentifier
	if _, err := asn1.Unmarshal(fields[0].full, &contentType); err != nil {
		return fmt.Errorf("%w: CMS content type: %w", ErrNoVerifier, err)
	}
	if !contentType.Equal(oidSignedData) {
		return fmt.Errorf("%w: CMS content type %v is not signedData", ErrNoVerifier, contentType)
	}
	if fields[1].class != asn1.ClassContextSpecific || fields[1].tag != 0 || !fields[1].compound {
		return fmt.Errorf("%w: CMS content is class %d tag %d, want [0]", ErrNoVerifier, fields[1].class, fields[1].tag)
	}
	signedData, err := berOnly(fields[1].content, 2, budget)
	if err != nil {
		return err
	}
	certificates, signerInfos, err := signedDataFields(signedData, budget)
	if err != nil {
		return err
	}
	chain, err := parseCertificates(certificates, budget)
	if err != nil {
		return err
	}
	leaf, err := resolveLeaf(chain, signerInfos, budget)
	if err != nil {
		return err
	}
	return assertDeveloperID(leaf, chain, teamID)
}

func signedDataFields(signedData berValue, budget *berBudget) (certificates, signerInfos berValue, err error) {
	if signedData.class != asn1.ClassUniversal || signedData.tag != asn1.TagSequence || !signedData.compound {
		return berValue{}, berValue{}, fmt.Errorf("%w: CMS SignedData is class %d tag %d, want SEQUENCE",
			ErrNoVerifier, signedData.class, signedData.tag)
	}
	fields, err := berChildren(signedData.content, 3, budget)
	if err != nil {
		return berValue{}, berValue{}, err
	}
	if len(fields) < 4 {
		return berValue{}, berValue{}, fmt.Errorf("%w: CMS SignedData holds %d fields, want at least 4", ErrNoVerifier, len(fields))
	}
	var haveCertificates, haveSigners bool
	for _, field := range fields[3:] {
		switch {
		case field.class == asn1.ClassContextSpecific && field.tag == 0:
			if haveCertificates {
				return berValue{}, berValue{}, fmt.Errorf("%w: CMS SignedData holds two certificate sets", ErrNoVerifier)
			}
			certificates, haveCertificates = field, true
		case field.class == asn1.ClassUniversal && field.tag == asn1.TagSet:
			if haveSigners {
				return berValue{}, berValue{}, fmt.Errorf("%w: CMS SignedData holds two SignerInfo sets", ErrNoVerifier)
			}
			signerInfos, haveSigners = field, true
		}
	}
	if !haveCertificates || !haveSigners {
		return berValue{}, berValue{}, fmt.Errorf("%w: CMS SignedData is missing its certificates or SignerInfos", ErrNoVerifier)
	}
	return certificates, signerInfos, nil
}

func resolveLeaf(chain []*x509.Certificate, signerInfos berValue, budget *berBudget) (*x509.Certificate, error) {
	signers, err := berChildren(signerInfos.content, 4, budget)
	if err != nil {
		return nil, err
	}
	if len(signers) != 1 {
		return nil, fmt.Errorf("%w: CMS holds %d SignerInfos, want exactly 1", ErrNoVerifier, len(signers))
	}
	fields, err := berChildren(signers[0].content, 5, budget)
	if err != nil {
		return nil, err
	}
	if len(fields) < 2 {
		return nil, fmt.Errorf("%w: CMS SignerInfo holds %d fields, want at least 2", ErrNoVerifier, len(fields))
	}
	match, err := signerMatcher(fields[1], budget)
	if err != nil {
		return nil, err
	}
	var leaf *x509.Certificate
	for _, candidate := range chain {
		if !match(candidate) {
			continue
		}
		if leaf != nil {
			return nil, fmt.Errorf("%w: two certificates answer the CMS SignerInfo", ErrNoVerifier)
		}
		leaf = candidate
	}
	if leaf == nil {
		return nil, fmt.Errorf("%w: no certificate answers the CMS SignerInfo", ErrUntrustedPeer)
	}
	return leaf, nil
}

func parseCertificates(certificates berValue, budget *berBudget) ([]*x509.Certificate, error) {
	entries, err := berChildren(certificates.content, 4, budget)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 || len(entries) > maxCMSCerts {
		return nil, fmt.Errorf("%w: CMS carries %d certificates, want [1, %d]", ErrNoVerifier, len(entries), maxCMSCerts)
	}
	parsed := make([]*x509.Certificate, 0, len(entries))
	for _, entry := range entries {
		if entry.class != asn1.ClassUniversal || entry.tag != asn1.TagSequence || !entry.compound {
			return nil, fmt.Errorf("%w: CMS certificate set holds a class %d tag %d choice", ErrNoVerifier, entry.class, entry.tag)
		}
		certificate, err := x509.ParseCertificate(entry.full)
		if err != nil {
			return nil, fmt.Errorf("%w: CMS certificate: %w", ErrNoVerifier, err)
		}
		parsed = append(parsed, certificate)
	}
	return parsed, nil
}

// signerMatcher renders the SignerInfo's SignerIdentifier as a predicate over
// the certificate set: issuerAndSerialNumber, or the [0] subjectKeyIdentifier
// alternative.
func signerMatcher(sid berValue, budget *berBudget) (func(*x509.Certificate) bool, error) {
	if sid.class == asn1.ClassContextSpecific && sid.tag == 0 {
		identifier := sid.content
		if len(identifier) == 0 {
			return nil, fmt.Errorf("%w: CMS SignerInfo carries an empty subject key identifier", ErrNoVerifier)
		}
		return func(candidate *x509.Certificate) bool {
			return bytes.Equal(candidate.SubjectKeyId, identifier)
		}, nil
	}
	if sid.class != asn1.ClassUniversal || sid.tag != asn1.TagSequence || !sid.compound {
		return nil, fmt.Errorf("%w: CMS SignerIdentifier is class %d tag %d", ErrNoVerifier, sid.class, sid.tag)
	}
	fields, err := berChildren(sid.content, 6, budget)
	if err != nil {
		return nil, err
	}
	if len(fields) != 2 {
		return nil, fmt.Errorf("%w: CMS issuerAndSerialNumber holds %d fields, want 2", ErrNoVerifier, len(fields))
	}
	issuer := fields[0].full
	var serial *big.Int
	if _, err := asn1.Unmarshal(fields[1].full, &serial); err != nil {
		return nil, fmt.Errorf("%w: CMS signer serial: %w", ErrNoVerifier, err)
	}
	return func(candidate *x509.Certificate) bool {
		return candidate.SerialNumber.Cmp(serial) == 0 && bytes.Equal(candidate.RawIssuer, issuer)
	}, nil
}

// assertDeveloperID is the clause AMFI is trusted for and cannot be observed
// failing: the signing leaf must carry exactly the Organizational Unit the
// CodeDirectory declares as its team, and the chain must carry both Developer
// ID marker OIDs. The issuer clause is corroboration of a chain AMFI already
// validated, so a second certificate sharing the issuer's subject — a renewed
// CA — is not a denial; a leaf whose issuer is absent or unmarked is.
func assertDeveloperID(leaf *x509.Certificate, chain []*x509.Certificate, teamID string) error {
	units := leaf.Subject.OrganizationalUnit
	if len(units) != 1 {
		return fmt.Errorf("%w: signing leaf carries %d organizational units, want exactly 1", ErrUntrustedPeer, len(units))
	}
	if units[0] != teamID {
		return fmt.Errorf("%w: signing leaf organizational unit does not match the CodeDirectory team", ErrUntrustedPeer)
	}
	if !hasExtension(leaf, oidDeveloperIDLeaf) {
		return fmt.Errorf("%w: signing leaf carries no Developer ID marker %v", ErrUntrustedPeer, oidDeveloperIDLeaf)
	}
	for _, candidate := range chain {
		if bytes.Equal(candidate.RawSubject, leaf.RawIssuer) && hasExtension(candidate, oidDeveloperIDIssuer) {
			return nil
		}
	}
	return fmt.Errorf("%w: issuing certificate carries no Developer ID CA marker %v", ErrUntrustedPeer, oidDeveloperIDIssuer)
}

func hasExtension(certificate *x509.Certificate, oid asn1.ObjectIdentifier) bool {
	for _, extension := range certificate.Extensions {
		if extension.Id.Equal(oid) {
			return true
		}
	}
	return false
}
