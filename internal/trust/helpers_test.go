package trust

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"math/big"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/csposture"
)

const (
	testTeam       = "SXKCTF23Q2"
	testIdentifier = "com.yasyf.daemonkit.fixture-a"
	testGroup      = "group.com.yasyf.daemonkit.fixture"
)

const admittedStatus = csposture.Valid | csposture.Runtime | csposture.Hard |
	csposture.Enforcement | csposture.RequireLV

// entitlement values, in the shapes AMFI's DER encoder emits.

func entBoolean(value bool) asn1.RawValue {
	content := []byte{0x00}
	if value {
		content = []byte{0xff}
	}
	return asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagBoolean, Bytes: content}
}

func entText(value string) asn1.RawValue {
	return asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagUTF8String, Bytes: []byte(value)}
}

func entInteger(t *testing.T, value int) asn1.RawValue {
	t.Helper()
	encoded, err := asn1.Marshal(value)
	if err != nil {
		t.Fatalf("marshal integer: %v", err)
	}
	return asn1.RawValue{FullBytes: encoded}
}

func entStrings(t *testing.T, values ...string) asn1.RawValue {
	t.Helper()
	content := make([]byte, 0, len(values)*8)
	for _, value := range values {
		content = append(content, marshalRaw(t, entText(value))...)
	}
	return asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true, Bytes: content}
}

func entDictionary(t *testing.T, pairs ...any) asn1.RawValue {
	t.Helper()
	return asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 16, IsCompound: true, Bytes: entElements(t, pairs...)}
}

func marshalRaw(t *testing.T, value asn1.RawValue) []byte {
	t.Helper()
	encoded, err := asn1.Marshal(value)
	if err != nil {
		t.Fatalf("marshal ASN.1 value: %v", err)
	}
	return encoded
}

func entElements(t *testing.T, pairs ...any) []byte {
	t.Helper()
	if len(pairs)%2 != 0 {
		t.Fatalf("entitlement pairs must come in twos, got %d", len(pairs))
	}
	var elements []byte
	for index := 0; index < len(pairs); index += 2 {
		key, ok := pairs[index].(string)
		if !ok {
			t.Fatalf("entitlement key %d is not a string", index)
		}
		value, ok := pairs[index+1].(asn1.RawValue)
		if !ok {
			t.Fatalf("entitlement value for %q is not an asn1.RawValue", key)
		}
		content := append(marshalRaw(t, entText(key)), marshalRaw(t, value)...)
		elements = append(elements, marshalRaw(t, asn1.RawValue{
			Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true, Bytes: content,
		})...)
	}
	return elements
}

// entitlementsDER renders one AMFI entitlements dictionary, header excluded.
func entitlementsDER(t *testing.T, pairs ...any) []byte {
	t.Helper()
	return entitlementsDERVersion(t, 1, pairs...)
}

func entitlementsDERVersion(t *testing.T, version int, pairs ...any) []byte {
	t.Helper()
	body := append(marshalRaw(t, entInteger(t, version)), marshalRaw(t, asn1.RawValue{
		Class: asn1.ClassContextSpecific, Tag: 16, IsCompound: true, Bytes: entElements(t, pairs...),
	})...)
	return marshalRaw(t, asn1.RawValue{Class: asn1.ClassApplication, Tag: 16, IsCompound: true, Bytes: body})
}

// entitlementsBlob wraps a DER dictionary in the kernel's 8-byte blob header.
func entitlementsBlob(t *testing.T, pairs ...any) []byte {
	t.Helper()
	return blobHeader(derEntitlementsMagic, entitlementsDER(t, pairs...))
}

func blobHeader(magic uint32, payload []byte) []byte {
	blob := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint32(blob[0:4], magic)
	binary.BigEndian.PutUint32(blob[4:8], uint32(8+len(payload)))
	return append(blob, payload...)
}

// CodeDirectory and superblob construction.

type codeDirectorySpec struct {
	version    uint32
	flags      uint32
	hashType   byte
	hashSize   byte
	identifier string
	teamID     string
}

func devIDCodeDirectory() codeDirectorySpec {
	return codeDirectorySpec{
		version:    cdVersionTeamID,
		hashType:   cdHashTypeSHA256,
		hashSize:   sha256.Size,
		identifier: testIdentifier,
		teamID:     testTeam,
	}
}

func (s codeDirectorySpec) build() []byte {
	strings := append(append([]byte(s.identifier), 0), append([]byte(s.teamID), 0)...)
	cd := make([]byte, cdHeaderLength, cdHeaderLength+len(strings))
	binary.BigEndian.PutUint32(cd[0:4], codeDirectoryMagic)
	binary.BigEndian.PutUint32(cd[4:8], uint32(cdHeaderLength+len(strings)))
	binary.BigEndian.PutUint32(cd[cdOffsetVersion:], s.version)
	binary.BigEndian.PutUint32(cd[cdOffsetFlags:], s.flags)
	binary.BigEndian.PutUint32(cd[cdOffsetIdentifier:], cdHeaderLength)
	cd[cdOffsetHashSize] = s.hashSize
	cd[cdOffsetHashType] = s.hashType
	binary.BigEndian.PutUint32(cd[cdOffsetTeamID:], uint32(cdHeaderLength+len(s.identifier)+1))
	return append(cd, strings...)
}

type superBlobSlot struct {
	slotType uint32
	blob     []byte
}

func buildSuperBlob(slots ...superBlobSlot) []byte {
	length := 12 + 8*len(slots)
	for _, slot := range slots {
		length += len(slot.blob)
	}
	blob := make([]byte, 12+8*len(slots), length)
	binary.BigEndian.PutUint32(blob[0:4], superBlobMagic)
	binary.BigEndian.PutUint32(blob[4:8], uint32(length))
	binary.BigEndian.PutUint32(blob[8:12], uint32(len(slots)))
	offset := uint32(12 + 8*len(slots))
	for index, slot := range slots {
		entry := 12 + 8*index
		binary.BigEndian.PutUint32(blob[entry:entry+4], slot.slotType)
		binary.BigEndian.PutUint32(blob[entry+4:entry+8], offset)
		offset += uint32(len(slot.blob))
	}
	for _, slot := range slots {
		blob = append(blob, slot.blob...)
	}
	return blob
}

func cdHashOf(cd []byte) []byte {
	sum := sha256.Sum256(cd)
	return sum[:cdHashLength]
}

// Certificate and CMS construction. The chain is synthetic; the real Apple
// BER CMS in testdata/ exercises the walker against what codesign emits.

type certificateSpec struct {
	commonName         string
	organizationalUnit []string
	markers            []asn1.ObjectIdentifier
}

type signingChain struct {
	leafDER   []byte
	leaf      *x509.Certificate
	issuerDER []byte
	issuer    *x509.Certificate
}

func newSigningChain(t *testing.T, leafSpec certificateSpec, issuerMarkers ...asn1.ObjectIdentifier) signingChain {
	t.Helper()
	if issuerMarkers == nil {
		issuerMarkers = []asn1.ObjectIdentifier{oidDeveloperIDIssuer}
	}
	issuerDER, issuerKey := issueCertificate(t, certificateSpec{
		commonName: "Developer ID Certification Authority",
		markers:    issuerMarkers,
	}, nil, nil)
	issuer, err := x509.ParseCertificate(issuerDER)
	if err != nil {
		t.Fatalf("parse issuer: %v", err)
	}
	leafDER, _ := issueCertificate(t, leafSpec, issuer, issuerKey)
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return signingChain{leafDER: leafDER, leaf: leaf, issuerDER: issuerDER, issuer: issuer}
}

func devIDChain(t *testing.T) signingChain {
	t.Helper()
	return newSigningChain(t, certificateSpec{
		commonName:         "Developer ID Application: Test (" + testTeam + ")",
		organizationalUnit: []string{testTeam},
		markers:            []asn1.ObjectIdentifier{oidDeveloperIDLeaf},
	})
}

var serialCounter int64

func issueCertificate(t *testing.T, spec certificateSpec, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	extensions := make([]pkix.Extension, 0, len(spec.markers))
	for _, marker := range spec.markers {
		extensions = append(extensions, pkix.Extension{Id: marker, Value: []byte{0x05, 0x00}})
	}
	serialCounter++
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serialCounter),
		Subject: pkix.Name{
			CommonName:         spec.commonName,
			OrganizationalUnit: spec.organizationalUnit,
		},
		NotBefore:       time.Unix(0, 0),
		NotAfter:        time.Unix(1<<31, 0),
		IsCA:            parent == nil,
		ExtraExtensions: extensions,
	}
	signer, signerKey := template, key
	if parent != nil {
		signer, signerKey = parent, parentKey
	}
	der, err := x509.CreateCertificate(rand.Reader, template, signer, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return der, key
}

type cmsIssuerAndSerial struct {
	Issuer asn1.RawValue
	Serial *big.Int
}

type cmsSignerInfo struct {
	Version            int
	SID                cmsIssuerAndSerial
	DigestAlgorithm    pkix.AlgorithmIdentifier
	SignatureAlgorithm pkix.AlgorithmIdentifier
	Signature          []byte
}

type cmsSignedData struct {
	Version          int
	DigestAlgorithms []pkix.AlgorithmIdentifier `asn1:"set"`
	EncapContentInfo asn1.RawValue
	Certificates     asn1.RawValue   `asn1:"tag:0"`
	SignerInfos      []cmsSignerInfo `asn1:"set"`
}

type cmsContentInfo struct {
	ContentType asn1.ObjectIdentifier
	Content     asn1.RawValue
}

// buildCMS renders a SignedData whose single SignerInfo designates signer by
// issuerAndSerialNumber, with certificates in the given order.
func buildCMS(t *testing.T, signer *x509.Certificate, certificates ...[]byte) []byte {
	t.Helper()
	var set []byte
	for _, certificate := range certificates {
		set = append(set, certificate...)
	}
	signedData := cmsSignedData{
		Version:          1,
		DigestAlgorithms: []pkix.AlgorithmIdentifier{{Algorithm: asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 2, 1}}},
		EncapContentInfo: asn1.RawValue{
			Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true,
			Bytes: marshalRaw(t, asn1.RawValue{FullBytes: mustMarshal(t, asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 1})}),
		},
		Certificates: asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: set},
		SignerInfos: []cmsSignerInfo{{
			Version:            1,
			SID:                cmsIssuerAndSerial{Issuer: asn1.RawValue{FullBytes: signer.RawIssuer}, Serial: signer.SerialNumber},
			DigestAlgorithm:    pkix.AlgorithmIdentifier{Algorithm: asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 2, 1}},
			SignatureAlgorithm: pkix.AlgorithmIdentifier{Algorithm: asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}},
			Signature:          []byte{0x00},
		}},
	}
	return mustMarshal(t, cmsContentInfo{
		ContentType: oidSignedData,
		Content: asn1.RawValue{
			Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true,
			Bytes: mustMarshal(t, signedData),
		},
	})
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := asn1.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return encoded
}

// The kernel fake. Every deny case mutates one field of an admitting answer.

type fakeKernel struct {
	status     int64
	category   uint32
	teamID     string
	identifier string
	cdHash     []byte
	signature  []byte
	entitle    []byte
	errnos     map[uint32]syscall.Errno
	reads      []uint32
}

func admittingKernel(t *testing.T) *fakeKernel {
	t.Helper()
	chain := devIDChain(t)
	cd := devIDCodeDirectory().build()
	signature := buildSuperBlob(
		superBlobSlot{0, cd},
		superBlobSlot{0x10000, blobHeader(blobWrapperMagic, buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER))},
	)
	return &fakeKernel{
		status:     admittedStatus,
		category:   validationCategoryDeveloperID,
		teamID:     testTeam,
		identifier: testIdentifier,
		cdHash:     cdHashOf(cd),
		signature:  signature,
		entitle:    entitlementsBlob(t, appGroupsEntitlement, entStrings(t, testGroup)),
		errnos:     map[uint32]syscall.Errno{},
	}
}

// replaceCodeDirectory re-signs the fake peer around a different
// CodeDirectory, keeping the kernel's cdhash consistent with it.
func (k *fakeKernel) replaceCodeDirectory(t *testing.T, spec codeDirectorySpec) {
	t.Helper()
	chain := devIDChain(t)
	cd := spec.build()
	k.cdHash = cdHashOf(cd)
	k.signature = buildSuperBlob(
		superBlobSlot{0, cd},
		superBlobSlot{0x10000, blobHeader(blobWrapperMagic, buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER))},
	)
}

func (k *fakeKernel) requirement() Requirement {
	return Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier, RequiredAppGroup: testGroup}
}

func (k *fakeKernel) verify(req Requirement) error {
	return verifyToken(k.read, req)
}

func (k *fakeKernel) read(op uint32, buf []byte) syscall.Errno {
	k.reads = append(k.reads, op)
	if errno, ok := k.errnos[op]; ok {
		return errno
	}
	switch op {
	case opStatus:
		binary.LittleEndian.PutUint32(buf, uint32(k.status)) //nolint:gosec // The corpus pins 32-bit status words.
		return 0
	case opValidationCategory:
		binary.LittleEndian.PutUint32(buf, k.category)
		return 0
	case opTeamID:
		return copyString(buf, k.teamID)
	case opIdentity:
		return copyString(buf, k.identifier)
	case opCDHash:
		copy(buf, k.cdHash)
		return 0
	case opBlob:
		return copyBlob(buf, k.signature)
	case opDEREntitlements:
		return copyBlob(buf, k.entitle)
	default:
		return syscall.EINVAL
	}
}

func copyString(buf []byte, value string) syscall.Errno {
	length := 8 + len(value) + 1
	if len(buf) < length {
		return rangeError(buf, length)
	}
	binary.BigEndian.PutUint32(buf[0:4], 0)
	binary.BigEndian.PutUint32(buf[4:8], uint32(length))
	copy(buf[8:], value)
	buf[length-1] = 0
	return 0
}

func copyBlob(buf, payload []byte) syscall.Errno {
	if len(buf) < len(payload) {
		return rangeError(buf, len(payload))
	}
	copy(buf, payload)
	return 0
}

// rangeError mirrors xnu's csops_copy_token: on ERANGE it writes a zero magic
// and the required size (measured 59104 for a 50153-byte superblob).
func rangeError(buf []byte, required int) syscall.Errno {
	if len(buf) >= 8 {
		binary.BigEndian.PutUint32(buf[0:4], 0)
		binary.BigEndian.PutUint32(buf[4:8], uint32(required))
	}
	return syscall.ERANGE
}
