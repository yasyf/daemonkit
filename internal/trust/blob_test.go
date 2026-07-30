package trust

import (
	"encoding/binary"
	"strings"
	"testing"
)

func devIDSignature(t *testing.T) ([]byte, []byte) {
	t.Helper()
	chain := devIDChain(t)
	cd := devIDCodeDirectory().build()
	blob := buildSuperBlob(
		superBlobSlot{0, cd},
		superBlobSlot{0x10000, blobHeader(blobWrapperMagic, buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER))},
	)
	return blob, cdHashOf(cd)
}

func devIDRequirement() Requirement {
	return Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
}

func TestVerifySignatureBlobAcceptsAMatchingSuperBlob(t *testing.T) {
	blob, cdHash := devIDSignature(t)
	if err := verifySignatureBlob(blob, cdHash, devIDRequirement()); err != nil {
		t.Fatalf("verifySignatureBlob = %v, want nil", err)
	}
}

// The kernel copies out more than the superblob's own length header declares
// (measured: 59104 bytes for a 50153-byte blob), so every bound must come from
// the header.
func TestVerifySignatureBlobClampsToItsOwnLengthHeader(t *testing.T) {
	blob, cdHash := devIDSignature(t)
	padded := append(blob[:len(blob):len(blob)], make([]byte, 8192)...)
	if err := verifySignatureBlob(padded, cdHash, devIDRequirement()); err != nil {
		t.Fatalf("verifySignatureBlob(padded) = %v, want nil", err)
	}
}

// A superblob may carry several CodeDirectories. The one that counts is the
// one the kernel is enforcing, which is the one whose SHA-256 prefix is
// CS_OPS_CDHASH — not slot 0.
func TestFindCodeDirectorySelectsByHashNotBySlotOrder(t *testing.T) {
	chain := devIDChain(t)
	decoy := devIDCodeDirectory()
	decoy.identifier = "com.attacker.decoy"
	decoy.teamID = "ZZ0FAKE9TX"
	enforced := devIDCodeDirectory().build()
	blob := buildSuperBlob(
		superBlobSlot{0, decoy.build()},
		superBlobSlot{0x1000, enforced},
		superBlobSlot{0x10000, blobHeader(blobWrapperMagic, buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER))},
	)
	if err := verifySignatureBlob(blob, cdHashOf(enforced), devIDRequirement()); err != nil {
		t.Fatalf("verifySignatureBlob(alternate CodeDirectory) = %v, want nil", err)
	}
	err := verifySignatureBlob(blob, cdHashOf(decoy.build()), devIDRequirement())
	assertDenies(t, err, ErrUntrustedPeer)
	if !strings.Contains(err.Error(), "CodeDirectory signing identifier does not match") {
		t.Errorf("verifySignatureBlob(decoy enforced) = %q, want the identifier denial", err)
	}
}

func TestParseSuperBlobDeniesFraming(t *testing.T) {
	base := func(t *testing.T) []byte {
		blob, _ := devIDSignature(t)
		return blob
	}
	tests := []struct {
		name   string
		blob   func(*testing.T) []byte
		reason string
	}{
		{"shorter than a superblob header", func(*testing.T) []byte { return make([]byte, 11) }, "want at least 12"},
		{"wrong magic", func(t *testing.T) []byte {
			blob := base(t)
			binary.BigEndian.PutUint32(blob[0:4], 0xfade0c02)
			return blob
		}, "magic 0xfade0c02"},
		{"length header below the minimum", func(t *testing.T) []byte {
			blob := base(t)
			binary.BigEndian.PutUint32(blob[4:8], 11)
			return blob
		}, "length header 11"},
		{"length header overruns the buffer", func(t *testing.T) []byte {
			blob := base(t)
			binary.BigEndian.PutUint32(blob[4:8], uint32(len(blob)+1))
			return blob
		}, "length header"},
		{"length header above the cap", func(t *testing.T) []byte {
			blob := base(t)
			binary.BigEndian.PutUint32(blob[4:8], maxSignatureBlob+1)
			return blob
		}, "length header"},
		{"more slots than the cap", func(t *testing.T) []byte {
			blob := base(t)
			binary.BigEndian.PutUint32(blob[8:12], maxSignatureSlot+1)
			return blob
		}, "declares 65 slots"},
		{"slot index does not fit the blob", func(*testing.T) []byte {
			blob := make([]byte, 12)
			binary.BigEndian.PutUint32(blob[0:4], superBlobMagic)
			binary.BigEndian.PutUint32(blob[4:8], 12)
			binary.BigEndian.PutUint32(blob[8:12], 1)
			return blob
		}, "declares 1 slots in 12 bytes"},
		{"slot offset past the blob", func(t *testing.T) []byte {
			blob := base(t)
			binary.BigEndian.PutUint32(blob[16:20], uint32(len(blob)))
			return blob
		}, "past the"},
		{"slot length past the blob", func(t *testing.T) []byte {
			blob := base(t)
			offset := binary.BigEndian.Uint32(blob[16:20])
			binary.BigEndian.PutUint32(blob[offset+4:offset+8], uint32(len(blob)))
			return blob
		}, "past the"},
		{"slot shorter than its own header", func(t *testing.T) []byte {
			blob := base(t)
			offset := binary.BigEndian.Uint32(blob[16:20])
			binary.BigEndian.PutUint32(blob[offset+4:offset+8], 7)
			return blob
		}, "declares 7 bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSuperBlob(tt.blob(t))
			assertDenies(t, err, ErrNoVerifier)
			if !strings.Contains(err.Error(), tt.reason) {
				t.Errorf("parseSuperBlob = %q, want the reason to name %q", err, tt.reason)
			}
		})
	}
}

func TestParseSuperBlobDeniesTwoCMSSlots(t *testing.T) {
	chain := devIDChain(t)
	cms := blobHeader(blobWrapperMagic, buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER))
	blob := buildSuperBlob(
		superBlobSlot{0, devIDCodeDirectory().build()},
		superBlobSlot{0x10000, cms},
		superBlobSlot{0x10001, cms},
	)
	_, err := parseSuperBlob(blob)
	assertDenies(t, err, ErrNoVerifier)
	if !strings.Contains(err.Error(), "two CMS slots") {
		t.Errorf("parseSuperBlob = %q, want the two-CMS denial", err)
	}
}

func TestVerifySignatureBlobDeniesAMissingCodeDirectoryOrSignature(t *testing.T) {
	chain := devIDChain(t)
	cd := devIDCodeDirectory().build()

	noSignature := buildSuperBlob(superBlobSlot{0, cd})
	err := verifySignatureBlob(noSignature, cdHashOf(cd), devIDRequirement())
	assertDenies(t, err, ErrUntrustedPeer)
	if !strings.Contains(err.Error(), "no CMS payload") {
		t.Errorf("verifySignatureBlob(no CMS) = %q, want the CMS denial", err)
	}

	noDirectory := buildSuperBlob(
		superBlobSlot{0x10000, blobHeader(blobWrapperMagic, buildCMS(t, chain.leaf, chain.issuerDER, chain.leafDER))},
	)
	err = verifySignatureBlob(noDirectory, cdHashOf(cd), devIDRequirement())
	assertDenies(t, err, ErrUntrustedPeer)
	if !strings.Contains(err.Error(), "hashes to the kernel's cdhash") {
		t.Errorf("verifySignatureBlob(no CodeDirectory) = %q, want the cdhash denial", err)
	}
}

func TestParseCodeDirectoryDeniesMalformedStrings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte)
		reason string
	}{
		{"identifier offset inside the header", func(cd []byte) {
			binary.BigEndian.PutUint32(cd[cdOffsetIdentifier:], cdHeaderLength-1)
		}, "signing identifier offset"},
		{"team offset past the directory", func(cd []byte) {
			binary.BigEndian.PutUint32(cd[cdOffsetTeamID:], uint32(len(cd)))
		}, "team identifier offset"},
		{"identifier is empty", func(cd []byte) { cd[cdHeaderLength] = 0 }, "signing identifier is empty"},
		{"identifier is unterminated", func(cd []byte) {
			for index := cdHeaderLength; index < len(cd); index++ {
				cd[index] = 'x'
			}
		}, "unterminated"},
		{"identifier is not valid UTF-8", func(cd []byte) { cd[cdHeaderLength] = 0xff }, "not valid UTF-8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := devIDCodeDirectory().build()
			tt.mutate(cd)
			_, err := parseCodeDirectory(cd)
			assertDenies(t, err, ErrUntrustedPeer)
			if !strings.Contains(err.Error(), tt.reason) {
				t.Errorf("parseCodeDirectory = %q, want the reason to name %q", err, tt.reason)
			}
		})
	}
}

func TestFindCodeDirectoryDeniesAWrongSizedKernelHash(t *testing.T) {
	_, err := findCodeDirectory(nil, make([]byte, cdHashLength-1))
	assertDenies(t, err, ErrNoVerifier)
	if !strings.Contains(err.Error(), "kernel cdhash is 19 bytes") {
		t.Errorf("findCodeDirectory = %q, want the cdhash-length denial", err)
	}
}
