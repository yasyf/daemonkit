package trust

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

const (
	superBlobMagic     = 0xfade0cc0
	codeDirectoryMagic = 0xfade0c02
	blobWrapperMagic   = 0xfade0b01

	maxSignatureBlob = 8 << 20
	maxSignatureSlot = 64
	cdHashLength     = 20

	// CodeDirectory header offsets, from xnu's CS_CodeDirectory.
	cdOffsetVersion    = 8
	cdOffsetFlags      = 12
	cdOffsetIdentifier = 20
	cdOffsetHashSize   = 36
	cdOffsetHashType   = 37
	cdOffsetTeamID     = 48
	cdHeaderLength     = 52

	cdVersionTeamID  = 0x20200
	cdHashTypeSHA256 = 2
	cdFlagAdhoc      = 0x00000002
	cdFlagLinkerSign = 0x00020000
)

type superBlob struct {
	directories [][]byte
	signature   []byte
}

type codeDirectory struct {
	identifier string
	teamID     string
}

// verifySignatureBlob is the in-process half of the identity proof: it finds
// the CodeDirectory the kernel is actually enforcing — the one whose SHA-256
// prefix is cdHash, never "slot 0" — reads the identifier and team it
// declares, and asserts them against the certificate the CMS SignerInfo
// designates. That last step is what a peer whose signing leaf carries no
// Organizational Unit cannot pass: Apple's own verifySignature() silently
// skips the team comparison on such a leaf, and the kernel then serves
// whatever team the CodeDirectory declares.
func verifySignatureBlob(blob, cdHash []byte, req Requirement) error {
	parsed, err := parseSuperBlob(blob)
	if err != nil {
		return err
	}
	directory, err := findCodeDirectory(parsed.directories, cdHash)
	if err != nil {
		return err
	}
	if directory.identifier != req.SigningIdentifier {
		return fmt.Errorf("%w: CodeDirectory signing identifier does not match the requirement", ErrUntrustedPeer)
	}
	if directory.teamID != req.TeamID {
		return fmt.Errorf("%w: CodeDirectory team identifier does not match the requirement", ErrUntrustedPeer)
	}
	if len(parsed.signature) == 0 {
		return fmt.Errorf("%w: signature blob carries no CMS payload (ad-hoc signature)", ErrUntrustedPeer)
	}
	return verifySigner(parsed.signature, directory.teamID)
}

// parseSuperBlob returns every CodeDirectory and the CMS payload. Every bound
// comes from the superblob's own length header, never from the size the kernel
// copied out — the kernel's buffer is larger than the blob it holds (measured:
// 59104 copied for a 50153-byte superblob).
func parseSuperBlob(blob []byte) (superBlob, error) {
	if len(blob) < 12 {
		return superBlob{}, fmt.Errorf("%w: signature blob is %d bytes, want at least 12", ErrNoVerifier, len(blob))
	}
	if magic := binary.BigEndian.Uint32(blob[0:4]); magic != superBlobMagic {
		return superBlob{}, fmt.Errorf("%w: signature blob magic 0x%08x is not 0x%08x", ErrNoVerifier, magic, superBlobMagic)
	}
	length := uint64(binary.BigEndian.Uint32(blob[4:8]))
	if length < 12 || length > maxSignatureBlob || length > uint64(len(blob)) {
		return superBlob{}, fmt.Errorf("%w: signature blob length header %d is outside [12, min(%d, %d)]",
			ErrNoVerifier, length, maxSignatureBlob, len(blob))
	}
	blob = blob[:length]
	count := uint64(binary.BigEndian.Uint32(blob[8:12]))
	if count > maxSignatureSlot || 12+count*8 > length {
		return superBlob{}, fmt.Errorf("%w: signature blob declares %d slots in %d bytes", ErrNoVerifier, count, length)
	}
	var parsed superBlob
	for index := uint64(0); index < count; index++ {
		entry := 12 + index*8
		offset := uint64(binary.BigEndian.Uint32(blob[entry+4 : entry+8]))
		if offset+8 > length {
			return superBlob{}, fmt.Errorf("%w: signature slot %d starts at %d, past the %d-byte blob",
				ErrNoVerifier, index, offset, length)
		}
		magic := binary.BigEndian.Uint32(blob[offset : offset+4])
		size := uint64(binary.BigEndian.Uint32(blob[offset+4 : offset+8]))
		if size < 8 || offset+size > length {
			return superBlob{}, fmt.Errorf("%w: signature slot %d declares %d bytes at %d, past the %d-byte blob",
				ErrNoVerifier, index, size, offset, length)
		}
		switch magic {
		case codeDirectoryMagic:
			parsed.directories = append(parsed.directories, blob[offset:offset+size])
		case blobWrapperMagic:
			if parsed.signature != nil {
				return superBlob{}, fmt.Errorf("%w: signature blob carries two CMS slots", ErrNoVerifier)
			}
			parsed.signature = blob[offset+8 : offset+size]
		}
	}
	return parsed, nil
}

// findCodeDirectory selects the CodeDirectory the kernel is enforcing. A
// superblob may carry several; the one that counts is the one whose SHA-256
// truncated to 20 bytes equals CS_OPS_CDHASH.
func findCodeDirectory(directories [][]byte, cdHash []byte) (codeDirectory, error) {
	if len(cdHash) != cdHashLength {
		return codeDirectory{}, fmt.Errorf("%w: kernel cdhash is %d bytes, want %d", ErrNoVerifier, len(cdHash), cdHashLength)
	}
	for _, cd := range directories {
		sum := sha256.Sum256(cd)
		if string(sum[:cdHashLength]) != string(cdHash) {
			continue
		}
		return parseCodeDirectory(cd)
	}
	return codeDirectory{}, fmt.Errorf("%w: no CodeDirectory in the signature blob hashes to the kernel's cdhash", ErrUntrustedPeer)
}

func parseCodeDirectory(cd []byte) (codeDirectory, error) {
	if len(cd) < cdHeaderLength {
		return codeDirectory{}, fmt.Errorf("%w: CodeDirectory is %d bytes, want at least %d", ErrNoVerifier, len(cd), cdHeaderLength)
	}
	version := binary.BigEndian.Uint32(cd[cdOffsetVersion : cdOffsetVersion+4])
	if version < cdVersionTeamID {
		return codeDirectory{}, fmt.Errorf("%w: CodeDirectory version 0x%x declares no team identifier", ErrUntrustedPeer, version)
	}
	if hashType := cd[cdOffsetHashType]; hashType != cdHashTypeSHA256 {
		return codeDirectory{}, fmt.Errorf("%w: CodeDirectory hash type %d is not SHA-256", ErrUntrustedPeer, hashType)
	}
	if hashSize := cd[cdOffsetHashSize]; hashSize != sha256.Size {
		return codeDirectory{}, fmt.Errorf("%w: CodeDirectory hash size %d is not %d", ErrUntrustedPeer, hashSize, sha256.Size)
	}
	flags := binary.BigEndian.Uint32(cd[cdOffsetFlags : cdOffsetFlags+4])
	if flags&cdFlagAdhoc != 0 {
		return codeDirectory{}, fmt.Errorf("%w: CodeDirectory is ad-hoc signed (flags 0x%x)", ErrUntrustedPeer, flags)
	}
	if flags&cdFlagLinkerSign != 0 {
		return codeDirectory{}, fmt.Errorf("%w: CodeDirectory is linker signed (flags 0x%x)", ErrUntrustedPeer, flags)
	}
	identifier, err := codeDirectoryString(cd, cdOffsetIdentifier, "signing identifier")
	if err != nil {
		return codeDirectory{}, err
	}
	teamID, err := codeDirectoryString(cd, cdOffsetTeamID, "team identifier")
	if err != nil {
		return codeDirectory{}, err
	}
	return codeDirectory{identifier: identifier, teamID: teamID}, nil
}

func codeDirectoryString(cd []byte, field int, name string) (string, error) {
	offset := uint64(binary.BigEndian.Uint32(cd[field : field+4]))
	if offset < cdHeaderLength || offset >= uint64(len(cd)) {
		return "", fmt.Errorf("%w: CodeDirectory %s offset %d is outside the %d-byte directory",
			ErrUntrustedPeer, name, offset, len(cd))
	}
	tail := cd[offset:]
	end := -1
	for index, b := range tail {
		if b == 0 {
			end = index
			break
		}
	}
	if end <= 0 {
		return "", fmt.Errorf("%w: CodeDirectory %s is empty or unterminated", ErrUntrustedPeer, name)
	}
	value := string(tail[:end])
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("%w: CodeDirectory %s is not valid UTF-8", ErrUntrustedPeer, name)
	}
	return value, nil
}
