package trust

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func fuzzSeeds(f *testing.F) [][]byte {
	f.Helper()
	entries, err := filepath.Glob(filepath.Join("testdata", "entitlements-*.bin"))
	if err != nil || len(entries) == 0 {
		f.Fatalf("no measured entitlement blobs in testdata: %v", err)
	}
	seeds := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		blob, err := os.ReadFile(entry)
		if err != nil {
			f.Fatalf("read %s: %v", entry, err)
		}
		seeds = append(seeds, blob)
	}
	return seeds
}

// FuzzDecodeEntitlementsBlob drives the one pre-authentication parser in the
// package. Seeds are real kernel answers plus every framing shape the deny
// corpus pins; the properties are that it never panics, never returns both a
// dictionary and an error, and never leaves an injection entitlement
// undecided.
func FuzzDecodeEntitlementsBlob(f *testing.F) {
	for _, seed := range fuzzSeeds(f) {
		f.Add(seed)
		for _, length := range []int{0, 4, 8, 12, len(seed) - 1} {
			if length >= 0 && length <= len(seed) {
				f.Add(seed[:length:length])
			}
		}
		flipped := append([]byte(nil), seed...)
		flipped[3] ^= 1
		f.Add(flipped)
		oversized := append([]byte(nil), seed...)
		binary.BigEndian.PutUint32(oversized[4:8], 0xFFFFFFFF)
		f.Add(oversized)
	}
	f.Add(make([]byte, 8))
	f.Add([]byte{0xfa, 0xde, 0x71, 0x72, 0x00, 0x00, 0x00, 0x08})

	f.Fuzz(func(t *testing.T, blob []byte) {
		ents, err := decodeEntitlementsBlob(blob)
		if err != nil {
			if ents != nil {
				t.Fatalf("decodeEntitlementsBlob returned %v with error %v", ents, err)
			}
			if !errors.Is(err, ErrNoVerifier) {
				t.Fatalf("decodeEntitlementsBlob = %v, want ErrNoVerifier", err)
			}
			return
		}
		if len(ents) > maxEntitlementCount {
			t.Fatalf("decoded %d entitlements, past the %d cap", len(ents), maxEntitlementCount)
		}
		strict := rejectInjection(ents, Requirement{})
		relaxed := rejectInjection(ents, Requirement{AllowJIT: true})
		if strict == nil && relaxed != nil {
			t.Fatalf("relaxing allow-jit denied a peer the strict policy admitted: %v", relaxed)
		}
		if strict != nil && !errors.Is(strict, ErrUntrustedPeer) {
			t.Fatalf("rejectInjection = %v, want ErrUntrustedPeer", strict)
		}
		if strict != nil {
			return
		}
		for _, name := range injectionEntitlements {
			value, present := ents[name]
			if present && (value.kind != entBool || value.boolean) {
				t.Fatalf("rejectInjection admitted a peer carrying %s as kind %d", name, value.kind)
			}
		}
	})
}

// FuzzVerifySigner drives the BER envelope walker, which exists because
// Apple's CMS is not DER and encoding/asn1 refuses it outright.
func FuzzVerifySigner(f *testing.F) {
	cms, err := os.ReadFile(filepath.Join("testdata", "cms-developerid.der"))
	if err != nil {
		f.Fatalf("read measured CMS: %v", err)
	}
	f.Add(cms)
	for _, length := range []int{0, 1, 2, 8, 64, 1024, len(cms) / 2, len(cms) - 1} {
		f.Add(cms[:length:length])
	}
	truncated := append([]byte(nil), cms...)
	truncated[1] = 0x82
	f.Add(truncated)

	f.Fuzz(func(t *testing.T, blob []byte) {
		err := verifySigner(blob, "24VZTF6M5V")
		if err == nil {
			return
		}
		if !errors.Is(err, ErrNoVerifier) && !errors.Is(err, ErrUntrustedPeer) {
			t.Fatalf("verifySigner = %v, want a typed denial", err)
		}
	})
}

// FuzzParseSuperBlob drives the superblob and CodeDirectory framing, whose
// every bound comes from attacker-adjacent length headers.
func FuzzParseSuperBlob(f *testing.F) {
	f.Add(make([]byte, 12))
	f.Add([]byte{0xfa, 0xde, 0x0c, 0xc0, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xfa, 0xde, 0x0c, 0xc0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, blob []byte) {
		parsed, err := parseSuperBlob(blob)
		if err != nil {
			if !errors.Is(err, ErrNoVerifier) {
				t.Fatalf("parseSuperBlob = %v, want ErrNoVerifier", err)
			}
			return
		}
		for _, cd := range parsed.directories {
			if _, err := parseCodeDirectory(cd); err != nil && !errors.Is(err, ErrNoVerifier) && !errors.Is(err, ErrUntrustedPeer) {
				t.Fatalf("parseCodeDirectory = %v, want a typed denial", err)
			}
		}
	})
}
