package trust

import (
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func measuredBlob(t *testing.T, name string) []byte {
	t.Helper()
	blob, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read measured blob: %v", err)
	}
	return blob
}

// The three blobs are real op-16 answers: two read off live processes on macOS
// 26.5.2 (a Developer ID app and a platform binary), one produced by codesign
// itself, which is the encoder AMFI's blobs come from.
func TestDecodeEntitlementsBlobAcceptsMeasuredBlobs(t *testing.T) {
	tests := []struct {
		file string
		want map[string]entKind
	}{
		{"entitlements-developerid.bin", map[string]entKind{
			"com.apple.security.automation.apple-events": entBool,
		}},
		{"entitlements-platform.bin", map[string]entKind{
			"com.apple.apfs.get-dev-by-role": entBool,
		}},
		{"entitlements-measured.bin", map[string]entKind{
			appGroupsEntitlement: entArray,
			entAllowJIT:          entBool,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			ents, err := decodeEntitlementsBlob(measuredBlob(t, tt.file))
			if err != nil {
				t.Fatalf("decodeEntitlementsBlob(%s) = %v, want nil", tt.file, err)
			}
			if len(ents) == 0 {
				t.Fatalf("decodeEntitlementsBlob(%s) decoded no entitlements", tt.file)
			}
			for key, kind := range tt.want {
				value, ok := ents[key]
				if !ok {
					t.Fatalf("%s: entitlement %q is absent", tt.file, key)
				}
				if value.kind != kind {
					t.Errorf("%s: entitlement %q kind = %d, want %d", tt.file, key, value.kind, kind)
				}
			}
		})
	}
}

func TestDecodeEntitlementsBlobDecodesTheMeasuredAppGroupArray(t *testing.T) {
	ents, err := decodeEntitlementsBlob(measuredBlob(t, "entitlements-measured.bin"))
	if err != nil {
		t.Fatalf("decodeEntitlementsBlob: %v", err)
	}
	groups := ents[appGroupsEntitlement]
	if groups.kind != entArray || len(groups.array) != 2 {
		t.Fatalf("application-groups = kind %d with %d members, want an array of 2", groups.kind, len(groups.array))
	}
	want := []string{"SXKCTF23Q2.com.example.group", "SXKCTF23Q2.two"}
	for index, member := range groups.array {
		if member.kind != entString || member.str != want[index] {
			t.Errorf("application-groups[%d] = kind %d %q, want %q", index, member.kind, member.str, want[index])
		}
	}
	if jit := ents[entAllowJIT]; jit.kind != entBool || !jit.boolean {
		t.Errorf("allow-jit = kind %d %t, want a true boolean", jit.kind, jit.boolean)
	}
}

func TestDecodeEntitlementsBlobDeniesFraming(t *testing.T) {
	valid := func(t *testing.T) []byte { return entitlementsBlob(t, entAllowJIT, entBoolean(true)) }
	tests := []struct {
		name   string
		blob   func(*testing.T) []byte
		reason string
	}{
		{"blob shorter than its header", func(*testing.T) []byte { return make([]byte, 7) }, "want at least 8"},
		{"XML entitlements magic", func(t *testing.T) []byte {
			blob := valid(t)
			binary.BigEndian.PutUint32(blob[0:4], 0xfade7171)
			return blob
		}, "magic 0xfade7171"},
		{"one bit flipped in the magic", func(t *testing.T) []byte {
			blob := valid(t)
			blob[3] ^= 1
			return blob
		}, "magic"},
		{"zero magic in a non-zero buffer", func(t *testing.T) []byte {
			blob := valid(t)
			binary.BigEndian.PutUint32(blob[0:4], 0)
			return blob
		}, "magic 0x00000000"},
		{"length header below the minimum", func(t *testing.T) []byte {
			blob := valid(t)
			binary.BigEndian.PutUint32(blob[4:8], 7)
			return blob
		}, "length header 7"},
		{"length header overruns the buffer", func(t *testing.T) []byte {
			blob := valid(t)
			binary.BigEndian.PutUint32(blob[4:8], uint32(len(blob)+1))
			return blob
		}, "length header"},
		{"length header above the cap", func(t *testing.T) []byte {
			blob := valid(t)
			binary.BigEndian.PutUint32(blob[4:8], maxEntitlementsBlob+1)
			return blob
		}, "length header"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeEntitlementsBlob(tt.blob(t))
			assertDenies(t, err, ErrNoVerifier)
			if !strings.Contains(err.Error(), tt.reason) {
				t.Errorf("decodeEntitlementsBlob = %q, want the reason to name %q", err, tt.reason)
			}
		})
	}
}

// A Developer ID binary signed with no entitlements answers op 16 with rc=0
// and an all-zero header (measured), which is not an error and not a pass:
// the six rejections are vacuous and every required entitlement hard-fails.
func TestDecodeEntitlementsBlobTreatsAnAllZeroHeaderAsAbsent(t *testing.T) {
	ents, err := decodeEntitlementsBlob(make([]byte, 4096))
	if err != nil {
		t.Fatalf("decodeEntitlementsBlob(all zero) = %v, want nil", err)
	}
	if ents != nil {
		t.Fatalf("decodeEntitlementsBlob(all zero) = %v, want a nil dictionary", ents)
	}
	if err := rejectInjection(ents, Requirement{}); err != nil {
		t.Errorf("rejectInjection(absent) = %v, want nil", err)
	}
	req := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier, RequiredAppGroup: testGroup}
	assertDenies(t, requireEntitlements(ents, req), ErrUntrustedPeer)
}

func TestDecodeEntitlementsNeverSlicesPastATruncatedBlob(t *testing.T) {
	full := measuredBlob(t, "entitlements-measured.bin")
	for length := 0; length < len(full); length++ {
		prefix := full[:length:length]
		ents, err := decodeEntitlementsBlob(prefix)
		if err == nil && ents != nil {
			t.Fatalf("decodeEntitlementsBlob(%d-byte prefix) = %v, want an error", length, ents)
		}
		if err != nil && !errors.Is(err, ErrNoVerifier) {
			t.Fatalf("decodeEntitlementsBlob(%d-byte prefix) = %v, want ErrNoVerifier", length, err)
		}
	}
}

func TestDecodeEntitlementsDeniesGrammarViolations(t *testing.T) {
	tests := []struct {
		name   string
		der    func(*testing.T) []byte
		reason string
	}{
		{"trailing bytes after a valid structure", func(t *testing.T) []byte {
			return append(entitlementsDER(t, entAllowJIT, entBoolean(true)), 0x00)
		}, "trailing bytes"},
		{"version other than 1", func(t *testing.T) []byte {
			return entitlementsDERVersion(t, 2, entAllowJIT, entBoolean(true))
		}, "version 2 is not 1"},
		{"root is not [APPLICATION 16]", func(t *testing.T) []byte {
			return marshalRaw(t, asn1.RawValue{
				Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true,
				Bytes: entElements(t, entAllowJIT, entBoolean(true)),
			})
		}, "want [APPLICATION 16]"},
		{"dictionary is not [CONTEXT 16]", func(t *testing.T) []byte {
			body := append(marshalRaw(t, entInteger(t, 1)), marshalRaw(t, asn1.RawValue{
				Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true,
				Bytes: entElements(t, entAllowJIT, entBoolean(true)),
			})...)
			return marshalRaw(t, asn1.RawValue{Class: asn1.ClassApplication, Tag: 16, IsCompound: true, Bytes: body})
		}, "want [CONTEXT 16]"},
		{"duplicate key", func(t *testing.T) []byte {
			return entitlementsDER(t, entAllowJIT, entBoolean(false), entAllowJIT, entBoolean(true))
		}, "duplicate key"},
		{"key is not a UTF8String", func(t *testing.T) []byte {
			element := marshalRaw(t, asn1.RawValue{
				Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true,
				Bytes: append(marshalRaw(t, entInteger(t, 7)), marshalRaw(t, entBoolean(true))...),
			})
			body := append(marshalRaw(t, entInteger(t, 1)), marshalRaw(t, asn1.RawValue{
				Class: asn1.ClassContextSpecific, Tag: 16, IsCompound: true, Bytes: element,
			})...)
			return marshalRaw(t, asn1.RawValue{Class: asn1.ClassApplication, Tag: 16, IsCompound: true, Bytes: body})
		}, "want UTF8String"},
		{"key is not valid UTF-8", func(t *testing.T) []byte {
			return entitlementsDER(t, string([]byte{0xff, 0xfe}), entBoolean(true))
		}, "not valid UTF-8"},
		{"boolean is neither 0x00 nor 0xff", func(t *testing.T) []byte {
			return entitlementsDER(t, entAllowJIT, asn1.RawValue{
				Class: asn1.ClassUniversal, Tag: asn1.TagBoolean, Bytes: []byte{0x01},
			})
		}, "DER 0x00 or 0xff"},
		{"element is not a SEQUENCE", func(t *testing.T) []byte {
			body := append(marshalRaw(t, entInteger(t, 1)), marshalRaw(t, asn1.RawValue{
				Class: asn1.ClassContextSpecific, Tag: 16, IsCompound: true,
				Bytes: marshalRaw(t, entBoolean(true)),
			})...)
			return marshalRaw(t, asn1.RawValue{Class: asn1.ClassApplication, Tag: 16, IsCompound: true, Bytes: body})
		}, "want SEQUENCE"},
		{"element carries trailing bytes", func(t *testing.T) []byte {
			content := append(marshalRaw(t, entText(entAllowJIT)), marshalRaw(t, entBoolean(true))...)
			content = append(content, marshalRaw(t, entBoolean(false))...)
			body := append(marshalRaw(t, entInteger(t, 1)), marshalRaw(t, asn1.RawValue{
				Class: asn1.ClassContextSpecific, Tag: 16, IsCompound: true,
				Bytes: marshalRaw(t, asn1.RawValue{
					Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true, Bytes: content,
				}),
			})...)
			return marshalRaw(t, asn1.RawValue{Class: asn1.ClassApplication, Tag: 16, IsCompound: true, Bytes: body})
		}, "trailing bytes"},
		{"nested deeper than the depth cap", func(t *testing.T) []byte {
			value := entBoolean(true)
			for depth := 0; depth <= maxEntitlementDepth; depth++ {
				value = asn1.RawValue{
					Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true,
					Bytes: marshalRaw(t, value),
				}
			}
			return entitlementsDER(t, "com.yasyf.deep", value)
		}, "nest deeper than"},
		{"more elements than the count cap", func(t *testing.T) []byte {
			pairs := make([]any, 0, 2*(maxEntitlementCount+1))
			for index := 0; index <= maxEntitlementCount; index++ {
				pairs = append(pairs, fmt.Sprintf("com.yasyf.key-%d", index), entBoolean(false))
			}
			return entitlementsDER(t, pairs...)
		}, "more than"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeEntitlements(tt.der(t))
			assertDenies(t, err, ErrNoVerifier)
			if !strings.Contains(err.Error(), tt.reason) {
				t.Errorf("decodeEntitlements = %q, want the reason to name %q", err, tt.reason)
			}
		})
	}
}

func TestDecodeEntitlementsRejectsADuplicateKeyInsideANestedDictionary(t *testing.T) {
	nested := asn1.RawValue{
		Class: asn1.ClassContextSpecific, Tag: 16, IsCompound: true,
		Bytes: entElements(t, "com.yasyf.inner", entBoolean(true), "com.yasyf.inner", entBoolean(false)),
	}
	_, err := decodeEntitlements(entitlementsDER(t, "com.yasyf.outer", nested))
	assertDenies(t, err, ErrNoVerifier)
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("decodeEntitlements = %q, want a duplicate-key denial", err)
	}
}

func TestMatchEntitlementIsShapeExact(t *testing.T) {
	tests := []struct {
		name        string
		value       entValue
		requirement EntitlementRequirement
		want        bool
	}{
		{"boolean true", entValue{kind: entBool, boolean: true}, EntitlementRequirement{Match: EntitlementBoolean, Boolean: true}, true},
		{"boolean false against true", entValue{kind: entBool}, EntitlementRequirement{Match: EntitlementBoolean, Boolean: true}, false},
		{"string exact", entValue{kind: entString, str: "broker"}, EntitlementRequirement{Match: EntitlementString, String: "broker"}, true},
		{"string mismatch", entValue{kind: entString, str: "other"}, EntitlementRequirement{Match: EntitlementString, String: "broker"}, false},
		{"string against a boolean predicate", entValue{kind: entString, str: "true"}, EntitlementRequirement{Match: EntitlementBoolean, Boolean: true}, false},
		{
			"array contains",
			entValue{kind: entArray, array: []entValue{{kind: entString, str: "a"}, {kind: entString, str: testGroup}}},
			EntitlementRequirement{Match: EntitlementStringArrayContains, String: testGroup},
			true,
		},
		{
			"array lacks the member",
			entValue{kind: entArray, array: []entValue{{kind: entString, str: "a"}}},
			EntitlementRequirement{Match: EntitlementStringArrayContains, String: testGroup},
			false,
		},
		{"opaque satisfies nothing", entValue{kind: entOpaque}, EntitlementRequirement{Match: EntitlementBoolean}, false},
		{"unknown predicate satisfies nothing", entValue{kind: entBool, boolean: true}, EntitlementRequirement{Match: 99}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchEntitlement(tt.value, tt.requirement); got != tt.want {
				t.Errorf("matchEntitlement = %t, want %t", got, tt.want)
			}
		})
	}
}
