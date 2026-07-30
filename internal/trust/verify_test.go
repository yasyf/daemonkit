package trust

import (
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/yasyf/daemonkit/internal/csposture"
	"github.com/yasyf/daemonkit/internal/csposture/csposturetest"
)

func assertDenies(t *testing.T, err, want error) {
	t.Helper()
	if err == nil {
		t.Fatalf("verify = nil, want %v", want)
	}
	if !errors.Is(err, want) {
		t.Fatalf("verify = %v, want %v", err, want)
	}
	for _, sentinel := range []error{ErrUntrustedPeer, ErrNoVerifier, ErrPeerGone} {
		if sentinel != want && errors.Is(err, sentinel) {
			t.Errorf("verify = %v, must not also match %v", err, sentinel)
		}
	}
}

func TestVerifyTokenAdmitsAMatchingDeveloperIDPeer(t *testing.T) {
	kernel := admittingKernel(t)
	if err := kernel.verify(kernel.requirement()); err != nil {
		t.Fatalf("verify(matching Developer ID peer) = %v, want nil", err)
	}
	want := []uint32{opStatus, opValidationCategory, opTeamID, opIdentity, opCDHash, opBlob, opDEREntitlements}
	if len(kernel.reads) != len(want) {
		t.Fatalf("read ops = %v, want %v", kernel.reads, want)
	}
	for index, op := range want {
		if kernel.reads[index] != op {
			t.Fatalf("read ops = %v, want %v", kernel.reads, want)
		}
	}
}

func TestVerifyTokenDeniesSignatureShape(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *fakeKernel)
		want   error
		reason string
	}{
		{
			"unsigned peer",
			func(_ *testing.T, k *fakeKernel) { k.status = 0 },
			ErrUntrustedPeer, "CS_VALID clear",
		},
		{
			"ad-hoc peer: no team identifier",
			func(_ *testing.T, k *fakeKernel) { k.errnos[opTeamID] = syscall.ENOENT },
			ErrUntrustedPeer, "declares no identifier or team",
		},
		{
			"ad-hoc peer: validation category 10",
			func(_ *testing.T, k *fakeKernel) { k.category = 10 },
			ErrUntrustedPeer, "validation category 10",
		},
		{
			"platform peer: validation category 1",
			func(_ *testing.T, k *fakeKernel) { k.category = 1 },
			ErrUntrustedPeer, "validation category 1",
		},
		{
			"local signing: validation category 7",
			func(_ *testing.T, k *fakeKernel) { k.category = 7 },
			ErrUntrustedPeer, "validation category 7",
		},
		{
			"validation category 0",
			func(_ *testing.T, k *fakeKernel) { k.category = 0 },
			ErrUntrustedPeer, "validation category 0",
		},
		{
			"wrong team",
			func(_ *testing.T, k *fakeKernel) { k.teamID = "ZZ0FAKE9TX" },
			ErrUntrustedPeer, "team identifier does not match",
		},
		{
			"wrong signing identifier",
			func(_ *testing.T, k *fakeKernel) { k.identifier = "com.attacker.other" },
			ErrUntrustedPeer, "signing identifier does not match",
		},
		{
			"kernel team disagrees with the CodeDirectory team",
			func(t *testing.T, k *fakeKernel) {
				cd := devIDCodeDirectory()
				cd.teamID = "ZZ0FAKE9TX"
				k.replaceCodeDirectory(t, cd)
			},
			ErrUntrustedPeer, "CodeDirectory team identifier does not match",
		},
		{
			"kernel identifier disagrees with the CodeDirectory identifier",
			func(t *testing.T, k *fakeKernel) {
				cd := devIDCodeDirectory()
				cd.identifier = "com.attacker.other"
				k.replaceCodeDirectory(t, cd)
			},
			ErrUntrustedPeer, "CodeDirectory signing identifier does not match",
		},
		{
			"CodeDirectory does not hash to the kernel cdhash",
			func(_ *testing.T, k *fakeKernel) { k.cdHash[0] ^= 1 },
			ErrUntrustedPeer, "hashes to the kernel's cdhash",
		},
		{
			"CS_ADHOC set in the CodeDirectory flags",
			func(t *testing.T, k *fakeKernel) {
				cd := devIDCodeDirectory()
				cd.flags = cdFlagAdhoc
				k.replaceCodeDirectory(t, cd)
			},
			ErrUntrustedPeer, "ad-hoc signed",
		},
		{
			"CS_LINKER_SIGNED set in the CodeDirectory flags",
			func(t *testing.T, k *fakeKernel) {
				cd := devIDCodeDirectory()
				cd.flags = cdFlagLinkerSign
				k.replaceCodeDirectory(t, cd)
			},
			ErrUntrustedPeer, "linker signed",
		},
		{
			"CodeDirectory hashed with SHA-1",
			func(t *testing.T, k *fakeKernel) {
				cd := devIDCodeDirectory()
				cd.hashType = 1
				k.replaceCodeDirectory(t, cd)
			},
			ErrUntrustedPeer, "hash type 1 is not SHA-256",
		},
		{
			"CodeDirectory too old to declare a team",
			func(t *testing.T, k *fakeKernel) {
				cd := devIDCodeDirectory()
				cd.version = 0x20100
				k.replaceCodeDirectory(t, cd)
			},
			ErrUntrustedPeer, "declares no team identifier",
		},
		{
			"empty CMS payload (ad-hoc signature)",
			func(_ *testing.T, k *fakeKernel) {
				cd := devIDCodeDirectory().build()
				k.cdHash = cdHashOf(cd)
				k.signature = buildSuperBlob(
					superBlobSlot{0, cd},
					superBlobSlot{0x10000, blobHeader(blobWrapperMagic, nil)},
				)
			},
			ErrUntrustedPeer, "no CMS payload",
		},
		{
			"peer exited between accept and verification",
			func(_ *testing.T, k *fakeKernel) { k.errnos[opStatus] = syscall.ESRCH },
			ErrPeerGone, "csops op 0",
		},
		{
			"peer exited before the signature read",
			func(_ *testing.T, k *fakeKernel) { k.errnos[opBlob] = syscall.ESRCH },
			ErrPeerGone, "csops op 10",
		},
		{
			"kernel refuses the read",
			func(_ *testing.T, k *fakeKernel) { k.errnos[opValidationCategory] = syscall.EPERM },
			ErrNoVerifier, "csops op 17",
		},
		{
			"op renumbering answers EINVAL",
			func(_ *testing.T, k *fakeKernel) { k.errnos[opIdentity] = syscall.EINVAL },
			ErrNoVerifier, "csops op 11",
		},
		{
			"ERANGE outside the two sized reads",
			func(_ *testing.T, k *fakeKernel) { k.errnos[opCDHash] = syscall.ERANGE },
			ErrNoVerifier, "csops op 5",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kernel := admittingKernel(t)
			tt.mutate(t, kernel)
			req := kernel.requirement()
			err := kernel.verify(req)
			assertDenies(t, err, tt.want)
			if !strings.Contains(err.Error(), tt.reason) {
				t.Errorf("verify = %q, want the reason to name %q", err, tt.reason)
			}
		})
	}
}

func TestVerifyTokenDeniesEveryInjectionEntitlement(t *testing.T) {
	for _, entitlement := range injectionEntitlements {
		t.Run(entitlement, func(t *testing.T) {
			kernel := admittingKernel(t)
			kernel.entitle = entitlementsBlob(
				t,
				appGroupsEntitlement, entStrings(t, testGroup),
				entitlement, entBoolean(true),
			)
			err := kernel.verify(kernel.requirement())
			assertDenies(t, err, ErrUntrustedPeer)
			if !strings.Contains(err.Error(), entitlement) {
				t.Errorf("verify = %q, want the reason to name %s", err, entitlement)
			}
		})
	}
}

// The old verifier skipped disable-library-validation whenever the status word
// proved library validation, which every peer that reached the clause did — so
// the clause was dead. It is unconditional now.
func TestVerifyTokenDeniesDisableLibraryValidationEvenWhenTheStatusWordProvesIt(t *testing.T) {
	for _, status := range []int64{
		admittedStatus,
		admittedStatus | csposture.ForcedLV,
		admittedStatus&^csposture.RequireLV | csposture.ForcedLV,
	} {
		kernel := admittingKernel(t)
		kernel.status = status
		kernel.entitle = entitlementsBlob(
			t,
			appGroupsEntitlement, entStrings(t, testGroup),
			entDisableLV, entBoolean(true),
		)
		err := kernel.verify(kernel.requirement())
		assertDenies(t, err, ErrUntrustedPeer)
		if !strings.Contains(err.Error(), entDisableLV) {
			t.Errorf("verify(status 0x%x) = %q, want the disable-library-validation denial", status, err)
		}
	}
}

func TestVerifyTokenDeniesInjectionEntitlementOfEveryShape(t *testing.T) {
	shapes := map[string]asn1.RawValue{
		"true":        entBoolean(true),
		"string":      entText("yes"),
		"integer":     entInteger(t, 1),
		"array":       entStrings(t, "yes"),
		"nested dict": entDictionary(t, "nested", entBoolean(true)),
		"unknown tag": {Class: asn1.ClassUniversal, Tag: asn1.TagBitString, Bytes: []byte{0x00, 0x01}},
	}
	for name, shape := range shapes {
		t.Run(name, func(t *testing.T) {
			kernel := admittingKernel(t)
			kernel.entitle = entitlementsBlob(
				t,
				appGroupsEntitlement, entStrings(t, testGroup),
				entAllowJIT, shape,
			)
			err := kernel.verify(kernel.requirement())
			assertDenies(t, err, ErrUntrustedPeer)
			if !strings.Contains(err.Error(), entAllowJIT) {
				t.Errorf("verify = %q, want the allow-jit denial", err)
			}
		})
	}
}

func TestVerifyTokenAdmitsAnInjectionEntitlementSetFalse(t *testing.T) {
	kernel := admittingKernel(t)
	kernel.entitle = entitlementsBlob(
		t,
		appGroupsEntitlement, entStrings(t, testGroup),
		entAllowJIT, entBoolean(false),
		entGetTaskAllow, entBoolean(false),
	)
	if err := kernel.verify(kernel.requirement()); err != nil {
		t.Fatalf("verify(injection entitlements set false) = %v, want nil", err)
	}
}

func TestAllowJITIsTheOnlyRelaxation(t *testing.T) {
	for _, entitlement := range injectionEntitlements {
		t.Run(entitlement, func(t *testing.T) {
			kernel := admittingKernel(t)
			kernel.entitle = entitlementsBlob(
				t,
				appGroupsEntitlement, entStrings(t, testGroup),
				entitlement, entBoolean(true),
			)
			req := kernel.requirement()
			req.AllowJIT = true
			err := kernel.verify(req)
			if entitlement == entAllowJIT {
				if err != nil {
					t.Fatalf("verify(AllowJIT peer with allow-jit) = %v, want nil", err)
				}
				return
			}
			assertDenies(t, err, ErrUntrustedPeer)
		})
	}
}

func TestVerifyTokenDeniesRequiredEntitlementFailures(t *testing.T) {
	tests := []struct {
		name    string
		entitle func(*testing.T) []byte
		require map[string]EntitlementRequirement
	}{
		{
			"app group absent",
			func(t *testing.T) []byte { return entitlementsBlob(t) },
			nil,
		},
		{
			"app group array lacks the member",
			func(t *testing.T) []byte {
				return entitlementsBlob(t, appGroupsEntitlement, entStrings(t, "group.com.attacker"))
			},
			nil,
		},
		{
			"app group is a bare string, not an array",
			func(t *testing.T) []byte {
				return entitlementsBlob(t, appGroupsEntitlement, entText(testGroup))
			},
			nil,
		},
		{
			"required boolean has the wrong value",
			func(t *testing.T) []byte {
				return entitlementsBlob(t,
					appGroupsEntitlement, entStrings(t, testGroup),
					"com.yasyf.enabled", entBoolean(false))
			},
			map[string]EntitlementRequirement{"com.yasyf.enabled": {Match: EntitlementBoolean, Boolean: true}},
		},
		{
			"required string has the wrong value",
			func(t *testing.T) []byte {
				return entitlementsBlob(t,
					appGroupsEntitlement, entStrings(t, testGroup),
					"com.yasyf.role", entText("intruder"))
			},
			map[string]EntitlementRequirement{"com.yasyf.role": {Match: EntitlementString, String: "broker"}},
		},
		{
			"required key absent entirely",
			func(t *testing.T) []byte {
				return entitlementsBlob(t, appGroupsEntitlement, entStrings(t, testGroup))
			},
			map[string]EntitlementRequirement{"com.yasyf.role": {Match: EntitlementString, String: "broker"}},
		},
		{
			"peer carries no entitlements at all",
			func(_ *testing.T) []byte { return make([]byte, 8) },
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kernel := admittingKernel(t)
			kernel.entitle = tt.entitle(t)
			req := kernel.requirement()
			req.RequiredEntitlements = tt.require
			assertDenies(t, kernel.verify(req), ErrUntrustedPeer)
		})
	}
}

// An entitlement-free Developer ID binary answers op 16 with an all-zero
// header (measured), which vacuously passes the six and hard-fails only the
// required-entitlement predicates.
func TestVerifyTokenAdmitsAnEntitlementFreePeerWithNoRequirements(t *testing.T) {
	kernel := admittingKernel(t)
	kernel.entitle = make([]byte, 8)
	req := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
	if err := kernel.verify(req); err != nil {
		t.Fatalf("verify(entitlement-free peer) = %v, want nil", err)
	}
}

func TestVerifyTokenMatchesThePostureCorpus(t *testing.T) {
	for _, tt := range csposturetest.Cases() {
		t.Run(tt.Name, func(t *testing.T) {
			kernel := admittingKernel(t)
			kernel.status = tt.Status
			err := kernel.verify(kernel.requirement())
			if !tt.Denies {
				if err != nil {
					t.Fatalf("verify(status 0x%x) = %v, want nil", tt.Status, err)
				}
				return
			}
			assertDenies(t, err, ErrUntrustedPeer)
			if !strings.Contains(err.Error(), "status 0x") {
				t.Errorf("verify(status 0x%x) = %q, want the status word in the reason", tt.Status, err)
			}
		})
	}
}

func TestVerifyTokenReReadsAnOversizedBlob(t *testing.T) {
	kernel := admittingKernel(t)
	kernel.entitle = entitlementsBlob(
		t,
		appGroupsEntitlement, entStrings(t, testGroup),
		"com.yasyf.padding", entText(strings.Repeat("p", initialEntitlementsBlob)),
	)
	if len(kernel.entitle) <= initialEntitlementsBlob {
		t.Fatalf("padded entitlements are %d bytes, want more than %d", len(kernel.entitle), initialEntitlementsBlob)
	}
	if err := kernel.verify(kernel.requirement()); err != nil {
		t.Fatalf("verify(oversized entitlements) = %v, want nil", err)
	}
	var derReads int
	for _, op := range kernel.reads {
		if op == opDEREntitlements {
			derReads++
		}
	}
	if derReads != 2 {
		t.Errorf("op 16 read %d times, want 2 (ERANGE then the exact size)", derReads)
	}
}

func TestVerifyTokenNeverLoopsOnRepeatedERANGE(t *testing.T) {
	kernel := admittingKernel(t)
	kernel.errnos[opDEREntitlements] = syscall.ERANGE
	assertDenies(t, kernel.verify(kernel.requirement()), ErrNoVerifier)
	var derReads int
	for _, op := range kernel.reads {
		if op == opDEREntitlements {
			derReads++
		}
	}
	if derReads > 2 {
		t.Errorf("op 16 read %d times, want at most 2", derReads)
	}
}

func TestReadSigningStringFraming(t *testing.T) {
	valid := func() []byte {
		buf := make([]byte, maxStringBlob)
		_ = copyString(buf, testTeam)
		return buf
	}
	tests := []struct {
		name   string
		mutate func([]byte)
		reason string
	}{
		{"reserved word non-zero", func(b []byte) { b[3] = 1 }, "reserved word"},
		{"length below the minimum", func(b []byte) { binary.BigEndian.PutUint32(b[4:8], 9) }, "length header 9"},
		{"length overruns the buffer", func(b []byte) {
			binary.BigEndian.PutUint32(b[4:8], maxStringBlob+1)
		}, "length header 4097"},
		{"missing NUL at the declared length", func(b []byte) { b[8+len(testTeam)] = 'x' }, "not NUL-terminated"},
		{"interior NUL", func(b []byte) {
			binary.BigEndian.PutUint32(b[4:8], uint32(8+len(testTeam)+2))
			b[8+len(testTeam)] = 0
			b[8+len(testTeam)+1] = 0
		}, "interior NUL"},
		{"invalid UTF-8", func(b []byte) { b[8] = 0xff }, "not valid UTF-8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := valid()
			tt.mutate(buf)
			_, err := readSigningString(func(_ uint32, out []byte) syscall.Errno {
				copy(out, buf)
				return 0
			}, opTeamID)
			assertDenies(t, err, ErrNoVerifier)
			if !strings.Contains(err.Error(), tt.reason) {
				t.Errorf("readSigningString = %q, want the reason to name %q", err, tt.reason)
			}
		})
	}
}

func TestFloorIsUnconditionalOnEveryPlatform(t *testing.T) {
	assertDenies(t, Verify(Peer{UID: os.Geteuid() + 1}, nil), ErrUntrustedPeer)
	if err := Verify(Peer{UID: os.Geteuid()}, nil); err != nil {
		t.Errorf("Verify(same uid, no requirement) = %v, want nil", err)
	}
	req := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
	assertDenies(t, Verify(Peer{UID: os.Geteuid() + 1}, &req), ErrUntrustedPeer)
}

func TestVerifyRejectsAnInvalidRequirementAsAConfigurationError(t *testing.T) {
	err := Verify(Peer{UID: os.Geteuid()}, &Requirement{TeamID: testTeam})
	if err == nil {
		t.Fatal("Verify with an invalid Requirement = nil, want an error")
	}
	for _, sentinel := range []error{ErrUntrustedPeer, ErrNoVerifier, ErrPeerGone} {
		if errors.Is(err, sentinel) {
			t.Errorf("invalid-requirement error should be a config error, not %v", sentinel)
		}
	}
}
