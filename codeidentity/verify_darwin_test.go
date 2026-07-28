//go:build darwin && !daemonkit_unsigned

package codeidentity

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

const hardenedPosture = csRuntime | csEnforcement | csHard

func TestCodeStatusRequiresHardenedRuntimeAndProvenLibraryValidation(t *testing.T) {
	for _, flags := range []int64{
		hardenedPosture | csRequireLV,
		hardenedPosture | csForcedLV,
		hardenedPosture | csRequireLV | csForcedLV,
	} {
		if err := checkCodeStatus(flags); err != nil {
			t.Errorf("checkCodeStatus(0x%x) = %v, want nil", flags, err)
		}
	}
}

func TestCodeStatusRejectsUnsafeOrUnprovenPosture(t *testing.T) {
	for _, flags := range []int64{
		csEnforcement | csHard | csRequireLV,
		hardenedPosture | csRequireLV | csGetTaskAllow,
		hardenedPosture | csRequireLV | csDebugged,
		hardenedPosture,
	} {
		err := checkCodeStatus(flags)
		if !errors.Is(err, ErrUntrustedPeer) {
			t.Errorf("checkCodeStatus(0x%x) = %v, want ErrUntrustedPeer", flags, err)
		}
	}
}

func TestCodeStatusRequiresSignatureEnforcement(t *testing.T) {
	const clean int64 = hardenedPosture | csForcedLV
	tests := []struct {
		name  string
		flags int64
		want  error
	}{
		{"clean hardened runtime", clean, nil},
		{"allow-unsigned-executable-memory clears CS_ENFORCEMENT", clean &^ csEnforcement, ErrUntrustedPeer},
		{"disable-executable-page-protection clears CS_ENFORCEMENT and CS_HARD", clean &^ (csEnforcement | csHard), ErrUntrustedPeer},
		{"CS_HARD alone cleared", clean &^ csHard, ErrUntrustedPeer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkCodeStatus(tt.flags)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("checkCodeStatus(0x%x) = %v, want nil", tt.flags, err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("checkCodeStatus(0x%x) = %v, want %v", tt.flags, err, tt.want)
			}
		})
	}
}

func TestGuestLookupError(t *testing.T) {
	tests := []struct {
		name    string
		status  int32
		want    error
		notWant error
	}{
		{"success", errSecSuccess, nil, nil},
		{"departed peer", osStatusPeerGone, ErrPeerGone, ErrNoVerifier},
		{"posix non-esrch", 100002, ErrNoVerifier, ErrPeerGone},
		{"csreq failure", -67062, ErrNoVerifier, ErrPeerGone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := guestLookupError(tt.status)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("guestLookupError(%d) = %v, want nil", tt.status, err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("guestLookupError(%d) = %v, want %v", tt.status, err, tt.want)
			}
			if errors.Is(err, tt.notWant) {
				t.Errorf("guestLookupError(%d) = %v, must not match %v", tt.status, err, tt.notWant)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("OSStatus %d", tt.status)) {
				t.Errorf("guestLookupError(%d) message %q lacks the OSStatus", tt.status, err)
			}
		})
	}
}

func TestValidityAndSigningInfoErrorsClassifyDeadPeer(t *testing.T) {
	sites := []struct {
		name       string
		classify   func(int32) error
		failClosed error
	}{
		{"check validity", validityError, ErrUntrustedPeer},
		{"signing information", signingInfoError, ErrNoVerifier},
	}
	tests := []struct {
		name   string
		status int32
		want   func(failClosed error) error
	}{
		{"success", errSecSuccess, func(error) error { return nil }},
		{"departed peer", osStatusPeerGone, func(error) error { return ErrPeerGone }},
		{"no such guest", osStatusNoSuchCode, func(error) error { return ErrPeerGone }},
		{"posix non-esrch", 100002, func(failClosed error) error { return failClosed }},
		{"csreq failure", -67062, func(failClosed error) error { return failClosed }},
	}
	for _, site := range sites {
		for _, tt := range tests {
			t.Run(site.name+"/"+tt.name, func(t *testing.T) {
				err := site.classify(tt.status)
				want := tt.want(site.failClosed)
				if want == nil {
					if err != nil {
						t.Fatalf("classify(%d) = %v, want nil", tt.status, err)
					}
					return
				}
				if !errors.Is(err, want) {
					t.Errorf("classify(%d) = %v, want %v", tt.status, err, want)
				}
				for _, sentinel := range []error{ErrPeerGone, ErrNoVerifier, ErrUntrustedPeer} {
					if sentinel != want && errors.Is(err, sentinel) {
						t.Errorf("classify(%d) = %v, must not match %v", tt.status, err, sentinel)
					}
				}
				if !strings.Contains(err.Error(), fmt.Sprintf("OSStatus %d", tt.status)) {
					t.Errorf("classify(%d) message %q lacks the OSStatus", tt.status, err)
				}
			})
		}
	}
}
