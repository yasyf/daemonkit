//go:build darwin && !daemonkit_unsigned

package codeidentity

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/internal/csposture/csposturetest"
)

func TestCodeStatusMatchesSharedPostureRequiringLibraryValidation(t *testing.T) {
	for _, tt := range csposturetest.Cases() {
		t.Run(tt.Name, func(t *testing.T) {
			err := checkCodeStatus(tt.Status)
			if !tt.RequireLVDenies {
				if err != nil {
					t.Fatalf("checkCodeStatus(0x%x) = %v, want nil", tt.Status, err)
				}
				return
			}
			if !errors.Is(err, ErrUntrustedPeer) {
				t.Fatalf("checkCodeStatus(0x%x) = %v, want ErrUntrustedPeer", tt.Status, err)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("status 0x%x", tt.Status)) {
				t.Errorf("checkCodeStatus(0x%x) message %q lacks the status word", tt.Status, err)
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
