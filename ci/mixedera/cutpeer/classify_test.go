//go:build mixedera

package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/wire"
)

func TestClassifyControlSeparatesTheTrustRefusalFromEveryOtherFailure(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		failure string
	}{
		{"a trust refusal", daemonkit.ErrUntrusted, failureUntrusted},
		{"a trust refusal under a wrap", fmt.Errorf("attach: %w", daemonkit.ErrUntrusted), failureUntrusted},
		{"an incumbent already leaving", daemonkit.ErrDraining, failureDraining},
		{"no listener at all", daemonkit.ErrAbsent, failureAbsent},
		{"a drain whose exit went unobserved", daemonkit.ErrUnsettled, failureUnsettled},
		{"the wrong incumbent", daemonkit.ErrWrongIncumbent, failureRefused},
		{"anything else is transport", errors.New("dial unix: connection refused"), failureTransport},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyControl(tt.err).Failure; got != tt.failure {
				t.Errorf("failure = %q, want %q", got, tt.failure)
			}
		})
	}
}

func TestClassifyDialCallsItAMismatchOnlyWhenItCarriesThePeerProtocol(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		failure string
		peer    uint16
	}{
		{
			"typed mismatch carries the peer's protocol",
			&wire.ProtocolMismatchError{Theirs: 1, Ours: wire.ProtocolVersion},
			failureProtocolMismatch, 1,
		},
		{
			"typed under a wrap",
			fmt.Errorf("handshake: %w", &wire.ProtocolMismatchError{Theirs: 7, Ours: wire.ProtocolVersion}),
			failureProtocolMismatch, 7,
		},
		{"a build mismatch is a refusal", wire.ErrBuildMismatch, "refused", 0},
		{"a handshake rejection is a refusal", &wire.HandshakeRejectionError{Code: wire.ResponseCodeBuildMismatch, Reason: "no"}, "refused", 0},
		{"a draining peer", wire.ErrDraining, "draining", 0},
		{"a handshake framing failure is malformed", fmt.Errorf("%w: invalid hello frame", wire.ErrHandshake), "malformed", 0},
		{"anything else is transport", errors.New("dial unix: connection refused"), "transport", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyDial("", tt.err)
			if got.Failure != tt.failure {
				t.Errorf("failure = %q, want %q", got.Failure, tt.failure)
			}
			if got.PeerProtocol != tt.peer {
				t.Errorf("peer protocol = %d, want %d", got.PeerProtocol, tt.peer)
			}
		})
	}
}
