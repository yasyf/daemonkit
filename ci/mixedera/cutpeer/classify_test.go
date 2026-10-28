//go:build mixedera

package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/wire"
)

func TestClassifyDrainSeparatesTheProtocolMismatchFromEveryOtherFailure(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		failure string
	}{
		{"a daemon of another protocol", daemonkit.ErrProtocolMismatch, failureProtocolMismatch},
		{"a daemon of another protocol under a wrap", fmt.Errorf("attach: %w", &wire.ProtocolMismatchError{Theirs: 1, Ours: wire.ProtocolVersion}), failureProtocolMismatch},
		{"an incumbent already leaving", daemonkit.ErrDraining, failureDraining},
		{"no listener at all", daemonkit.ErrAbsent, failureAbsent},
		{"a drain whose exit went unobserved", daemonkit.ErrUnsettled, failureUnsettled},
		{"the wrong incumbent", daemonkit.ErrWrongIncumbent, failureRefused},
		{"anything else is transport", errors.New("dial unix: connection refused"), failureTransport},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyDrain(tt.err).Failure; got != tt.failure {
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
		{"a handshake rejection is a refusal", &wire.HandshakeRejectionError{Code: wire.ResponseCodeHandshake, Reason: "no"}, "refused", 0},
		{"a full session pool is a refusal", &wire.HandshakeRejectionError{Code: wire.ResponseCodeSessionCapacity, Reason: "full"}, "refused", 0},
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

func TestClassifyRejectionCallsOnlyTheSchemaGateABuildMismatch(t *testing.T) {
	tests := []struct {
		name      string
		rejection error
		failure   string
	}{
		{"the schema gate refusing product dispatch", &wire.RejectionError{Code: wire.ResponseCodeBuildMismatch, Reason: "no"}, failureBuildMismatch},
		{"a runtime still starting", &wire.RejectionError{Code: wire.ResponseCodeRuntimeStarting, Reason: "wait"}, failureRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyRejection("", protocol, tt.rejection)
			if got.Failure != tt.failure {
				t.Errorf("failure = %q, want %q", got.Failure, tt.failure)
			}
			if got.PeerProtocol != protocol {
				t.Errorf("peer protocol = %d, want the admitting daemon's %d", got.PeerProtocol, protocol)
			}
		})
	}
}
