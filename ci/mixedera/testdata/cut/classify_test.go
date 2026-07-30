package main

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyCallsItAMismatchOnlyWhenItCarriesThePeerProtocol(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		failure string
		peer    uint16
	}{
		{"typed", &mismatchError{theirs: 1, ours: protocol}, "protocol-mismatch", 1},
		{
			"typed under a wrap",
			fmt.Errorf("read frame: %w", &mismatchError{theirs: 7, ours: protocol}),
			"protocol-mismatch", 7,
		},
		{"untyped", fmt.Errorf("read frame: %w", errProtocolMismatch), "transport", 0},
		{"malformed", fmt.Errorf("%w: magic %q", errMalformedFrame, "junk"), "malformed", 0},
		{"transport", errors.New("read: connection reset by peer"), "transport", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.err)
			if got.Failure != tt.failure {
				t.Errorf("failure = %q, want %q", got.Failure, tt.failure)
			}
			if got.PeerProtocol != tt.peer {
				t.Errorf("peer protocol = %d, want %d", got.PeerProtocol, tt.peer)
			}
		})
	}
}
