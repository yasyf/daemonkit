package wire

import (
	"context"
	"encoding/json"
	"errors"
)

const sessionGenerationBytes = 16

var (
	// ErrDraining means intake is closed and the request was not dispatched.
	ErrDraining = errors.New("wire: server is draining")
	// ErrDuplicateID means a session reused a request identifier.
	ErrDuplicateID = errors.New("wire: duplicate request id")
	// ErrStreamOrder means stream chunks arrived out of sequence.
	ErrStreamOrder = errors.New("wire: stream sequence violation")
)

// ResponseCode is a stable machine-readable terminal status.
type ResponseCode string

const (
	// ResponseCodeRuntimeStarting identifies pre-ready non-dispatch.
	ResponseCodeRuntimeStarting ResponseCode = "runtime_starting"
	// ResponseCodeServerDraining identifies closed-intake non-dispatch.
	ResponseCodeServerDraining ResponseCode = "server_draining"
)

// WireIdentity is exchanged during the mandatory exact-version handshake.
//
//nolint:revive // The qualifier distinguishes wire identity from process identity.
type WireIdentity struct {
	Protocol  uint16 `json:"protocol"`
	WireBuild string `json:"wire_build"`
	Session   []byte `json:"session,omitempty"`
}

type handshakeIdentity struct {
	Protocol  uint16 `json:"protocol"`
	WireBuild string `json:"wire_build"`
	Session   []byte `json:"session,omitempty"`
}

// Request is one admitted request on a persistent session.
type Request struct {
	ID        uint64
	Op        Op
	Tenant    string
	Peer      Peer
	WireBuild string
	Payload   []byte
	Chunks    <-chan Chunk
	Session   *AcceptedSession
}

// Chunk is one ordered streaming payload.
type Chunk struct {
	Sequence uint32
	Payload  []byte
	End      bool
}

// Event is a server-pushed session event.
type Event struct {
	Topic   string
	Payload []byte
}

// Response is the terminal response for one request.
type Response struct {
	Rejected bool            `json:"rejected,omitempty"`
	Ack      bool            `json:"ack,omitempty"`
	Code     ResponseCode    `json:"code,omitempty"`
	Reason   string          `json:"reason,omitempty"`
	Err      string          `json:"err,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

// RejectionError is a typed rejected outcome.
type RejectionError struct {
	Code   ResponseCode
	Reason string
}

func (e *RejectionError) Error() string { return e.Reason }

func (e *RejectionError) Unwrap() error {
	switch e.Code {
	case ResponseCodeRuntimeStarting:
		return ErrNotReady
	case ResponseCodeServerDraining:
		return ErrDraining
	default:
		return nil
	}
}

// StreamResponse asks the server to emit Chunks in order before its terminal response.
type StreamResponse struct {
	Chunks <-chan []byte
	Value  any
}

// Handler runs one request. Its context is cancelled by a cancel frame,
// disconnect, server shutdown, or the earlier client/server deadline.
type Handler func(ctx context.Context, req Request) (any, error)

// Admission gates every request before dispatch. A failure proves non-dispatch.
type Admission interface {
	Admit() (done func(), err error)
	Close()
	Draining() bool
	Settle(context.Context) error
}
