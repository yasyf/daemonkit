package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/internal/trust"
)

var (
	// ErrQueueFull means a bounded session queue cannot accept more work.
	ErrQueueFull = errors.New("wire: queue at capacity")
	// ErrFlowControl means a peer exceeded a fixed stream bound.
	ErrFlowControl = errors.New("wire: peer exceeded fixed stream bound")
	// ErrHandshake means the first frame did not establish a session.
	ErrHandshake = errors.New("wire: handshake failed")
	// ErrServerStarted means Serve was called more than once.
	ErrServerStarted = errors.New("wire: server already started")
	// ErrBuildMismatch means a business hello presented a schema outside the accepted set.
	ErrBuildMismatch = errors.New("wire: client schema is not accepted by this server")
	// ErrDraining means intake is closed and the request was not dispatched.
	ErrDraining = errors.New("wire: server is draining")
	// ErrDuplicateID means a session reused a request identifier.
	ErrDuplicateID = errors.New("wire: duplicate request id")
	// ErrStreamOrder means stream chunks arrived out of sequence.
	ErrStreamOrder = errors.New("wire: stream sequence violation")
	// ErrSessionCapacity means the lane has no session slot.
	ErrSessionCapacity = errors.New("wire: session capacity exhausted")
	// ErrHandoffReplay means a control session reused a handoff nonce.
	ErrHandoffReplay = errors.New("wire: broker handoff nonce replay")
	// ErrUntrustedPeer means the accepted unix peer failed its lane's trust gate.
	ErrUntrustedPeer = errors.New("wire: untrusted peer")
	// ErrPermissionDenied means a session's lane lacks authority for the operation.
	ErrPermissionDenied = errors.New("wire: control permission denied")
	// ErrNotReady means the runtime has not published PhaseReady.
	ErrNotReady = errors.New("wire: runtime is starting")
)

// Phase is the runtime lifecycle state the server publishes to every session.
type Phase string

const (
	// PhaseStarting precedes readiness; business dispatch is typed-rejected.
	PhaseStarting Phase = "runtime_starting"
	// PhaseReady admits business dispatch.
	PhaseReady Phase = "runtime_ready"
	// PhaseDraining means intake is closing; reconnect elsewhere.
	PhaseDraining Phase = "runtime_draining"
	// PhaseFailed is the runtime's terminal failure.
	PhaseFailed Phase = "runtime_failed"
)

// MaxPhaseDetailBytes bounds opaque progress detail on the wire.
const MaxPhaseDetailBytes = 4096

// PhaseSnapshot is one monotonic lifecycle publication.
type PhaseSnapshot struct {
	Sequence uint64          `json:"sequence"`
	Phase    Phase           `json:"phase"`
	Detail   json.RawMessage `json:"detail,omitempty"`
}

// Runtime is everything internal/wire needs from the process it serves.
// Phase 3's root Serve implements it; tests use wiretest.StubRuntime.
type Runtime interface {
	// Handle dispatches one admitted business request: the Op mux lives behind
	// this method, not in wire. ctx carries the earlier of the client's conveyed
	// deadline and session/server cancellation. An unknown op returns an error;
	// wire turns it into a terminal Response.
	Handle(ctx context.Context, req Request) (any, error)

	// Phase returns the current snapshot; WaitPhase blocks until Sequence >
	// after or ctx ends. This is the stream the per-session phase pump and
	// every client WaitReady ride.
	Phase() PhaseSnapshot
	WaitPhase(ctx context.Context, after uint64) (PhaseSnapshot, error)

	// Drain is the trust-gated control verb's landing point. Idempotent. The
	// runtime closes product intake and drives Phase to PhaseDraining; the
	// wire server observes the transition through the phase stream.
	Drain()
}

// RuntimeFailedError reports the runtime's terminal failed phase.
type RuntimeFailedError struct {
	Snapshot PhaseSnapshot
}

func (e *RuntimeFailedError) Error() string {
	return fmt.Sprintf("wire: runtime failed (sequence %d)", e.Snapshot.Sequence)
}

// ResponseCode is a stable machine-readable terminal status.
type ResponseCode string

const (
	// ResponseCodeRuntimeStarting identifies pre-ready non-dispatch.
	ResponseCodeRuntimeStarting ResponseCode = "runtime_starting"
	// ResponseCodeRuntimeDraining identifies closed-intake non-dispatch.
	ResponseCodeRuntimeDraining ResponseCode = "runtime_draining"
	// ResponseCodeBuildMismatch identifies a schema-set attach rejection.
	ResponseCodeBuildMismatch ResponseCode = "build_mismatch"
	// ResponseCodeSessionCapacity identifies transient session saturation.
	ResponseCodeSessionCapacity ResponseCode = "session_capacity"
	// ResponseCodePeerUntrusted identifies terminal peer rejection.
	ResponseCodePeerUntrusted ResponseCode = "peer_untrusted"
	// ResponseCodePermissionDenied rejects an operation outside the session lane's authority.
	ResponseCodePermissionDenied ResponseCode = "permission_denied"
	// ResponseCodeHandoffReplay identifies a nonce already consumed by this session.
	ResponseCodeHandoffReplay ResponseCode = "handoff_replay"
)

// Request is one admitted request on a persistent session.
type Request struct {
	ID      uint64
	Op      Op
	Peer    trust.Peer
	Schema  string
	Payload []byte
	Chunks  <-chan Chunk
	Session *AcceptedSession
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

func (e *RejectionError) Unwrap() error { return responseCodeCause(e.Code) }

func responseCodeCause(code ResponseCode) error {
	switch code {
	case ResponseCodeRuntimeStarting:
		return ErrNotReady
	case ResponseCodeRuntimeDraining:
		return ErrDraining
	case ResponseCodeBuildMismatch:
		return ErrBuildMismatch
	case ResponseCodeSessionCapacity:
		return ErrSessionCapacity
	case ResponseCodePeerUntrusted:
		return ErrUntrustedPeer
	case ResponseCodePermissionDenied:
		return ErrPermissionDenied
	case ResponseCodeHandoffReplay:
		return ErrHandoffReplay
	default:
		return nil
	}
}

// StreamResponse asks the server to emit Chunks in order before its terminal response.
type StreamResponse struct {
	Chunks <-chan []byte
	Value  any
}

// Handler runs one spawned-session request. Its context is cancelled by a
// cancel frame, disconnect, session shutdown, or the client deadline.
type Handler func(ctx context.Context, req Request) (any, error)

// HandlerSpec defines one spawned-session handler.
type HandlerSpec struct {
	Op         Op
	Handler    Handler
	Concurrent bool
}
