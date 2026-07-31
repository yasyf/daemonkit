package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const sessionGenerationBytes = 16

// Lane names the two session kinds protocol 2 admits: the trust-gated control
// lane and the schema-gated business lane.
type Lane string

const (
	// LaneControl is the repair channel: drain and broker handoff, gated by
	// Trust.Control, never by an application schema.
	LaneControl Lane = "control"
	// LaneBusiness is the product channel: schema set-membership at attach,
	// Concurrency-bounded dispatch through Runtime.Handle.
	LaneBusiness Lane = "business"
)

func (l Lane) valid() bool { return l == LaneControl || l == LaneBusiness }

type helloIdentity struct {
	Protocol uint16 `json:"protocol"`
	Lane     Lane   `json:"lane"`
	Schema   string `json:"schema,omitempty"`
	Nonce    []byte `json:"nonce,omitempty"`
}

type helloAck struct {
	Protocol uint16       `json:"protocol"`
	Schema   string       `json:"schema"`
	Session  []byte       `json:"session,omitempty"`
	Phase    Phase        `json:"phase"`
	Rejected bool         `json:"rejected,omitempty"`
	Code     ResponseCode `json:"code,omitempty"`
	Reason   string       `json:"reason,omitempty"`
}

// Schemas is the accepted business-schema set. Index 0 is what this build
// speaks (presented in the ack and by this build's own clients); the rest are
// prior eras still accepted.
type Schemas []string

// Own returns the schema this build speaks.
func (s Schemas) Own() string { return s[0] }

// Accepts reports whether digest is a member of the accepted set.
func (s Schemas) Accepts(digest string) bool {
	if digest == "" {
		return false
	}
	for _, accepted := range s {
		if accepted == digest {
			return true
		}
	}
	return false
}

// WireIdentity is the server identity established by the mandatory handshake.
//
//nolint:revive // The qualifier distinguishes wire identity from process identity.
type WireIdentity struct {
	Protocol uint16
	Schema   string
	Session  []byte
	Phase    Phase
}

// HandshakeRejectionError is the server's typed HelloAck denial.
type HandshakeRejectionError struct {
	Code   ResponseCode
	Reason string
}

func (e *HandshakeRejectionError) Error() string { return e.Reason }

func (e *HandshakeRejectionError) Unwrap() error { return responseCodeCause(e.Code) }

// ProtocolMismatchError is the handshake's terminal answer to a peer speaking
// another wire protocol. ProtocolVersion is the whole compatibility axis: the
// schema digests either side presents are attach gates, never protocol gates.
type ProtocolMismatchError struct {
	Theirs uint16
	Ours   uint16
}

func (e *ProtocolMismatchError) Error() string {
	return fmt.Sprintf("%s: peer=%d self=%d", ErrProtocolVersion, e.Theirs, e.Ours)
}

func (*ProtocolMismatchError) Unwrap() error { return ErrProtocolVersion }

func readClientHello(codec *Codec) (helloIdentity, error) {
	frame, err := codec.ReadFrame()
	if err != nil {
		return helloIdentity{}, fmt.Errorf("%w: %w", ErrHandshake, err)
	}
	if frame.Kind != FrameHello {
		return helloIdentity{}, fmt.Errorf("%w: invalid hello frame", ErrHandshake)
	}
	var hello helloIdentity
	if err := decodeStrict(frame.Payload, &hello); err != nil {
		return helloIdentity{}, fmt.Errorf("%w: identity: %w", ErrHandshake, err)
	}
	if hello.Protocol != ProtocolVersion {
		return helloIdentity{}, &ProtocolMismatchError{Theirs: hello.Protocol, Ours: ProtocolVersion}
	}
	if !hello.Lane.valid() {
		return helloIdentity{}, fmt.Errorf("%w: lane %q", ErrHandshake, hello.Lane)
	}
	if hello.Lane == LaneControl && (hello.Schema != "" || len(hello.Nonce) != 0) {
		return helloIdentity{}, fmt.Errorf("%w: control hello carries business fields", ErrHandshake)
	}
	if hello.Lane == LaneBusiness && hello.Schema == "" {
		return helloIdentity{}, fmt.Errorf("%w: empty business schema", ErrHandshake)
	}
	return hello, nil
}

// clientHandshake writes hello, peeks for the drain preamble, and reads the
// ack. The hello and a draining server's preamble cross on the socket; the
// unread hello sits in the peer's buffers harmlessly.
func clientHandshake(codec *Codec, hello helloIdentity) (WireIdentity, error) {
	payload, err := json.Marshal(hello)
	if err != nil {
		return WireIdentity{}, err
	}
	if err := codec.WriteFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: payload}); err != nil {
		return WireIdentity{}, fmt.Errorf("%w: write hello: %w", ErrHandshake, err)
	}
	drain, err := codec.PeekPreamble()
	if err != nil {
		return WireIdentity{}, fmt.Errorf("%w: read acknowledge: %w", ErrHandshake, err)
	}
	if drain {
		return WireIdentity{}, ErrDraining
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		return WireIdentity{}, fmt.Errorf("%w: read acknowledge: %w", ErrHandshake, err)
	}
	if frame.Kind != FrameHelloAck {
		return WireIdentity{}, fmt.Errorf("%w: invalid acknowledge", ErrHandshake)
	}
	var ack helloAck
	if err := decodeStrict(frame.Payload, &ack); err != nil {
		return WireIdentity{}, fmt.Errorf("%w: acknowledge: %w", ErrHandshake, err)
	}
	if ack.Protocol != ProtocolVersion {
		return WireIdentity{}, &ProtocolMismatchError{Theirs: ack.Protocol, Ours: ProtocolVersion}
	}
	if ack.Schema == "" {
		return WireIdentity{}, fmt.Errorf("%w: empty server schema", ErrHandshake)
	}
	if ack.Rejected {
		if len(ack.Session) != 0 || ack.Code == "" || ack.Reason == "" {
			return WireIdentity{}, fmt.Errorf("%w: invalid rejection", ErrHandshake)
		}
		switch ack.Code {
		case ResponseCodeSessionCapacity, ResponseCodePeerUntrusted, ResponseCodeBuildMismatch:
		default:
			return WireIdentity{}, fmt.Errorf("%w: invalid rejection code %q", ErrHandshake, ack.Code)
		}
		return WireIdentity{}, &HandshakeRejectionError{Code: ack.Code, Reason: ack.Reason}
	}
	if ack.Code != "" || ack.Reason != "" {
		return WireIdentity{}, fmt.Errorf("%w: success carried rejection", ErrHandshake)
	}
	if len(ack.Session) != sessionGenerationBytes {
		return WireIdentity{}, fmt.Errorf("%w: invalid session generation", ErrHandshake)
	}
	if ack.Phase == "" {
		return WireIdentity{}, fmt.Errorf("%w: empty phase", ErrHandshake)
	}
	return WireIdentity{Protocol: ack.Protocol, Schema: ack.Schema, Session: ack.Session, Phase: ack.Phase}, nil
}

func decodeStrict(payload []byte, dst any) error {
	dec := json.NewDecoder(bytesReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

func bytesReader(payload []byte) *sliceReader { return &sliceReader{payload: payload} }

type sliceReader struct{ payload []byte }

func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.payload) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.payload)
	r.payload = r.payload[n:]
	return n, nil
}
