package wire

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	brokerHandoffOp                  Op = "daemon.broker-handoff.v1"
	drainControlOp                   Op = "daemon.control.drain"
	brokerHandoffNonceBytes             = 32
	brokerHandoffMaximumPayloadBytes    = 1024
	brokerHandoffMaximumAttempts        = 256
)

type brokerHandoffEnvelope struct {
	Nonce []byte `json:"nonce"`
}

func decodeBrokerHandoff(payload []byte) (brokerHandoffEnvelope, error) {
	if len(payload) == 0 || len(payload) > brokerHandoffMaximumPayloadBytes {
		return brokerHandoffEnvelope{}, errors.New("wire: invalid broker handoff payload size")
	}
	var envelope brokerHandoffEnvelope
	if err := decodeStrict(payload, &envelope); err != nil {
		return brokerHandoffEnvelope{}, fmt.Errorf("wire: decode broker handoff: %w", err)
	}
	if len(envelope.Nonce) != brokerHandoffNonceBytes {
		return brokerHandoffEnvelope{}, errors.New("wire: invalid broker handoff nonce")
	}
	return envelope, nil
}

// reserveHandoffNonce enforces the single-use replay guard. The attempts cap
// bounds the consumed-nonce map for the session's lifetime: past it, every
// handoff is refused as capacity, replayed or not.
func (s *session) reserveHandoffNonce(nonce []byte) error {
	var key [brokerHandoffNonceBytes]byte
	copy(key[:], nonce)
	s.handoffMu.Lock()
	defer s.handoffMu.Unlock()
	if s.handoffNonces == nil {
		s.handoffNonces = make(map[[brokerHandoffNonceBytes]byte]struct{})
	}
	if _, exists := s.handoffNonces[key]; exists {
		return ErrHandoffReplay
	}
	if s.handoffAttempts >= brokerHandoffMaximumAttempts {
		return fmt.Errorf("%w: handoff attempts", ErrSessionCapacity)
	}
	s.handoffNonces[key] = struct{}{}
	s.handoffAttempts++
	return nil
}

func (s *session) executeBrokerHandoff(frame Frame, state *requestState) (any, error) {
	envelope, err := decodeBrokerHandoff(frame.Payload)
	if err != nil {
		return nil, err
	}
	if err := s.reserveHandoffNonce(envelope.Nonce); err != nil {
		return nil, err
	}
	sidecar := state.takeSidecar()
	if sidecar == nil {
		return nil, fmt.Errorf("%w: broker handoff descriptor missing", errInvalidFrameSidecar)
	}
	conn, err := sidecar.takeUnixConn()
	if err != nil {
		_ = sidecar.close()
		return nil, err
	}
	if err := s.server.AdoptHandoff(conn); err != nil {
		return nil, err
	}
	return json.RawMessage("{}"), nil
}
