package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
)

const (
	brokerHandoffOp                  Op = "daemon.broker-handoff.v1"
	brokerHandoffProtocol               = uint16(1)
	brokerHandoffNonceBytes             = 32
	brokerHandoffMaximumPayloadBytes    = 1024
	brokerHandoffMaximumPending         = 4
	brokerHandoffMaximumAttempts        = 256
)

type brokerHandoffEnvelope struct {
	Nonce           []byte                       `json:"nonce"`
	Protocol        uint16                       `json:"protocol"`
	RuntimeIdentity brokerHandoffRuntimeIdentity `json:"runtime_identity"`
}

type brokerHandoffRuntimeIdentity struct {
	ProcessGeneration proc.OwnerGeneration `json:"process_generation"`
	RuntimeBuild      string               `json:"runtime_build"`
}

type brokerHandoffReservation struct {
	session *session
	done    bool
}

func decodeBrokerHandoff(payload []byte) (brokerHandoffEnvelope, error) {
	if len(payload) == 0 || len(payload) > brokerHandoffMaximumPayloadBytes {
		return brokerHandoffEnvelope{}, errors.New("wire: invalid broker handoff payload size")
	}
	var envelope brokerHandoffEnvelope
	if err := decodeStrict(payload, &envelope); err != nil {
		return brokerHandoffEnvelope{}, fmt.Errorf("wire: decode broker handoff: %w", err)
	}
	if envelope.Protocol != brokerHandoffProtocol || len(envelope.Nonce) != brokerHandoffNonceBytes ||
		envelope.RuntimeIdentity.ProcessGeneration == (proc.OwnerGeneration{}) ||
		envelope.RuntimeIdentity.RuntimeBuild == "" {
		return brokerHandoffEnvelope{}, errors.New("wire: invalid broker handoff envelope")
	}
	canonical, err := marshalBrokerHandoff(envelope)
	if err != nil {
		return brokerHandoffEnvelope{}, fmt.Errorf("wire: encode broker handoff: %w", err)
	}
	if !bytes.Equal(canonical, payload) {
		return brokerHandoffEnvelope{}, errors.New("wire: broker handoff payload is not canonical JSON")
	}
	return envelope, nil
}

func marshalBrokerHandoff(envelope brokerHandoffEnvelope) ([]byte, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(envelope); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'}), nil
}

func (s *Server) authorizeBrokerHandoff(role trust.PeerRole, payload []byte) (brokerHandoffEnvelope, error) {
	if !s.trustPolicy.AllowsHandoff(role) {
		return brokerHandoffEnvelope{}, ErrPermissionDenied
	}
	envelope, err := decodeBrokerHandoff(payload)
	if err != nil {
		return brokerHandoffEnvelope{}, err
	}
	if envelope.RuntimeIdentity.RuntimeBuild != s.runtimeBuild ||
		envelope.RuntimeIdentity.ProcessGeneration != s.processGeneration {
		return brokerHandoffEnvelope{}, errors.New("wire: broker handoff runtime identity mismatch")
	}
	return envelope, nil
}

func (s *session) reserveBrokerHandoff(nonce []byte) (*brokerHandoffReservation, error) {
	if len(nonce) != brokerHandoffNonceBytes {
		return nil, errors.New("wire: invalid broker handoff nonce")
	}
	var key [brokerHandoffNonceBytes]byte
	copy(key[:], nonce)
	s.handoffMu.Lock()
	defer s.handoffMu.Unlock()
	if s.handoffNonces == nil {
		s.handoffNonces = make(map[[brokerHandoffNonceBytes]byte]struct{})
	}
	if _, exists := s.handoffNonces[key]; exists {
		return nil, ErrHandoffReplay
	}
	if s.handoffAttempts >= brokerHandoffMaximumAttempts {
		return nil, ErrHandoffSessionExhausted
	}
	if s.handoffPending >= brokerHandoffMaximumPending {
		return nil, ErrHandoffPendingCapacity
	}
	s.handoffNonces[key] = struct{}{}
	s.handoffPending++
	s.handoffAttempts++
	return &brokerHandoffReservation{session: s}, nil
}

func (r *brokerHandoffReservation) finish() {
	if r == nil || r.done {
		return
	}
	r.done = true
	r.session.handoffMu.Lock()
	defer r.session.handoffMu.Unlock()
	if r.session.handoffPending == 0 {
		panic("wire: broker handoff pending count underflow")
	}
	r.session.handoffPending--
}

func brokerHandoffRejection(err error) (ResponseCode, bool) {
	switch {
	case errors.Is(err, ErrHandoffPendingCapacity), errors.Is(err, ErrSessionCapacity):
		return ResponseCodeHandoffPendingCapacity, true
	case errors.Is(err, ErrHandoffReplay):
		return ResponseCodeHandoffReplay, true
	case errors.Is(err, ErrHandoffSessionExhausted):
		return ResponseCodeHandoffSessionExhausted, true
	case errors.Is(err, ErrPermissionDenied):
		return ResponseCodePermissionDenied, true
	default:
		return "", false
	}
}

func (s *Server) executeBrokerHandoff(
	ctx context.Context,
	sess *session,
	state *requestState,
	envelope brokerHandoffEnvelope,
) (json.RawMessage, error) {
	reservation, err := sess.reserveBrokerHandoff(envelope.Nonce)
	if err != nil {
		return nil, err
	}
	defer reservation.finish()
	sidecar := state.takeSidecar()
	if sidecar == nil {
		return nil, fmt.Errorf("%w: broker handoff descriptor missing", errInvalidFrameSidecar)
	}
	conn, err := sidecar.takeUnixConn()
	if err != nil {
		_ = sidecar.close()
		return nil, err
	}
	if err := s.adoptHandoffConnection(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	payload, err := marshalBrokerHandoff(envelope)
	if err != nil {
		return nil, fmt.Errorf("wire: encode broker handoff acknowledgement: %w", err)
	}
	return json.RawMessage(payload), nil
}
