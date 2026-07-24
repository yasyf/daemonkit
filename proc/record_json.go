package proc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type recordJSON struct {
	RecoveryID              RecoveryID           `json:"recovery_id"`
	PID                     int                  `json:"pid"`
	StartTime               string               `json:"start_time"`
	Boot                    string               `json:"boot"`
	Comm                    string               `json:"comm"`
	Executable              string               `json:"executable"`
	AuditToken              AuditToken           `json:"audit_token"`
	Generation              OwnerGeneration      `json:"generation"`
	ProcessGroup            bool                 `json:"process_group"`
	SessionID               int                  `json:"session_id"`
	Role                    string               `json:"role"`
	OperationID             string               `json:"operation_id"`
	StopSession             StopSessionID        `json:"stop_session"`
	PreparationNonce        StopPreparationNonce `json:"preparation_nonce"`
	RuntimeProtocol         int                  `json:"runtime_protocol"`
	TargetProcessGeneration *OwnerGeneration     `json:"target_process_generation"`
	StopAuthorityState      StopAuthorityState   `json:"stop_authority_state"`
	ExpiresUnixMilli        int64                `json:"expires_unix_milli"`
}

// MarshalJSON encodes absent stop-control generation authority as null; a
// zero OwnerGeneration is never a scalar wire value.
func (r Record) MarshalJSON() ([]byte, error) {
	var target *OwnerGeneration
	if r.TargetProcessGeneration != (OwnerGeneration{}) {
		value := r.TargetProcessGeneration
		target = &value
	}
	return json.Marshal(recordJSON{
		RecoveryID: r.RecoveryID, PID: r.PID, StartTime: r.StartTime, Boot: r.Boot,
		Comm: r.Comm, Executable: r.Executable, AuditToken: r.AuditToken,
		Generation: r.Generation, ProcessGroup: r.ProcessGroup, SessionID: r.SessionID,
		Role: r.Role, OperationID: r.OperationID, StopSession: r.StopSession,
		PreparationNonce: r.PreparationNonce, RuntimeProtocol: r.RuntimeProtocol,
		TargetProcessGeneration: target, StopAuthorityState: r.StopAuthorityState,
		ExpiresUnixMilli: r.ExpiresUnixMilli,
	})
}

// UnmarshalJSON decodes only the exact v1 process-record shape.
func (r *Record) UnmarshalJSON(data []byte) error {
	if r == nil {
		return errors.New("proc: record target is nil")
	}
	var value recordJSON
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("proc: decode process record: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("proc: decode process record: trailing JSON")
	}
	var target OwnerGeneration
	if value.TargetProcessGeneration != nil {
		target = *value.TargetProcessGeneration
	}
	*r = Record{
		RecoveryID: value.RecoveryID, PID: value.PID, StartTime: value.StartTime,
		Boot: value.Boot, Comm: value.Comm, Executable: value.Executable,
		AuditToken: value.AuditToken, Generation: value.Generation,
		ProcessGroup: value.ProcessGroup, SessionID: value.SessionID,
		Role: value.Role, OperationID: value.OperationID, StopSession: value.StopSession,
		PreparationNonce: value.PreparationNonce, RuntimeProtocol: value.RuntimeProtocol,
		TargetProcessGeneration: target, StopAuthorityState: value.StopAuthorityState,
		ExpiresUnixMilli: value.ExpiresUnixMilli,
	}
	return nil
}
