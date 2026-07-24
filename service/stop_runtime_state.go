package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	bolt "go.etcd.io/bbolt"
)

type stopRuntimeTargetWire struct {
	RuntimeBuild      string               `json:"runtime_build"`
	ProcessGeneration proc.OwnerGeneration `json:"process_generation"`
}

type stopRuntimeProcessWire struct {
	PID        int             `json:"pid"`
	StartTime  string          `json:"start_time"`
	Boot       string          `json:"boot"`
	Comm       string          `json:"comm"`
	Executable string          `json:"executable"`
	AuditToken proc.AuditToken `json:"audit_token"`
}

type stopRuntimeIntentWire struct {
	Identity             string                 `json:"identity"`
	Schema               int                    `json:"schema"`
	Fingerprint          string                 `json:"fingerprint"`
	OperationID          string                 `json:"operation_id"`
	RequestDigest        string                 `json:"request_digest"`
	ExpectedRuntimeBuild string                 `json:"expected_runtime_build"`
	ControlRole          string                 `json:"control_role"`
	Target               stopRuntimeTargetWire  `json:"target"`
	Process              stopRuntimeProcessWire `json:"process"`
	ProcessRecordDigest  string                 `json:"process_record_digest"`
	RuntimeProtocol      int                    `json:"runtime_protocol"`
}

type stopRuntimeReceiptWire struct {
	Identity             string                `json:"identity"`
	Schema               int                   `json:"schema"`
	Fingerprint          string                `json:"fingerprint"`
	OperationID          string                `json:"operation_id"`
	RequestDigest        string                `json:"request_digest"`
	ExpectedRuntimeBuild string                `json:"expected_runtime_build"`
	ControlRole          string                `json:"control_role"`
	Target               stopRuntimeTargetWire `json:"target"`
	ProcessRecordDigest  string                `json:"process_record_digest"`
	Settlement           string                `json:"settlement"`
	Digest               string                `json:"digest"`
}

func (s *boltControllerStore) LoadStopRuntime(
	ctx context.Context,
	operationID string,
) (*stopRuntimeIntent, *StopReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	var intent *stopRuntimeIntent
	var receipt *StopReceipt
	err := s.db.View(func(tx *bolt.Tx) error {
		intentPayload := tx.Bucket(controllerStopIntentBucket).Get([]byte(operationID))
		receiptPayload := tx.Bucket(controllerStopReceiptBucket).Get([]byte(operationID))
		var err error
		if intentPayload != nil {
			intent, err = decodeStopRuntimeIntent(intentPayload)
			if err != nil {
				return err
			}
			if intent.OperationID != operationID {
				return errors.New("service: stop runtime intent key mismatch")
			}
		}
		if receiptPayload != nil {
			receipt, err = decodeStopRuntimeReceipt(receiptPayload)
			if err != nil {
				return err
			}
			if receipt.operationID != operationID {
				return errors.New("service: stop runtime receipt key mismatch")
			}
			if intent != nil {
				return errors.New("service: stop runtime receipt and intent coexist")
			}
		}
		return nil
	})
	return intent, receipt, err
}

func (s *boltControllerStore) PutStopRuntimeIntent(ctx context.Context, intent stopRuntimeIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := encodeStopRuntimeIntent(intent)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(controllerStopIntentBucket)
		if existing := bucket.Get([]byte(intent.OperationID)); existing != nil {
			stored, err := decodeStopRuntimeIntent(existing)
			if err != nil {
				return err
			}
			if !stopRuntimeIntentsEqual(*stored, intent) {
				return ErrStopRuntimeConflict
			}
			return nil
		}
		return bucket.Put([]byte(intent.OperationID), payload)
	})
}

func (s *boltControllerStore) PutStopRuntimeReceipt(
	ctx context.Context,
	intent stopRuntimeIntent,
	receipt StopReceipt,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := encodeStopRuntimeReceipt(receipt)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		receipts := tx.Bucket(controllerStopReceiptBucket)
		if existing := receipts.Get([]byte(intent.OperationID)); existing != nil {
			storedReceipt, err := decodeStopRuntimeReceipt(existing)
			if err != nil {
				return err
			}
			if *storedReceipt != receipt {
				return errors.New("service: stop runtime receipt differs from durable receipt")
			}
			if tx.Bucket(controllerStopIntentBucket).Get([]byte(intent.OperationID)) != nil {
				return errors.New("service: stop runtime receipt and intent coexist")
			}
			return nil
		}
		intentPayload := tx.Bucket(controllerStopIntentBucket).Get([]byte(intent.OperationID))
		if intentPayload == nil {
			return errors.New("service: stop runtime intent is absent")
		}
		storedIntent, err := decodeStopRuntimeIntent(intentPayload)
		if err != nil {
			return err
		}
		if !stopRuntimeIntentsEqual(*storedIntent, intent) {
			return ErrStopRuntimeConflict
		}
		if err := receipts.Put([]byte(intent.OperationID), payload); err != nil {
			return err
		}
		return tx.Bucket(controllerStopIntentBucket).Delete([]byte(intent.OperationID))
	})
}

func encodeStopRuntimeIntent(intent stopRuntimeIntent) ([]byte, error) {
	if err := validateStopRuntimeIntent(intent); err != nil {
		return nil, err
	}
	w := stopRuntimeIntentWire{
		Identity: stopRuntimeIntentIdentity, Schema: 1, Fingerprint: stopRuntimeIntentFingerprint,
		OperationID: intent.OperationID, RequestDigest: hex.EncodeToString(intent.RequestDigest[:]),
		ExpectedRuntimeBuild: intent.ExpectedRuntimeBuild, ControlRole: string(intent.ControlRole),
		Target: stopRuntimeTargetWire{
			RuntimeBuild: intent.Target.RuntimeBuild, ProcessGeneration: intent.Target.ProcessGeneration,
		},
		Process: stopRuntimeProcessWire{
			PID: intent.Process.PID, StartTime: intent.Process.StartTime, Boot: intent.Process.Boot,
			Comm: intent.Process.Comm, Executable: intent.Process.Executable, AuditToken: intent.Process.AuditToken,
		},
		ProcessRecordDigest: hex.EncodeToString(intent.ProcessRecordDigest[:]),
		RuntimeProtocol:     intent.RuntimeProtocol,
	}
	return json.Marshal(w)
}

func decodeStopRuntimeIntent(payload []byte) (*stopRuntimeIntent, error) {
	if err := exactStopRuntimeObject(payload, []string{
		"identity", "schema", "fingerprint", "operation_id", "request_digest",
		"expected_runtime_build", "control_role", "target", "process",
		"process_record_digest", "runtime_protocol",
	}, map[string][]string{
		"target":  {"runtime_build", "process_generation"},
		"process": {"pid", "start_time", "boot", "comm", "executable", "audit_token"},
	}); err != nil {
		return nil, fmt.Errorf("service: stop runtime intent field set: %w", err)
	}
	var w stopRuntimeIntentWire
	if err := decodeStopRuntimeJSON(payload, &w); err != nil {
		return nil, err
	}
	if w.Identity != stopRuntimeIntentIdentity || w.Schema != 1 || w.Fingerprint != stopRuntimeIntentFingerprint {
		return nil, errors.New("service: stop runtime intent identity or schema mismatch")
	}
	requestDigest, err := decodeExactDigest(w.RequestDigest)
	if err != nil {
		return nil, err
	}
	processDigest, err := decodeExactDigest(w.ProcessRecordDigest)
	if err != nil {
		return nil, err
	}
	intent := &stopRuntimeIntent{
		OperationID: w.OperationID, RequestDigest: requestDigest,
		ExpectedRuntimeBuild: w.ExpectedRuntimeBuild, ControlRole: trust.PeerRole(w.ControlRole),
		Target: wire.RuntimeIdentity{
			RuntimeBuild: w.Target.RuntimeBuild, ProcessGeneration: w.Target.ProcessGeneration,
		},
		Process: proc.Identity{
			PID: w.Process.PID, StartTime: w.Process.StartTime, Boot: w.Process.Boot,
			Comm: w.Process.Comm, Executable: w.Process.Executable, AuditToken: w.Process.AuditToken,
		},
		ProcessRecordDigest: proc.RecordDigest(processDigest), RuntimeProtocol: w.RuntimeProtocol,
	}
	if err := validateStopRuntimeIntent(*intent); err != nil {
		return nil, err
	}
	return intent, nil
}

func validateStopRuntimeIntent(intent stopRuntimeIntent) error {
	if intent.OperationID == "" || intent.ExpectedRuntimeBuild == "" || intent.ControlRole == "" ||
		intent.Target.RuntimeBuild != intent.ExpectedRuntimeBuild || intent.Target.ProcessGeneration == (proc.OwnerGeneration{}) ||
		intent.RuntimeProtocol <= 0 || intent.RequestDigest == ([sha256.Size]byte{}) {
		return errors.New("service: stop runtime intent is incomplete")
	}
	digest, err := proc.NewRecordDigest(intent.Process)
	if err != nil {
		return err
	}
	if digest != intent.ProcessRecordDigest {
		return errors.New("service: stop runtime process digest mismatch")
	}
	return nil
}

func encodeStopRuntimeReceipt(receipt StopReceipt) ([]byte, error) {
	if err := validateStopRuntimeReceipt(receipt); err != nil {
		return nil, err
	}
	w := stopRuntimeReceiptWire{
		Identity: stopRuntimeReceiptIdentity, Schema: 1, Fingerprint: stopRuntimeReceiptFingerprint,
		OperationID: receipt.operationID, RequestDigest: hex.EncodeToString(receipt.requestDigest[:]),
		ExpectedRuntimeBuild: receipt.expectedRuntimeBuild, ControlRole: string(receipt.controlRole),
		Target: stopRuntimeTargetWire{
			RuntimeBuild: receipt.target.RuntimeBuild, ProcessGeneration: receipt.target.ProcessGeneration,
		},
		ProcessRecordDigest: hex.EncodeToString(receipt.processRecordDigest[:]),
		Settlement:          "gone", Digest: hex.EncodeToString(receipt.digest[:]),
	}
	return json.Marshal(w)
}

func decodeStopRuntimeReceipt(payload []byte) (*StopReceipt, error) {
	if err := exactStopRuntimeObject(payload, []string{
		"identity", "schema", "fingerprint", "operation_id", "request_digest",
		"expected_runtime_build", "control_role", "target",
		"process_record_digest", "settlement", "digest",
	}, map[string][]string{"target": {"runtime_build", "process_generation"}}); err != nil {
		return nil, fmt.Errorf("service: stop runtime receipt field set: %w", err)
	}
	var w stopRuntimeReceiptWire
	if err := decodeStopRuntimeJSON(payload, &w); err != nil {
		return nil, err
	}
	if w.Identity != stopRuntimeReceiptIdentity || w.Schema != 1 ||
		w.Fingerprint != stopRuntimeReceiptFingerprint || w.Settlement != "gone" {
		return nil, errors.New("service: stop runtime receipt identity or schema mismatch")
	}
	processDigest, err := decodeExactDigest(w.ProcessRecordDigest)
	if err != nil {
		return nil, err
	}
	receiptDigest, err := decodeExactDigest(w.Digest)
	if err != nil {
		return nil, err
	}
	requestDigest, err := decodeExactDigest(w.RequestDigest)
	if err != nil {
		return nil, err
	}
	receipt := &StopReceipt{
		operationID: w.OperationID, requestDigest: requestDigest,
		expectedRuntimeBuild: w.ExpectedRuntimeBuild, controlRole: trust.PeerRole(w.ControlRole),
		target: wire.RuntimeIdentity{
			RuntimeBuild: w.Target.RuntimeBuild, ProcessGeneration: w.Target.ProcessGeneration,
		},
		processRecordDigest: proc.RecordDigest(processDigest), settlement: StopSettlementGone,
		digest: StopReceiptDigest(receiptDigest),
	}
	if err := validateStopRuntimeReceipt(*receipt); err != nil {
		return nil, err
	}
	return receipt, nil
}

func validateStopRuntimeReceipt(receipt StopReceipt) error {
	if receipt.operationID == "" || receipt.requestDigest == ([sha256.Size]byte{}) ||
		receipt.expectedRuntimeBuild == "" || receipt.controlRole == "" ||
		receipt.target.RuntimeBuild != receipt.expectedRuntimeBuild || receipt.target.ProcessGeneration == (proc.OwnerGeneration{}) ||
		receipt.processRecordDigest == (proc.RecordDigest{}) || receipt.settlement != StopSettlementGone {
		return errors.New("service: stop runtime receipt is incomplete")
	}
	digest, err := digestStopReceipt(
		receipt.operationID, receipt.requestDigest, receipt.expectedRuntimeBuild, receipt.controlRole,
		receipt.target, receipt.processRecordDigest, receipt.settlement,
	)
	if err != nil {
		return err
	}
	if digest != receipt.digest {
		return errors.New("service: stop runtime receipt digest mismatch")
	}
	return nil
}

func decodeStopRuntimeJSON(payload []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("service: decode stop runtime state: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("service: trailing stop runtime state")
	}
	return nil
}

func exactStopRuntimeObject(payload []byte, fields []string, nested map[string][]string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		return err
	}
	if len(object) != len(fields) {
		return errors.New("field count mismatch")
	}
	for _, field := range fields {
		raw, ok := object[field]
		if !ok {
			return fmt.Errorf("field %q is missing", field)
		}
		if nestedFields := nested[field]; nestedFields != nil {
			if err := exactStopRuntimeObject(raw, nestedFields, nil); err != nil {
				return fmt.Errorf("field %q: %w", field, err)
			}
		}
	}
	return nil
}
