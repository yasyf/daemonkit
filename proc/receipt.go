package proc

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
)

// ReapReceiptPageLimit bounds one recovery and acknowledgement exchange.
const ReapReceiptPageLimit = 128

var (
	// ErrInvalidReapReceipt means a receipt is incomplete or not canonical.
	ErrInvalidReapReceipt = errors.New("proc: invalid reap receipt")
	// ErrUnrecognizedReapReceipt means no durable unacknowledged receipt
	// exactly matches the supplied proof.
	ErrUnrecognizedReapReceipt = errors.New("proc: unrecognized reap receipt")
	// ErrReapReceiptOrder means an acknowledgement skipped a receipt sequence.
	ErrReapReceiptOrder = errors.New("proc: reap receipt acknowledgement is out of order")
	// ErrReapReceiptStale means a receipt predates the retained acknowledged floor.
	ErrReapReceiptStale = errors.New("proc: stale reap receipt")
)

// ReceiptLedgerID identifies one durable receipt ledger across process restarts.
type ReceiptLedgerID [16]byte

// ReapReceiptCursor resumes a class page after one exact sequence.
type ReapReceiptCursor struct {
	LedgerID ReceiptLedgerID
	Sequence uint64
}

// ReapReceiptFloor is the highest contiguously acknowledged class sequence.
type ReapReceiptFloor struct {
	LedgerID      ReceiptLedgerID
	RecoveryClass RecoveryClass
	Sequence      uint64
}

// ReapOutcome records how the exact prior process identity became retired.
type ReapOutcome uint8

const (
	// ReapCrossBoot proves the recorded boot is no longer current.
	ReapCrossBoot ReapOutcome = iota + 1
	// ReapAbsent proves the exact recorded process was already absent.
	ReapAbsent
	// ReapIdentityReused proves the PID now names a different process instance.
	ReapIdentityReused
	// ReapTerminated proves the identity-gated TERM/KILL ladder settled.
	ReapTerminated
)

// ReapReceipt is the durable exact proof for one retired process generation.
// Digest covers the complete Record and Outcome; no wall-clock field can make
// replay produce different bytes.
type ReapReceipt struct {
	LedgerID         ReceiptLedgerID `json:"ledger_id"`
	Sequence         uint64          `json:"sequence"`
	Record           Record          `json:"record"`
	ReaperGeneration string          `json:"reaper_generation"`
	Outcome          ReapOutcome     `json:"outcome"`
	Digest           [32]byte        `json:"digest"`
}

// Validate requires the exact canonical digest of a valid process record and
// typed retirement outcome.
func (r ReapReceipt) Validate() error {
	if r.LedgerID == (ReceiptLedgerID{}) || r.Sequence == 0 {
		return fmt.Errorf("%w: ledger identity and sequence are required", ErrInvalidReapReceipt)
	}
	if err := r.Record.Validate(); err != nil {
		return errors.Join(ErrInvalidReapReceipt, err)
	}
	if r.ReaperGeneration == "" || r.ReaperGeneration == r.Record.Generation {
		return fmt.Errorf("%w: successor generation is invalid", ErrInvalidReapReceipt)
	}
	switch r.Outcome {
	case ReapCrossBoot, ReapAbsent, ReapIdentityReused, ReapTerminated:
	default:
		return fmt.Errorf("%w: unknown outcome %d", ErrInvalidReapReceipt, r.Outcome)
	}
	digest, err := reapReceiptDigest(
		r.LedgerID, r.Sequence, r.Record, r.ReaperGeneration, r.Outcome,
	)
	if err != nil {
		return err
	}
	if r.Digest != digest {
		return fmt.Errorf("%w: digest mismatch", ErrInvalidReapReceipt)
	}
	return nil
}

// ReapReceiptPage is one stable class-filtered page of durable receipts.
type ReapReceiptPage struct {
	Receipts []ReapReceipt
	Next     ReapReceiptCursor
	More     bool
	Floor    ReapReceiptFloor
}

func newReapReceipt(
	ledgerID ReceiptLedgerID,
	sequence uint64,
	record Record,
	reaperGeneration string,
	outcome ReapOutcome,
) (ReapReceipt, error) {
	digest, err := reapReceiptDigest(ledgerID, sequence, record, reaperGeneration, outcome)
	if err != nil {
		return ReapReceipt{}, err
	}
	receipt := ReapReceipt{
		LedgerID: ledgerID, Sequence: sequence,
		Record: record, ReaperGeneration: reaperGeneration,
		Outcome: outcome, Digest: digest,
	}
	if err := receipt.Validate(); err != nil {
		return ReapReceipt{}, err
	}
	return receipt, nil
}

func reapReceiptDigest(
	ledgerID ReceiptLedgerID,
	sequence uint64,
	record Record,
	reaperGeneration string,
	outcome ReapOutcome,
) ([32]byte, error) {
	payload, err := json.Marshal(struct {
		LedgerID         ReceiptLedgerID `json:"ledger_id"`
		Sequence         uint64          `json:"sequence"`
		Record           Record          `json:"record"`
		ReaperGeneration string          `json:"reaper_generation"`
		Outcome          ReapOutcome     `json:"outcome"`
	}{
		LedgerID: ledgerID, Sequence: sequence, Record: record,
		ReaperGeneration: reaperGeneration, Outcome: outcome,
	})
	if err != nil {
		return [32]byte{}, fmt.Errorf("proc: encode reap receipt: %w", err)
	}
	return sha256.Sum256(payload), nil
}

type reapClaim struct {
	Record           Record `json:"record"`
	ReaperGeneration string `json:"reaper_generation"`
}

func (c reapClaim) validate() error {
	if err := c.Record.Validate(); err != nil {
		return err
	}
	if c.ReaperGeneration == "" || c.ReaperGeneration == c.Record.Generation {
		return errors.New("proc: invalid reap claim successor generation")
	}
	return nil
}

// VerifyReapReceipt proves receipt is the exact durable unacknowledged result
// previously committed by this reaper store.
func (r *Reaper) VerifyReapReceipt(ctx context.Context, receipt ReapReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	ok, err := r.Store.HasReapReceipt(ctx, receipt)
	if err != nil {
		return fmt.Errorf("proc: inspect reap receipt: %w", err)
	}
	if ok {
		return nil
	}
	return ErrUnrecognizedReapReceipt
}

// AcknowledgeReap forgets one exact durable receipt. Repeating an
// acknowledgement after it is absent is a successful no-op.
func (r *Reaper) AcknowledgeReap(ctx context.Context, receipt ReapReceipt) (ReapReceiptFloor, error) {
	if err := receipt.Validate(); err != nil {
		return ReapReceiptFloor{}, err
	}
	floor, err := r.Store.AcknowledgeReap(ctx, receipt)
	if err != nil {
		return ReapReceiptFloor{}, fmt.Errorf("proc: acknowledge reap receipt: %w", err)
	}
	return floor, nil
}
