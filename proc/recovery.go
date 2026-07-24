package proc

import (
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
)

const recoveryReceiptDomain = "daemonkit.proc.recovery-receipt.v1"

// RecoveryID names one stable consumer-owned recovery barrier.
type RecoveryID string

var (
	_ encoding.TextMarshaler   = RecoveryID("")
	_ encoding.TextUnmarshaler = (*RecoveryID)(nil)
)

const (
	// RecoveryTaskID gates daemonkit disposable-task recovery.
	RecoveryTaskID RecoveryID = "daemonkit.task.v1"
	// RecoveryServiceID gates daemonkit launch-service reconciliation.
	RecoveryServiceID RecoveryID = "daemonkit.service.v1"
	// RecoveryTrustID gates daemonkit trust-verifier process settlement.
	RecoveryTrustID RecoveryID = "daemonkit.trust.v1"
	// RecoveryStopControlID authenticates one daemonkit cross-process stop caller.
	RecoveryStopControlID RecoveryID = "daemonkit.stop-control.v1"
)

const maxRecoveryIDBytes = 127

// ParseRecoveryID validates and returns one stable namespaced v1 identifier.
func ParseRecoveryID(value string) (RecoveryID, error) {
	id := RecoveryID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

// String returns the exact namespaced v1 representation.
func (id RecoveryID) String() string { return string(id) }

// MarshalText encodes one validated recovery identifier.
func (id RecoveryID) MarshalText() ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return []byte(id), nil
}

// UnmarshalText replaces id only after strict validation.
func (id *RecoveryID) UnmarshalText(text []byte) error {
	if id == nil {
		return errors.New("proc: recovery id target is nil")
	}
	parsed, err := ParseRecoveryID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// Validate rejects a noncanonical namespaced v1 recovery identifier.
func (id RecoveryID) Validate() error {
	if len(id) == 0 || len(id) > maxRecoveryIDBytes {
		return errors.New("proc: recovery id length is invalid")
	}
	segmentStart := true
	segments := 1
	for _, value := range []byte(id) {
		switch {
		case value == '.':
			if segmentStart {
				return fmt.Errorf("proc: invalid recovery id %q", id)
			}
			segmentStart = true
			segments++
		case segmentStart && value >= 'a' && value <= 'z':
			segmentStart = false
		case !segmentStart && ((value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '-'):
		default:
			return fmt.Errorf("proc: invalid recovery id %q", id)
		}
	}
	if segmentStart || segments < 3 || !strings.HasSuffix(string(id), ".v1") {
		return fmt.Errorf("proc: invalid recovery id %q", id)
	}
	return nil
}

// RecoveryReceipt proves one recovery ID was covered by an exhaustive store
// scan and identifies the exact prior owner generations that were settled.
type RecoveryReceipt struct {
	id      RecoveryID
	current OwnerGeneration
	settled []OwnerGeneration
	digest  [sha256.Size]byte
}

// RecoveryID returns the exact settled recovery barrier.
func (r RecoveryReceipt) RecoveryID() RecoveryID { return r.id }

// Current returns the current process-owner generation.
func (r RecoveryReceipt) Current() OwnerGeneration { return r.current }

// Settled returns a copy of sorted unique prior owner generations.
func (r RecoveryReceipt) Settled() []OwnerGeneration {
	return append([]OwnerGeneration(nil), r.settled...)
}

// Digest returns the canonical immutable receipt digest.
func (r RecoveryReceipt) Digest() [sha256.Size]byte { return r.digest }

// Validate rejects a forged, noncanonical, or self-settling receipt.
func (r RecoveryReceipt) Validate() error {
	if err := r.id.Validate(); err != nil {
		return errors.Join(errors.New("proc: invalid recovery receipt"), err)
	}
	if r.current == (OwnerGeneration{}) {
		return errors.New("proc: invalid recovery receipt: current owner generation is zero")
	}
	for index, generation := range r.settled {
		if generation == (OwnerGeneration{}) || generation == r.current {
			return errors.New("proc: invalid recovery receipt: settled owner generation is invalid")
		}
		if index > 0 && generationCompare(r.settled[index-1], generation) >= 0 {
			return errors.New("proc: invalid recovery receipt: settled owner generations are not canonical")
		}
	}
	if r.digest != recoveryReceiptDigest(r.id, r.current, r.settled) {
		return errors.New("proc: invalid recovery receipt: digest mismatch")
	}
	return nil
}

func newRecoveryReceipt(id RecoveryID, current OwnerGeneration, settled []OwnerGeneration) (RecoveryReceipt, error) {
	if err := id.Validate(); err != nil {
		return RecoveryReceipt{}, err
	}
	if current == (OwnerGeneration{}) {
		return RecoveryReceipt{}, errors.New("proc: current owner generation is zero")
	}
	canonical := append([]OwnerGeneration(nil), settled...)
	slices.SortFunc(canonical, generationCompare)
	canonical = slices.Compact(canonical)
	for _, generation := range canonical {
		if generation == (OwnerGeneration{}) || generation == current {
			return RecoveryReceipt{}, errors.New("proc: settled owner generation is invalid")
		}
	}
	receipt := RecoveryReceipt{id: id, current: current, settled: canonical}
	receipt.digest = recoveryReceiptDigest(id, current, canonical)
	return receipt, nil
}

// CombineRecoveryReceipts creates one canonical proof from independently
// recovered runtime-owned sources for the same ID and current generation.
func CombineRecoveryReceipts(id RecoveryID, current OwnerGeneration, receipts ...RecoveryReceipt) (RecoveryReceipt, error) {
	if len(receipts) == 0 {
		return RecoveryReceipt{}, errors.New("proc: recovery receipt inputs are required")
	}
	settled := make([]OwnerGeneration, 0)
	for _, receipt := range receipts {
		if err := receipt.Validate(); err != nil {
			return RecoveryReceipt{}, err
		}
		if receipt.id != id || receipt.current != current {
			return RecoveryReceipt{}, errors.New("proc: recovery receipt authority mismatch")
		}
		settled = append(settled, receipt.settled...)
	}
	return newRecoveryReceipt(id, current, settled)
}

func recoveryReceiptDigest(id RecoveryID, current OwnerGeneration, settled []OwnerGeneration) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(recoveryReceiptDomain))
	var idLength [8]byte
	binary.BigEndian.PutUint64(idLength[:], uint64(len(id)))
	_, _ = hash.Write(idLength[:])
	_, _ = hash.Write([]byte(id))
	_, _ = hash.Write(current[:])
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(settled)))
	_, _ = hash.Write(count[:])
	for _, generation := range settled {
		_, _ = hash.Write(generation[:])
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func generationCompare(left, right OwnerGeneration) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}
