package proc

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
)

const recordDigestIdentity = "daemonkit.proc.record.v1"

// RecordDigest is the immutable canonical identity of one exact process.
type RecordDigest [sha256.Size]byte

// NewRecordDigest binds one exact PID/start/boot/executable process identity.
func NewRecordDigest(identity Identity) (RecordDigest, error) {
	if identity.PID <= 1 || identity.StartTime == "" || identity.Boot == "" || identity.Executable == "" {
		return RecordDigest{}, errors.New("proc: record digest identity is incomplete")
	}
	payload, err := json.Marshal(struct {
		Identity   string `json:"identity"`
		PID        int    `json:"pid"`
		StartTime  string `json:"start_time"`
		Boot       string `json:"boot"`
		Executable string `json:"executable"`
	}{
		Identity: recordDigestIdentity, PID: identity.PID, StartTime: identity.StartTime,
		Boot: identity.Boot, Executable: identity.Executable,
	})
	if err != nil {
		return RecordDigest{}, err
	}
	return RecordDigest(sha256.Sum256(payload)), nil
}
