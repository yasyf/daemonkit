package supervise

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/yasyf/daemonkit/proc"
)

const (
	trackedOwnerFD         = 6
	trackedOwnerPayloadMax = 16 * 1024
)

var trackedOwnerMagic = [8]byte{'D', 'K', 'O', 'W', 'N', 'R', '0', '1'}

// ReceiveTrackedOwner consumes and verifies the exact durable owner record
// inherited by a Pool task. It must run before closing inherited descriptors.
func ReceiveTrackedOwner(ctx context.Context, expected proc.RecoveryClass) (proc.Record, error) {
	if err := ctx.Err(); err != nil {
		return proc.Record{}, fmt.Errorf("supervise: receive tracked owner: %w", err)
	}
	owner := os.NewFile(uintptr(trackedOwnerFD), "daemonkit-tracked-owner")
	if owner == nil {
		return proc.Record{}, errors.New("supervise: tracked owner descriptor is unavailable")
	}
	defer owner.Close()

	type result struct {
		record proc.Record
		err    error
	}
	read := make(chan result, 1)
	go func() {
		record, err := readTrackedOwner(owner)
		read <- result{record: record, err: err}
	}()
	select {
	case result := <-read:
		if result.err != nil {
			return proc.Record{}, fmt.Errorf("supervise: receive tracked owner: %w", result.err)
		}
		if err := proc.VerifyCurrentOwner(result.record, expected); err != nil {
			return proc.Record{}, fmt.Errorf("supervise: verify tracked owner: %w", err)
		}
		return result.record, nil
	case <-ctx.Done():
		_ = owner.Close()
		<-read
		return proc.Record{}, fmt.Errorf("supervise: receive tracked owner: %w", ctx.Err())
	}
}

func writeTrackedOwner(owner io.WriteCloser, record proc.Record) error {
	payload, err := json.Marshal(record)
	if err != nil {
		_ = owner.Close()
		return err
	}
	if len(payload) > trackedOwnerPayloadMax {
		_ = owner.Close()
		return errors.New("supervise: tracked owner payload exceeds limit")
	}
	header := make([]byte, len(trackedOwnerMagic)+4)
	copy(header, trackedOwnerMagic[:])
	//nolint:gosec // payload is bounded to 16 KiB above.
	binary.BigEndian.PutUint32(header[len(trackedOwnerMagic):], uint32(len(payload)))
	_, writeErr := io.Copy(owner, io.MultiReader(bytes.NewReader(header), bytes.NewReader(payload)))
	closeErr := owner.Close()
	return errors.Join(writeErr, closeErr)
}

func readTrackedOwner(owner io.Reader) (proc.Record, error) {
	header := make([]byte, len(trackedOwnerMagic)+4)
	if _, err := io.ReadFull(owner, header); err != nil {
		return proc.Record{}, err
	}
	if string(header[:len(trackedOwnerMagic)]) != string(trackedOwnerMagic[:]) {
		return proc.Record{}, errors.New("tracked owner protocol mismatch")
	}
	length := binary.BigEndian.Uint32(header[len(trackedOwnerMagic):])
	if length == 0 || length > trackedOwnerPayloadMax {
		return proc.Record{}, errors.New("tracked owner payload length is invalid")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(owner, payload); err != nil {
		return proc.Record{}, err
	}
	var trailing [1]byte
	if count, err := owner.Read(trailing[:]); count != 0 || !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing tracked owner bytes")
		}
		return proc.Record{}, err
	}
	var record proc.Record
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return proc.Record{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return proc.Record{}, errors.New("tracked owner payload contains multiple values")
	}
	return record, nil
}
