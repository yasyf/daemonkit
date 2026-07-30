package proc

// TODO(v<cut+1>): delete this sweep and the go.etcd.io/bbolt dependency with it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

const legacyOpenShare = 0.1

type legacyRecord struct {
	PID          int    `json:"pid"`
	StartTime    string `json:"start_time"`
	Boot         string `json:"boot"`
	ProcessGroup bool   `json:"process_group"`
	SessionID    int    `json:"session_id"`
}

type legacyIdentity struct {
	id      identity
	session int
}

// sweepLegacy reaps every identity a v1 bbolt store recorded, then archives
// the file aside. Every legacy generation is prior by construction; a failure
// keeps the file for the next open and surfaces in the returned error.
func (s *Store) sweepLegacy(ctx context.Context, path string) (string, []Reclaimed, error) {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return "", nil, nil
	} else if err != nil {
		return "", nil, fmt.Errorf("proc: inspect legacy store %s: %w", path, err)
	}
	identities, err := readLegacyIdentities(ctx, path, clockOrReal(s.clock))
	if err != nil {
		return "", nil, err
	}
	var reclaimed []Reclaimed
	var errs []error
	for _, legacy := range identities {
		outcome, err := s.reapIdentity(ctx, legacy.id, legacy.session)
		if err != nil {
			errs = append(errs, fmt.Errorf("reap legacy child %d: %w", legacy.id.pid, err))
			continue
		}
		reclaimed = append(reclaimed, Reclaimed{PID: legacy.id.pid, Exit: Exit{Code: -1, Reap: outcome, Record: RecordAbandoned}})
	}
	if err := errors.Join(errs...); err != nil {
		return "", reclaimed, fmt.Errorf("proc: legacy store %s kept: %w", path, err)
	}
	archived, err := archiveAside(path)
	if err != nil {
		return "", reclaimed, err
	}
	for i := range reclaimed {
		reclaimed[i].Exit.Record = RecordRemoved
	}
	return archived, reclaimed, nil
}

func readLegacyIdentities(ctx context.Context, path string, clk clock) ([]legacyIdentity, error) {
	deadline, _ := ctx.Deadline()
	// bbolt reads Timeout 0 as "wait for the file lock forever", so a spent
	// deadline must refuse before Open rather than pass the zero through.
	timeout := fractionOf(deadline.Sub(clk.Now()), legacyOpenShare)
	if timeout <= 0 {
		return nil, fmt.Errorf("proc: open legacy store %s: %w", path, context.DeadlineExceeded)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{
		ReadOnly: true,
		Timeout:  timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("proc: open legacy store %s: %w", path, err)
	}
	defer db.Close()
	var identities []legacyIdentity
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("records"))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, value []byte) error {
			var rec legacyRecord
			if err := json.Unmarshal(value, &rec); err != nil {
				return fmt.Errorf("decode legacy record: %w", err)
			}
			start, err := parseLegacyStart(rec.StartTime)
			if err != nil {
				return err
			}
			boot, err := parseLegacyBoot(rec.Boot)
			if err != nil {
				return err
			}
			session := 0
			if rec.ProcessGroup {
				session = rec.SessionID
			}
			identities = append(identities, legacyIdentity{
				id:      identity{pid: rec.PID, start: start, boot: boot},
				session: session,
			})
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("proc: read legacy store %s: %w", path, err)
	}
	return identities, nil
}

func archiveAside(path string) (string, error) {
	dir := filepath.Dir(path)
	reserved, err := os.CreateTemp(dir, filepath.Base(path)+".*.bak")
	if err != nil {
		return "", fmt.Errorf("proc: reserve archive beside %s: %w", path, err)
	}
	aside := reserved.Name()
	_ = reserved.Close()
	if err := os.Rename(path, aside); err != nil {
		_ = os.Remove(aside)
		return "", fmt.Errorf("proc: archive %s: %w", path, err)
	}
	dirHandle, err := os.Open(dir)
	if err != nil {
		return "", fmt.Errorf("proc: persist archive of %s: %w", path, err)
	}
	syncErr := dirHandle.Sync()
	closeErr := dirHandle.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return "", fmt.Errorf("proc: persist archive of %s: %w", path, err)
	}
	return aside, nil
}
