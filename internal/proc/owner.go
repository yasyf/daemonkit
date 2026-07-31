package proc

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/yasyf/daemonkit/internal/state"
)

// Owner is the serving instance's durable identity core plus its build,
// persisted into the record file behind the flock before bind so settlement
// needs no session. It rides schema 1 as an additive payload field: an era
// that cannot read it still extracts the live-record cores.
type Owner struct {
	PID        int    `json:"pid"`
	Start      uint64 `json:"start"`
	Boot       uint64 `json:"boot"`
	Generation uint64 `json:"generation"`
	Build      string `json:"build"`
}

// Identity returns the owner's process-instance pin.
func (o Owner) Identity() Identity {
	return Identity{PID: o.PID, Start: o.Start, Boot: o.Boot}
}

// wellFormed reports whether every field a settlement probe keys on is
// present. A record file is same-UID writable input, and a partial owner
// block settles trivially — a zero boot reads as a foreign boot session, a
// zero start as a reused pid — so an ill-formed owner is no incumbent at all.
func (o Owner) wellFormed() bool {
	return o.PID > 0 && o.Start > 0 && o.Boot > 0 && o.Generation > 0 && o.Build != ""
}

// RecordOwner persists this process as the record file's owner: the caller's
// own kernel-probed {pid, start, boot}, this instance's generation, and build.
// The write is verified by a post-write re-read, like every record write.
func (s *Store) RecordOwner(build string) (Owner, error) {
	pid := os.Getpid()
	owner, err := s.currentOwner(pid, build)
	if err != nil {
		return Owner{}, err
	}
	reply := make(chan error, 1)
	sent := s.send(func(value *records) {
		next := *value
		next.Owner = &owner
		if err := s.file.Store(next); err != nil {
			reply <- fmt.Errorf("proc: persist owner record: %w", err)
			return
		}
		verified, err := s.file.Load()
		if err != nil {
			reply <- fmt.Errorf("proc: observe owner record: %w", err)
			return
		}
		if verified.Value.Owner == nil || *verified.Value.Owner != owner {
			reply <- errors.New("proc: owner record absent from post-write re-read")
			return
		}
		*value = verified.Value
		reply <- nil
	})
	if !sent {
		return Owner{}, errors.New("proc: record store is closed")
	}
	if err := <-reply; err != nil {
		return Owner{}, err
	}
	return owner, nil
}

func (s *Store) currentOwner(pid int, build string) (Owner, error) {
	info, err := s.prober.probe(pid)
	if err != nil {
		return Owner{}, fmt.Errorf("proc: probe own identity: %w", err)
	}
	boot, err := s.prober.boot()
	if err != nil {
		return Owner{}, fmt.Errorf("proc: load own boot identity: %w", err)
	}
	return Owner{PID: pid, Start: info.start, Boot: boot, Generation: s.generation, Build: build}, nil
}

// ReadOwner shared-reads path's owner block with no flock and no mutation.
// ok is false when the file is missing, unreadable, names no owner, or names
// one that is not well-formed — never a proof of anything.
func ReadOwner(path string) (Owner, bool, error) {
	loaded, ok, err := state.New[records](path, recordSchema).Peek()
	if err != nil {
		return Owner{}, false, fmt.Errorf("proc: read owner record: %w", err)
	}
	if !ok || loaded.Value.Owner == nil {
		return Owner{}, false, nil
	}
	owner := *loaded.Value.Owner
	if !owner.wellFormed() {
		slog.Warn("proc: ignoring an ill-formed owner record", "path", path, "owner", owner)
		return Owner{}, false, nil
	}
	return owner, true, nil
}
