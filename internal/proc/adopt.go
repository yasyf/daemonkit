package proc

import (
	"context"
	"errors"
	"fmt"
)

// Adopted is one externally started process recorded under this generation:
// terminable and reclaimable, never waited. This package issues no wait4
// against it, so the caller's own Wait and this record cannot race a lost
// wakeup.
type Adopted struct {
	store   *Store
	id      identity
	session int
}

// Adopt records pid as a durable child of this generation. ctx must carry a
// deadline; it bounds the record's fsync and the process-table probe. Session
// leadership is probed, never declared: a leader is recorded as its own
// session so reclaim covers its descendants.
func (s *Store) Adopt(ctx context.Context, pid int) (*Adopted, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("proc: adopt requires a context deadline")
	}
	if pid <= 1 {
		return nil, fmt.Errorf("proc: refusing to adopt pid %d", pid)
	}
	boot, err := s.prober.boot()
	if err != nil {
		return nil, fmt.Errorf("proc: snapshot boot identity: %w", err)
	}
	info, err := s.prober.probe(pid)
	if err != nil {
		return nil, fmt.Errorf("proc: snapshot pid %d: %w", pid, err)
	}
	session := 0
	if info.session == pid {
		session = pid
	}
	id := identity{pid: pid, start: info.start, boot: boot}
	rec := record{PID: pid, Start: info.start, Boot: boot, Generation: s.generation, Session: session, Comm: info.comm}
	if err := s.add(ctx, rec); err != nil {
		return nil, err
	}
	return &Adopted{store: s, id: id, session: session}, nil
}

// PID returns the adopted process id.
func (a *Adopted) PID() int { return a.id.pid }

// Stop signals the recorded identity and observes it gone, bounded by ctx. The
// proof is observational — the caller's own Wait still reaps the zombie — so
// the record retires on an observation, never on a status this package read.
func (a *Adopted) Stop(ctx context.Context) (Reap, error) {
	outcome, err := a.store.reapIdentity(ctx, a.id, a.session)
	if err != nil {
		return reapUndetermined, err
	}
	if err := a.Release(); err != nil {
		return outcome, err
	}
	return outcome, nil
}

// Release retires the record without touching the process, for a caller whose
// own Wait already proved the exit.
func (a *Adopted) Release() error {
	if fate := <-a.store.retire(a.id); fate != RecordRemoved {
		return fmt.Errorf("proc: adopted record %d was not observed removed", a.id.pid)
	}
	return nil
}
