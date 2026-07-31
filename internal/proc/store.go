package proc

import (
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/daemonkit/internal/state"
)

const recordSchema state.Schema = 1

type record struct {
	PID        int    `json:"pid"`
	Start      uint64 `json:"start"`
	Boot       uint64 `json:"boot"`
	Generation uint64 `json:"generation"`
	Session    int    `json:"session,omitempty"`
	Comm       string `json:"comm,omitempty"`
}

func (r record) id() identity { return identity{pid: r.PID, start: r.Start, boot: r.Boot} }

type records struct {
	Live  []record `json:"live"`
	Owner *Owner   `json:"owner,omitempty"`
}

// Cores names every live record's frozen identity core, so an era that cannot
// decode the payload still extracts what it must reap — session authority
// included, or an archived leader's descendants would be orphaned.
func (r records) Cores() []state.Core {
	cores := make([]state.Core, len(r.Live))
	for i, rec := range r.Live {
		cores[i] = state.Core{PID: rec.PID, Start: rec.Start, Boot: rec.Boot, Generation: rec.Generation, Session: rec.Session}
	}
	return cores
}

// An archived era's extracted cores re-materialize as live records before the
// writer starts, so a crash between open and Recover cannot orphan them.
func rescued(cores []state.Core) records {
	live := make([]record, len(cores))
	for i, core := range cores {
		live[i] = record{PID: core.PID, Start: core.Start, Boot: core.Boot, Generation: core.Generation, Session: core.Session}
	}
	return records{Live: live}
}

func retained(value records, id identity) records {
	live := make([]record, 0, len(value.Live))
	for _, rec := range value.Live {
		if rec.id().matches(id) {
			continue
		}
		live = append(live, rec)
	}
	return records{Live: live, Owner: value.Owner}
}

func holds(value records, id identity) bool {
	for _, rec := range value.Live {
		if rec.id().matches(id) {
			return true
		}
	}
	return false
}

// Store is one daemon's durable record file behind its one writer goroutine.
// The caller holds the singleton flock before opening it (Serve's own step
// precedes recover), so the writer loop is the only serialization needed.
type Store struct {
	file       *state.File[records]
	generation uint64
	archived   string

	ops       chan func(*records)
	closeOnce sync.Once
	closed    chan struct{}
	done      chan struct{}

	prober   prober
	signaler signaler
	clock    clock

	beforeRelease func(pid int)
}

// OpenStore binds the record file, mints this instance's generation, and
// starts the writer. It performs no reaping; the one read it makes seeds the
// writer's value so a later persist can never drop prior-generation records,
// and an archived era's cores are re-persisted as prior-generation records so
// a crash before Recover cannot orphan them.
func OpenStore(path string) (*Store, error) {
	generation, err := mintGeneration()
	if err != nil {
		return nil, err
	}
	file := state.New[records](path, recordSchema)
	loaded, err := file.Load()
	if err != nil {
		return nil, fmt.Errorf("proc: open record store: %w", err)
	}
	value := loaded.Value
	if loaded.Archived != "" {
		value = rescued(loaded.Cores)
		if err := file.Store(value); err != nil {
			return nil, fmt.Errorf("proc: rescue archived cores: %w", err)
		}
	}
	s := &Store{
		file:       file,
		generation: generation,
		archived:   loaded.Archived,
		ops:        make(chan func(*records)),
		closed:     make(chan struct{}),
		done:       make(chan struct{}),
		prober:     sysProber{},
		signaler:   sysSignaler{},
		clock:      realClock{},
	}
	go s.writer(value)
	return s, nil
}

// Generation is this instance's record tag, surfaced as Health.Generation.
func (s *Store) Generation() uint64 { return s.generation }

// Close idempotently waits out the writer loop. Live children are the
// caller's to settle first (Serve's drain order).
func (s *Store) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	<-s.done
	return nil
}

func (s *Store) writer(value records) {
	for {
		select {
		case op := <-s.ops:
			op(&value)
		case <-s.closed:
			close(s.done)
			return
		}
	}
}

func (s *Store) send(op func(*records)) bool {
	select {
	case s.ops <- op:
		return true
	case <-s.closed:
		return false
	}
}

func (s *Store) add(rec record) error {
	reply := make(chan error, 1)
	sent := s.send(func(value *records) {
		next := retained(*value, rec.id())
		next.Live = append(next.Live, rec)
		if err := s.file.Store(next); err != nil {
			reply <- fmt.Errorf("proc: persist record %d: %w", rec.PID, err)
			return
		}
		verified, err := s.file.Load()
		if err != nil {
			reply <- fmt.Errorf("proc: observe record %d: %w", rec.PID, err)
			return
		}
		if !holds(verified.Value, rec.id()) {
			reply <- fmt.Errorf("proc: record %d absent from post-write re-read", rec.PID)
			return
		}
		*value = verified.Value
		reply <- nil
	})
	if !sent {
		return errors.New("proc: record store is closed")
	}
	return <-reply
}

// A store failure is RecordAbandoned, never a control edge: the caller bounds
// its own wait and an unread reply is simply dropped.
func (s *Store) retire(id identity) <-chan RecordFate {
	reply := make(chan RecordFate, 1)
	sent := s.send(func(value *records) {
		next := retained(*value, id)
		if err := s.file.Store(next); err != nil {
			reply <- RecordAbandoned
			return
		}
		verified, err := s.file.Load()
		if err != nil {
			reply <- RecordAbandoned
			return
		}
		*value = verified.Value
		if holds(verified.Value, id) {
			reply <- RecordAbandoned
			return
		}
		reply <- RecordRemoved
	})
	if !sent {
		reply <- RecordAbandoned
	}
	return reply
}

func (s *Store) snapshot() []record {
	reply := make(chan []record, 1)
	sent := s.send(func(value *records) {
		live := make([]record, len(value.Live))
		copy(live, value.Live)
		reply <- live
	})
	if !sent {
		return nil
	}
	return <-reply
}
