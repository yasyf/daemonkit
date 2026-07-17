package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

// RowState is a journal row's drain state.
type RowState string

const (
	// RowPending means the resource still awaits idle-attested yield.
	RowPending RowState = "pending"
	// RowYielded is terminal: the resource was handed off or proven absent.
	RowYielded RowState = "yielded"
)

// seqKey is the reserved state key persisting the seq high-water mark across
// Truncate, so a new epoch never re-issues an already-issued seq.
const seqKey = "~seq"

// Row is one journal row; Seq is the monotonic transition seq CAS updates key on.
type Row struct {
	Key   Key      `json:"key"`
	Seq   uint64   `json:"seq"`
	State RowState `json:"state"`
}

// Journal is a flock-guarded JSON ownership journal, one row per resource key.
// Writes go through daemon.StateFile, so untouched rows survive byte-for-byte.
type Journal struct {
	file daemon.StateFile
}

// NewJournal opens the journal at path; the file need not exist yet.
func NewJournal(path string) Journal {
	return Journal{file: daemon.StateFile{Path: path}}
}

// Path returns the journal file path.
func (j Journal) Path() string { return j.file.Path }

func (j Journal) lockPath() string { return j.file.Path + ".lock" }

// Rows returns every row keyed by resource key; a missing file is empty.
func (j Journal) Rows(ctx context.Context) (map[Key]Row, error) {
	var rows map[Key]Row
	err := withFlock(ctx, j.lockPath(), func() error {
		state, err := readState(j.file.Path)
		if err != nil {
			return err
		}
		rows = make(map[Key]Row, len(state))
		for k, raw := range state {
			if k == seqKey {
				continue
			}
			row, err := decodeRow(raw)
			if err != nil {
				return fmt.Errorf("row %q: %w", k, err)
			}
			rows[Key(k)] = row
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// Apply CAS-applies rows: each lands only when its Seq exceeds the stored row's,
// so a stale replay (Seq <= stored) is a no-op. Returns the applied row count.
func (j Journal) Apply(ctx context.Context, rows ...Row) (int, error) {
	applied := 0
	err := withFlock(ctx, j.lockPath(), func() error {
		return j.file.UpdateUnlocked(func(state map[string]json.RawMessage) error {
			for _, r := range rows {
				stored, err := decodeRow(state[string(r.Key)])
				if err != nil {
					return fmt.Errorf("row %q: %w", r.Key, err)
				}
				if r.Seq <= stored.Seq {
					continue
				}
				b, err := json.Marshal(r)
				if err != nil {
					return err
				}
				state[string(r.Key)] = b
				applied++
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return applied, nil
}

// Bump atomically advances key to state at the next seq and returns the new
// row; the seq never falls below the Truncate-preserved high-water mark.
func (j Journal) Bump(ctx context.Context, key Key, state RowState) (Row, error) {
	var out Row
	err := withFlock(ctx, j.lockPath(), func() error {
		return j.file.UpdateUnlocked(func(st map[string]json.RawMessage) error {
			stored, err := decodeRow(st[string(key)])
			if err != nil {
				return fmt.Errorf("row %q: %w", key, err)
			}
			high, err := decodeSeq(st[seqKey])
			if err != nil {
				return err
			}
			next := stored.Seq + 1
			if next <= high {
				next = high + 1
			}
			out = Row{Key: key, Seq: next, State: state}
			b, err := json.Marshal(out)
			if err != nil {
				return err
			}
			st[string(key)] = b
			return nil
		})
	})
	if err != nil {
		return Row{}, err
	}
	return out, nil
}

// Truncate deletes every row: after the generation snapshot, each row must live
// in exactly one authoritative journal. The seq high-water mark survives, so a
// later Bump never re-issues a seq from before the truncate.
func (j Journal) Truncate(ctx context.Context) error {
	return withFlock(ctx, j.lockPath(), func() error {
		return j.file.UpdateUnlocked(func(state map[string]json.RawMessage) error {
			high, err := decodeSeq(state[seqKey])
			if err != nil {
				return err
			}
			for k, raw := range state {
				if k == seqKey {
					continue
				}
				row, err := decodeRow(raw)
				if err != nil {
					return fmt.Errorf("row %q: %w", k, err)
				}
				if row.Seq > high {
					high = row.Seq
				}
				delete(state, k)
			}
			b, err := json.Marshal(high)
			if err != nil {
				return err
			}
			state[seqKey] = b
			return nil
		})
	})
}

func decodeSeq(raw json.RawMessage) (uint64, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	var high uint64
	if err := json.Unmarshal(raw, &high); err != nil {
		return 0, fmt.Errorf("parse seq high-water: %w", err)
	}
	return high, nil
}

func decodeRow(raw json.RawMessage) (Row, error) {
	if len(raw) == 0 {
		return Row{}, nil
	}
	var r Row
	if err := json.Unmarshal(raw, &r); err != nil {
		return Row{}, err
	}
	return r, nil
}

func readState(path string) (map[string]json.RawMessage, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]json.RawMessage{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read journal %s: %w", path, err)
	}
	if len(data) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parse journal %s: %w", path, err)
	}
	return state, nil
}

func withFlock(ctx context.Context, path string, fn func() error) error {
	lock, err := proc.Flock(ctx, path)
	if err != nil {
		return err
	}
	defer lock.Release()
	return fn()
}

// OwnedBy identifies which journal owns a row.
type OwnedBy int

const (
	// OwnedByNone means neither journal holds the key.
	OwnedByNone OwnedBy = iota
	// OwnedByCanonical means the canonical journal owns the key.
	OwnedByCanonical
	// OwnedByGeneration means the drain generation journal owns the key.
	OwnedByGeneration
)

// ResolveOwner applies the canonical-row-wins-only-when-proven-newer rule: the
// generation owns every key it holds unless the canonical row's Seq is higher.
func ResolveOwner(canonical, generation map[Key]Row, key Key) OwnedBy {
	c, cok := canonical[key]
	g, gok := generation[key]
	switch {
	case !cok && !gok:
		return OwnedByNone
	case cok && !gok:
		return OwnedByCanonical
	case !cok:
		return OwnedByGeneration
	case c.Seq > g.Seq:
		return OwnedByCanonical
	default:
		return OwnedByGeneration
	}
}

type ownerRecord struct {
	PID       int    `json:"pid"`
	StartTime string `json:"start_time"`
	Comm      string `json:"comm"`
}

// Generation is one drain generation's on-disk record under
// <dotdir>/drain/<gen>: its owner identity and its ownership journal.
type Generation struct {
	dir string
}

// NewGeneration addresses generation gen under dotdir's drain layout.
func NewGeneration(dotdir, gen string) Generation {
	return Generation{dir: filepath.Join(dotdir, "drain", gen)}
}

// Dir returns the generation directory.
func (g Generation) Dir() string { return g.dir }

// Name returns the generation's identifier (the directory basename).
func (g Generation) Name() string { return filepath.Base(g.dir) }

// Journal returns the generation's ownership journal.
func (g Generation) Journal() Journal {
	return NewJournal(filepath.Join(g.dir, "journal.json"))
}

func (g Generation) ownerPath() string { return filepath.Join(g.dir, "owner.json") }

// WriteOwner records id as the generation's owning process; a torn write reads
// back unreadable, which scans treat as Undetermined (never adopted).
func (g Generation) WriteOwner(id proc.Identity) error {
	if err := os.MkdirAll(g.dir, 0o700); err != nil {
		return fmt.Errorf("create generation dir: %w", err)
	}
	b, err := json.Marshal(ownerRecord{PID: id.PID, StartTime: id.StartTime, Comm: id.Comm})
	if err != nil {
		return err
	}
	if err := os.WriteFile(g.ownerPath(), b, 0o600); err != nil {
		return fmt.Errorf("write owner %s: %w", g.ownerPath(), err)
	}
	return nil
}

// ReadOwner returns the recorded owner identity; any read, parse, or validity
// failure is an error the caller treats as Undetermined.
func (g Generation) ReadOwner() (proc.Identity, error) {
	data, err := os.ReadFile(g.ownerPath())
	if err != nil {
		return proc.Identity{}, fmt.Errorf("read owner %s: %w", g.ownerPath(), err)
	}
	var rec ownerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return proc.Identity{}, fmt.Errorf("parse owner %s: %w", g.ownerPath(), err)
	}
	if rec.PID <= 0 || rec.StartTime == "" {
		return proc.Identity{}, fmt.Errorf("owner %s: incomplete identity", g.ownerPath())
	}
	return proc.Identity{PID: rec.PID, StartTime: rec.StartTime, Comm: rec.Comm}, nil
}

// Remove deletes the generation directory once its rows are terminal or adopted.
func (g Generation) Remove() error { return os.RemoveAll(g.dir) }

// Generations lists every drain generation under dotdir; a missing layout is
// empty, but an unreadable one errors (an enumeration failure proves nothing).
func Generations(dotdir string) ([]Generation, error) {
	root := filepath.Join(dotdir, "drain")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("enumerate generations %s: %w", root, err)
	}
	gens := make([]Generation, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		gens = append(gens, Generation{dir: filepath.Join(root, e.Name())})
	}
	return gens, nil
}
