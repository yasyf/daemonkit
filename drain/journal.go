package drain

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

// ErrStaleJournal refuses a snapshot into a generation journal that already holds a row.
var ErrStaleJournal = errors.New("stale journal is not empty")

// ErrStaleGeneration means a bound Generation handle outlived its incarnation.
var ErrStaleGeneration = errors.New("stale generation handle")

// ErrSeqExhausted refuses to advance a key into the reserved seq
// math.MaxUint64: a row there could byte-collide with a truncated snapshot
// row, which a retried scoped truncate would then delete.
var ErrSeqExhausted = errors.New("seq space exhausted")

const rootLockName = ".lock"

// RowState is a journal row's drain state.
type RowState string

const (
	// RowPending means the resource still awaits idle-attested yield.
	RowPending RowState = "pending"
	// RowYielded is terminal: the resource was handed off or proven absent.
	RowYielded RowState = "yielded"
)

// Row is one journal row; Seq is the monotonic transition seq CAS updates key on.
type Row struct {
	Key   Key      `json:"key"`
	Seq   uint64   `json:"seq"`
	State RowState `json:"state"`
}

// Journal is a flock-guarded JSON ownership journal, one row per resource key.
type Journal struct {
	file      daemon.ExactStateFile[journalState]
	lock      string
	genDir    string
	inc       string
	ownerPath string
}

// NewJournal opens the canonical journal at path.
func NewJournal(path string) Journal {
	return Journal{file: journalStateFile(path), lock: path + ".lock"}
}

func (j Journal) withLock(ctx context.Context, fn func() error) error {
	lock, err := (proc.FileLockSpec{
		Path:     j.lock,
		Mode:     proc.FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	if j.genDir != "" {
		if _, err := os.Stat(j.genDir); err != nil {
			return fmt.Errorf("generation %s: %w", filepath.Base(j.genDir), err)
		}
		if j.inc != "" {
			inc, err := readOwnerInc(j.ownerPath)
			if err != nil {
				return fmt.Errorf("%w: generation %s owner: %w", ErrStaleGeneration, filepath.Base(j.genDir), err)
			}
			if inc != j.inc {
				return fmt.Errorf("%w: generation %s was re-created", ErrStaleGeneration, filepath.Base(j.genDir))
			}
		}
	}
	return fn()
}

// Path returns the journal file path.
func (j Journal) Path() string { return j.file.Path }

func (j Journal) lockPath() string { return j.lock }

// Rows returns every row keyed by resource key; a missing file is empty.
func (j Journal) Rows(ctx context.Context) (map[Key]Row, error) {
	var rows map[Key]Row
	err := j.withLock(ctx, func() error {
		var err error
		rows, err = j.rowsUnlocked()
		return err
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (j Journal) rowsUnlocked() (map[Key]Row, error) {
	state, err := j.file.Read()
	if err != nil {
		return nil, err
	}
	rows := make(map[Key]Row, len(state.Rows))
	for k, row := range state.Rows {
		rows[Key(k)] = row
	}
	return rows, nil
}

// Unexported: canonical seq advancement must route through advanceSeq; a caller-supplied seq could reconstitute truncated rows.
func (j Journal) apply(ctx context.Context, rows ...Row) (int, error) {
	applied := 0
	err := j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(state *journalState) error {
			for _, r := range rows {
				stored := state.Rows[string(r.Key)]
				if r.Seq <= stored.Seq {
					continue
				}
				state.Rows[string(r.Key)] = r
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

// Bump atomically advances key to state at the next seq and returns the new row.
func (j Journal) Bump(ctx context.Context, key Key, state RowState) (Row, error) {
	var out Row
	err := j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(st *journalState) error {
			stored := st.Rows[string(key)]
			next, ok := advanceSeq(stored.Seq, st.Sequence)
			if !ok {
				return fmt.Errorf("%w: key %q", ErrSeqExhausted, key)
			}
			out = Row{Key: key, Seq: next, State: state}
			st.Rows[string(key)] = out
			return nil
		})
	})
	if err != nil {
		return Row{}, err
	}
	return out, nil
}

func (j Journal) adopt(ctx context.Context, rows []Row) error {
	return j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(st *journalState) error {
			for _, r := range rows {
				stored := st.Rows[string(r.Key)]
				if stored.Seq > r.Seq {
					continue
				}
				next, ok := advanceSeq(r.Seq, stored.Seq, st.Sequence)
				if !ok {
					return fmt.Errorf("%w: key %q", ErrSeqExhausted, r.Key)
				}
				st.Rows[string(r.Key)] = Row{Key: r.Key, Seq: next, State: RowPending}
			}
			return nil
		})
	})
}

// Copies caller-supplied seqs verbatim — only correct for the transition's
// canonical→generation snapshot; must not be exported.
func (j Journal) claimSnapshot(ctx context.Context, rows []Row, claim func() error) error {
	return j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(state *journalState) error {
			stored := make(map[Key]Row, len(state.Rows))
			for k, row := range state.Rows {
				stored[Key(k)] = row
			}
			if len(stored) > 0 {
				if len(stored) != len(rows) {
					return ErrStaleJournal
				}
				for _, r := range rows {
					if stored[r.Key] != r {
						return ErrStaleJournal
					}
				}
			}
			if err := claim(); err != nil {
				return err
			}
			if len(stored) > 0 {
				return nil
			}
			for _, r := range rows {
				state.Rows[string(r.Key)] = r
			}
			return nil
		})
	})
}

// Truncate deletes exactly the snapshotted rows — a row admitted or bumped
// after the snapshot survives — and the seq high-water mark absorbs deleted
// seqs so a later Bump never re-issues one.
func (j Journal) Truncate(ctx context.Context, scope map[Key]Row) error {
	return j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(state *journalState) error {
			high := state.Sequence
			for key, want := range scope {
				stored := state.Rows[string(key)]
				if stored != want {
					continue
				}
				if stored.Seq > high {
					high = stored.Seq
				}
				delete(state.Rows, string(key))
			}
			state.Sequence = high
			return nil
		})
	})
}

func (j Journal) markComplete(ctx context.Context) error {
	if j.inc == "" {
		return errors.New("complete marker requires an incarnation-bound journal")
	}
	return j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(state *journalState) error {
			state.Complete = j.inc
			return nil
		})
	})
}

func (j Journal) isComplete(ctx context.Context) (bool, error) {
	var complete bool
	err := j.withLock(ctx, func() error {
		state, err := j.file.Read()
		if err != nil {
			return err
		}
		complete = j.inc != "" && state.Complete == j.inc
		return nil
	})
	if err != nil {
		return false, err
	}
	return complete, nil
}

func (j Journal) terminalize(ctx context.Context, row Row) error {
	return j.withLock(ctx, func() error {
		return j.terminalizeUnlocked(row)
	})
}

func (j Journal) terminalizeUnlocked(row Row) error {
	return j.file.UpdateUnlocked(func(state *journalState) error {
		stored := state.Rows[string(row.Key)]
		if stored != row {
			return fmt.Errorf("terminalize row %q: stored %+v does not match expected %+v", row.Key, stored, row)
		}
		out := Row{Key: row.Key, Seq: nextSeq(row.Seq), State: RowYielded}
		state.Rows[string(row.Key)] = out
		return nil
	})
}

func nextSeq(s uint64) uint64 {
	if s == math.MaxUint64 {
		return s
	}
	return s + 1
}

func advanceSeq(floors ...uint64) (uint64, bool) {
	var next uint64
	for _, f := range floors {
		if n := nextSeq(f); n > next {
			next = n
		}
	}
	if next == math.MaxUint64 {
		return 0, false
	}
	return next, true
}

func withFileLock(ctx context.Context, path string, fn func() error) error {
	lock, err := (proc.FileLockSpec{
		Path:     path,
		Mode:     proc.FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
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

// ResolveOwner applies the canonical-row-wins-only-when-proven-newer rule.
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
	Boot      string `json:"boot"`
}

type transitionRecord struct {
	Generation string      `json:"generation"`
	Owner      ownerRecord `json:"owner"`
	Step       Step        `json:"step"`
}

func newOwnerRecord(id proc.Identity) ownerRecord {
	return ownerRecord{PID: id.PID, StartTime: id.StartTime, Comm: id.Comm, Boot: id.Boot}
}

func (r ownerRecord) identity() proc.Identity {
	return proc.Identity{PID: r.PID, StartTime: r.StartTime, Comm: r.Comm, Boot: r.Boot}
}

type ownerFile struct {
	ownerRecord
	Inc string `json:"inc"`
}

func (j Journal) claimTransition(ctx context.Context, generation string, owner proc.Identity) (Step, error) {
	want := transitionRecord{Generation: generation, Owner: newOwnerRecord(owner)}
	var phase Step
	err := j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(state *journalState) error {
			if state.Transition != nil {
				current := *state.Transition
				if current.Generation != want.Generation || current.Owner != want.Owner {
					return ErrDrainInProgress
				}
				phase = current.Step
				return nil
			}
			state.Transition = &want
			return nil
		})
	})
	return phase, err
}

func (j Journal) advanceTransition(ctx context.Context, generation string, owner proc.Identity, step Step) error {
	wantOwner := newOwnerRecord(owner)
	return j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(state *journalState) error {
			if state.Transition == nil || state.Transition.Generation != generation || state.Transition.Owner != wantOwner {
				return ErrDrainInProgress
			}
			current := *state.Transition
			if current.Step >= step {
				return nil
			}
			if step != current.Step+1 {
				return fmt.Errorf("active transition: advance from step %d to %d", current.Step, step)
			}
			current.Step = step
			state.Transition = &current
			return nil
		})
	})
}

func (j Journal) activeTransition(ctx context.Context) (transitionRecord, bool, error) {
	var rec transitionRecord
	var ok bool
	err := j.withLock(ctx, func() error {
		state, err := j.file.Read()
		if err != nil {
			return err
		}
		if state.Transition != nil {
			rec = *state.Transition
			ok = true
		}
		return nil
	})
	return rec, ok, err
}

func (j Journal) releaseTransition(ctx context.Context, generation string, owner proc.Identity) error {
	wantOwner := newOwnerRecord(owner)
	return j.withLock(ctx, func() error {
		return j.file.UpdateUnlocked(func(state *journalState) error {
			if state.Transition == nil || state.Transition.Generation != generation {
				return nil
			}
			current := *state.Transition
			if current.Owner != wantOwner {
				return fmt.Errorf("active transition %s owner %+v does not match %+v", generation, current.Owner.identity(), owner)
			}
			state.Transition = nil
			return nil
		})
	})
}

// Generation is one drain generation's on-disk record under
// <dotdir>/drain/<gen>. All generation-layout mutation serializes on the one
// never-unlinked drain-root lock — flock identity is the inode, so no
// removable path is ever a synchronization point. Lock order: the root lock
// nests OUTSIDE the canonical journal flock; the strike-store flock never
// nests with either.
type Generation struct {
	dir string
	inc string
}

// Allowlist, not blocklist: filepath cleaning is no guard (Base("/") is "/"),
// so a blocklist would let a name alias the drain root and Remove delete it.
var genName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// NewGeneration addresses generation gen under dotdir's drain layout.
func NewGeneration(dotdir, gen string) (Generation, error) {
	if !filepath.IsAbs(dotdir) {
		return Generation{}, fmt.Errorf("dotdir %q is not absolute", dotdir)
	}
	if !genName.MatchString(gen) {
		return Generation{}, fmt.Errorf("invalid generation name %q", gen)
	}
	return Generation{dir: filepath.Join(dotdir, "drain", gen)}, nil
}

// Dir returns the generation directory.
func (g Generation) Dir() string { return g.dir }

// Name returns the generation's identifier (the directory basename).
func (g Generation) Name() string { return filepath.Base(g.dir) }

func (g Generation) journal() Journal {
	path := filepath.Join(g.dir, "journal.json")
	return Journal{
		file:      journalStateFile(path),
		lock:      g.rootLock(),
		genDir:    g.dir,
		inc:       g.inc,
		ownerPath: g.ownerPath(),
	}
}

func (g Generation) rootLock() string {
	return filepath.Join(filepath.Dir(g.dir), rootLockName)
}

func (g Generation) ownerPath() string { return filepath.Join(g.dir, "owner.json") }

func (g Generation) writeOwnerUnlocked(id proc.Identity, inc string) error {
	file := ownerStateFile(g.ownerPath())
	if _, err := file.Read(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read owner %s before write: %w", g.ownerPath(), err)
	}
	if err := file.ReplaceUnlocked(ownerFile{ownerRecord: newOwnerRecord(id), Inc: inc}); err != nil {
		return fmt.Errorf("write owner %s: %w", g.ownerPath(), err)
	}
	return nil
}

// A matching retry rewrites the full durable chain: a readable record can still be undurable (rename landed, dir fsync failed).
func (g Generation) claimOwner(ctx context.Context, id proc.Identity) (Generation, error) {
	var bound Generation
	err := withFileLock(ctx, g.rootLock(), func() error {
		// A preseeded symlink would route the owner write outside the layout.
		if fi, err := os.Lstat(g.dir); err == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("generation %s: %s is a symlink", g.Name(), g.dir)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		rec, err := readOwnerFile(g.ownerPath())
		if err == nil {
			if rec.identity() != id {
				return fmt.Errorf("%w: generation %s is owned by %+v", ErrStaleJournal, g.Name(), rec.identity())
			}
			inc := rec.Inc
			bound = Generation{dir: g.dir, inc: inc}
			return g.writeOwnerUnlocked(id, inc)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		state, err := journalStateFile(filepath.Join(g.dir, "journal.json")).Read()
		if err != nil {
			return err
		}
		if len(state.Rows) != 0 {
			return ErrStaleJournal
		}
		inc, err := mintInc()
		if err != nil {
			return err
		}
		if err := g.clearStaleMarker(); err != nil {
			return err
		}
		bound = Generation{dir: g.dir, inc: inc}
		return g.writeOwnerUnlocked(id, inc)
	})
	if err != nil {
		return Generation{}, err
	}
	return bound, nil
}

func (g Generation) clearStaleMarker() error {
	path := filepath.Join(g.dir, "journal.json")
	file := journalStateFile(path)
	state, err := file.Read()
	if err != nil {
		return err
	}
	if state.Complete == "" {
		return nil
	}
	return file.UpdateUnlocked(func(st *journalState) error {
		st.Complete = ""
		return nil
	})
}

func (g Generation) bind(ctx context.Context) (Generation, error) {
	var bound Generation
	err := withFileLock(ctx, g.rootLock(), func() error {
		rec, err := readOwnerFile(g.ownerPath())
		if err != nil {
			return err
		}
		bound = Generation{dir: g.dir, inc: rec.Inc}
		return nil
	})
	if err != nil {
		return Generation{}, err
	}
	return bound, nil
}

// ReadOwner returns the recorded owner identity.
func (g Generation) ReadOwner() (proc.Identity, error) {
	rec, err := readOwnerFile(g.ownerPath())
	if err != nil {
		return proc.Identity{}, err
	}
	return rec.identity(), nil
}

func readOwnerFile(path string) (ownerFile, error) {
	rec, err := ownerStateFile(path).Read()
	if err != nil {
		return ownerFile{}, fmt.Errorf("read owner %s: %w", path, err)
	}
	return rec, nil
}

func readOwnerInc(path string) (string, error) {
	rec, err := readOwnerFile(path)
	if err != nil {
		return "", err
	}
	return rec.Inc, nil
}

func mintInc() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mint incarnation token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// Remove durably deletes the generation directory once its rows are terminal or adopted.
func (g Generation) Remove(ctx context.Context) error {
	return withFileLock(ctx, g.rootLock(), func() error {
		if g.inc != "" {
			inc, err := readOwnerInc(g.ownerPath())
			if err == nil && inc != g.inc {
				return fmt.Errorf("%w: generation %s was re-created", ErrStaleGeneration, g.Name())
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		return g.removeUnlocked()
	})
}

func (g Generation) removeUnlocked() error {
	if err := os.RemoveAll(g.dir); err != nil {
		return err
	}
	err := daemon.SyncDir(filepath.Dir(g.dir))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Generations lists every drain generation under dotdir (absolute).
func Generations(dotdir string) ([]Generation, error) {
	if !filepath.IsAbs(dotdir) {
		return nil, fmt.Errorf("dotdir %q is not absolute", dotdir)
	}
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
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		gens = append(gens, Generation{dir: filepath.Join(root, e.Name())})
	}
	return gens, nil
}
