package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

// DefaultTick is Run's babysit-and-sweep cadence when Tick is unset.
const DefaultTick = time.Second

const (
	// DefaultStrikeLimit is the respawn attempts allowed per strike window.
	DefaultStrikeLimit = 3
	// DefaultStrikeWindow is the sliding window respawn attempts count within.
	DefaultStrikeWindow = 10 * time.Minute
)

// DefaultParkLadder escalates park durations across breaker trips; the last
// step repeats.
var DefaultParkLadder = []time.Duration{
	10 * time.Minute, 30 * time.Minute, 2 * time.Hour, 12 * time.Hour,
}

var defaultBackoff = proc.Backoff{Base: 30 * time.Second, Cap: 10 * time.Minute}

// StrikeStore is a disk-backed respawn breaker on proc.Strikes and proc.Ladder:
// its window, ladder level, and park deadline survive process restarts.
type StrikeStore struct {
	// Path is the persisted breaker state file.
	Path string
	// Limit is the attempts per window before parking; zero means DefaultStrikeLimit.
	Limit int
	// Window is the sliding attempt window; zero means DefaultStrikeWindow.
	Window time.Duration
	// Ladder escalates park durations per trip; empty means DefaultParkLadder.
	Ladder []time.Duration
}

type strikeState struct {
	Times       []time.Time `json:"times"`
	Level       int         `json:"level"`
	ParkedUntil time.Time   `json:"parked_until"`
}

func (s StrikeStore) limit() int {
	if s.Limit > 0 {
		return s.Limit
	}
	return DefaultStrikeLimit
}

func (s StrikeStore) window() time.Duration {
	if s.Window > 0 {
		return s.Window
	}
	return DefaultStrikeWindow
}

func (s StrikeStore) ladder() []time.Duration {
	if len(s.Ladder) > 0 {
		return s.Ladder
	}
	return DefaultParkLadder
}

// Parked reports whether the breaker is parked at now, and until when.
func (s StrikeStore) Parked(ctx context.Context, now time.Time) (bool, time.Time, error) {
	var st strikeState
	err := withFlock(ctx, s.Path+".lock", func() error {
		var err error
		st, err = s.load()
		return err
	})
	if err != nil {
		return false, time.Time{}, err
	}
	return now.Before(st.ParkedUntil), st.ParkedUntil, nil
}

// Gate admits one respawn attempt at now in a single locked transaction: a
// parked breaker refuses without recording, and an admitted attempt's strike
// lands durably before Gate returns, so the caller spawns only behind a
// recorded strike. Tripping the window still admits the threshold attempt but
// parks the breaker for later ones; a nonzero until reports the park deadline
// either way. A crash between the durable strike and the launch it admitted
// costs one unconsumed strike; the park ladder must tolerate that over-count.
func (s StrikeStore) Gate(ctx context.Context, now time.Time) (allowed bool, until time.Time, err error) {
	file := daemon.StateFile{Path: s.Path}
	err = withFlock(ctx, s.Path+".lock", func() error {
		st, err := s.load()
		if err != nil {
			return err
		}
		if now.Before(st.ParkedUntil) {
			until = st.ParkedUntil
			return nil
		}
		allowed = true
		return file.UpdateUnlocked(func(state map[string]json.RawMessage) error {
			st, err := decodeStrikes(state["strikes"])
			if err != nil {
				return err
			}
			strikes := proc.Strikes{Limit: s.limit(), Window: s.window()}
			strikes.Load(st.Times)
			if strikes.Strike(now) {
				lad := proc.Ladder{Steps: s.ladder()}
				for range st.Level {
					lad.Next()
				}
				st.ParkedUntil = now.Add(lad.Next())
				st.Level++
				until = st.ParkedUntil
			}
			st.Times = strikes.Times()
			b, err := json.Marshal(st)
			if err != nil {
				return err
			}
			state["strikes"] = b
			return nil
		})
	})
	if err != nil {
		return false, time.Time{}, err
	}
	return allowed, until, nil
}

// SpawnGate adapts the store to proc.Spawn.Gate: each actual child launch
// lands a durable strike first, and a parked breaker refuses with
// ErrSpawnParked. Wiring it into every Spawn is what makes an unaccounted
// launch unrepresentable — transition and babysit share one launch-site gate.
func (s StrikeStore) SpawnGate() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		allowed, until, err := s.Gate(ctx, time.Now())
		if err != nil {
			return fmt.Errorf("drain: record launch strike: %w", err)
		}
		if !allowed {
			return fmt.Errorf("%w until %s", ErrSpawnParked, until)
		}
		return nil
	}
}

func (s StrikeStore) load() (strikeState, error) {
	state, err := readState(s.Path)
	if err != nil {
		return strikeState{}, err
	}
	return decodeStrikes(state["strikes"])
}

func decodeStrikes(raw json.RawMessage) (strikeState, error) {
	if len(raw) == 0 {
		return strikeState{}, nil
	}
	var st strikeState
	if err := json.Unmarshal(raw, &st); err != nil {
		return strikeState{}, fmt.Errorf("parse strike state: %w", err)
	}
	return st, nil
}

// Breakers applies per-id failure backoff on a proc.Backoff, so one peer's
// failures never suppress another's attempts. Not safe for concurrent use.
type Breakers struct {
	backoff  proc.Backoff
	failures map[string]int
	next     map[string]time.Time
}

// NewBreakers builds a Breakers over backoff.
func NewBreakers(backoff proc.Backoff) *Breakers {
	return &Breakers{
		backoff:  backoff,
		failures: map[string]int{},
		next:     map[string]time.Time{},
	}
}

// Allow reports whether id may be attempted at now.
func (b *Breakers) Allow(id string, now time.Time) bool {
	return !now.Before(b.next[id])
}

// Fail records a failed attempt for id, backing off its next attempt.
func (b *Breakers) Fail(id string, now time.Time) {
	b.failures[id]++
	b.next[id] = now.Add(b.backoff.After(b.failures[id]))
}

// OK clears id's failure history.
func (b *Breakers) OK(id string) {
	delete(b.failures, id)
	delete(b.next, id)
}

// RunConfig drives one draining generation's Run loop.
type RunConfig struct {
	// Generation is the drain generation whose journal Run sweeps.
	Generation Generation
	// Canonical is the canonical ownership journal; sweep skips (and
	// terminalizes) rows canonical owns with a proven-newer seq, so a retried
	// transition never yields a resource the canonical re-admitted. Required.
	Canonical Journal
	// Resources is the consumer resource seam. Required.
	Resources Resources
	// CanonicalAlive probes the canonical per tick; only Dead triggers respawn,
	// Undetermined does nothing. Required.
	CanonicalAlive func(ctx context.Context) Liveness
	// Ready reports the successor can receive handoffs; false skips the sweep,
	// never the babysit. Required.
	Ready func(ctx context.Context) bool
	// Spawn respawns the canonical; must be idempotent and launch-gated: wire
	// proc.Spawn.EnsureRunning with Spawn.Gate set to StrikeStore.SpawnGate,
	// so every actual launch lands a durable strike first. Required.
	Spawn func(ctx context.Context) error
	// Backoff spaces per-key sweep retries; the zero value uses a default.
	Backoff proc.Backoff
	// Tick is the loop cadence; zero means DefaultTick.
	Tick time.Duration
	// Log receives loop diagnostics; nil uses slog.Default.
	Log *slog.Logger

	clock clock
}

func (cfg RunConfig) tick() time.Duration {
	if cfg.Tick > 0 {
		return cfg.Tick
	}
	return DefaultTick
}

func (cfg RunConfig) log() *slog.Logger {
	if cfg.Log != nil {
		return cfg.Log
	}
	return slog.Default()
}

func (cfg RunConfig) perKeyBackoff() proc.Backoff {
	if cfg.Backoff != (proc.Backoff{}) {
		return cfg.Backoff
	}
	return defaultBackoff
}

// Run babysits the canonical (probe per tick, launch-gated respawn) and sweeps
// the generation journal, removing the generation and returning at zero pending.
// It binds to the generation's current incarnation at entry: a generation
// removed or re-created mid-run fails with ErrStaleGeneration instead of
// touching the new incarnation's state.
func Run(ctx context.Context, cfg RunConfig) error {
	gen, err := cfg.Generation.bind(ctx)
	if err != nil {
		return fmt.Errorf("drain: bind generation: %w", err)
	}
	cfg.Generation = gen
	clk := clockOrReal(cfg.clock)
	brk := NewBreakers(cfg.perKeyBackoff())
	for {
		cfg.babysit(ctx)
		pending, err := cfg.sweep(ctx, brk, clk)
		if err != nil {
			return err
		}
		if pending == 0 {
			if err := cfg.Generation.Remove(ctx); err != nil {
				return fmt.Errorf("drain: remove generation: %w", err)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(cfg.tick()):
		}
	}
}

func (cfg RunConfig) babysit(ctx context.Context) {
	if cfg.CanonicalAlive(ctx) != Dead {
		return
	}
	if err := cfg.Spawn(ctx); err != nil {
		if errors.Is(err, ErrSpawnParked) {
			cfg.log().Error("drain: canonical respawn parked", "err", err)
			return
		}
		cfg.log().Error("drain: respawn canonical", "err", err)
	}
}

func (cfg RunConfig) sweep(ctx context.Context, brk *Breakers, clk clock) (int, error) {
	rows, err := cfg.Generation.journal().Rows(ctx)
	if err != nil {
		return 0, fmt.Errorf("drain: read generation journal: %w", err)
	}
	keys := make([]Key, 0, len(rows))
	for k, r := range rows {
		if r.State == RowPending {
			keys = append(keys, k)
		}
	}
	pending := len(keys)
	if pending == 0 {
		return 0, nil
	}
	canonical, err := cfg.Canonical.Rows(ctx)
	if err != nil {
		return pending, fmt.Errorf("drain: read canonical: %w", err)
	}
	superseded := make([]Key, 0)
	kept := keys[:0]
	for _, k := range keys {
		if ResolveOwner(canonical, rows, k) == OwnedByCanonical {
			superseded = append(superseded, k)
			continue
		}
		kept = append(kept, k)
	}
	for _, k := range superseded {
		// Canonical re-owns the key with a proven-newer seq: the generation's
		// row is stale and must never be yielded.
		if err := cfg.Generation.journal().terminalize(ctx, rows[k]); err != nil {
			return pending, fmt.Errorf("drain: advance superseded %s: %w", k, err)
		}
		pending--
		brk.OK(string(k))
	}
	keys = kept
	if pending == 0 {
		return 0, nil
	}
	if !cfg.Ready(ctx) {
		return pending, nil
	}
	sort.Slice(keys, func(a, b int) bool { return keys[a] < keys[b] })
	known, scanned := cfg.knownKeys(ctx)
	for _, k := range keys {
		if scanned && !known[k] {
			// A complete successful Keys scan proves the resource absent: terminal.
			if err := cfg.Generation.journal().terminalize(ctx, rows[k]); err != nil {
				return pending, fmt.Errorf("drain: advance absent %s: %w", k, err)
			}
			pending--
			brk.OK(string(k))
			continue
		}
		if !brk.Allow(string(k), clk.Now()) {
			continue
		}
		settled, err := cfg.sweepKey(ctx, k, rows[k])
		if err != nil {
			return pending, err
		}
		if settled {
			pending--
			brk.OK(string(k))
		} else {
			brk.Fail(string(k), clk.Now())
		}
	}
	return pending, nil
}

// knownKeys returns the live-resource set only when the enumeration fully
// succeeded; a failed scan proves nothing and never reads as zero candidates.
func (cfg RunConfig) knownKeys(ctx context.Context) (map[Key]bool, bool) {
	keys, err := cfg.Resources.Keys(ctx)
	if err != nil {
		return nil, false
	}
	m := make(map[Key]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}
	return m, true
}

func (cfg RunConfig) sweepKey(ctx context.Context, key Key, row Row) (bool, error) {
	log := cfg.log()
	fence, err := cfg.Resources.Seize(ctx, key)
	if err != nil {
		log.Warn("drain: seize", "key", key, "err", err)
		return false, nil
	}
	verdict, err := cfg.Resources.AttestIdle(ctx, key)
	if err != nil || verdict != IdleConfirmed {
		log.Warn("drain: not provably idle", "key", key, "verdict", verdict, "err", err)
		cfg.restore(ctx, key, fence)
		return false, nil
	}
	if !fence.Held() {
		log.Warn("drain: fence lost mid-sweep", "key", key)
		cfg.restore(ctx, key, fence)
		return false, nil
	}
	// A registration that landed after the sweep classified this key re-owns it;
	// yielding the stale row would double-apply. The held fence excludes later ones.
	canonical, err := cfg.Canonical.Rows(ctx)
	if err != nil {
		cfg.restore(ctx, key, fence)
		return false, fmt.Errorf("drain: reread canonical for %s: %w", key, err)
	}
	if c, ok := canonical[key]; ok && c.Seq > row.Seq {
		restoreErr := cfg.Resources.Restore(context.WithoutCancel(ctx), key, fence)
		if err := cfg.Generation.journal().terminalize(context.WithoutCancel(ctx), row); err != nil {
			return false, errors.Join(fmt.Errorf("drain: advance superseded %s: %w", key, err), restoreErr)
		}
		if restoreErr != nil {
			return true, fmt.Errorf("drain: restore superseded %s: %w", key, restoreErr)
		}
		return true, nil
	}
	if err := cfg.Resources.Yield(ctx, key, fence); err != nil {
		log.Warn("drain: yield", "key", key, "err", err)
		cfg.restore(ctx, key, fence)
		return false, nil
	}
	// Handed off: the row must advance, and Restore must never run past here.
	if err := cfg.Generation.journal().terminalize(context.WithoutCancel(ctx), row); err != nil {
		fence.Release()
		return false, fmt.Errorf("drain: advance %s: %w", key, err)
	}
	fence.Release()
	return true, nil
}

func (cfg RunConfig) restore(ctx context.Context, key Key, fence Fence) {
	if err := cfg.Resources.Restore(context.WithoutCancel(ctx), key, fence); err != nil {
		cfg.log().Error("drain: restore", "key", key, "err", err)
	}
}
