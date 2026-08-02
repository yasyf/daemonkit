package daemonkit

import (
	"errors"

	"github.com/yasyf/daemonkit/internal/trust"
)

// Sentinel identity is load-bearing: consumers alias these and match with
// errors.Is across module boundaries. Each is declared exactly once, here,
// and one this module already declares deeper down is aliased rather than
// re-declared, so the two spellings cannot name two errors.
var (
	// ErrBusy means Serve found a live incumbent owning the socket; no
	// takeover exists here.
	ErrBusy = errors.New("daemonkit: daemon already serving")

	// ErrAbsent means a proven no-listener: the socket is gone or nothing
	// accepts on it. It never means "unhealthy" or "process gone" — a live
	// process with a dead listener is settled by Settle, not assumed absent.
	ErrAbsent = errors.New("daemonkit: no daemon is listening")

	// ErrDraining means the peer is already leaving: the frozen drain
	// preamble on a fresh attach, or a typed rejection on a live session.
	// Settlement continues without a session via Settle.
	ErrDraining = errors.New("daemonkit: daemon is draining")

	// ErrNotReady means the daemon is starting; retry in-session.
	ErrNotReady = errors.New("daemonkit: daemon is starting")

	// ErrUntrusted means the peer failed a lane's trust requirement.
	ErrUntrusted = errors.New("daemonkit: peer failed trust verification")

	// ErrPeerGone means the process accepting on the socket ended its
	// execution generation before verification could finish: the ordinary
	// daemon-restart race, never a verdict about the peer. It is
	// internal/trust's own sentinel, so the branch this package already has on
	// it and any branch a consumer writes are the same identity.
	ErrPeerGone = trust.ErrPeerGone

	// ErrNoVerifier means a requirement was stated with no code-identity
	// verifier to run it — a build defect, never a trust verdict. It is
	// internal/trust's own sentinel, aliased for the same reason.
	ErrNoVerifier = trust.ErrNoVerifier

	// ErrOversize means the Call body is larger than the session's MaxFrame
	// can carry once the envelope is on it. Nothing was written.
	ErrOversize = errors.New("daemonkit: payload exceeds the session's MaxFrame")

	// ErrLaneClosed means a business lane will not acquire another session:
	// Close took it, or its one caller-authenticated session failed
	// terminally.
	ErrLaneClosed = errors.New("daemonkit: business lane is closed")

	// ErrWrongIncumbent means a non-zero Expect field disagreed with the
	// pinned incumbent's served Health (Drain) or with the durable owner
	// record (Settle). Nothing was dispatched, stopped, or settled.
	ErrWrongIncumbent = errors.New("daemonkit: incumbent is not the expected runtime")

	// ErrUnsettled means the target was still in the process table when ctx
	// ended: a delivered drain whose exit was not yet observed, a Settle whose
	// recorded incumbent is still live, or an owned child, adopted record, or
	// ownership scope whose settlement outran its deadline. The target keeps
	// settling on its own ladder; re-observe with Settle, or leave the record
	// for the next generation to reclaim.
	ErrUnsettled = errors.New("daemonkit: process did not provably exit")

	// errPinMoved means the process answering on a pinned session is no longer
	// the one the attach pinned: the incumbent was replaced between two reads.
	// It is unexported because it is nobody's error to handle — Ensure absorbs
	// it by observing again, exactly as it absorbs ErrWrongIncumbent.
	errPinMoved = errors.New("daemonkit: pinned incumbent moved")

	// ErrUnrecorded means Settle found no owner record naming an incumbent:
	// nothing of this daemon ever recorded itself here. Absence is then an
	// inventory question — deploy.Inventory over the daemon's executables.
	ErrUnrecorded = errors.New("daemonkit: no owner record names an incumbent")
)
