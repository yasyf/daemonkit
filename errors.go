package daemonkit

import "errors"

// Sentinel identity is load-bearing: consumers alias these and match with
// errors.Is across module boundaries. Each is declared exactly once, here.
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

	// ErrWrongIncumbent means a non-zero Expect field disagreed with the
	// pinned incumbent's served Health (Drain) or with the durable owner
	// record (Settle). Nothing was dispatched, stopped, or settled.
	ErrWrongIncumbent = errors.New("daemonkit: incumbent is not the expected runtime")

	// ErrUnsettled means the target was still in the process table when ctx
	// ended: a delivered drain whose exit was not yet observed, or a Settle
	// whose recorded incumbent is still live. The incumbent keeps draining on
	// its own Shutdown budget; re-observe with Settle.
	ErrUnsettled = errors.New("daemonkit: daemon did not provably exit")

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
