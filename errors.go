package daemonkit

import (
	"errors"

	"github.com/yasyf/daemonkit/internal/maintenance"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
)

// Sentinel identity is load-bearing: consumers alias these and match with
// errors.Is across module boundaries. Each is declared exactly once, here,
// and one this module already declares deeper down is aliased rather than
// re-declared, so the two spellings cannot name two errors.
var (
	// ErrDrainBusy refuses shutdown before protected work can be disturbed.
	ErrDrainBusy = maintenance.ErrDrainBusy

	// ErrDrainPreparationTimeout refuses shutdown after its reversible wait expires.
	ErrDrainPreparationTimeout = maintenance.ErrDrainPreparationTimeout

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

	// ErrSessionCapacity means the daemon had no free session slot. It is
	// transient and says nothing about whether the daemon is installed.
	ErrSessionCapacity = wire.ErrSessionCapacity

	// ErrProtocolMismatch means the process answering on the socket speaks
	// another wire protocol: a daemon of another daemonkit era. Nothing can be
	// read or drained over its session; Ensure and Stop settle it from its
	// owner record by signal.
	ErrProtocolMismatch = wire.ErrProtocolVersion

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
	// recorded incumbent is still live, a Terminate whose kill was not yet
	// observed, or an owned child, adopted record, or ownership scope whose
	// settlement outran its deadline. The target keeps settling on its own
	// ladder; re-observe with Settle, or leave the record for the next
	// generation to reclaim.
	ErrUnsettled = proc.ErrUnsettled

	// ErrUnrecorded means Settle found no owner record naming an incumbent:
	// nothing of this daemon ever recorded itself here. Absence is then an
	// inventory question — deploy.Inventory over the daemon's executables.
	ErrUnrecorded = errors.New("daemonkit: no owner record names an incumbent")
)

// RemoteError is the daemon session's own failure on a request it admitted,
// not the product's *ProductError. Err is what the response code classified
// Message as — context.DeadlineExceeded when the conveyed deadline ended on
// the serving side. It proves the daemon answered; its absence proves
// nothing, one conveyed deadline ending on both sides at once.
type RemoteError struct {
	Message string
	Err     error
}

// Error returns the daemon's failure text verbatim.
func (e *RemoteError) Error() string { return e.Message }

// Unwrap returns Err, nil when the response code classified nothing.
func (e *RemoteError) Unwrap() error { return e.Err }
