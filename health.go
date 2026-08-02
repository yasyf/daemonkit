package daemonkit

import "github.com/yasyf/daemonkit/internal/wire"

// frameEnvelopeReserve is what one terminal spends outside the bytes it
// carries: the terminal's own fields, the response envelope, and the frame
// header.
const frameEnvelopeReserve Bytes = 4 << 10

// MaxDetail is the largest Ctx.Report a daemon serving frames of maxFrame
// bytes may publish, zero meaning MaxFrame's own default. A larger report
// cannot be serialized, and a health terminal that cannot be written kills the
// session that asked — so Report refuses one instead.
func MaxDetail(maxFrame Bytes) Bytes { return maxFramedBytes(maxFrame) }

// maxFramedBytes is the largest []byte one terminal of a maxFrame session can
// carry, zero meaning MaxFrame's own default: encoding/json base64s a []byte,
// so it claims four bytes of frame for every three it holds, beside the
// envelope reserve.
func maxFramedBytes(maxFrame Bytes) Bytes {
	if maxFrame <= 0 {
		maxFrame = wire.DefaultMaxFrame
	}
	usable := maxFrame - frameEnvelopeReserve
	if usable <= 0 {
		return 0
	}
	return usable * 3 / 4
}

// Phase is the runtime lifecycle state a daemon publishes.
type Phase uint8

const (
	phaseInvalid Phase = iota
	// PhaseStarting precedes readiness; business dispatch is typed-rejected.
	PhaseStarting
	// PhaseReady admits business dispatch.
	PhaseReady
	// PhaseDraining means intake is closing; the daemon is leaving.
	PhaseDraining
	// PhaseFailed is the runtime's terminal failure: Start returned an error
	// and no product was ever mounted. The health verb answers below the
	// phase gate, so a failed daemon still reports it.
	PhaseFailed
)

// Health is one observation of a serving daemon. Build is diagnostic to
// daemonkit and compared nowhere inside it; callers compare it (upgrade
// decisions, Expect). Generation is the serving instance's record-store
// generation — minted once per instance, random and non-zero, never reused
// by another instance; it is an instance name, not an ordering. Detail
// carries the product's Report bytes verbatim.
type Health struct {
	Phase      Phase
	Protocol   uint16
	Generation uint64
	PID        int
	Build      string
	Detail     []byte
}

func phaseFromWire(phase wire.Phase) Phase {
	switch phase {
	case wire.PhaseStarting:
		return PhaseStarting
	case wire.PhaseReady:
		return PhaseReady
	case wire.PhaseDraining:
		return PhaseDraining
	case wire.PhaseFailed:
		return PhaseFailed
	default:
		return phaseInvalid
	}
}

func healthFromReport(report wire.HealthReport) Health {
	return Health{
		Phase:      phaseFromWire(report.Phase),
		Protocol:   report.Protocol,
		Generation: report.Generation,
		PID:        report.PID,
		Build:      report.Build,
		Detail:     report.Detail,
	}
}
