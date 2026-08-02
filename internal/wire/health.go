package wire

import (
	"context"
	"encoding/json"
	"fmt"
)

const healthOp Op = "daemon.health"

// Serving is the serving process identity the health verb reports beside
// wire's own Phase and Protocol. Detail returns the product's report bytes
// verbatim; nil means no product detail.
type Serving struct {
	PID        int
	Build      string
	Generation uint64
	Detail     func() []byte
}

// HealthReport is the daemon.health verb's terminal payload: one observation
// of the serving daemon, answered on both lanes below product dispatch and
// outside the phase gate, so a starting or draining daemon still answers.
//
// The report is additive forever and decoded leniently: it rides inside the
// payload, not the protocol envelope, so a field added in a later daemonkit
// needs no ProtocolVersion bump and an older client must keep attaching to
// the newer daemon it is about to drain.
type HealthReport struct {
	Phase      Phase  `json:"phase"`
	Protocol   uint16 `json:"protocol"`
	Generation uint64 `json:"generation"`
	PID        int    `json:"pid"`
	Build      string `json:"build"`
	Detail     []byte `json:"detail,omitempty"`
}

func (s *Server) executeHealth() (any, error) {
	report := HealthReport{
		Phase:      s.rt.Phase().Phase,
		Protocol:   ProtocolVersion,
		Generation: s.cfg.Serving.Generation,
		PID:        s.cfg.Serving.PID,
		Build:      s.cfg.Serving.Build,
	}
	if s.cfg.Serving.Detail != nil {
		report.Detail = s.cfg.Serving.Detail()
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("wire: marshal health report: %w", err)
	}
	return json.RawMessage(payload), nil
}

// InFlight reports the number of admitted requests. A request counts from
// admission — under the session lock that installs it, so neither the window
// before its dispatch goroutine runs nor a request parked on the concurrency
// semaphore is invisible — until its terminal response is written and
// acknowledged, so response marshaling and the write itself are inside the
// count, never after it.
func (s *Server) InFlight() int { return int(s.admitted.Load()) }

// Health asks the daemon.health verb on this session and decodes its report.
// A rejected result returns the typed RejectionError.
func (c *Client) Health(ctx context.Context) (HealthReport, error) {
	result, err := c.Call(ctx, healthOp, nil)
	if err != nil {
		return HealthReport{}, err
	}
	if rejection := result.Rejection(); rejection != nil {
		return HealthReport{}, rejection
	}
	if result.Response.Err != "" {
		return HealthReport{}, fmt.Errorf("wire: health verb: %s", result.Response.Err)
	}
	return decodeHealthReport(result.Response.Payload)
}

func decodeHealthReport(payload []byte) (HealthReport, error) {
	var report HealthReport
	if err := json.Unmarshal(payload, &report); err != nil {
		return HealthReport{}, fmt.Errorf("%w: health report: %w", ErrInvalidFrame, err)
	}
	return report, nil
}

// Drain dispatches the trust-gated drain verb and returns the transport
// result so the caller can distinguish a typed refusal from a lost terminal.
func (c *Client) Drain(ctx context.Context) (Result, error) {
	return c.Call(ctx, drainControlOp, nil)
}
