package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	stopcontract "github.com/yasyf/daemonkit/internal/stopcontrol"
	"github.com/yasyf/daemonkit/proc"
)

const (
	stopControlOp = Op("daemon.control.stop")
)

// StopIntent is the launcher-authorized reason for stopping one runtime.
type StopIntent string

const (
	// StopIntentUpgrade replaces the incumbent with a newer runtime build.
	StopIntentUpgrade StopIntent = "upgrade"
	// StopIntentRestart restarts the same runtime build.
	StopIntentRestart StopIntent = "restart"
	// StopIntentUninstall removes the incumbent runtime.
	StopIntentUninstall StopIntent = "uninstall"
)

// StopControlVerifier authenticates the exact process receipt and product role
// permitted to request cross-process runtime settlement.
type StopControlVerifier interface {
	Validate() error
	VerifyStopControl(context.Context, Peer, string) (proc.Record, error)
}

// StopVerifier composes product-specific peer identity with the generic exact,
// one-shot durable stop receipt.
type StopVerifier struct {
	Classifier ProtectedSessionClassifier
	Role       string
	Store      proc.StopControlStore
}

// Validate rejects a verifier without all three independent authorities.
func (v StopVerifier) Validate() error {
	if v.Classifier == nil {
		return errors.New("wire: stop verifier classifier is required")
	}
	if err := v.Classifier.Validate(); err != nil {
		return fmt.Errorf("wire: stop verifier classifier: %w", err)
	}
	if v.Role == "" {
		return errors.New("wire: stop verifier role is required")
	}
	if v.Store == nil {
		return errors.New("wire: stop verifier store is required")
	}
	return nil
}

// VerifyStopControl authenticates product identity, exact role, and atomically
// consumes the complete unexpired process authority.
func (v StopVerifier) VerifyStopControl(
	ctx context.Context,
	peer Peer,
	targetProcessGeneration string,
) (proc.Record, error) {
	if err := v.Validate(); err != nil {
		return proc.Record{}, err
	}
	accepted, err := v.Classifier.Classify(ctx, peer)
	if err != nil {
		return proc.Record{}, err
	}
	if !accepted {
		return proc.Record{}, ErrProtectedSessionRequired
	}
	record, consumed, err := v.Store.ConsumeStopControl(
		ctx, peer.ProcessIdentity(), v.Role, targetProcessGeneration, time.Now(),
	)
	if err != nil {
		return proc.Record{}, fmt.Errorf("wire: consume stop control record: %w", err)
	}
	if !consumed {
		return proc.Record{}, errors.New("wire: no exact unexpired stop control authority")
	}
	return record, nil
}

var _ StopControlVerifier = StopVerifier{}

// StopControlConfig identifies one exact runtime and bounds proof of its
// endpoint and process settlement.
type StopControlConfig struct {
	Dial            Dialer
	WireBuild       string
	RuntimeProtocol int
}

// StopResult records the exact process identity and runtime settled by
// RunStopControl. Stopped is false only when an upgrade authority was not
// newer than the incumbent.
type StopResult struct {
	Process           proc.Identity
	ProcessGeneration string
	RuntimeBuild      string
	RuntimeProtocol   int
	Stopped           bool
}

type stopControlRequest struct {
	Version uint16 `json:"version"`
}

type stopControlResponse struct {
	Version         uint16            `json:"version"`
	Target          stopControlTarget `json:"target"`
	RuntimeBuild    string            `json:"runtime_build"`
	RuntimeProtocol int               `json:"runtime_protocol"`
	Stopped         bool              `json:"stopped"`
}

type stopControlTarget struct {
	PID               int    `json:"pid"`
	StartTime         string `json:"start_time"`
	Boot              string `json:"boot"`
	Comm              string `json:"comm"`
	Executable        string `json:"executable"`
	Audit             []byte `json:"audit,omitempty"`
	ProcessGeneration string `json:"process_generation"`
}

type stopControlEnvironment struct {
	probe   func(int) (proc.Identity, error)
	wait    func(context.Context, time.Duration) error
	selfPID func() int
}

// RunStopControl authenticates one exact-role session, commits an orderly
// shutdown request, and returns only after both endpoint and captured process
// ownership have settled.
func RunStopControl(ctx context.Context, config StopControlConfig) (StopResult, error) {
	return runStopControl(ctx, config, stopControlEnvironment{
		probe: proc.Probe, wait: waitStopControl, selfPID: os.Getpid,
	})
}

func runStopControl(ctx context.Context, config StopControlConfig, env stopControlEnvironment) (StopResult, error) {
	if config.Dial == nil {
		return StopResult{}, errors.New("wire: stop control dialer is required")
	}
	if config.WireBuild == "" {
		return StopResult{}, errors.New("wire: stop control build is required")
	}
	if config.RuntimeProtocol <= 0 {
		return StopResult{}, errors.New("wire: stop control protocol is required")
	}
	settleCtx, cancel := context.WithTimeout(ctx, stopcontract.ChildSettlementBound)
	defer cancel()
	client, err := NewClient(settleCtx, ClientConfig{
		Dial: config.Dial, WireBuild: config.WireBuild,
	})
	if err != nil {
		return StopResult{}, fmt.Errorf("wire: stop control connect: %w", err)
	}
	closed := false
	//nolint:contextcheck // Client.Close has no context and settles only local session state.
	defer func() {
		if !closed {
			_ = client.Close()
		}
	}()
	payload, err := json.Marshal(stopControlRequest{Version: 1})
	if err != nil {
		return StopResult{}, fmt.Errorf("wire: encode stop control request: %w", err)
	}
	result, err := client.Call(settleCtx, stopControlOp, "", payload)
	if err != nil {
		return StopResult{}, fmt.Errorf("wire: stop control request: %w", err)
	}
	if result.Outcome != Delivered {
		return StopResult{}, fmt.Errorf("wire: stop control rejected: %w", result.Rejection())
	}
	if result.Response.Rejected {
		return StopResult{}, fmt.Errorf("wire: stop control rejected: %w", result.Rejection())
	}
	if result.Response.Err != "" {
		return StopResult{}, fmt.Errorf("wire: stop control failed: %s", result.Response.Err)
	}
	var response stopControlResponse
	if err := decodeStrict(result.Response.Payload, &response); err != nil {
		return StopResult{}, fmt.Errorf("wire: decode stop control response: %w", err)
	}
	if response.Version != 1 || response.RuntimeBuild == "" || response.RuntimeProtocol != config.RuntimeProtocol {
		return StopResult{}, fmt.Errorf(
			"wire: stop control identity mismatch: version=%d build=%q protocol=%d",
			response.Version, response.RuntimeBuild, response.RuntimeProtocol,
		)
	}
	target, err := response.Target.identity()
	if err != nil {
		return StopResult{}, fmt.Errorf("wire: stop control target: %w", err)
	}
	if target.PID <= 1 || target.PID == env.selfPID() {
		return StopResult{}, fmt.Errorf("wire: stop control refuses pid %d", target.PID)
	}
	_ = client.Close() //nolint:contextcheck // Client.Close has no context.
	closed = true
	stop := StopResult{
		Process: target, ProcessGeneration: response.Target.ProcessGeneration,
		RuntimeBuild: response.RuntimeBuild, RuntimeProtocol: response.RuntimeProtocol, Stopped: response.Stopped,
	}
	if !response.Stopped {
		return stop, nil
	}
	if err := settleStopControl(settleCtx, config.Dial, target, stopcontract.PollInterval, env); err != nil {
		return StopResult{}, err
	}
	return stop, nil
}

func (t stopControlTarget) identity() (proc.Identity, error) {
	if t.PID <= 1 || t.StartTime == "" || t.Boot == "" || t.Executable == "" || t.ProcessGeneration == "" {
		return proc.Identity{}, errors.New("incomplete stop target")
	}
	identity := proc.Identity{
		PID: t.PID, StartTime: t.StartTime, Boot: t.Boot, Comm: t.Comm, Executable: t.Executable,
	}
	if len(t.Audit) != 0 {
		token, err := proc.AuditTokenFromBytes(t.Audit)
		if err != nil {
			return proc.Identity{}, err
		}
		identity.AuditToken = token
	}
	return identity, nil
}

func newStopControlTarget(identity proc.Identity, generation string) stopControlTarget {
	target := stopControlTarget{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Executable: identity.Executable, ProcessGeneration: generation,
	}
	if !identity.AuditToken.IsZero() {
		target.Audit = identity.AuditToken[:]
	}
	return target
}

func currentStopControlIdentity() (proc.Identity, error) {
	identity, err := proc.CurrentIdentity()
	if err == nil {
		return identity, nil
	}
	if !errors.Is(err, proc.ErrNoAuditToken) {
		return proc.Identity{}, err
	}
	identity, err = proc.Probe(os.Getpid())
	if err != nil {
		return proc.Identity{}, err
	}
	identity.Executable, err = proc.ExecutablePath(os.Getpid())
	if err != nil {
		return proc.Identity{}, err
	}
	return identity, nil
}

func settleStopControl(
	ctx context.Context,
	dial Dialer,
	identity proc.Identity,
	poll time.Duration,
	env stopControlEnvironment,
) error {
	endpointGone := false
	processSettled := false
	for !endpointGone || !processSettled {
		if !endpointGone {
			conn, err := dial(ctx)
			switch {
			case err == nil:
				_ = conn.Close()
			case provesNoListener(err):
				endpointGone = true
			case ctx.Err() != nil:
				return fmt.Errorf("wire: stop control settle endpoint: %w", ctx.Err())
			default:
				return fmt.Errorf("wire: stop control probe endpoint: %w", err)
			}
		}
		if !processSettled {
			current, err := env.probe(identity.PID)
			switch {
			case errors.Is(err, proc.ErrNoProcess):
				processSettled = true
			case err != nil:
				return fmt.Errorf("wire: stop control probe process %d: %w", identity.PID, err)
			case current.Boot != identity.Boot || current.StartTime != identity.StartTime:
				processSettled = true
			}
		}
		if endpointGone && processSettled {
			return nil
		}
		if err := env.wait(ctx, poll); err != nil {
			return fmt.Errorf("wire: stop control settlement: %w", err)
		}
	}
	return nil
}

func waitStopControl(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
