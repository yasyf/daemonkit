package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	stopcontract "github.com/yasyf/daemonkit/internal/stopcontrol"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

const (
	stopReportFD            = 3
	stopReleaseFD           = 4
	stopControlFrameLimit   = 16 << 10
	stopChildKillGrace      = 500 * time.Millisecond
	stopChildTerminateBound = 3 * time.Second
)

type stopControlTiming struct {
	identity     time.Duration
	track        time.Duration
	authority    time.Duration
	child        time.Duration
	parentMargin time.Duration
	untrack      time.Duration
	now          func() time.Time
	afterRevoke  func()
}

func (t stopControlTiming) withDefaults() stopControlTiming {
	budget := StandardStopBudget()
	if t.identity == 0 {
		t.identity = budget.IdentityReport
	}
	if t.track == 0 {
		t.track = budget.DurableTrack
	}
	if t.authority == 0 {
		t.authority = stopcontract.AuthorityBound
	}
	if t.child == 0 {
		t.child = budget.ChildSettlement
	}
	if t.parentMargin == 0 {
		t.parentMargin = budget.ParentMargin
	}
	if t.untrack == 0 {
		t.untrack = budget.DeferredUntrack
	}
	if t.now == nil {
		t.now = time.Now
	}
	return t
}

// StopBudget exposes the controller-owned bounds for one complete runtime stop.
type StopBudget struct {
	IdentityReport  time.Duration
	DurableTrack    time.Duration
	ChildSettlement time.Duration
	ParentMargin    time.Duration
	DeferredUntrack time.Duration
}

// StandardStopBudget returns the fixed runtime stop budget enforced by Controller.
func StandardStopBudget() StopBudget {
	return StopBudget{
		IdentityReport:  stopcontract.IdentityBound,
		DurableTrack:    stopcontract.TrackBound,
		ChildSettlement: stopcontract.ChildSettlementBound,
		ParentMargin:    stopcontract.ParentSettlementMargin,
		DeferredUntrack: stopcontract.DeferredUntrackBound,
	}
}

// ChildOperation returns the child settlement bound plus the parent settlement margin.
func (b StopBudget) ChildOperation() time.Duration { return b.ChildSettlement + b.ParentMargin }

// Total returns the complete sequential stop bound.
func (b StopBudget) Total() time.Duration {
	return b.IdentityReport + b.DurableTrack + b.ChildOperation() + b.DeferredUntrack
}

// ErrStopDeclined means the exact target remained live because an upgrade
// authority was no longer newer by the time the stop request was handled.
var ErrStopDeclined = errors.New("service: runtime stop was declined")

// StopControlSpec is the controller-owned exact hidden-role invocation for one
// runtime stop. RuntimeBuild and RuntimeProtocol identify the caller deployment; the target
// generation comes from the product RuntimeHealth observation.
type StopControlSpec struct {
	Executable              string
	Args                    []string
	Role                    string
	RuntimeBuild            string
	RuntimeProtocol         int
	TargetProcessGeneration string
	Intent                  wire.StopIntent
}

// StopControlClientConfig is the non-authority transport configuration used by
// the hidden role after Controller has durably authorized and released it.
type StopControlClientConfig struct {
	Dial            wire.Dialer
	WireBuild       string
	RuntimeProtocol int
}

type stopChildResult struct {
	Result wire.StopResult `json:"result"`
	Error  string          `json:"error,omitempty"`
}

func stopAuthorityRetainsFullWindow(record proc.Record, now time.Time, window time.Duration) bool {
	expires := time.UnixMilli(record.ExpiresUnixMilli)
	return record.StopAuthorityState == proc.StopAuthorityArmed && expires.After(now.Add(window))
}

// StopRuntime starts one exact hidden-role child, records its post-exec kernel
// identity and complete authority before release, and returns only after the
// child and target runtime have settled.
func (c *Controller) StopRuntime(
	ctx context.Context,
	spec StopControlSpec,
) (result wire.StopResult, returnErr error) {
	if c.stopReaper == nil {
		return wire.StopResult{}, errors.New("service: stop control is unavailable")
	}
	rolePath, err := validateStopControlSpec(spec)
	if err != nil {
		return wire.StopResult{}, err
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return wire.StopResult{}, err
	}
	defer finish()

	timing := c.stopTiming.withDefaults()
	reportReader, reportWriter, err := os.Pipe()
	if err != nil {
		return wire.StopResult{}, fmt.Errorf("service: create stop report pipe: %w", err)
	}
	defer reportReader.Close()
	defer reportWriter.Close()
	releaseReader, releaseWriter, err := os.Pipe()
	if err != nil {
		return wire.StopResult{}, fmt.Errorf("service: create stop release pipe: %w", err)
	}
	defer releaseReader.Close()
	defer releaseWriter.Close()
	cmd := exec.Command(rolePath, spec.Args...)
	cmd.ExtraFiles = []*os.File{reportWriter, releaseReader}
	if err := cmd.Start(); err != nil {
		return wire.StopResult{}, fmt.Errorf("service: start stop control: %w", err)
	}
	_ = reportWriter.Close()
	_ = releaseReader.Close()
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	var identity proc.Identity
	var tracked *proc.Record
	settled := false
	var operationDeadline time.Time
	defer func() {
		if !settled {
			_ = reportReader.Close()
			_ = releaseWriter.Close()
			cleanupParent := context.WithoutCancel(opCtx)
			if !operationDeadline.IsZero() {
				var cancelDeadline context.CancelFunc
				cleanupParent, cancelDeadline = context.WithDeadline(cleanupParent, operationDeadline)
				defer cancelDeadline()
			}
			cleanupCtx, cancelCleanup := context.WithTimeout(cleanupParent, timing.parentMargin)
			defer cancelCleanup()
			var terminateErr error
			if identity.PID == cmd.Process.Pid {
				terminateCtx, cancelTerminate := context.WithTimeout(cleanupCtx, stopChildTerminateBound)
				terminateErr = c.stopReaper.TerminateIdentityWithin(terminateCtx, identity, stopChildKillGrace)
				cancelTerminate()
			}
			if identity.PID != cmd.Process.Pid || terminateErr != nil {
				if killErr := cmd.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
					terminateErr = errors.Join(terminateErr, fmt.Errorf("kill exact stop child: %w", killErr))
				}
			}
			select {
			case <-waitDone:
				settled = true
			case <-cleanupCtx.Done():
				returnErr = errors.Join(returnErr, errors.New("service: stop control child did not settle"), terminateErr)
				return
			}
			if terminateErr != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("service: exact stop child termination: %w", terminateErr))
			}
		}
		if settled && tracked != nil {
			untrackCtx, cancelUntrack := context.WithTimeout(context.WithoutCancel(opCtx), timing.untrack)
			defer cancelUntrack()
			if err := c.stopReaper.Untrack(untrackCtx, *tracked); err != nil {
				returnErr = errors.Join(returnErr, err)
			}
		}
	}()

	identityCtx, cancelIdentity := context.WithTimeout(opCtx, timing.identity)
	defer cancelIdentity()
	reports := bufio.NewReaderSize(reportReader, stopControlFrameLimit+1)
	if err := readStopFrame(identityCtx, reports, &identity); err != nil {
		return wire.StopResult{}, fmt.Errorf("service: read stop identity: %w", err)
	}
	roleTarget, err := canonicalExecutablePath(rolePath)
	if err != nil {
		return wire.StopResult{}, err
	}
	if identity.PID != cmd.Process.Pid || identity.Executable != roleTarget {
		return wire.StopResult{}, errors.New("service: stop child reported another process identity")
	}
	authorityCtx, cancelAuthority := context.WithTimeout(opCtx, timing.track)
	record, err := c.stopReaper.TrackStopControl(
		authorityCtx, identity, spec.Role, spec.RuntimeBuild, spec.RuntimeProtocol,
		spec.TargetProcessGeneration, string(spec.Intent), timing.authority,
	)
	cancelAuthority()
	if record.RecoveryClass == proc.RecoveryStopControl {
		tracked = &record
	}
	if err != nil {
		return wire.StopResult{}, fmt.Errorf("service: record stop control: %w", err)
	}
	if !stopAuthorityRetainsFullWindow(record, timing.now(), timing.authority) {
		revokeCtx, cancelRevoke := context.WithTimeout(context.WithoutCancel(opCtx), timing.identity)
		revoked, revokeErr := c.stopReaper.RevokeStopControl(revokeCtx, record)
		cancelRevoke()
		if revokeErr == nil {
			tracked = &revoked
			if timing.afterRevoke != nil {
				timing.afterRevoke()
			}
		}
		return wire.StopResult{}, errors.Join(
			errors.New("service: stop control authority does not retain the full window"), revokeErr,
		)
	}
	if _, err := releaseWriter.Write([]byte{1}); err != nil {
		return wire.StopResult{}, fmt.Errorf("service: release stop control: %w", err)
	}
	_ = releaseWriter.Close()

	operationDeadline = time.Now().Add(timing.child + timing.parentMargin)
	operationCtx, cancelOperation := context.WithDeadline(opCtx, operationDeadline)
	defer cancelOperation()
	childCtx, cancelChild := context.WithTimeout(operationCtx, timing.child)
	defer cancelChild()
	var report stopChildResult
	if err := readStopFrame(childCtx, reports, &report); err != nil {
		return wire.StopResult{}, fmt.Errorf("service: read stop result: %w", err)
	}
	var childWaitErr error
	select {
	case waitErr := <-waitDone:
		settled = true
		if waitErr != nil {
			childWaitErr = fmt.Errorf("service: stop control child: %w", waitErr)
		}
	case <-operationCtx.Done():
		return wire.StopResult{}, fmt.Errorf("service: wait for stop control child: %w", operationCtx.Err())
	}
	if report.Error != "" {
		return report.Result, errors.Join(errors.New(report.Error), childWaitErr)
	}
	if childWaitErr != nil {
		return wire.StopResult{}, childWaitErr
	}
	if report.Result.ProcessGeneration != spec.TargetProcessGeneration {
		return report.Result, errors.New("service: stop result targets another runtime generation")
	}
	if !report.Result.Stopped {
		return report.Result, ErrStopDeclined
	}
	return report.Result, nil
}

// RunStopControlChild reports its exact post-exec identity, waits for the
// durable-record release, and performs the opaque stop call.
func RunStopControlChild(ctx context.Context, config StopControlClientConfig) (wire.StopResult, error) {
	report := os.NewFile(stopReportFD, "daemonkit-stop-report")
	release := os.NewFile(stopReleaseFD, "daemonkit-stop-release")
	if report == nil || release == nil {
		return wire.StopResult{}, errors.New("service: stop control gates are absent")
	}
	defer report.Close()
	defer release.Close()
	identity, err := currentStopChildIdentity()
	if err != nil {
		return wire.StopResult{}, fmt.Errorf("service: capture stop child identity: %w", err)
	}
	if err := writeStopFrame(report, identity); err != nil {
		return wire.StopResult{}, fmt.Errorf("service: report stop identity: %w", err)
	}
	released := []byte{0}
	readDone := make(chan error, 1)
	go func() {
		_, readErr := io.ReadFull(release, released)
		readDone <- readErr
	}()
	select {
	case <-ctx.Done():
		return wire.StopResult{}, ctx.Err()
	case err := <-readDone:
		if err != nil || released[0] != 1 {
			return wire.StopResult{}, errors.New("service: stop control was not released")
		}
	}
	result, runErr := wire.RunStopControl(ctx, wire.StopControlConfig{
		Dial: config.Dial, WireBuild: config.WireBuild, RuntimeProtocol: config.RuntimeProtocol,
	})
	reportResult := stopChildResult{Result: result}
	if runErr != nil {
		reportResult.Error = runErr.Error()
	}
	if err := writeStopFrame(report, reportResult); err != nil {
		return result, fmt.Errorf("service: report stop result: %w", err)
	}
	return result, runErr
}

func validateStopControlSpec(spec StopControlSpec) (string, error) {
	if !filepath.IsAbs(spec.Executable) || filepath.Clean(spec.Executable) != spec.Executable {
		return "", errors.New("service: stop control executable must be exact and absolute")
	}
	exact, err := exactCommandPath(spec.Executable)
	if err != nil {
		return "", err
	}
	if spec.Role == "" || spec.RuntimeBuild == "" || spec.RuntimeProtocol <= 0 || spec.TargetProcessGeneration == "" {
		return "", errors.New("service: stop control identity is incomplete")
	}
	if spec.Intent != wire.StopIntentUpgrade && spec.Intent != wire.StopIntentRestart &&
		spec.Intent != wire.StopIntentUninstall {
		return "", errors.New("service: stop control intent is invalid")
	}
	return exact, nil
}

func currentStopChildIdentity() (proc.Identity, error) {
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
	return identity, err
}

func writeStopFrame(writer io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) > stopControlFrameLimit {
		return errors.New("service: stop control frame is too large")
	}
	payload = append(payload, '\n')
	for len(payload) != 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func readStopFrame(ctx context.Context, reader *bufio.Reader, value any) error {
	done := make(chan error, 1)
	go func() {
		payload, err := reader.ReadSlice('\n')
		if err != nil {
			if errors.Is(err, bufio.ErrBufferFull) {
				done <- errors.New("service: stop control frame is too large")
				return
			}
			done <- err
			return
		}
		if len(payload) > stopControlFrameLimit+1 {
			done <- errors.New("service: stop control frame is too large")
			return
		}
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(value); err != nil {
			done <- err
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			done <- errors.New("service: stop control frame has trailing data")
			return
		}
		done <- nil
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}
