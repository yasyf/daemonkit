package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/internal/runtimeauth"
	stopcontract "github.com/yasyf/daemonkit/internal/stopcontrol"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
)

const stopRuntimeRequestIdentity = "daemonkit.service.stop-runtime-request.v1"

var (
	// ErrStopRuntimeConflict means an operation ID names another canonical request.
	ErrStopRuntimeConflict = errors.New("service: stop runtime operation conflicts with durable intent")
	// ErrStopRuntimeUnsettled means exact target absence was not proved.
	ErrStopRuntimeUnsettled = errors.New("service: stop runtime target remains live")
)

// StopRuntimeRequest names one operation-idempotent exact runtime stop.
type StopRuntimeRequest struct {
	OperationID          string
	RuntimeClientConfig  wire.RuntimeClientConfig
	ExpectedRuntimeBuild string
	ControlRole          trust.PeerRole
}

// StopSettlement classifies the exact terminal stop proof.
type StopSettlement uint8

const (
	// StopSettlementGone proves the pinned PID/start/boot identity is absent or reused.
	StopSettlementGone StopSettlement = iota + 1
)

// StopReceiptDigest is the immutable canonical identity of one stop receipt.
type StopReceiptDigest [sha256.Size]byte

// StopReceipt is immutable durable proof that one exact runtime target is gone.
type StopReceipt struct {
	operationID          string
	requestDigest        [sha256.Size]byte
	expectedRuntimeBuild string
	controlRole          trust.PeerRole
	target               wire.RuntimeIdentity
	processRecordDigest  proc.RecordDigest
	settlement           StopSettlement
	digest               StopReceiptDigest
}

// OperationID returns the exact durable operation identifier.
func (r StopReceipt) OperationID() string { return r.operationID }

// Target returns the exact runtime identity pinned before dispatch.
func (r StopReceipt) Target() wire.RuntimeIdentity { return r.target }

// ProcessRecordDigest returns the exact stopped process-record identity.
func (r StopReceipt) ProcessRecordDigest() proc.RecordDigest { return r.processRecordDigest }

// Settlement returns the exact terminal settlement proof.
func (r StopReceipt) Settlement() StopSettlement { return r.settlement }

// Digest returns the immutable canonical receipt identity.
func (r StopReceipt) Digest() StopReceiptDigest { return r.digest }

type stopRuntimePrepared interface {
	Target() wire.RuntimeIdentity
	Process() proc.Identity
	RuntimeProtocol() int
	StopSession() proc.StopSessionID
	PreparationNonce() proc.StopPreparationNonce
	Dispatch(context.Context, string) (wire.StopResult, wire.Outcome, error)
	Close() error
}

type stopRuntimeIntent struct {
	OperationID          string
	RequestDigest        [sha256.Size]byte
	ExpectedRuntimeBuild string
	ControlRole          trust.PeerRole
	Target               wire.RuntimeIdentity
	Process              proc.Identity
	ProcessRecordDigest  proc.RecordDigest
	RuntimeProtocol      int
}

type stopRuntimeStateStore interface {
	LoadStopRuntime(context.Context, string) (*stopRuntimeIntent, *StopReceipt, error)
	PutStopRuntimeIntent(context.Context, stopRuntimeIntent) error
	PutStopRuntimeReceipt(context.Context, stopRuntimeIntent, StopReceipt) error
}

type stopRuntimeCrashPoint uint8

const (
	stopRuntimeCrashAfterIntent stopRuntimeCrashPoint = iota + 1
	stopRuntimeCrashAfterStop
	stopRuntimeCrashAfterAbsence
	stopRuntimeCrashBeforeCompletion
)

// StopRuntime durably stops one exact runtime and replays the same receipt.
func (c *Controller) StopRuntime(ctx context.Context, request StopRuntimeRequest) (StopReceipt, error) {
	if c == nil || c.stopReaper == nil {
		return StopReceipt{}, errors.New("service: stop runtime is unavailable")
	}
	if err := validateStopRuntimeRequest(request); err != nil {
		return StopReceipt{}, err
	}
	requestDigest, err := digestStopRuntimeRequest(request)
	if err != nil {
		return StopReceipt{}, err
	}
	store, ok := c.store.(stopRuntimeStateStore)
	if !ok {
		return StopReceipt{}, errors.New("service: durable stop runtime state is unavailable")
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return StopReceipt{}, err
	}
	defer finish()

	intent, receipt, err := store.LoadStopRuntime(opCtx, request.OperationID)
	if err != nil {
		return StopReceipt{}, err
	}
	if receipt != nil {
		if intent != nil || !stopRuntimeReceiptMatches(*receipt, request, requestDigest) {
			return StopReceipt{}, ErrStopRuntimeConflict
		}
		return *receipt, nil
	}
	if intent != nil {
		if !stopRuntimeRequestMatches(*intent, request, requestDigest) {
			return StopReceipt{}, ErrStopRuntimeConflict
		}
		gone, err := c.stopRuntimeGone(*intent)
		if err != nil {
			return StopReceipt{}, err
		}
		if gone {
			return c.completeStopRuntime(opCtx, store, *intent)
		}
	}

	prepared, err := c.prepareStopRuntime(opCtx, request.RuntimeClientConfig, request.ControlRole)
	if err != nil {
		return StopReceipt{}, err
	}
	defer prepared.Close()
	if prepared.Target().RuntimeBuild != request.ExpectedRuntimeBuild {
		return StopReceipt{}, fmt.Errorf(
			"service: stop runtime build got %q, want %q",
			prepared.Target().RuntimeBuild, request.ExpectedRuntimeBuild,
		)
	}
	processDigest, err := proc.NewRecordDigest(prepared.Process())
	if err != nil {
		return StopReceipt{}, err
	}
	preparedIntent := stopRuntimeIntent{
		OperationID: request.OperationID, RequestDigest: requestDigest,
		ExpectedRuntimeBuild: request.ExpectedRuntimeBuild, ControlRole: request.ControlRole,
		Target: prepared.Target(), Process: prepared.Process(), ProcessRecordDigest: processDigest,
		RuntimeProtocol: prepared.RuntimeProtocol(),
	}
	if intent != nil && !stopRuntimeIntentsEqual(*intent, preparedIntent) {
		return StopReceipt{}, errors.New("service: live runtime differs from durable stop target")
	}
	if intent == nil {
		if err := store.PutStopRuntimeIntent(opCtx, preparedIntent); err != nil {
			return StopReceipt{}, err
		}
		intent = &preparedIntent
		if err := c.stopRuntimeCrash(stopRuntimeCrashAfterIntent); err != nil {
			return StopReceipt{}, err
		}
	}

	caller, err := currentStopRuntimeIdentity()
	if err != nil {
		return StopReceipt{}, fmt.Errorf("service: capture stop caller: %w", err)
	}
	authority, err := c.stopReaper.TrackStopControl(
		opCtx, caller, string(request.ControlRole), request.OperationID,
		prepared.StopSession(), prepared.PreparationNonce(),
		intent.RuntimeProtocol, intent.Target.ProcessGeneration,
		stopcontract.AuthorityBound,
	)
	if err != nil {
		return StopReceipt{}, fmt.Errorf("service: arm stop authority: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(opCtx), 5*time.Second)
		defer cancel()
		_ = c.stopReaper.Untrack(cleanupCtx, authority)
	}()
	result, outcome, dispatchErr := prepared.Dispatch(opCtx, request.OperationID)
	if outcome == wire.PreSendFailure && dispatchErr != nil && opCtx.Err() == nil {
		result, outcome, dispatchErr = prepared.Dispatch(opCtx, request.OperationID)
	}
	if outcome == wire.PreSendFailure || outcome == wire.Rejected {
		if dispatchErr == nil {
			dispatchErr = errors.New("service: stop dispatch ended before delivery")
		}
		return StopReceipt{}, dispatchErr
	}
	if dispatchErr == nil && !result.Stopped {
		dispatchErr = errors.New("service: prepared runtime declined stop")
	}
	if dispatchErr == nil {
		if result.Process != intent.Process || result.ProcessGeneration != intent.Target.ProcessGeneration ||
			result.RuntimeBuild != intent.Target.RuntimeBuild || result.RuntimeProtocol != intent.RuntimeProtocol {
			dispatchErr = errors.New("service: stop result differs from durable intent")
		}
	}
	if err := c.stopRuntimeCrash(stopRuntimeCrashAfterStop); err != nil {
		return StopReceipt{}, err
	}
	if err := c.awaitStopRuntimeGone(opCtx, *intent); err != nil {
		return StopReceipt{}, errors.Join(dispatchErr, err)
	}
	if err := c.stopRuntimeCrash(stopRuntimeCrashAfterAbsence); err != nil {
		return StopReceipt{}, err
	}
	return c.completeStopRuntime(opCtx, store, *intent)
}

func validateStopRuntimeRequest(request StopRuntimeRequest) error {
	if request.OperationID == "" || strings.TrimSpace(request.OperationID) != request.OperationID ||
		len(request.OperationID) > 256 {
		return errors.New("service: stop runtime operation ID must be exact and non-empty")
	}
	if request.ExpectedRuntimeBuild == "" || request.ControlRole == "" {
		return errors.New("service: stop runtime build and control role are required")
	}
	if request.RuntimeClientConfig.Client.Dial == nil || request.RuntimeClientConfig.Client.WireBuild == "" {
		return errors.New("service: stop runtime client is incomplete")
	}
	return nil
}

func digestStopRuntimeRequest(request StopRuntimeRequest) ([sha256.Size]byte, error) {
	prepareServer, prepareClient, prepareDeadline := request.RuntimeClientConfig.Client.Ladder.Deadlines("daemon.control.stop.prepare")
	stopServer, stopClient, stopDeadline := request.RuntimeClientConfig.Client.Ladder.Deadlines("daemon.control.stop")
	client := request.RuntimeClientConfig.Client
	payload, err := json.Marshal(struct {
		Identity, OperationID, ExpectedRuntimeBuild, ControlRole, WireBuild string
		MaxFrame, OutboundQueue, StreamQueue, EventQueue                    int
		HandshakeTimeout, WriteTimeout, CancelSettlementTimeout             int64
		NoProgressTimeout                                                   int64
		PrepareServer, PrepareClient, StopServer, StopClient                int64
		PrepareDeadline, StopDeadline                                       bool
	}{
		Identity: stopRuntimeRequestIdentity, OperationID: request.OperationID,
		ExpectedRuntimeBuild: request.ExpectedRuntimeBuild, ControlRole: string(request.ControlRole),
		WireBuild: client.WireBuild, MaxFrame: client.MaxFrame, OutboundQueue: client.OutboundQueue,
		StreamQueue: client.StreamQueue, EventQueue: client.EventQueue,
		HandshakeTimeout: int64(client.HandshakeTimeout), WriteTimeout: int64(client.WriteTimeout),
		CancelSettlementTimeout: int64(client.CancelSettlementTimeout),
		NoProgressTimeout:       int64(request.RuntimeClientConfig.NoProgressTimeout),
		PrepareServer:           int64(prepareServer), PrepareClient: int64(prepareClient),
		StopServer: int64(stopServer), StopClient: int64(stopClient),
		PrepareDeadline: prepareDeadline, StopDeadline: stopDeadline,
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("service: encode stop runtime request: %w", err)
	}
	return sha256.Sum256(payload), nil
}

func stopRuntimeRequestMatches(intent stopRuntimeIntent, request StopRuntimeRequest, digest [sha256.Size]byte) bool {
	return intent.OperationID == request.OperationID && intent.RequestDigest == digest &&
		intent.ExpectedRuntimeBuild == request.ExpectedRuntimeBuild && intent.ControlRole == request.ControlRole
}

func stopRuntimeReceiptMatches(receipt StopReceipt, request StopRuntimeRequest, digest [sha256.Size]byte) bool {
	return receipt.operationID == request.OperationID && receipt.requestDigest == digest &&
		receipt.expectedRuntimeBuild == request.ExpectedRuntimeBuild && receipt.controlRole == request.ControlRole
}

func stopRuntimeIntentsEqual(left, right stopRuntimeIntent) bool {
	return left == right
}

func (c *Controller) prepareStopRuntime(
	ctx context.Context,
	config wire.RuntimeClientConfig,
	controlRole trust.PeerRole,
) (stopRuntimePrepared, error) {
	if c.stopRuntimePrepare != nil {
		return c.stopRuntimePrepare(ctx, config, controlRole)
	}
	return wire.PrepareStop(ctx, config, controlRole, runtimeauth.NewStopControlAuthority())
}

func (c *Controller) stopRuntimeGone(intent stopRuntimeIntent) (bool, error) {
	probe := proc.Probe
	if c.stopRuntimeProbe != nil {
		probe = c.stopRuntimeProbe
	}
	identity, err := probe(intent.Process.PID)
	if errors.Is(err, proc.ErrNoProcess) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("service: probe stop target: %w", err)
	}
	return identity.StartTime != intent.Process.StartTime || identity.Boot != intent.Process.Boot, nil
}

func (c *Controller) awaitStopRuntimeGone(ctx context.Context, intent stopRuntimeIntent) error {
	settleCtx, cancel := context.WithTimeout(ctx, stopcontract.ChildSettlementBound)
	defer cancel()
	for {
		gone, err := c.stopRuntimeGone(intent)
		if err != nil {
			return err
		}
		if gone {
			return nil
		}
		wait := time.NewTimer(10 * time.Millisecond)
		select {
		case <-settleCtx.Done():
			if !wait.Stop() {
				<-wait.C
			}
			return errors.Join(ErrStopRuntimeUnsettled, settleCtx.Err())
		case <-wait.C:
		}
	}
}

func (c *Controller) completeStopRuntime(
	ctx context.Context,
	store stopRuntimeStateStore,
	intent stopRuntimeIntent,
) (StopReceipt, error) {
	receipt, err := newStopReceipt(intent)
	if err != nil {
		return StopReceipt{}, err
	}
	if err := store.PutStopRuntimeReceipt(ctx, intent, receipt); err != nil {
		return StopReceipt{}, err
	}
	if err := c.stopRuntimeCrash(stopRuntimeCrashBeforeCompletion); err != nil {
		return StopReceipt{}, err
	}
	return receipt, nil
}

func newStopReceipt(intent stopRuntimeIntent) (StopReceipt, error) {
	digest, err := digestStopReceipt(
		intent.OperationID, intent.RequestDigest, intent.ExpectedRuntimeBuild, intent.ControlRole,
		intent.Target, intent.ProcessRecordDigest, StopSettlementGone,
	)
	if err != nil {
		return StopReceipt{}, err
	}
	return StopReceipt{
		operationID: intent.OperationID, requestDigest: intent.RequestDigest,
		expectedRuntimeBuild: intent.ExpectedRuntimeBuild, controlRole: intent.ControlRole,
		target:              intent.Target,
		processRecordDigest: intent.ProcessRecordDigest, settlement: StopSettlementGone, digest: digest,
	}, nil
}

func digestStopReceipt(
	operationID string,
	requestDigest [sha256.Size]byte,
	expectedRuntimeBuild string,
	controlRole trust.PeerRole,
	target wire.RuntimeIdentity,
	processDigest proc.RecordDigest,
	settlement StopSettlement,
) (StopReceiptDigest, error) {
	payload, err := json.Marshal(struct {
		Identity             string               `json:"identity"`
		OperationID          string               `json:"operation_id"`
		RequestDigest        string               `json:"request_digest"`
		ExpectedRuntimeBuild string               `json:"expected_runtime_build"`
		ControlRole          trust.PeerRole       `json:"control_role"`
		Target               wire.RuntimeIdentity `json:"target"`
		ProcessRecordDigest  string               `json:"process_record_digest"`
		Settlement           StopSettlement       `json:"settlement"`
	}{
		Identity: stopRuntimeReceiptIdentity, OperationID: operationID, Target: target,
		RequestDigest: hex.EncodeToString(requestDigest[:]), ExpectedRuntimeBuild: expectedRuntimeBuild,
		ControlRole: controlRole, ProcessRecordDigest: hex.EncodeToString(processDigest[:]), Settlement: settlement,
	})
	if err != nil {
		return StopReceiptDigest{}, fmt.Errorf("service: encode stop receipt: %w", err)
	}
	return StopReceiptDigest(sha256.Sum256(payload)), nil
}

func currentStopRuntimeIdentity() (proc.Identity, error) {
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

func (c *Controller) stopRuntimeCrash(point stopRuntimeCrashPoint) error {
	if c.stopRuntimeCrashHook == nil {
		return nil
	}
	return c.stopRuntimeCrashHook(point)
}
