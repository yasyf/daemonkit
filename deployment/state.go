package deployment

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

const (
	activationIdentity   = "daemonkit.deployment.activation.v1"
	deactivationIdentity = "daemonkit.deployment.deactivation.v1"
	activationSchema     = 1
)

type activationPhase string

const (
	activationPrepared activationPhase = "prepared"
	activationActive   activationPhase = "active"
)

type deactivationPhase string

const (
	deactivationPrepared deactivationPhase = "prepared"
	deactivationInactive deactivationPhase = "inactive"
)

type storedGeneration struct {
	Path                  string `json:"path"`
	Version               string `json:"version"`
	TeamID                string `json:"team_id"`
	SigningIdentifier     string `json:"signing_identifier"`
	DesignatedRequirement string `json:"designated_requirement"`
	CDHash                string `json:"cdhash"`
	EntitlementsDigest    string `json:"entitlements_digest"`
	BundleDigest          string `json:"bundle_digest"`
	FileID                fileID `json:"file_id"`
}

type storedPlan struct {
	Agents []service.Agent `json:"agents"`
	Digest string          `json:"digest"`
}

type storedReadinessProof struct {
	RuntimeBuild      string               `json:"runtime_build"`
	ProcessGeneration proc.OwnerGeneration `json:"process_generation"`
	ResourceDigest    string               `json:"resource_digest"`
}

type storedRuntimeProof struct {
	Absent            bool                  `json:"absent"`
	ProcessGeneration *proc.OwnerGeneration `json:"process_generation,omitempty"`
	Digest            string                `json:"digest"`
}

type activationReceiptWire struct {
	Identity          string                `json:"identity"`
	Schema            int                   `json:"schema"`
	OperationID       string                `json:"operation_id"`
	ConfigFingerprint string                `json:"config_fingerprint"`
	ConsumerBuild     string                `json:"consumer_build"`
	PolicyDigest      string                `json:"policy_digest"`
	Phase             activationPhase       `json:"phase"`
	Generation        storedGeneration      `json:"generation"`
	Plan              storedPlan            `json:"plan"`
	Readiness         *storedReadinessProof `json:"readiness,omitempty"`
}

type deactivationReceiptWire struct {
	Identity              string              `json:"identity"`
	Schema                int                 `json:"schema"`
	OperationID           string              `json:"operation_id"`
	ActivationOperationID string              `json:"activation_operation_id"`
	ConsumerBuild         string              `json:"consumer_build"`
	PolicyDigest          string              `json:"policy_digest"`
	ActivationFingerprint string              `json:"activation_fingerprint"`
	Phase                 deactivationPhase   `json:"phase"`
	RuntimeProof          *storedRuntimeProof `json:"runtime_proof,omitempty"`
}

func storePlan(plan service.Plan) storedPlan {
	return storedPlan{Agents: plan.Agents(), Digest: plan.Digest().String()}
}

func restorePlan(stored storedPlan) (service.Plan, error) {
	digest, err := parsePlanDigest(stored.Digest)
	if err != nil {
		return service.Plan{}, err
	}
	return service.RestorePlan(stored.Agents, digest)
}

func parsePlanDigest(value string) (service.PlanDigest, error) {
	parsed, err := ParseSHA256(value)
	if err != nil {
		return service.PlanDigest{}, fmt.Errorf("%w: invalid service plan digest: %w", ErrInstallState, err)
	}
	return service.PlanDigest(parsed), nil
}

func readActivation(path string) (*activationReceiptWire, error) {
	var receipt activationReceiptWire
	if err := readExactJSON(path, &receipt); err != nil {
		return nil, err
	}
	if err := receipt.validate(); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func readDeactivation(path string) (*deactivationReceiptWire, error) {
	var receipt deactivationReceiptWire
	if err := readExactJSON(path, &receipt); err != nil {
		return nil, err
	}
	if err := receipt.validate(); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func readExactJSON(path string, value any) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("%w: decode %s: %w", ErrInstallState, path, err)
	}
	if decoder.More() {
		return fmt.Errorf("%w: trailing JSON in %s", ErrInstallState, path)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing JSON value in %s", ErrInstallState, path)
	}
	return nil
}

func writeExactJSON(path string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("deployment: encode state: %w", err)
	}
	if err := daemon.WriteFileDurable(path, append(payload, '\n'), 0o600); err != nil {
		return fmt.Errorf("deployment: persist state: %w", err)
	}
	return nil
}

func (receipt activationReceiptWire) validate() error {
	if receipt.Identity != activationIdentity || receipt.Schema != activationSchema ||
		!validOperationID(receipt.OperationID) || receipt.ConfigFingerprint == "" ||
		receipt.ConsumerBuild == "" || !validDigestString(receipt.PolicyDigest) {
		return ErrInstallState
	}
	if receipt.Phase != activationPrepared && receipt.Phase != activationActive {
		return ErrInstallState
	}
	if err := receipt.Generation.validate(); err != nil {
		return err
	}
	if _, err := restorePlan(receipt.Plan); err != nil {
		return fmt.Errorf("%w: %w", ErrInstallState, err)
	}
	if receipt.Phase == activationPrepared && receipt.Readiness != nil {
		return ErrInstallState
	}
	if receipt.Phase == activationActive {
		if receipt.Readiness == nil || receipt.Readiness.RuntimeBuild == "" ||
			receipt.Readiness.ProcessGeneration == (proc.OwnerGeneration{}) ||
			!validDigestString(receipt.Readiness.ResourceDigest) {
			return ErrInstallState
		}
	}
	return nil
}

func (receipt deactivationReceiptWire) validate() error {
	if receipt.Identity != deactivationIdentity || receipt.Schema != activationSchema ||
		!validOperationID(receipt.OperationID) || !validOperationID(receipt.ActivationOperationID) || receipt.ConsumerBuild == "" ||
		!validDigestString(receipt.PolicyDigest) || receipt.ActivationFingerprint == "" {
		return ErrInstallState
	}
	if receipt.Phase != deactivationPrepared && receipt.Phase != deactivationInactive {
		return ErrInstallState
	}
	if receipt.Phase == deactivationPrepared && receipt.RuntimeProof != nil {
		return ErrInstallState
	}
	if receipt.Phase == deactivationInactive {
		if receipt.RuntimeProof == nil || !receipt.RuntimeProof.Absent ||
			!validDigestString(receipt.RuntimeProof.Digest) {
			return ErrInstallState
		}
	}
	return nil
}

func (generation storedGeneration) validate() error {
	if err := validateCanonicalAppPath(generation.Path); err != nil {
		return errors.Join(ErrInstallState, err)
	}
	if generation.Version == "" || generation.TeamID == "" || generation.SigningIdentifier == "" ||
		generation.DesignatedRequirement == "" || !validCDHash(generation.CDHash) ||
		!validDigestString(generation.EntitlementsDigest) || !validDigestString(generation.BundleDigest) ||
		generation.FileID == (fileID{}) {
		return ErrInstallState
	}
	return nil
}

func validDigestString(value string) bool {
	_, err := ParseSHA256(value)
	return err == nil
}
