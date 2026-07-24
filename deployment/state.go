package deployment

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

const (
	activationIdentity   = "daemonkit.deployment.activation.v1"
	deactivationIdentity = "daemonkit.deployment.deactivation.v1"
	applyIdentity        = "daemonkit.deployment.apply.v1"
	uninstallIdentity    = "daemonkit.deployment.uninstall.v1"
	activationSchema     = 1
)

type applyPhase string

type uninstallPhase string

const (
	applyPrepared   applyPhase = "prepared"
	applyQuiesced   applyPhase = "quiesced"
	applySwapped    applyPhase = "swapped"
	applyActive     applyPhase = "active"
	applyRollback   applyPhase = "rollback"
	applyRolledBack applyPhase = "rolled_back"
)

const (
	uninstallPrepared uninstallPhase = "prepared"
	uninstallMoved    uninstallPhase = "moved"
	uninstallRemoved  uninstallPhase = "removed"
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

type storedApplyPlan struct {
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
	Generation            storedGeneration    `json:"generation"`
}

type applyReceiptWire struct {
	Identity            string                 `json:"identity"`
	Schema              int                    `json:"schema"`
	OperationID         string                 `json:"operation_id"`
	ConfigFingerprint   string                 `json:"config_fingerprint"`
	Phase               applyPhase             `json:"phase"`
	TargetPath          string                 `json:"target_path"`
	Candidate           storedGeneration       `json:"candidate"`
	Prior               *activationReceiptWire `json:"prior,omitempty"`
	ConsumerBuild       string                 `json:"consumer_build"`
	PolicyDigest        string                 `json:"policy_digest"`
	Plan                storedApplyPlan        `json:"plan"`
	ActivationOperation string                 `json:"activation_operation,omitempty"`
	RollbackOperation   string                 `json:"rollback_operation,omitempty"`
}

type uninstallReceiptWire struct {
	Identity              string             `json:"identity"`
	Schema                int                `json:"schema"`
	OperationID           string             `json:"operation_id"`
	DeactivationOperation string             `json:"deactivation_operation"`
	Phase                 uninstallPhase     `json:"phase"`
	Generation            storedGeneration   `json:"generation"`
	RuntimeProof          storedRuntimeProof `json:"runtime_proof"`
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

func storeApplyPlan(sourceRoot string, plan service.Plan) (storedApplyPlan, error) {
	agents := plan.Agents()
	for index := range agents {
		relative, err := filepath.Rel(sourceRoot, agents[index].Program)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return storedApplyPlan{}, fmt.Errorf("%w: service program %q is outside candidate source", ErrInvalidConfig, agents[index].Program)
		}
		agents[index].Program = filepath.ToSlash(relative)
	}
	payload, err := json.Marshal(agents)
	if err != nil {
		return storedApplyPlan{}, err
	}
	return storedApplyPlan{Agents: agents, Digest: fmt.Sprintf("%x", sha256.Sum256(payload))}, nil
}

func (stored storedApplyPlan) validate(targetRoot string) error {
	if !validDigestString(stored.Digest) || len(stored.Agents) == 0 {
		return ErrInstallState
	}
	payload, err := json.Marshal(stored.Agents)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(payload)) != stored.Digest {
		return ErrInstallState
	}
	for _, agent := range stored.Agents {
		if agent.Program == "" || filepath.IsAbs(agent.Program) || filepath.Clean(agent.Program) != agent.Program ||
			agent.Program == "." || agent.Program == ".." || strings.HasPrefix(agent.Program, ".."+string(filepath.Separator)) {
			return ErrInstallState
		}
		agent.Program = filepath.Join(targetRoot, filepath.FromSlash(agent.Program))
		if _, err := agent.Plist(); err != nil {
			return fmt.Errorf("%w: invalid declarative service plan: %w", ErrInstallState, err)
		}
	}
	return nil
}

func (stored storedApplyPlan) bindInstalled(targetRoot string) (service.Plan, error) {
	if err := stored.validate(targetRoot); err != nil {
		return service.Plan{}, err
	}
	agents := append([]service.Agent(nil), stored.Agents...)
	for index := range agents {
		agents[index].Program = filepath.Join(targetRoot, filepath.FromSlash(agents[index].Program))
	}
	return service.NewPlan(agents)
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

func readApply(path string) (*applyReceiptWire, error) {
	var receipt applyReceiptWire
	if err := readExactJSON(path, &receipt); err != nil {
		return nil, err
	}
	if err := receipt.validate(); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func readUninstall(path string) (*uninstallReceiptWire, error) {
	var receipt uninstallReceiptWire
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
	if err := receipt.Generation.validateStored(); err != nil {
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
	if err := receipt.Generation.validateStored(); err != nil {
		return err
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

func (receipt applyReceiptWire) validate() error {
	if receipt.Identity != applyIdentity || receipt.Schema != activationSchema || !validOperationID(receipt.OperationID) ||
		receipt.ConfigFingerprint == "" || receipt.TargetPath == "" || receipt.ConsumerBuild == "" ||
		!validDigestString(receipt.PolicyDigest) {
		return ErrInstallState
	}
	if receipt.Phase != applyPrepared && receipt.Phase != applyQuiesced && receipt.Phase != applySwapped &&
		receipt.Phase != applyActive && receipt.Phase != applyRollback && receipt.Phase != applyRolledBack {
		return ErrInstallState
	}
	if err := receipt.Candidate.validateStored(); err != nil {
		return err
	}
	if receipt.Prior != nil {
		if err := receipt.Prior.validate(); err != nil {
			return err
		}
		if receipt.Prior.Phase != activationActive || receipt.Prior.Generation.Path != receipt.TargetPath {
			return ErrInstallState
		}
	}
	if err := receipt.Plan.validate(receipt.TargetPath); err != nil {
		return err
	}
	if receipt.ActivationOperation != "" && !validOperationID(receipt.ActivationOperation) {
		return ErrInstallState
	}
	if receipt.RollbackOperation != "" && !validOperationID(receipt.RollbackOperation) {
		return ErrInstallState
	}
	if receipt.Phase == applyActive && receipt.ActivationOperation == "" {
		return ErrInstallState
	}
	return nil
}

func (receipt uninstallReceiptWire) validate() error {
	if receipt.Identity != uninstallIdentity || receipt.Schema != activationSchema ||
		!validOperationID(receipt.OperationID) || !validOperationID(receipt.DeactivationOperation) {
		return ErrInstallState
	}
	if receipt.Phase != uninstallPrepared && receipt.Phase != uninstallMoved && receipt.Phase != uninstallRemoved {
		return ErrInstallState
	}
	if err := receipt.Generation.validateStored(); err != nil {
		return err
	}
	if !receipt.RuntimeProof.Absent || !validDigestString(receipt.RuntimeProof.Digest) {
		return ErrInstallState
	}
	return nil
}

func (generation storedGeneration) validate() error {
	if err := validateCanonicalAppPath(generation.Path); err != nil {
		return errors.Join(ErrInstallState, err)
	}
	return generation.validateFields()
}

func (generation storedGeneration) validateStored() error {
	if generation.Path == "" || !filepath.IsAbs(generation.Path) || filepath.Clean(generation.Path) != generation.Path ||
		!strings.HasSuffix(filepath.Base(generation.Path), ".app") || filepath.Base(generation.Path) == ".app" {
		return ErrInstallState
	}
	return generation.validateFields()
}

func (generation storedGeneration) validateFields() error {
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
