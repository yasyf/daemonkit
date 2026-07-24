package deployment

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/service"
)

var transactionFields = []string{
	"identity", "schema", "fingerprint", "operation_id", "artifact_fingerprint", "consumer_build", "policy_digest",
	"replacement_binding", "direction", "mode", "phase", "rollback_from", "stage",
	"prior_receipt", "candidate", "prior_plan", "next_plan", "activation", "prior_runtime_proof",
	"rollback_runtime_proof", "post_proof", "restore_proof",
	"candidate_readiness_proof", "prior_readiness_proof", "failure",
}

var deploymentReceiptFields = []string{
	"identity", "schema", "fingerprint", "artifact_fingerprint", "last_operation", "state", "current",
	"prior_plan", "plan", "activation_plan", "activation_operation", "failure",
}

func readDeploymentTransaction(path string) (*deploymentTransaction, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	object, err := exactObject(data, transactionFields)
	if err != nil {
		return nil, fmt.Errorf("%w: deployment transaction fields: %w", ErrInstallState, err)
	}
	if err := validateNestedTransactionObjects(object); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInstallState, err)
	}
	var tx deploymentTransaction
	if err := decodeDeploymentJSON(data, &tx); err != nil {
		return nil, fmt.Errorf("%w: decode deployment transaction: %w", ErrInstallState, err)
	}
	if err := tx.validate(); err != nil {
		return nil, err
	}
	return &tx, nil
}

func readDeploymentReceipt(path string) (*storedReceipt, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	object, err := exactObject(data, deploymentReceiptFields)
	if err != nil {
		return nil, fmt.Errorf("%w: deployment receipt fields: %w", ErrInstallState, err)
	}
	if err := validateNestedReceiptObjects(object); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInstallState, err)
	}
	var receipt storedReceipt
	if err := decodeDeploymentJSON(data, &receipt); err != nil {
		return nil, fmt.Errorf("%w: decode deployment receipt: %w", ErrInstallState, err)
	}
	if err := receipt.validate(); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func decodeDeploymentJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func validateNestedTransactionObjects(object map[string]json.RawMessage) error {
	if err := validateGenerationObject(object["candidate"]); err != nil {
		return fmt.Errorf("candidate: %w", err)
	}
	if err := validatePlanObject(object["prior_plan"]); err != nil {
		return fmt.Errorf("prior plan: %w", err)
	}
	if !bytes.Equal(object["prior_receipt"], []byte("null")) {
		prior, err := exactObject(object["prior_receipt"], deploymentReceiptFields)
		if err != nil {
			return fmt.Errorf("prior receipt: %w", err)
		}
		if err := validateNestedReceiptObjects(prior); err != nil {
			return fmt.Errorf("prior receipt: %w", err)
		}
	}
	if !bytes.Equal(object["next_plan"], []byte("null")) {
		if err := validatePlanObject(object["next_plan"]); err != nil {
			return fmt.Errorf("next plan: %w", err)
		}
	}
	if !bytes.Equal(object["activation"], []byte("null")) {
		if _, err := exactObject(object["activation"], []string{
			"operation_id", "binding", "epoch", "plan_digest",
		}); err != nil {
			return fmt.Errorf("activation: %w", err)
		}
	}
	for _, field := range []string{"post_proof", "restore_proof", "candidate_readiness_proof", "prior_readiness_proof"} {
		if bytes.Equal(object[field], []byte("null")) {
			continue
		}
		if _, err := exactObject(object[field], proofFields()); err != nil {
			return fmt.Errorf("%s: %w", field, err)
		}
	}
	for _, field := range []string{"prior_runtime_proof", "rollback_runtime_proof"} {
		if !bytes.Equal(object[field], []byte("null")) {
			if _, err := exactObject(object[field], []string{
				"operation_id", "role", "bundle_device", "bundle_inode", "bundle_cdhash", "bundle_digest",
				"absent", "process_generation", "digest",
			}); err != nil {
				return fmt.Errorf("%s: %w", field, err)
			}
		}
	}
	return nil
}

func validateNestedReceiptObjects(object map[string]json.RawMessage) error {
	if _, err := exactObject(object["last_operation"], []string{
		"operation_id", "consumer_build", "policy_digest", "replacement_binding",
	}); err != nil {
		return fmt.Errorf("last operation: %w", err)
	}
	if _, err := exactObject(object["activation_operation"], []string{
		"operation_id", "consumer_build", "policy_digest", "replacement_binding",
	}); err != nil {
		return fmt.Errorf("activation operation: %w", err)
	}
	if !bytes.Equal(object["current"], []byte("null")) {
		if err := validateGenerationObject(object["current"]); err != nil {
			return fmt.Errorf("current generation: %w", err)
		}
	}
	for _, field := range []string{"prior_plan", "plan", "activation_plan"} {
		if err := validatePlanObject(object[field]); err != nil {
			return fmt.Errorf("%s: %w", field, err)
		}
	}
	return nil
}

func generationFields() []string {
	return []string{"path", "version", "url", "sha256", "designated_requirement", "cdhash", "bundle_digest", "file_id"}
}

func planFields() []string { return []string{"agents", "digest"} }

func fileIDFields() []string { return []string{"device", "inode"} }

func serviceAgentFields() []string {
	return []string{
		"Label", "Program", "Args", "LogPath", "Env", "AssociatedBundleIdentifiers",
		"RestartPolicy", "StartInterval", "WatchPaths", "StartCalendarInterval",
		"ProcessType", "LimitLoadToSessionType",
	}
}

func validateGenerationObject(raw json.RawMessage) error {
	object, err := exactObject(raw, generationFields())
	if err != nil {
		return err
	}
	_, err = exactObject(object["file_id"], fileIDFields())
	return err
}

func validatePlanObject(raw json.RawMessage) error {
	object, err := exactObject(raw, planFields())
	if err != nil {
		return err
	}
	var agents []json.RawMessage
	if err := json.Unmarshal(object["agents"], &agents); err != nil {
		return err
	}
	for index, agent := range agents {
		if _, err := exactObject(agent, serviceAgentFields()); err != nil {
			return fmt.Errorf("agent %d: %w", index, err)
		}
	}
	return nil
}

func proofFields() []string {
	return []string{"operation_id", "role", "bundle_device", "bundle_inode", "bundle_cdhash", "bundle_digest", "plan_digest", "digest"}
}

func transactionSchemaFingerprint() string {
	descriptor := deploymentIdentity + "|" + strings.Join(transactionFields, ",") +
		"|generation:" + strings.Join(generationFields(), ",") +
		"|bundle_digest:sealed_tree_v1" +
		"|file_id:" + strings.Join(fileIDFields(), ",") +
		"|plan:" + strings.Join(planFields(), ",") +
		"|agent:" + strings.Join(serviceAgentFields(), ",") +
		"|activation:operation_id,binding,epoch,plan_digest" +
		"|runtime:operation_id,role,bundle_device,bundle_inode,bundle_cdhash,bundle_digest,absent,process_generation:owner_generation_v1_or_null,digest" +
		"|proof:" + strings.Join(proofFields(), ",") +
		"|receipt:" + receiptIdentity
	sum := sha256.Sum256([]byte(descriptor))
	return hex.EncodeToString(sum[:])
}

func receiptSchemaFingerprint() string {
	descriptor := receiptIdentity + "|" + strings.Join(deploymentReceiptFields, ",") +
		"|operation:operation_id,consumer_build,policy_digest,replacement_binding" +
		"|generation:" + strings.Join(generationFields(), ",") +
		"|bundle_digest:sealed_tree_v1" +
		"|file_id:" + strings.Join(fileIDFields(), ",") +
		"|plan:" + strings.Join(planFields(), ",") +
		"|agent:" + strings.Join(serviceAgentFields(), ",")
	sum := sha256.Sum256([]byte(descriptor))
	return hex.EncodeToString(sum[:])
}

func (tx deploymentTransaction) validate() error {
	if tx.Identity != deploymentIdentity || tx.Schema != deploymentSchema || tx.Fingerprint != deploymentFingerprint {
		return fmt.Errorf("%w: deployment transaction identity", ErrInstallState)
	}
	if !validOperationID(tx.OperationID) || !validDigestString(tx.ArtifactFingerprint) || tx.ConsumerBuild == "" ||
		!validDigestString(tx.PolicyDigest) || !validDigestString(tx.ReplacementBinding) {
		return fmt.Errorf("%w: deployment transaction operation or spec identity", ErrInstallState)
	}
	if tx.Direction != DirectionForward && tx.Direction != DirectionRollback {
		return fmt.Errorf("%w: deployment direction", ErrInstallState)
	}
	if tx.Mode != modeReplace && tx.Mode != modeReconfigure && tx.Mode != modeDeactivate {
		return fmt.Errorf("%w: deployment operation mode", ErrInstallState)
	}
	validStage := tx.Mode == modeReplace && filepath.Base(tx.Stage) == tx.Stage && strings.HasPrefix(tx.Stage, ".stage-")
	if tx.Mode != modeReplace {
		validStage = tx.Stage == ""
	}
	if !validPhase(tx.Phase) || !validStage {
		return fmt.Errorf("%w: deployment phase or stage", ErrInstallState)
	}
	if tx.Direction == DirectionForward && !validForwardPhase(tx.Phase) {
		return fmt.Errorf("%w: forward transaction has rollback-only phase %q", ErrInstallState, tx.Phase)
	}
	if tx.Direction == DirectionRollback && tx.Phase != PhaseRecoveryRequired &&
		tx.Phase != tx.RollbackFrom && !validRollbackPhase(tx.Phase) {
		return fmt.Errorf("%w: rollback transaction has invalid phase %q", ErrInstallState, tx.Phase)
	}
	if err := tx.Candidate.validate(); err != nil {
		return err
	}
	if tx.PriorReceipt != nil {
		if err := tx.PriorReceipt.validate(); err != nil {
			return fmt.Errorf("%w: prior receipt: %w", ErrInstallState, err)
		}
		if tx.PriorReceipt.Current == nil {
			return fmt.Errorf("%w: prior receipt has no current generation", ErrInstallState)
		}
		if tx.Mode != modeReplace && tx.ArtifactFingerprint != tx.PriorReceipt.ArtifactFingerprint {
			return fmt.Errorf("%w: no-swap transaction artifact differs from prior receipt", ErrInstallState)
		}
		if tx.Mode == modeDeactivate && tx.PolicyDigest != tx.PriorReceipt.LastOperation.PolicyDigest {
			return fmt.Errorf("%w: deactivate policy differs from prior receipt", ErrInstallState)
		}
		if tx.Mode != modeReplace && !reflect.DeepEqual(tx.Candidate, *tx.PriorReceipt.Current) {
			return fmt.Errorf("%w: no-swap candidate differs from prior receipt", ErrInstallState)
		}
	}
	if tx.Mode == modeDeactivate && (tx.PriorReceipt == nil || tx.PriorReceipt.State != DeploymentActive) {
		return fmt.Errorf("%w: deactivate transaction has no active prior receipt", ErrInstallState)
	}
	prior, err := restorePlan(tx.PriorPlan)
	if err != nil {
		return fmt.Errorf("%w: prior plan: %w", ErrInstallState, err)
	}
	if tx.PriorReceipt != nil {
		if err := validateDeploymentPlanPrograms(prior, *tx.PriorReceipt.Current); err != nil {
			return err
		}
		receiptPlan, err := restorePlan(tx.PriorReceipt.Plan)
		if err != nil || !samePlan(prior, receiptPlan) {
			return fmt.Errorf("%w: prior transaction plan differs from prior receipt", ErrInstallState)
		}
	} else if len(prior.Agents()) != 0 {
		return fmt.Errorf("%w: fresh transaction prior plan is not empty", ErrInstallState)
	}
	if tx.NextPlan != nil {
		next, err := restorePlan(*tx.NextPlan)
		if err != nil {
			return fmt.Errorf("%w: next plan: %w", ErrInstallState, err)
		}
		if err := validateDeploymentPlanPrograms(next, tx.Candidate); err != nil {
			return err
		}
	}
	if phaseAtLeast(tx.Phase, PhaseTargetPlanned) && tx.Direction == DirectionForward && tx.NextPlan == nil {
		return fmt.Errorf("%w: planned deployment has no next plan", ErrInstallState)
	}
	if tx.Direction == DirectionForward && tx.RollbackFrom != "" {
		return fmt.Errorf("%w: forward transaction has rollback source", ErrInstallState)
	}
	if tx.Direction == DirectionRollback && !validRollbackSource(tx.RollbackFrom) {
		return fmt.Errorf("%w: rollback transaction has invalid source", ErrInstallState)
	}
	forwardPhase := tx.Phase
	if tx.Direction == DirectionRollback {
		forwardPhase = tx.RollbackFrom
	}
	if tx.Mode == modeDeactivate && (forwardPhase == PhaseNamespaceCandidate || forwardPhase == PhaseCandidateProved) {
		return fmt.Errorf("%w: deactivate transaction has replacement-only phase %q", ErrInstallState, forwardPhase)
	}
	if tx.PostProof != nil && !tx.PostProof.matches(tx.OperationID, ProofPostInstall, tx.Candidate, "") {
		return fmt.Errorf("%w: post-install proof binding", ErrInstallState)
	}
	candidatePlanDigest := ""
	if tx.NextPlan != nil {
		candidatePlanDigest = tx.NextPlan.Digest
	}
	if tx.CandidateReadinessProof != nil &&
		!tx.CandidateReadinessProof.matches(tx.OperationID, ProofCandidateReady, tx.Candidate, candidatePlanDigest) {
		return fmt.Errorf("%w: candidate readiness proof binding", ErrInstallState)
	}
	if tx.PriorReadinessProof != nil {
		if tx.PriorReceipt == nil || tx.PriorReceipt.Current == nil ||
			!tx.PriorReadinessProof.matches(tx.OperationID, ProofPriorReady, *tx.PriorReceipt.Current, tx.PriorPlan.Digest) {
			return fmt.Errorf("%w: prior readiness proof binding", ErrInstallState)
		}
	}
	if tx.RestoreProof != nil {
		if tx.PriorReceipt == nil || tx.PriorReceipt.Current == nil ||
			!tx.RestoreProof.matches(tx.OperationID, ProofPriorRestore, *tx.PriorReceipt.Current, "") {
			return fmt.Errorf("%w: restore proof binding", ErrInstallState)
		}
	}
	if tx.PriorRuntimeProof != nil {
		if tx.PriorReceipt == nil || tx.PriorReceipt.Current == nil ||
			!tx.PriorRuntimeProof.matches(tx.OperationID, ProofPriorRuntime, *tx.PriorReceipt.Current) {
			return fmt.Errorf("%w: prior runtime proof binding", ErrInstallState)
		}
	}
	if tx.RollbackRuntimeProof != nil &&
		!tx.RollbackRuntimeProof.matches(tx.OperationID, ProofRollbackRuntime, tx.Candidate) {
		return fmt.Errorf("%w: rollback runtime proof binding", ErrInstallState)
	}
	if tx.Activation != nil && !tx.Activation.matches(tx.OperationID, tx.ReplacementBinding, tx.NextPlan) {
		return fmt.Errorf("%w: candidate activation binding", ErrInstallState)
	}
	if tx.Phase != PhaseRecoveryRequired {
		if err := tx.validatePhaseFacts(); err != nil {
			return err
		}
	}
	if !validFailure(tx.Failure) {
		return fmt.Errorf("%w: transaction failure text", ErrInstallState)
	}
	return nil
}

func (r storedReceipt) validate() error {
	if r.Identity != receiptIdentity || r.Schema != deploymentSchema || r.Fingerprint != receiptFingerprint {
		return fmt.Errorf("%w: deployment receipt identity", ErrInstallState)
	}
	if !validDigestString(r.ArtifactFingerprint) || !r.LastOperation.validate() || !r.ActivationOperation.validate() {
		return fmt.Errorf("%w: deployment receipt operation or spec identity", ErrInstallState)
	}
	if r.State != DeploymentActive && r.State != DeploymentInactive {
		return fmt.Errorf("%w: deployment receipt state", ErrInstallState)
	}
	if r.Current == nil {
		return fmt.Errorf("%w: completed receipt has no generation", ErrInstallState)
	}
	if r.Current != nil {
		if err := r.Current.validate(); err != nil {
			return err
		}
	}
	plans := make(map[string]service.Plan, 3)
	for name, plan := range map[string]storedPlan{"prior": r.PriorPlan, "current": r.Plan, "activation": r.ActivationPlan} {
		restored, err := restorePlan(plan)
		if err != nil {
			return fmt.Errorf("%w: receipt %s plan: %w", ErrInstallState, name, err)
		}
		plans[name] = restored
		if err := validateDeploymentPlanPrograms(restored, *r.Current); err != nil {
			return fmt.Errorf("%w: receipt %s plan programs: %w", ErrInstallState, name, err)
		}
	}
	if r.State == DeploymentActive && !samePlan(plans["current"], plans["activation"]) {
		return fmt.Errorf("%w: active receipt plan differs from activation plan", ErrInstallState)
	}
	if r.State == DeploymentActive && !reflect.DeepEqual(r.LastOperation, r.ActivationOperation) {
		return fmt.Errorf("%w: active receipt operation differs from activation operation", ErrInstallState)
	}
	if r.State == DeploymentInactive {
		empty, _ := service.NewPlan(nil)
		if !samePlan(plans["current"], empty) {
			return fmt.Errorf("%w: inactive receipt service plan is not empty", ErrInstallState)
		}
		if !samePlan(plans["prior"], plans["activation"]) {
			return fmt.Errorf("%w: inactive receipt prior plan differs from activation plan", ErrInstallState)
		}
		if reflect.DeepEqual(r.LastOperation, r.ActivationOperation) {
			return fmt.Errorf("%w: inactive receipt retirement operation equals activation operation", ErrInstallState)
		}
	}
	if r.Failure != "" {
		return fmt.Errorf("%w: completed receipt retains failure text", ErrInstallState)
	}
	return nil
}

func validateDeploymentPlanPrograms(plan service.Plan, generation storedGeneration) error {
	contents := filepath.Join(generation.Path, "Contents")
	for _, agent := range plan.Agents() {
		program := agent.Program
		if program == "" || !filepath.IsAbs(program) || filepath.Clean(program) != program || !within(contents, program) {
			return fmt.Errorf("%w: service program %q is not an exact path inside %s", ErrInstallState, program, contents)
		}
	}
	return nil
}

func (o storedOperation) validate() bool {
	return validOperationID(o.OperationID) && o.ConsumerBuild != "" &&
		validDigestString(o.PolicyDigest) && validDigestString(o.ReplacementBinding)
}

func (g storedGeneration) validate() error {
	if !filepath.IsAbs(g.Path) || filepath.Clean(g.Path) != g.Path || g.Version == "" ||
		g.DesignatedRequirement == "" || !validCDHash(g.CDHash) || !validDigestString(g.BundleDigest) ||
		g.FileID.Device == "" || g.FileID.Inode == "" {
		return fmt.Errorf("%w: canonical generation", ErrInstallState)
	}
	parsed, err := url.Parse(g.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("%w: canonical generation URL", ErrInstallState)
	}
	if len(g.SHA256) != sha256HexLength {
		return fmt.Errorf("%w: canonical generation SHA256", ErrInstallState)
	}
	if _, err := hex.DecodeString(g.SHA256); err != nil {
		return fmt.Errorf("%w: canonical generation SHA256", ErrInstallState)
	}
	return nil
}

func validCDHash(value string) bool {
	if strings.ToLower(value) != value || (len(value) != 40 && len(value) != 64) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

const sha256HexLength = 64

func (p storedProof) matches(operationID string, role ProofRole, generation storedGeneration, planDigest string) bool {
	return p.OperationID == operationID && p.Role == role && p.BundleDevice == generation.FileID.Device &&
		p.BundleInode == generation.FileID.Inode && p.BundleCDHash == generation.CDHash &&
		p.BundleDigest == generation.BundleDigest && p.PlanDigest == planDigest &&
		(planDigest == "" || validDigestString(planDigest)) && validDigestString(p.Digest)
}

func (p storedRuntimeProof) matches(operationID string, role ProofRole, generation storedGeneration) bool {
	return p.OperationID == operationID && p.Role == role && p.BundleDevice == generation.FileID.Device &&
		p.BundleInode == generation.FileID.Inode && p.BundleCDHash == generation.CDHash &&
		p.BundleDigest == generation.BundleDigest && p.Absent == (p.ProcessGeneration == nil) &&
		validDigestString(p.Digest)
}

func (a storedActivation) matches(operationID, binding string, plan *storedPlan) bool {
	return plan != nil && a.OperationID == operationID && a.Binding == binding && a.Epoch > 0 &&
		a.PlanDigest == plan.Digest && validDigestString(a.Binding) && validDigestString(a.PlanDigest)
}

func validDigestString(value string) bool {
	if len(value) != sha256HexLength || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

const failureLimit = 4096

func validFailure(value string) bool { return len(value) <= failureLimit && utf8.ValidString(value) }

func boundedFailure(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) <= failureLimit {
		return value
	}
	value = value[:failureLimit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func validOperationID(value string) bool {
	if len(value) != 32 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validPhase(phase Phase) bool {
	switch phase {
	case PhasePrepared, PhasePriorQuiesced, PhaseNamespaceCandidate,
		PhaseCandidateProved, PhaseTargetPlanned, PhaseCandidateActivated, PhaseCandidateReady,
		PhaseReceiptCommitted, PhaseServiceCommitPending, PhaseCleanupComplete,
		PhaseRollbackQuiesced, PhasePriorRestored,
		PhasePriorProved, PhasePriorActivated, PhasePriorReady, PhaseRecoveryRequired:
		return true
	default:
		return false
	}
}

func validForwardPhase(phase Phase) bool {
	return phaseAtLeast(phase, PhasePrepared) || phase == PhaseRecoveryRequired
}

func validRollbackPhase(phase Phase) bool {
	switch phase {
	case PhaseRollbackQuiesced, PhasePriorRestored, PhasePriorProved, PhasePriorActivated,
		PhasePriorReady, PhaseReceiptCommitted, PhaseServiceCommitPending, PhaseCleanupComplete:
		return true
	default:
		return false
	}
}

func phaseAtLeast(got, want Phase) bool {
	order := map[Phase]int{
		PhasePrepared: 1, PhasePriorQuiesced: 2, PhaseNamespaceCandidate: 3,
		PhaseCandidateProved: 4, PhaseTargetPlanned: 5, PhaseCandidateActivated: 6,
		PhaseCandidateReady: 7, PhaseReceiptCommitted: 8, PhaseServiceCommitPending: 9,
		PhaseCleanupComplete: 10,
	}
	return order[got] >= order[want] && order[want] != 0
}

func validRollbackSource(phase Phase) bool {
	return phaseAtLeast(phase, PhasePrepared) && !phaseAtLeast(phase, PhaseReceiptCommitted)
}

func (tx deploymentTransaction) validatePhaseFacts() error {
	require := func(ok bool, fact string) error {
		if !ok {
			return fmt.Errorf("%w: phase %q lacks %s", ErrInstallState, tx.Phase, fact)
		}
		return nil
	}
	forwardFact := tx.Phase
	if tx.Direction == DirectionRollback {
		forwardFact = tx.RollbackFrom
	}
	if phaseAtLeast(forwardFact, PhasePriorQuiesced) && tx.PriorReceipt != nil &&
		tx.PriorReceipt.State == DeploymentActive {
		if err := require(tx.PriorRuntimeProof != nil, "prior runtime proof"); err != nil {
			return err
		}
	}
	if tx.Mode != modeDeactivate && phaseAtLeast(forwardFact, PhaseCandidateProved) {
		if err := require(tx.PostProof != nil, "post-install proof"); err != nil {
			return err
		}
	}
	if phaseAtLeast(forwardFact, PhaseTargetPlanned) {
		if err := require(tx.NextPlan != nil, "target plan"); err != nil {
			return err
		}
	}
	if phaseAtLeast(forwardFact, PhaseCandidateActivated) {
		if err := require(tx.Activation != nil, "candidate activation receipt"); err != nil {
			return err
		}
	}
	if tx.Direction == DirectionForward {
		if tx.Mode != modeDeactivate && phaseAtLeast(tx.Phase, PhaseCandidateReady) {
			return require(tx.CandidateReadinessProof != nil, "candidate readiness proof")
		}
		return nil
	}
	if tx.Phase == tx.RollbackFrom {
		return nil
	}
	if tx.Activation != nil && tx.Mode != modeDeactivate && rollbackAtLeast(tx.Phase, PhaseRollbackQuiesced) {
		if err := require(tx.RollbackRuntimeProof != nil, "candidate rollback runtime proof"); err != nil {
			return err
		}
	}
	if rollbackAtLeast(tx.Phase, PhasePriorProved) && tx.Mode == modeReplace && tx.PriorReceipt != nil {
		if err := require(tx.RestoreProof != nil, "prior restore proof"); err != nil {
			return err
		}
	}
	if rollbackAtLeast(tx.Phase, PhasePriorReady) && tx.PriorReceipt != nil &&
		tx.PriorReceipt.State == DeploymentActive {
		return require(tx.PriorReadinessProof != nil, "prior readiness proof")
	}
	return nil
}

func rollbackAtLeast(got, want Phase) bool {
	order := map[Phase]int{
		PhaseRollbackQuiesced: 1, PhasePriorRestored: 2, PhasePriorProved: 3,
		PhasePriorActivated: 4, PhasePriorReady: 5, PhaseReceiptCommitted: 6,
		PhaseServiceCommitPending: 7, PhaseCleanupComplete: 8,
	}
	return order[got] >= order[want] && order[want] != 0
}

func exactProgramPaths(plan service.Plan) ([]string, error) {
	agents := plan.Agents()
	paths := make([]string, 0, len(agents))
	seen := make(map[string]struct{}, len(agents))
	for _, agent := range agents {
		if agent.Program == "" || !filepath.IsAbs(agent.Program) || filepath.Clean(agent.Program) != agent.Program {
			return nil, errors.New("deployment: replacement plan requires exact absolute Agent.Program paths")
		}
		if _, ok := seen[agent.Program]; ok {
			continue
		}
		seen[agent.Program] = struct{}{}
		paths = append(paths, agent.Program)
	}
	return paths, nil
}
