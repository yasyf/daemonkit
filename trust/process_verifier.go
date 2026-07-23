package trust

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	peer "github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/worker"
)

const (
	verifierChildMode     = "--daemonkit-trust-verifier-v1"
	verifierProtocol      = 1
	maxVerifierPayload    = 16 << 10
	maxVerifierResponse   = 4 << 10
	verifierResultTrusted = "trusted"
	verifierResultDenied  = "untrusted"
	verifierResultAbsent  = "no_verifier"
	verifierResultFailed  = "failed"
)

// ProcessVerifier runs code-identity verification in a disposable child.
type ProcessVerifier struct {
	Runner interface {
		RunVerifier(context.Context, worker.CommandRequest) (worker.CommandResult, error)
	}
	Executable string
	Policy     Policy
}

// Check verifies peer in a child that is killed and reaped when ctx expires.
func (v ProcessVerifier) Check(ctx context.Context, peer peer.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v.Runner == nil {
		return errors.New("trust: verifier task runner is required")
	}
	if strings.TrimSpace(v.Executable) == "" {
		return errors.New("trust: verifier executable is required")
	}
	if v.Policy.Requirement != nil {
		if err := v.Policy.Requirement.validate(); err != nil {
			return err
		}
	}
	payload, err := json.Marshal(verifierRequest{
		Protocol: verifierProtocol, Peer: peer, Requirement: v.Policy.Requirement,
	})
	if err != nil {
		return fmt.Errorf("trust: encode verifier request: %w", err)
	}
	if len(payload) > maxVerifierPayload {
		return fmt.Errorf("trust: verifier request is %d bytes, maximum is %d", len(payload), maxVerifierPayload)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("trust: verifier deadline is required")
	}
	result, err := v.Runner.RunVerifier(ctx, worker.CommandRequest{
		Path: v.Executable, Dir: filepath.Dir(v.Executable),
		Args:         []string{verifierChildMode, base64.RawURLEncoding.EncodeToString(payload)},
		TotalTimeout: time.Until(deadline),
	})
	if err != nil {
		return fmt.Errorf("trust: run verifier child: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var response verifierResponse
	if len(result.Stdout) > maxVerifierResponse {
		return fmt.Errorf("trust: verifier response is %d bytes, maximum is %d", len(result.Stdout), maxVerifierResponse)
	}
	if err := json.Unmarshal(result.Stdout, &response); err != nil {
		return fmt.Errorf("trust: decode verifier response: %w", err)
	}
	if response.Protocol != verifierProtocol {
		return fmt.Errorf("trust: verifier response protocol %d is not %d", response.Protocol, verifierProtocol)
	}
	switch response.Result {
	case verifierResultTrusted:
		if response.Error != "" {
			return errors.New("trust: trusted verifier response included an error")
		}
		return nil
	case verifierResultDenied:
		return fmt.Errorf("%w: %s", ErrUntrustedPeer, response.Error)
	case verifierResultAbsent:
		return fmt.Errorf("%w: %s", ErrNoVerifier, response.Error)
	case verifierResultFailed:
		return fmt.Errorf("trust: verifier child: %s", response.Error)
	default:
		return fmt.Errorf("trust: unknown verifier result %q", response.Result)
	}
}

// RunVerifierChild recognizes and executes one exact verifier-child invocation.
func RunVerifierChild(arguments []string, stdout io.Writer) (bool, error) {
	if len(arguments) == 0 || arguments[0] != verifierChildMode {
		return false, nil
	}
	if len(arguments) != 2 {
		return true, errors.New("trust: verifier child requires one request")
	}
	payload, err := base64.RawURLEncoding.DecodeString(arguments[1])
	if err != nil {
		return true, fmt.Errorf("trust: decode verifier child request: %w", err)
	}
	if len(payload) > maxVerifierPayload {
		return true, fmt.Errorf("trust: verifier child request is %d bytes, maximum is %d", len(payload), maxVerifierPayload)
	}
	var request verifierRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return true, fmt.Errorf("trust: parse verifier child request: %w", err)
	}
	if request.Protocol != verifierProtocol {
		return true, fmt.Errorf("trust: verifier child protocol %d is not %d", request.Protocol, verifierProtocol)
	}
	if stdout == nil {
		return true, errors.New("trust: verifier child stdout is required")
	}
	checkErr := (Policy{Requirement: request.Requirement}).Check(request.Peer)
	response := verifierResponse{Protocol: verifierProtocol, Result: verifierResultTrusted}
	switch {
	case checkErr == nil:
	case errors.Is(checkErr, ErrUntrustedPeer):
		response.Result, response.Error = verifierResultDenied, checkErr.Error()
	case errors.Is(checkErr, ErrNoVerifier):
		response.Result, response.Error = verifierResultAbsent, checkErr.Error()
	default:
		response.Result, response.Error = verifierResultFailed, checkErr.Error()
	}
	if err := json.NewEncoder(stdout).Encode(response); err != nil {
		return true, fmt.Errorf("trust: write verifier child response: %w", err)
	}
	return true, nil
}

type verifierRequest struct {
	Protocol    int           `json:"protocol"`
	Peer        peer.Identity `json:"peer"`
	Requirement *Requirement  `json:"requirement,omitempty"`
}

type verifierResponse struct {
	Protocol int    `json:"protocol"`
	Result   string `json:"result"`
	Error    string `json:"error,omitempty"`
}
