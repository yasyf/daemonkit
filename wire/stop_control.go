package wire

import (
	"errors"
	"os"

	"github.com/yasyf/daemonkit/proc"
)

const stopControlOp = Op("daemon.control.stop")

// StopResult records the exact process identity and runtime returned by Dispatch.
type StopResult struct {
	Process           proc.Identity
	ProcessGeneration proc.OwnerGeneration
	RuntimeBuild      string
	RuntimeProtocol   int
	Stopped           bool
}

type stopControlRequest struct {
	Version          uint16            `json:"version"`
	OperationID      string            `json:"operation_id"`
	StopSession      []byte            `json:"stop_session"`
	PreparationNonce []byte            `json:"preparation_nonce"`
	Target           stopControlTarget `json:"target"`
	RuntimeIdentity  RuntimeIdentity   `json:"runtime_identity"`
	RuntimeProtocol  int               `json:"runtime_protocol"`
}

type stopControlResponse struct {
	Version         uint16            `json:"version"`
	Target          stopControlTarget `json:"target"`
	RuntimeBuild    string            `json:"runtime_build"`
	RuntimeProtocol int               `json:"runtime_protocol"`
	Stopped         bool              `json:"stopped"`
}

type stopControlTarget struct {
	PID               int                  `json:"pid"`
	StartTime         string               `json:"start_time"`
	Boot              string               `json:"boot"`
	Comm              string               `json:"comm"`
	Executable        string               `json:"executable"`
	Audit             []byte               `json:"audit,omitempty"`
	ProcessGeneration proc.OwnerGeneration `json:"process_generation"`
}

func (t stopControlTarget) identity() (proc.Identity, error) {
	if t.PID <= 1 || t.StartTime == "" || t.Boot == "" || t.Executable == "" || t.ProcessGeneration == (proc.OwnerGeneration{}) {
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

func newStopControlTarget(identity proc.Identity, generation proc.OwnerGeneration) stopControlTarget {
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
