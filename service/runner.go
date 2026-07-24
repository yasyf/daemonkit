package service

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

const commandOutputLimit = 1 << 20

var errCommandOutputLimit = errors.New("service: command output exceeded limit")

type taskRunner interface {
	Run(context.Context, worker.CommandRequest) (worker.CommandResult, error)
}

func runCombined(
	ctx context.Context,
	runner taskRunner,
	path string,
	args ...string,
) (string, error) {
	if runner == nil {
		return "", errors.New("service: disposable task runner is required")
	}
	path, err := exactCommandPath(path)
	if err != nil {
		return "", err
	}
	result, runErr := runner.Run(ctx, worker.CommandRequest{
		Path: path, Dir: filepath.Dir(path), Args: append([]string(nil), args...),
		TotalTimeout: controllerCloseBound,
	})
	output := append(append([]byte(nil), result.Stdout...), result.Stderr...)
	var outputErr error
	if len(output) > commandOutputLimit {
		output = output[:commandOutputLimit]
		outputErr = errCommandOutputLimit
	}
	return string(output), errors.Join(runErr, outputErr)
}

func exactCommandPath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		resolved, err := exec.LookPath(path)
		if err != nil {
			return "", fmt.Errorf("service: find command %q: %w", path, err)
		}
		path = resolved
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", fmt.Errorf("service: command path %q is not exact and absolute", path)
	}
	return path, nil
}

type controllerWorkerRuntime struct {
	pool      *worker.Pool
	claim     *worker.RuntimeClaim
	recovered bool
	activated bool
}

func newControllerWorkerRuntime(limit int, reaper *proc.Reaper) (*controllerWorkerRuntime, error) {
	pool, err := worker.NewPool(worker.Config{
		Capacity: limit, QueueCapacity: limit, MaxTotalRun: controllerCloseBound,
		MaxStdinBytes: 0, MaxStdoutBytes: commandOutputLimit, MaxStderrBytes: commandOutputLimit,
	}, reaper)
	if err != nil {
		return nil, err
	}
	claim, err := pool.ClaimRuntime(trust.VerifierWorkerBudgets())
	if err != nil {
		return nil, err
	}
	return &controllerWorkerRuntime{pool: pool, claim: claim}, nil
}

func (r *controllerWorkerRuntime) Start(ctx context.Context) error {
	if err := r.claim.Recover(ctx); err != nil {
		return err
	}
	r.recovered = true
	if err := r.claim.Activate(); err != nil {
		return err
	}
	r.activated = true
	return nil
}

func (r *controllerWorkerRuntime) Run(ctx context.Context, request worker.CommandRequest) (worker.CommandResult, error) {
	return r.pool.Run(ctx, request)
}

func (r *controllerWorkerRuntime) Close(ctx context.Context) error {
	if r.activated {
		return r.claim.Close(ctx)
	}
	if r.recovered {
		return r.claim.Release(ctx)
	}
	return worker.ErrRuntimeOwnership
}
