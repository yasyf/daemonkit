package service

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/proc"
)

const commandOutputLimit = 1 << 20

var errCommandOutputLimit = errors.New("service: command output exceeded limit")

// taskRunner runs one bounded launchctl invocation under durable process
// ownership. It is the interim seam onto the root engine; at P3 it dies with
// service, when Ctx.Run is the one caller of the primitive.
type taskRunner func(context.Context, proc.Cmd) (proc.Result, error)

func runCombined(
	ctx context.Context,
	run taskRunner,
	path string,
	args ...string,
) (string, int, error) {
	if run == nil {
		return "", -1, errors.New("service: disposable task runner is required")
	}
	path, err := exactCommandPath(path)
	if err != nil {
		return "", -1, err
	}
	result, runErr := run(ctx, proc.Cmd{
		Path: path, Dir: filepath.Dir(path), Args: append([]string(nil), args...),
		MaxStdout: commandOutputLimit, MaxStderr: commandOutputLimit,
	})
	output := append(append([]byte(nil), result.Stdout...), result.Stderr...)
	var outputErr error
	if len(output) > commandOutputLimit {
		output = output[:commandOutputLimit]
		outputErr = errCommandOutputLimit
	}
	return string(output), result.Exit.Code, errors.Join(runErr, outputErr)
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

// controllerWorkerRuntime bounds concurrent launchctl runs with a semaphore —
// the caller's-semaphore pattern that replaces the pool's capacity — and owns
// every run through the shared process store.
type controllerWorkerRuntime struct {
	store *proc.Store
	slots chan struct{}
}

func newControllerWorkerRuntime(limit int, store *proc.Store) *controllerWorkerRuntime {
	return &controllerWorkerRuntime{store: store, slots: make(chan struct{}, limit)}
}

func (r *controllerWorkerRuntime) Run(ctx context.Context, c proc.Cmd) (proc.Result, error) {
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return proc.Result{}, ctx.Err()
	}
	defer func() { <-r.slots }()
	return daemonkit.Run(ctx, r.store, c)
}
