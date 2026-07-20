package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const commandOutputLimit = 1 << 20

var errCommandOutputLimit = errors.New("service: command output exceeded limit")

func runCombined(
	ctx context.Context,
	runner supervise.TaskRunner,
	class proc.RecoveryClass,
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
	output := &boundedCommandOutput{remaining: commandOutputLimit}
	runErr := runner.Run(ctx, supervise.Task{
		RecoveryClass: class,
		Path:          path, Args: append([]string(nil), args...), Stdout: output, Stderr: output,
	})
	return string(output.bytes()), errors.Join(runErr, output.err())
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

type boundedCommandOutput struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	remaining int
	overflow  bool
}

func (b *boundedCommandOutput) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	accepted := min(len(payload), b.remaining)
	_, _ = b.buffer.Write(payload[:accepted])
	b.remaining -= accepted
	if accepted != len(payload) {
		b.overflow = true
	}
	return len(payload), nil
}

func (b *boundedCommandOutput) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buffer.Bytes()...)
}

func (b *boundedCommandOutput) err() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.overflow {
		return errCommandOutputLimit
	}
	return nil
}
