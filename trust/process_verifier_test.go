package trust

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
)

func TestProcessVerifierUsesExactChildProtocol(t *testing.T) {
	runner := directVerifierRunner{}
	verifier := ProcessVerifier{
		Runner: runner, Executable: "/Applications/Holder.app/Contents/MacOS/Holder",
		Policy: Policy{},
	}
	if err := verifier.Check(t.Context(), wire.Peer{UID: os.Geteuid()}); err != nil {
		t.Fatalf("Check trusted peer: %v", err)
	}
	if err := verifier.Check(t.Context(), wire.Peer{UID: os.Geteuid() + 1}); !errors.Is(err, ErrUntrustedPeer) {
		t.Fatalf("Check foreign peer = %v, want ErrUntrustedPeer", err)
	}
}

func TestProcessVerifierPropagatesCancellationToRunner(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	called := false
	verifier := ProcessVerifier{
		Runner: verifierRunnerFunc(func(context.Context, supervise.Task) error {
			called = true
			return nil
		}),
		Executable: "/Applications/Holder.app/Contents/MacOS/Holder",
	}
	if err := verifier.Check(ctx, wire.Peer{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Check canceled = %v", err)
	}
	if called {
		t.Fatal("canceled verifier started a child")
	}
}

func TestRunVerifierChildHardCutsMalformedRequests(t *testing.T) {
	if recognized, err := RunVerifierChild([]string{"consumer-mode"}, os.Stdout); err != nil || recognized {
		t.Fatalf("consumer mode = %t, %v", recognized, err)
	}
	if recognized, err := RunVerifierChild([]string{verifierChildMode}, os.Stdout); err == nil || !recognized {
		t.Fatalf("missing request = %t, %v", recognized, err)
	}
	if recognized, err := RunVerifierChild([]string{verifierChildMode, "%%%"}, os.Stdout); err == nil || !recognized {
		t.Fatalf("invalid request = %t, %v", recognized, err)
	}
}

func TestProcessVerifierRejectsMalformedAndOversizedResponses(t *testing.T) {
	for _, test := range []struct {
		name string
		run  verifierRunnerFunc
		want string
	}{
		{
			name: "malformed",
			run: func(_ context.Context, task supervise.Task) error {
				_, err := task.Stdout.Write([]byte("not-json"))
				return err
			},
			want: "decode verifier response",
		},
		{
			name: "oversized",
			run: func(_ context.Context, task supervise.Task) error {
				_, err := task.Stdout.Write(bytes.Repeat([]byte("x"), maxVerifierResponse+1))
				return err
			},
			want: "response exceeded limit",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			verifier := ProcessVerifier{
				Runner: test.run, Executable: "/Applications/Holder.app/Contents/MacOS/Holder",
			}
			if err := verifier.Check(t.Context(), wire.Peer{}); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Check = %v, want %q", err, test.want)
			}
		})
	}
}

func TestProcessVerifierPreservesRunnerFailure(t *testing.T) {
	runnerErr := errors.New("runner failed closed")
	verifier := ProcessVerifier{
		Runner:     verifierRunnerFunc(func(context.Context, supervise.Task) error { return runnerErr }),
		Executable: "/Applications/Holder.app/Contents/MacOS/Holder",
	}
	if err := verifier.Check(t.Context(), wire.Peer{}); !errors.Is(err, runnerErr) {
		t.Fatalf("Check = %v, want runner failure", err)
	}
}

func TestProcessVerifierSupervisedCancellationReapsAndReusesDedicatedLane(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "verifier-child")
	script := `#!/bin/sh
if mkdir "$0.state" 2>/dev/null; then
    trap '' TERM
    while :; do
        sleep 3600 &
        wait $!
    done
fi
printf '{"protocol":1,"result":"trusted"}\n'
`
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	store := &proc.FileStore{Path: filepath.Join(directory, "workers.json")}
	reaper := &proc.Reaper{Store: store, Generation: "trust-test"}
	pool, err := supervise.NewPool(1, reaper)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := pool.Wait(ctx); err != nil {
			t.Errorf("wait for verifier pool: %v", err)
		}
	})
	verifier := ProcessVerifier{Runner: pool, Executable: executable}
	blockedCtx, cancel := context.WithCancel(t.Context())
	blocked := make(chan error, 1)
	go func() { blocked <- verifier.Check(blockedCtx, wire.Peer{UID: os.Geteuid()}) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(executable + ".state"); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("blocking verifier child did not enter")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Check = %v, want cancellation", err)
	}

	nextCtx, cancelNext := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelNext()
	if err := verifier.Check(nextCtx, wire.Peer{UID: os.Geteuid()}); err != nil {
		t.Fatalf("next Check after reaping: %v", err)
	}
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("verifier records after settled checks = %#v", records)
	}
}

type directVerifierRunner struct{}

func (directVerifierRunner) Run(_ context.Context, task supervise.Task) error {
	_, err := RunVerifierChild(task.Args, task.Stdout)
	return err
}

type verifierRunnerFunc func(context.Context, supervise.Task) error

func (f verifierRunnerFunc) Run(ctx context.Context, task supervise.Task) error { return f(ctx, task) }
