// Command precut is daemonkit's pre-cut mixed-era conformance peer: compiled
// only against the release the harness pins as the boundary, so the cut's
// daemons and clients can be driven at a real released binary rather than at a
// model of one. Every verdict it reports about a failure is produced by the
// released module's own errors.Is / errors.As classification.
//
//	precut serve       -socket PATH -build BUILD -state DIR
//	precut dial        -socket PATH -build BUILD
//	precut conformance
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
)

const (
	healthOp = wire.Op("mixedera.health")
	stopOp   = wire.Op("mixedera.stop")
	tenant   = "mixedera"
	era      = "precut"

	// exitTrustProbe and probeToken are the one start failure the harness
	// retries; every other failure leaves by the usual exit 1. The harness
	// requires both, because an exit status on its own is what any unrelated
	// wrapper dying at 69 would also present.
	exitTrustProbe = 69
	probeToken     = "mixedera-precut: trust-verifier-probe-deadline"
)

type healthReport struct {
	WireBuild string `json:"wire_build"`
	Protocol  int    `json:"protocol"`
	PID       int    `json:"pid"`
}

type report struct {
	Era          string       `json:"era"`
	Protocol     uint16       `json:"protocol"`
	PeerProtocol uint16       `json:"peer_protocol,omitempty"`
	Session      bool         `json:"session,omitempty"`
	Failure      string       `json:"failure,omitempty"`
	Detail       string       `json:"detail,omitempty"`
	SelfBuild    string       `json:"self_build,omitempty"`
	PeerBuild    string       `json:"peer_build,omitempty"`
	Health       healthReport `json:"health,omitzero"`
	StopAcked    bool         `json:"stop_acked,omitempty"`
}

type conformance struct {
	Era        string            `json:"era"`
	Protocol   uint16            `json:"protocol"`
	Mechanisms map[string]string `json:"mechanisms"`
}

func main() {
	// daemon.Runtime probes its trust verifier by exec'ing os.Executable();
	// without this dispatch the child re-enters main and forks without bound.
	if handled, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) < 2 {
		fail(errors.New("usage: precut serve|dial|conformance"))
	}
	switch mode, args := os.Args[1], os.Args[2:]; mode {
	case "serve":
		fail(serve(args))
	case "dial":
		fail(dial(args))
	case "conformance":
		fail(declare())
	default:
		fail(fmt.Errorf("unknown mode %q", mode))
	}
}

func declare() error {
	return json.NewEncoder(os.Stdout).Encode(conformance{
		Era: era, Protocol: wire.ProtocolVersion,
		Mechanisms: map[string]string{
			"frame-v1":                  "",
			"protocol-gate":             "",
			"session":                   "",
			"drain-sigterm":             "",
			"drain-preamble":            "predates the cut: a pre-cut server reads its first bytes as a frame length, so SIGTERM is the only repair channel that reaches a pre-cut incumbent",
			"drain-preamble-trust-gate": "predates the cut: there is no preamble to gate",
		},
	})
}

func fail(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
	if errors.Is(err, daemon.ErrTrustVerifierProbe) {
		fmt.Fprintln(os.Stderr, probeToken)
		os.Exit(exitTrustProbe)
	}
	os.Exit(1)
}

func ladder() (wire.Ladder, error) {
	return wire.NewLadder(
		map[wire.Op]time.Duration{healthOp: 15 * time.Second, stopOp: 15 * time.Second},
		map[wire.Op]time.Duration{healthOp: 30 * time.Second, stopOp: 30 * time.Second},
	)
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	socket := flags.String("socket", "", "unix socket path to own")
	build := flags.String("build", "", "wire build string presented in the handshake")
	state := flags.String("state", "", "directory for the runtime's durable stores")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *socket == "" || *build == "" || *state == "" {
		return errors.New("serve: -socket, -build, and -state are required")
	}
	deadlines, err := ladder()
	if err != nil {
		return fmt.Errorf("serve: ladder: %w", err)
	}
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
	})
	if err != nil {
		return fmt.Errorf("serve: trust policy: %w", err)
	}
	owner, err := proc.ProcessGeneration()
	if err != nil {
		return fmt.Errorf("serve: process generation: %w", err)
	}
	reaper := func(name string) *proc.Reaper {
		return &proc.Reaper{
			Store:      &proc.FileStore{Path: filepath.Join(*state, name)},
			Generation: owner, Grace: 50 * time.Millisecond, Settlement: 5 * time.Second,
		}
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 2, QueueCapacity: 2, MaxTotalRun: 30 * time.Second,
		MaxStdinBytes: 1 << 16, MaxStdoutBytes: 1 << 16, MaxStderrBytes: 1 << 16,
	}, reaper("workers.db"))
	if err != nil {
		return fmt.Errorf("serve: worker pool: %w", err)
	}
	children, err := proc.NewManager(2, reaper("children.db"))
	if err != nil {
		return fmt.Errorf("serve: child manager: %w", err)
	}

	stop := make(chan struct{})
	var stopOnce sync.Once
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		stopOnce.Do(func() { close(stop) })
	}()

	server := &wire.Server{WireBuild: *build, Ladder: deadlines, WriteTimeout: 15 * time.Second}
	server.Register(wire.HandlerSpec{
		Op: healthOp,
		Handler: func(context.Context, wire.Request) (any, error) {
			return healthReport{
				WireBuild: *build, Protocol: int(wire.ProtocolVersion), PID: os.Getpid(),
			}, nil
		},
	})
	server.Register(wire.HandlerSpec{
		Op: stopOp,
		Handler: func(context.Context, wire.Request) (any, error) {
			stopOnce.Do(func() { close(stop) })
			return "stopping", nil
		},
	})

	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: *socket, RuntimeBuild: *build, RuntimeProtocol: int(wire.ProtocolVersion),
		Wire: server, TrustPolicy: policy,
		StopControlStore: &proc.FileStore{Path: filepath.Join(*state, "stop.db")},
		Workers:          workers, Children: children, ShutdownTimeout: 20 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("serve: runtime: %w", err)
	}
	slot := daemon.NewPublicationSlot[string](runtime)
	activation, err := runtime.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("serve: begin: %w", err)
	}
	publication, err := slot.Stage(activation, *build)
	if err != nil {
		return fmt.Errorf("serve: stage: %w", err)
	}
	if err := activation.CommitReady(publication); err != nil {
		return fmt.Errorf("serve: commit ready: %w", err)
	}
	fmt.Println("READY")

	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		return fmt.Errorf("serve: close: %w", err)
	}
	return nil
}

func dial(args []string) error {
	flags := flag.NewFlagSet("dial", flag.ContinueOnError)
	socket := flags.String("socket", "", "unix socket path to dial")
	build := flags.String("build", "", "wire build string presented in the handshake")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *socket == "" || *build == "" {
		return errors.New("dial: -socket and -build are required")
	}
	deadlines, err := ladder()
	if err != nil {
		return fmt.Errorf("dial: ladder: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(*socket), WireBuild: *build,
		Role: trust.UnprotectedRole, Ladder: deadlines,
		HandshakeTimeout: 30 * time.Second, WriteTimeout: 15 * time.Second,
	})
	if err != nil {
		return emit(classify(*build, err))
	}
	defer client.Close()

	identity := client.PeerWireIdentity()
	result := report{
		Era: era, Protocol: wire.ProtocolVersion, PeerProtocol: identity.Protocol,
		SelfBuild: *build, PeerBuild: identity.WireBuild,
	}

	health, err := call(ctx, client, healthOp)
	if err != nil {
		return emit(classify(*build, err))
	}
	if err := json.Unmarshal(health, &result.Health); err != nil {
		return fmt.Errorf("dial: decode health: %w", err)
	}

	stopped, err := call(ctx, client, stopOp)
	if err != nil {
		return emit(classify(*build, err))
	}
	var acknowledgement string
	if err := json.Unmarshal(stopped, &acknowledgement); err != nil {
		return fmt.Errorf("dial: decode stop: %w", err)
	}
	result.StopAcked = acknowledgement == "stopping"
	result.Session = true

	return emit(result)
}

func classify(build string, err error) report {
	failed := report{Era: era, Protocol: wire.ProtocolVersion, SelfBuild: build, Detail: err.Error()}
	var rejection *wire.HandshakeRejectionError
	switch {
	case errors.Is(err, wire.ErrProtocolVersion):
		failed.Failure = "protocol-mismatch"
	case errors.Is(err, wire.ErrBuildMismatch), errors.As(err, &rejection):
		failed.Failure = "refused"
	case errors.Is(err, wire.ErrHandshake):
		failed.Failure = "malformed"
	default:
		failed.Failure = "transport"
	}
	return failed
}

func emit(r report) error {
	return json.NewEncoder(os.Stdout).Encode(r)
}

func call(ctx context.Context, client *wire.Client, op wire.Op) ([]byte, error) {
	result, err := client.Call(ctx, op, tenant, nil)
	if err != nil {
		return nil, err
	}
	if rejection := result.Rejection(); rejection != nil {
		return nil, rejection
	}
	if result.Response.Err != "" {
		return nil, errors.New(result.Response.Err)
	}
	return result.Response.Payload, nil
}
