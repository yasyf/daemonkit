// Command peer is daemonkit's mixed-era conformance peer: one source compiled
// twice — once against the previous released tag, once against the working
// tree — so daemons and clients from different eras can be driven at each other.
//
//	peer serve -socket PATH -build BUILD -state DIR
//	peer dial  -socket PATH -build BUILD
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
)

type healthReport struct {
	WireBuild string `json:"wire_build"`
	Protocol  int    `json:"protocol"`
	PID       int    `json:"pid"`
}

type dialReport struct {
	SelfBuild string       `json:"self_build"`
	PeerBuild string       `json:"peer_build"`
	Protocol  uint16       `json:"protocol"`
	Health    healthReport `json:"health"`
	StopAcked bool         `json:"stop_acked"`
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
		fail(errors.New("usage: peer serve|dial -socket PATH -build BUILD"))
	}
	switch mode, args := os.Args[1], os.Args[2:]; mode {
	case "serve":
		fail(serve(args))
	case "dial":
		fail(dial(args))
	default:
		fail(fmt.Errorf("unknown mode %q", mode))
	}
}

func fail(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
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
		return fmt.Errorf("dial: handshake: %w", err)
	}
	defer client.Close()

	report := dialReport{SelfBuild: *build}
	identity := client.PeerWireIdentity()
	report.PeerBuild, report.Protocol = identity.WireBuild, identity.Protocol

	health, err := call(ctx, client, healthOp)
	if err != nil {
		return fmt.Errorf("dial: health: %w", err)
	}
	if err := json.Unmarshal(health, &report.Health); err != nil {
		return fmt.Errorf("dial: decode health: %w", err)
	}

	stopped, err := call(ctx, client, stopOp)
	if err != nil {
		return fmt.Errorf("dial: stop: %w", err)
	}
	var acknowledgement string
	if err := json.Unmarshal(stopped, &acknowledgement); err != nil {
		return fmt.Errorf("dial: decode stop: %w", err)
	}
	report.StopAcked = acknowledgement == "stopping"

	return json.NewEncoder(os.Stdout).Encode(report)
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
