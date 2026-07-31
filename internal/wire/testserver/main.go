// Command wire-test-server is the Swift integration rig's Go entrypoint: a
// real internal/wire server on a unix socket, a fixed op catalog covering what
// the Swift client suites exercise, and a -phases script driving the stub
// runtime's lifecycle. It prints READY <socket> once listening and exits on
// stdin EOF or SIGTERM, so orphan cleanup is unconditional even if the test
// runner dies.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/trust"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "wire-test-server: %v\n", err)
		os.Exit(1)
	}
}

type phaseStep struct {
	phase wire.Phase
	hold  time.Duration
}

func run() error {
	socket := flag.String("socket", "", "unix socket path (required)")
	schema := flag.String("schema", "", "the server's own schema digest (required)")
	var accepted []string
	flag.Func("accept", "additional accepted schema digest (repeatable)", func(v string) error {
		accepted = append(accepted, v)
		return nil
	})
	phases := flag.String("phases", "ready", `phase script: "ready" | "starting:200ms,ready" | "ready,draining:1s"`)
	controlTeam := flag.String("control-team", "", "Trust.Control team identifier")
	controlIdentifier := flag.String("control-identifier", "", "Trust.Control signing identifier")
	flag.Parse()
	if *socket == "" || *schema == "" {
		return fmt.Errorf("-socket and -schema are required")
	}
	script, err := parsePhases(*phases)
	if err != nil {
		return err
	}

	rt := wiretest.NewStubRuntime()
	rt.SetHandle(handle)
	rt.SetPhase(script[0].phase, nil)

	cfg := wire.Config{Schemas: append(wire.Schemas{*schema}, accepted...)}
	if *controlTeam != "" {
		cfg.Trust.Control = &trust.Requirement{TeamID: *controlTeam, SigningIdentifier: *controlIdentifier}
	}
	server, err := wire.NewServer(rt, cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ln, err := net.Listen("unix", *socket)
	if err != nil {
		return fmt.Errorf("listen %s: %w", *socket, err)
	}
	fmt.Printf("READY %s\n", *socket)

	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	go runPhases(ctx, rt, script)

	return server.Serve(ctx, ln)
}

func parsePhases(script string) ([]phaseStep, error) {
	names := map[string]wire.Phase{
		"starting": wire.PhaseStarting,
		"ready":    wire.PhaseReady,
		"draining": wire.PhaseDraining,
		"failed":   wire.PhaseFailed,
	}
	var steps []phaseStep
	for _, entry := range strings.Split(script, ",") {
		name, hold, _ := strings.Cut(strings.TrimSpace(entry), ":")
		phase, ok := names[name]
		if !ok {
			return nil, fmt.Errorf("unknown phase %q", name)
		}
		step := phaseStep{phase: phase}
		if hold != "" {
			d, err := time.ParseDuration(hold)
			if err != nil {
				return nil, fmt.Errorf("phase %q hold: %w", name, err)
			}
			step.hold = d
		}
		steps = append(steps, step)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("empty phase script")
	}
	return steps, nil
}

func runPhases(ctx context.Context, rt *wiretest.StubRuntime, script []phaseStep) {
	for i, step := range script {
		if i > 0 {
			rt.SetPhase(step.phase, nil)
		}
		if step.hold == 0 {
			continue
		}
		select {
		case <-time.After(step.hold):
		case <-ctx.Done():
			return
		}
	}
}

func handle(ctx context.Context, req wire.Request) (any, error) {
	switch req.Op {
	case "test.echo.v1":
		return echoValue(req.Payload), nil
	case "test.sleep.v1":
		var body struct {
			Milliseconds int `json:"ms"`
		}
		if err := json.Unmarshal(req.Payload, &body); err != nil {
			return nil, fmt.Errorf("sleep payload: %w", err)
		}
		select {
		case <-time.After(time.Duration(body.Milliseconds) * time.Millisecond):
			return echoValue(req.Payload), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case "test.reject.v1":
		var body struct {
			Code   string `json:"code"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(req.Payload, &body); err != nil {
			return nil, fmt.Errorf("reject payload: %w", err)
		}
		return nil, fmt.Errorf("%s: %s", body.Code, body.Reason)
	case "test.chunks.v1":
		var body struct {
			Chunks []string        `json:"chunks"`
			Value  json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(req.Payload, &body); err != nil {
			return nil, fmt.Errorf("chunks payload: %w", err)
		}
		chunks := make(chan []byte, len(body.Chunks))
		for _, chunk := range body.Chunks {
			chunks <- []byte(chunk)
		}
		close(chunks)
		return wire.StreamResponse{Chunks: chunks, Value: echoValue(body.Value)}, nil
	case "test.event.v1":
		var body struct {
			Topic   string          `json:"topic"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(req.Payload, &body); err != nil {
			return nil, fmt.Errorf("event payload: %w", err)
		}
		if err := req.Session.PushEvent(ctx, wire.Event{Topic: body.Topic, Payload: body.Payload}); err != nil {
			return nil, err
		}
		return json.RawMessage("{}"), nil
	default:
		return nil, fmt.Errorf("unknown op %q", req.Op)
	}
}

func echoValue(payload []byte) json.RawMessage {
	if len(payload) == 0 || !json.Valid(payload) {
		return json.RawMessage("{}")
	}
	return json.RawMessage(payload)
}
