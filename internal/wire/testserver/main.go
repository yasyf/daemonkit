// Command wire-test-server is the Swift integration rig's Go entrypoint: a
// real internal/wire server on a unix socket, a fixed op catalog covering what
// the Swift client suites exercise, and a -phases script driving the stub
// runtime's lifecycle. The script advances one step per line on stdin, so the
// caller decides when a transition happens rather than racing a hold. It
// prints READY <socket> once listening and exits on stdin EOF or SIGTERM, so
// orphan cleanup is unconditional even if the test runner dies.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

func run() error {
	socket := flag.String("socket", "", "unix socket path (required)")
	schema := flag.String("schema", "", "the server's own schema digest (required)")
	var accepted []string
	flag.Func("accept", "additional accepted schema digest (repeatable)", func(v string) error {
		accepted = append(accepted, v)
		return nil
	})
	phases := flag.String("phases", "ready", `phase script advanced one step per stdin line: "ready" | "starting,ready" | "ready,draining"`)
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
	rt.SetPhase(script[0], nil)

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

	go runPhases(rt, script, cancel)

	return server.Serve(ctx, ln)
}

func parsePhases(script string) ([]wire.Phase, error) {
	names := map[string]wire.Phase{
		"starting": wire.PhaseStarting,
		"ready":    wire.PhaseReady,
		"draining": wire.PhaseDraining,
		"failed":   wire.PhaseFailed,
	}
	var steps []wire.Phase
	for _, entry := range strings.Split(script, ",") {
		phase, ok := names[strings.TrimSpace(entry)]
		if !ok {
			return nil, fmt.Errorf("unknown phase %q", entry)
		}
		steps = append(steps, phase)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("empty phase script")
	}
	return steps, nil
}

func runPhases(rt *wiretest.StubRuntime, script []wire.Phase, done func()) {
	lines := bufio.NewScanner(os.Stdin)
	for step := 1; lines.Scan(); step++ {
		if step >= len(script) {
			fmt.Fprintf(os.Stderr, "wire-test-server: phase advance %d past script %v\n", step, script)
			os.Exit(1)
		}
		rt.SetPhase(script[step], nil)
	}
	done()
}

func handle(ctx context.Context, req wire.Request) (any, error) {
	switch req.Op {
	case "test.echo.v1":
		return echoValue(req.Payload), nil
	case "test.drain.v1":
		// Ranging with no time bound is what makes this an ordering guarantee
		// for its Swift tests rather than a race, and it holds only because
		// session.deliverRequestChunks selects on deliveryDone and the session
		// context, never on requestCtx. Hardening wire to also close
		// state.chunks on requestCtx.Done() — reasonable in itself — silently
		// gives this loop the request's own deadline as a bound and turns
		// those tests back into wall-clock races.
		received := []string{}
		for chunk := range req.Chunks {
			if chunk.End && len(chunk.Payload) == 0 {
				continue
			}
			received = append(received, string(chunk.Payload))
		}
		return map[string]any{"chunks": received}, nil
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
