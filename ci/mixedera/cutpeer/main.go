//go:build mixedera

// Command cutpeer is daemonkit's cut-era mixed-era conformance peer, driven on
// this working tree's internal/wire directly. It lives in-tree, under the main
// module, because Go's internal-package rule forbids the foreign per-era module
// the harness builds precut in from importing internal/wire.
//
// Its transport is entirely real wire: serve stands up a wire.Server, dial runs
// a wire.Client, and a refusal is wire's own typed ProtocolMismatchError. The
// two seams a phase-3 daemonkit root will own are shimmed here over that real
// transport: a peer that pushes the frozen drain preamble to trigger drain (the
// wire server only emits the preamble from a draining server, never drains on an
// inbound one), and reading a refusing peer's advertised protocol off the wire
// (wire's decode discards the version once it rejects a foreign frame). Both are
// noted where they occur and honestly in the conformance report.
//
//	cutpeer serve       -socket PATH
//	cutpeer dial        -socket PATH
//	cutpeer drain       -socket PATH
//	cutpeer conformance
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

const (
	era      = "cut"
	protocol = wire.ProtocolVersion

	cutSchema = "daemonkit.mixedera.cut.v2"
	echoOp    = wire.Op("mixedera.echo")
	tenant    = "mixedera"

	failureProtocolMismatch = "protocol-mismatch"

	dialTimeout      = 60 * time.Second
	handshakeTimeout = 10 * time.Second
	settleTimeout    = 5 * time.Second
	peekTimeout      = 5 * time.Second

	framePrefixSize = 4

	mechanismFrame     = "frame-v1"
	mechanismGate      = "protocol-gate"
	mechanismSession   = "session"
	mechanismSigterm   = "drain-sigterm"
	mechanismPreamble  = "drain-preamble"
	mechanismTrustGate = "drain-preamble-trust-gate"
)

// drainPreamble is the two bytes a draining wire server emits instead of a hello
// ack; the harness pushes them at a running daemon to request drain. It mirrors
// the unexported wire.drainPreamble, pinned to the frozen fixture by
// TestFrozenFixturesMatchRealWire.
var drainPreamble = [2]byte{0x44, 0x52}

type report struct {
	Era          string `json:"era"`
	Protocol     uint16 `json:"protocol"`
	PeerProtocol uint16 `json:"peer_protocol,omitempty"`
	Session      bool   `json:"session,omitempty"`
	Failure      string `json:"failure,omitempty"`
	Detail       string `json:"detail,omitempty"`
}

type conformance struct {
	Era        string            `json:"era"`
	Protocol   uint16            `json:"protocol"`
	Mechanisms map[string]string `json:"mechanisms"`
}

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("usage: cutpeer serve|dial|drain|conformance"))
	}
	switch mode, args := os.Args[1], os.Args[2:]; mode {
	case "serve":
		fail(serve(args))
	case "dial":
		fail(dial(args))
	case "drain":
		fail(drain(args))
	case "conformance":
		fail(declare())
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

func declare() error {
	return json.NewEncoder(os.Stdout).Encode(conformance{
		Era: era, Protocol: protocol,
		Mechanisms: map[string]string{
			mechanismFrame:     "",
			mechanismGate:      "",
			mechanismSession:   "",
			mechanismSigterm:   "",
			mechanismPreamble:  "",
			mechanismTrustGate: "phase 2 drives the cut peer on internal/wire, whose draining server emits the preamble but never verifies an inbound one against a Trust requirement; the gate that refuses an untrusted peer's preamble arrives with the phase-3 root Control.Drain and its signed-peer fixtures",
		},
	})
}

func socketFlag(mode string, args []string) (string, error) {
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	socket := flags.String("socket", "", "unix socket path")
	if err := flags.Parse(args); err != nil {
		return "", err
	}
	if *socket == "" {
		return "", fmt.Errorf("%s: -socket is required", mode)
	}
	return *socket, nil
}

// serve runs a real wire.Server over a preamble-peeking listener: a session and
// a typed refusal are pure wire, while an inbound frozen preamble — which wire
// itself never reads as a drain request — runs the wire drain triad and settles
// the process, exactly as SIGTERM does.
func serve(args []string) error {
	socket, err := socketFlag("serve", args)
	if err != nil {
		return err
	}

	runtime := wiretest.NewStubRuntime()
	runtime.SetHandle(func(_ context.Context, req wire.Request) (any, error) {
		return json.RawMessage(req.Payload), nil
	})
	server, err := wire.NewServer(runtime, wire.Config{Schemas: wire.Schemas{cutSchema}})
	if err != nil {
		return fmt.Errorf("serve: new server: %w", err)
	}

	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("serve: listen: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var drainOnce sync.Once
	drainDown := func() {
		drainOnce.Do(func() {
			_ = server.CloseIntake()
			server.CancelRequests()
			settleCtx, settleCancel := context.WithTimeout(context.Background(), settleTimeout)
			_ = server.Settle(settleCtx)
			settleCancel()
			runtime.Drain()
			cancel()
		})
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		drainDown()
	}()

	fmt.Println("READY")
	if err := server.Serve(ctx, &preambleListener{Listener: listener, onPreamble: drainDown}); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// preambleListener hands wire.Server every connection untouched except a
// non-consuming two-byte peek: a connection that opens with the frozen drain
// preamble never reaches wire — it triggers onPreamble and is closed — while any
// other connection reaches wire with its bytes still buffered.
type preambleListener struct {
	net.Listener
	onPreamble func()
}

func (l *preambleListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		unixConn, ok := conn.(*net.UnixConn)
		if !ok {
			_ = conn.Close()
			continue
		}
		drain, err := peekDrainPreamble(unixConn)
		if err != nil {
			_ = unixConn.Close()
			continue
		}
		if drain {
			_ = unixConn.Close()
			l.onPreamble()
			continue
		}
		return unixConn, nil
	}
}

// peekDrainPreamble reports whether conn opens with the drain preamble, reading
// its lead bytes with MSG_PEEK so wire reads the same bytes afterward. A lead
// shorter than the preamble is not one, and never blocks a real frame whose
// length prefix opens with a zero byte.
func peekDrainPreamble(conn *net.UnixConn) (bool, error) {
	if err := conn.SetReadDeadline(time.Now().Add(peekTimeout)); err != nil {
		return false, err
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	raw, err := conn.SyscallConn()
	if err != nil {
		return false, err
	}
	head := make([]byte, len(drainPreamble))
	var peeked int
	var peekErr error
	controlErr := raw.Read(func(fd uintptr) bool {
		peeked, _, peekErr = syscall.Recvfrom(int(fd), head, syscall.MSG_PEEK)
		return peekErr != syscall.EAGAIN && peekErr != syscall.EINTR
	})
	if controlErr != nil {
		return false, controlErr
	}
	if peekErr != nil {
		return false, peekErr
	}
	if peeked < len(drainPreamble) {
		return false, nil
	}
	return head[0] == drainPreamble[0] && head[1] == drainPreamble[1], nil
}

// dial completes a real wire session and reports it, or classifies the wire
// error that refused it.
func dial(args []string) error {
	socket, err := socketFlag("dial", args)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(socket), Lane: wire.LaneBusiness, Schema: cutSchema,
		HandshakeTimeout: handshakeTimeout,
	})
	if err != nil {
		return emit(classifyDial(socket, err))
	}
	defer func() { _ = client.Abort(nil) }()

	if err := client.WaitReady(ctx); err != nil {
		return emit(classifyDial(socket, err))
	}
	body := []byte(`{"op":"echo"}`)
	result, err := client.Call(ctx, echoOp, tenant, body)
	if err != nil {
		return emit(classifyDial(socket, err))
	}
	if rejection := result.Rejection(); rejection != nil {
		return emit(report{
			Era: era, Protocol: protocol, PeerProtocol: client.PeerWireIdentity().Protocol,
			Failure: "refused", Detail: rejection.Error(),
		})
	}
	if result.Outcome != wire.Delivered || !bytes.Equal(result.Response.Payload, body) {
		return emit(report{
			Era: era, Protocol: protocol, Failure: "malformed",
			Detail: fmt.Sprintf("outcome=%s payload=%q", result.Outcome, result.Response.Payload),
		})
	}
	return emit(report{
		Era: era, Protocol: protocol, PeerProtocol: client.PeerWireIdentity().Protocol, Session: true,
	})
}

// classifyDial types one failed handshake the way the harness reads it. A
// mismatch is a mismatch only when it names the peer's protocol: wire types it
// directly when the foreign version rode a decodable frame, and otherwise the
// version has to be read off the wire against the same peer.
func classifyDial(socket string, err error) report {
	failed := report{Era: era, Protocol: protocol, Detail: err.Error()}
	var mismatch *wire.ProtocolMismatchError
	var rejection *wire.HandshakeRejectionError
	switch {
	case errors.As(err, &mismatch):
		failed.Failure = failureProtocolMismatch
		failed.PeerProtocol = mismatch.Theirs
	case errors.Is(err, wire.ErrProtocolVersion):
		failed.Failure = failureProtocolMismatch
		failed.PeerProtocol = probePeerProtocol(socket)
	case errors.Is(err, wire.ErrDraining):
		failed.Failure = "draining"
	case errors.Is(err, wire.ErrBuildMismatch), errors.As(err, &rejection):
		failed.Failure = "refused"
	case errors.Is(err, wire.ErrHandshake):
		failed.Failure = "malformed"
	default:
		failed.Failure = "transport"
	}
	return failed
}

// probePeerProtocol reads the wire version a refusing peer advertised in the
// frame it answered with, which wire's decode drops once it rejects a foreign
// version. It opens its own connection and reads only the frame prefix, so the
// typed mismatch still carries the peer's version.
func probePeerProtocol(socket string) uint16 {
	conn, err := net.Dial("unix", socket)
	if err != nil {
		return 0
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return 0
	}
	hello, err := wire.EncodePacket(wire.Frame{
		Kind: wire.FrameHello, Flags: wire.FlagEnd,
		Payload: []byte(fmt.Sprintf(`{"protocol":%d,"lane":"business","schema":%q}`, protocol, cutSchema)),
	})
	if err != nil {
		return 0
	}
	if _, err := conn.Write(hello); err != nil {
		return 0
	}
	head := make([]byte, framePrefixSize+6)
	if _, err := io.ReadFull(conn, head); err != nil {
		return 0
	}
	if string(head[framePrefixSize:framePrefixSize+4]) != "DKS1" {
		return 0
	}
	return binary.BigEndian.Uint16(head[framePrefixSize+4 : framePrefixSize+6])
}

// drain pushes the frozen preamble at a running daemon and waits for it to close
// the connection as it settles.
func drain(args []string) error {
	socket, err := socketFlag("drain", args)
	if err != nil {
		return err
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		return emit(report{Era: era, Protocol: protocol, Failure: "transport", Detail: err.Error()})
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return err
	}
	if _, err := conn.Write(drainPreamble[:]); err != nil {
		return emit(report{Era: era, Protocol: protocol, Failure: "transport", Detail: err.Error()})
	}
	if _, err := io.Copy(io.Discard, conn); err != nil {
		return emit(report{Era: era, Protocol: protocol, Failure: "transport", Detail: err.Error()})
	}
	return emit(report{Era: era, Protocol: protocol, Session: true})
}

func emit(r report) error {
	return json.NewEncoder(os.Stdout).Encode(r)
}
