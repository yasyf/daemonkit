//go:build mixedera

// Command cutpeer is daemonkit's cut-era mixed-era conformance peer, driven on
// this working tree's real transport: serve runs daemonkit.Serve, so the socket,
// the per-lane trust gate, the drain verb and the shutdown ladder are the ones a
// consumer gets, and drain runs Control.Drain over that trust-gated control
// lane. It lives in-tree, under the main module, because Go's internal-package
// rule forbids the foreign per-era module the harness builds precut in from
// importing internal/wire.
//
// dial reaches for internal/wire rather than the root, because the root publishes
// no business-lane client: a refusal has to stay wire's own typed
// ProtocolMismatchError for the matrix to read the peer's protocol out of it.
//
//	cutpeer serve -home DIR [-strict]
//	cutpeer dial  -socket PATH
//	cutpeer drain -home DIR
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
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/internal/wire"
)

const (
	era      = "cut"
	protocol = wire.ProtocolVersion

	label     = daemonkit.Label("mixedera")
	cutSchema = "daemonkit.mixedera.cut.v2"
	echoOp    = "mixedera.echo"

	readyLine = "READY"

	failureProtocolMismatch = "protocol-mismatch"
	failureUntrusted        = "untrusted"
	failureDraining         = "draining"
	failureAbsent           = "absent"
	failureUnsettled        = "unsettled"
	failureRefused          = "refused"
	failureMalformed        = "malformed"
	failureTransport        = "transport"

	shutdownGrace    = 20 * time.Second
	dialTimeout      = 60 * time.Second
	controlTimeout   = 60 * time.Second
	handshakeTimeout = 10 * time.Second

	framePrefixSize = 4

	mechanismFrame            = "frame-v1"
	mechanismGate             = "protocol-gate"
	mechanismSession          = "session"
	mechanismSigterm          = "drain-sigterm"
	mechanismPreamble         = "drain-preamble"
	mechanismPreambleEmitted  = "drain-preamble-emitted"
	mechanismTrustGate        = "drain-preamble-trust-gate"
	mechanismControlTrustGate = "drain-control-trust-gate"
)

// Each proposition below is quoted verbatim from
// ci/mixedera/testdata/frozen/mechanisms.txt, where the claim each mechanism
// name denotes is frozen. The manifest refuses this peer if a single byte
// differs, so the era this binary speaks for cannot come to answer a different
// question than the other era's column does.
const (
	propositionFrame            = "a packet the relay copied carries that era's frozen 6-byte frame identity at the head of its body: the DKS1 magic and the era's own protocol number"
	propositionGate             = "a handshake between the two eras is promptly refused as a typed protocol mismatch"
	propositionSession          = "a client and a daemon at one protocol complete a request and its response over one unix socket"
	propositionSigterm          = "SIGTERM ends the daemon at exit status 0"
	propositionPreamble         = "a connection whose first bytes are the frozen two-byte drain preamble drains the daemon it reaches"
	propositionPreambleEmitted  = "a daemon already draining answers a handshake it is still reading with exactly the frozen two-byte preamble and nothing else"
	propositionTrustGate        = "the drain an inbound frozen preamble admits is still authorized by Trust.Control, the preamble sitting above the trust gate, so an untrusted peer's preamble leaves the incumbent running"
	propositionControlTrustGate = "a drain arriving on the control lane is authorized by Trust.Control: one peer's drain of a daemon whose control lane names a requirement that peer cannot prove is refused untrusted, that daemon still completes the same peer's session at one protocol on the socket it refused the drain from, and that same peer's drain of a daemon naming no control requirement is honoured, reaping the pid of the daemon it stopped"
)

// strictControl is a Developer ID requirement no `go build` output can satisfy,
// so a daemon serving under it refuses this very binary's control attach. It is
// how the harness reaches the untrusted half of the drain trust gate on a runner
// that holds no signing identity.
var strictControl = &daemonkit.Requirement{
	TeamID:            "SXKCTF23Q2",
	SigningIdentifier: "com.yasyf.daemonkit.mixedera.no-such-peer",
}

var reapNames = map[daemonkit.Reap]string{
	daemonkit.ReapUndetermined: "undetermined",
	daemonkit.ReapAbsent:       "absent",
	daemonkit.ReapCrossBoot:    "cross-boot",
	daemonkit.ReapReused:       "reused",
	daemonkit.ReapTerminated:   "terminated",
}

type report struct {
	Era          string `json:"era"`
	Protocol     uint16 `json:"protocol"`
	PeerProtocol uint16 `json:"peer_protocol,omitempty"`
	Session      bool   `json:"session,omitempty"`
	Failure      string `json:"failure,omitempty"`
	Detail       string `json:"detail,omitempty"`
	Reap         string `json:"reap,omitempty"`
	DrainedPID   int    `json:"drained_pid,omitempty"`
	Socket       string `json:"socket,omitempty"`
	Self         string `json:"self"`
}

type verdict struct {
	Proposition string `json:"proposition"`
	Absence     string `json:"absence,omitempty"`
}

type conformance struct {
	Era        string             `json:"era"`
	Protocol   uint16             `json:"protocol"`
	Mechanisms map[string]verdict `json:"mechanisms"`
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
		Mechanisms: map[string]verdict{
			mechanismFrame:            {Proposition: propositionFrame},
			mechanismGate:             {Proposition: propositionGate},
			mechanismSession:          {Proposition: propositionSession},
			mechanismSigterm:          {Proposition: propositionSigterm},
			mechanismPreamble:         {Proposition: propositionPreamble},
			mechanismPreambleEmitted:  {Proposition: propositionPreambleEmitted},
			mechanismTrustGate:        {Proposition: propositionTrustGate},
			mechanismControlTrustGate: {Proposition: propositionControlTrustGate},
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

// homed parses -home and pins every daemonkit path under it, so a peer process
// reaches exactly the state root the harness minted for one case.
func homed(flags *flag.FlagSet, args []string) error {
	home := flags.String("home", "", "state root every daemonkit path derives from")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *home == "" {
		return fmt.Errorf("%s: -home is required", flags.Name())
	}
	return os.Setenv(realhome.EnvOverride, *home)
}

// serve runs the real root daemon: Serve owns the socket, the trust gate, the
// drain verb, and the shutdown ladder, so every era claim the harness redeems
// against this process is redeemed against the transport a consumer gets.
func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	strict := flags.Bool("strict", false, "pin the control lane to a requirement this peer cannot prove")
	if err := homed(flags, args); err != nil {
		return err
	}
	d := daemonkit.Daemon{
		Label:    label,
		Schemas:  []daemonkit.Schema{cutSchema},
		Shutdown: daemonkit.Grace(shutdownGrace),
	}
	if *strict {
		d.Trust.Control = strictControl
	}
	_, err := daemonkit.Serve(context.Background(), d, func(daemonkit.Ctx) (daemonkit.Product, error) {
		fmt.Println(readyLine)
		return echo{}, nil
	})
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// echo is the product under the cut daemon: it answers one op with the bytes it
// was given, so a session the harness drives is a real round trip through the
// root's own dispatch.
type echo struct{}

func (echo) Handle(_ context.Context, req daemonkit.Request) (daemonkit.Reply, error) {
	if req.Op != echoOp {
		return daemonkit.Reply{}, fmt.Errorf("unknown op %q", req.Op)
	}
	return daemonkit.Reply{Body: req.Body}, nil
}

func (echo) Drain(daemonkit.Budget) error { return nil }

func (echo) Close(daemonkit.Budget) error { return nil }

// drain runs the cut era's repair channel: attach the trust-gated control lane
// and stop the pinned incumbent, reporting the reap the process table proved.
func drain(args []string) error {
	if err := homed(flag.NewFlagSet("drain", flag.ContinueOnError), args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
	defer cancel()

	client, err := daemonkit.Open(daemonkit.Daemon{
		Label:   label,
		Schemas: []daemonkit.Schema{cutSchema},
		Trust:   daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
	})
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	control, err := client.Control(ctx)
	if err != nil {
		return emit(classifyControl(err))
	}
	stopped, err := control.Drain(ctx, daemonkit.Expect{})
	if err != nil {
		return emit(classifyControl(err))
	}
	return emit(report{
		Era: era, Protocol: protocol, Session: true,
		Reap: reapNames[stopped.Reap], DrainedPID: stopped.Before.PID,
	})
}

// classifyControl types one failed control verb the way the harness reads it,
// off the root's own sentinels rather than off a message.
func classifyControl(err error) report {
	failed := report{Era: era, Protocol: protocol, Detail: err.Error()}
	switch {
	case errors.Is(err, daemonkit.ErrUntrusted):
		failed.Failure = failureUntrusted
	case errors.Is(err, daemonkit.ErrDraining):
		failed.Failure = failureDraining
	case errors.Is(err, daemonkit.ErrAbsent):
		failed.Failure = failureAbsent
	case errors.Is(err, daemonkit.ErrUnsettled):
		failed.Failure = failureUnsettled
	case errors.Is(err, daemonkit.ErrWrongIncumbent):
		failed.Failure = failureRefused
	default:
		failed.Failure = failureTransport
	}
	return failed
}

// authorizeMatrixDaemon is this peer's named Authorize waiver: what the matrix
// measures is the two eras' answers to each other on the socket — the refusal,
// the preamble, the session — so the peer judges nothing before the handshake
// and classifies exactly what it read back.
func authorizeMatrixDaemon(net.Conn) error { return nil }

// dial completes a real wire session on the business lane and reports it, or
// classifies the wire error that refused it.
func dial(args []string) error {
	socket, err := socketFlag("dial", args)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(socket), Authorize: authorizeMatrixDaemon,
		Lane: wire.LaneBusiness, Schema: cutSchema,
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
	result, err := client.Call(ctx, echoOp, body)
	if err != nil {
		return emit(classifyDial(socket, err))
	}
	if rejection := result.Rejection(); rejection != nil {
		return emit(report{
			Era: era, Protocol: protocol, PeerProtocol: client.PeerWireIdentity().Protocol,
			Socket: socket, Failure: failureRefused, Detail: rejection.Error(),
		})
	}
	var replied daemonkit.Reply
	if err := json.Unmarshal(result.Response.Payload, &replied); err != nil {
		return fmt.Errorf("dial: decode reply %q: %w", result.Response.Payload, err)
	}
	if result.Outcome != wire.Delivered || !bytes.Equal(replied.Body, body) {
		return emit(report{
			Era: era, Protocol: protocol, Socket: socket, Failure: failureMalformed,
			Detail: fmt.Sprintf("outcome=%s payload=%q", result.Outcome, result.Response.Payload),
		})
	}
	return emit(report{
		Era: era, Protocol: protocol, PeerProtocol: client.PeerWireIdentity().Protocol,
		Socket: socket, Session: true,
	})
}

// classifyDial types one failed handshake the way the harness reads it. A
// mismatch is a mismatch only when it names the peer's protocol: wire types it
// directly when the foreign version rode a decodable frame, and otherwise the
// version has to be read off the wire against the same peer.
func classifyDial(socket string, err error) report {
	failed := report{Era: era, Protocol: protocol, Socket: socket, Detail: err.Error()}
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
		failed.Failure = failureDraining
	case errors.Is(err, wire.ErrBuildMismatch), errors.As(err, &rejection):
		failed.Failure = failureRefused
	case errors.Is(err, wire.ErrHandshake):
		failed.Failure = failureMalformed
	default:
		failed.Failure = failureTransport
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

// emit names the binary that produced a report before writing it, so a harness
// holding several reports can tell whether one peer identity met two daemons or
// two peers met one each.
func emit(r report) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("name this peer's own executable: %w", err)
	}
	r.Self = self
	return json.NewEncoder(os.Stdout).Encode(r)
}
