// Command cut is daemonkit's cut-era mixed-era conformance peer: it speaks the
// cut's frame layout and the frozen drain preamble directly, and reports era
// "stub" from the conformance verb.
//
// TODO(phase 2): rewrite the body onto daemonkit.Serve, Open, and
// Control.Drain and report era "cut". The verbs and the report shape are the
// harness's contract and do not move.
//
//	cut serve       -socket PATH
//	cut dial        -socket PATH
//	cut drain       -socket PATH
//	cut conformance
package main

import (
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
)

const (
	era             = "stub"
	protocol uint16 = 2

	kindHello    = 1
	kindHelloAck = 2
	kindRequest  = 3
	kindResponse = 4

	flagEnd    = 1
	headerSize = 32
	maxFrame   = 4 << 20
	handshake  = 10 * time.Second
)

var (
	frameMagic    = [4]byte{'D', 'K', 'S', '1'}
	drainPreamble = [2]byte{'D', 'R'}

	errProtocolMismatch = errors.New("cut: protocol mismatch")
	errMalformedFrame   = errors.New("cut: malformed frame")
)

type frame struct {
	protocol uint16
	kind     byte
	flags    byte
	id       uint64
	payload  []byte
}

type hello struct {
	Protocol uint16 `json:"protocol"`
	Lane     string `json:"lane"`
}

type ack struct {
	Protocol uint16 `json:"protocol"`
	Phase    string `json:"phase"`
	Refused  string `json:"refused,omitempty"`
}

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
		fail(errors.New("usage: cut serve|dial|drain|conformance"))
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
			"frame-v1":                  "",
			"protocol-gate":             "",
			"session":                   "",
			"drain-sigterm":             "",
			"drain-preamble":            "",
			"drain-preamble-trust-gate": "phase 0 stub: internal/trust lands in phase 1 and the cut server in phase 2, so no Trust.Control requirement exists to refuse a peer against",
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

func serve(args []string) error {
	socket, err := socketFlag("serve", args)
	if err != nil {
		return err
	}

	drained := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(drained) }) }

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		stop()
	}()

	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("serve: listen: %w", err)
	}
	go func() {
		<-drained
		_ = listener.Close()
	}()

	fmt.Println("READY")
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-drained:
				return nil
			default:
				return fmt.Errorf("serve: accept: %w", err)
			}
		}
		go admit(conn, stop)
	}
}

func admit(conn net.Conn, stop func()) {
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(handshake)); err != nil {
		return
	}
	var lead [2]byte
	if _, err := io.ReadFull(conn, lead[:]); err != nil {
		return
	}
	if lead == drainPreamble {
		stop()
		return
	}
	incoming, err := readFrameAfterLead(conn, lead)
	if err != nil {
		if errors.Is(err, errProtocolMismatch) {
			_ = writeFrame(conn, frame{kind: kindHelloAck, flags: flagEnd, payload: encode(ack{
				Protocol: protocol, Phase: "ready", Refused: "protocol",
			})})
		}
		return
	}
	if incoming.kind != kindHello {
		return
	}
	if err := writeFrame(conn, frame{kind: kindHelloAck, flags: flagEnd, payload: encode(ack{
		Protocol: protocol, Phase: "ready",
	})}); err != nil {
		return
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return
	}
	for {
		request, err := readFrame(conn)
		if err != nil {
			return
		}
		if request.kind != kindRequest {
			return
		}
		if err := writeFrame(conn, frame{
			kind: kindResponse, flags: flagEnd, id: request.id, payload: request.payload,
		}); err != nil {
			return
		}
	}
}

func dial(args []string) error {
	socket, err := socketFlag("dial", args)
	if err != nil {
		return err
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		return emit(report{Era: era, Protocol: protocol, Failure: "transport", Detail: err.Error()})
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(handshake)); err != nil {
		return err
	}

	if err := writeFrame(conn, frame{kind: kindHello, flags: flagEnd, payload: encode(hello{
		Protocol: protocol, Lane: "business",
	})}); err != nil {
		return emit(report{Era: era, Protocol: protocol, Failure: "transport", Detail: err.Error()})
	}
	accepted, err := readFrame(conn)
	if err != nil {
		return emit(classify(err))
	}
	var accepting ack
	if err := json.Unmarshal(accepted.payload, &accepting); err != nil {
		return emit(report{Era: era, Protocol: protocol, Failure: "malformed", Detail: err.Error()})
	}
	if accepting.Refused != "" {
		return emit(report{
			Era: era, Protocol: protocol, PeerProtocol: accepted.protocol,
			Failure: "refused", Detail: accepting.Refused,
		})
	}

	body := []byte(`{"op":"echo"}`)
	if err := writeFrame(conn, frame{kind: kindRequest, flags: flagEnd, id: 1, payload: body}); err != nil {
		return emit(report{Era: era, Protocol: protocol, Failure: "transport", Detail: err.Error()})
	}
	response, err := readFrame(conn)
	if err != nil {
		return emit(classify(err))
	}
	if response.kind != kindResponse || response.id != 1 || string(response.payload) != string(body) {
		return emit(report{
			Era: era, Protocol: protocol, Failure: "malformed",
			Detail: fmt.Sprintf("kind=%d id=%d payload=%q", response.kind, response.id, response.payload),
		})
	}
	return emit(report{
		Era: era, Protocol: protocol, PeerProtocol: accepted.protocol, Session: true,
	})
}

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
	if err := conn.SetDeadline(time.Now().Add(handshake)); err != nil {
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

func classify(err error) report {
	var mismatch *mismatchError
	switch {
	case errors.As(err, &mismatch):
		return report{
			Era: era, Protocol: protocol, PeerProtocol: mismatch.theirs,
			Failure: "protocol-mismatch", Detail: err.Error(),
		}
	case errors.Is(err, errMalformedFrame):
		return report{Era: era, Protocol: protocol, Failure: "malformed", Detail: err.Error()}
	default:
		return report{Era: era, Protocol: protocol, Failure: "transport", Detail: err.Error()}
	}
}

func emit(r report) error {
	return json.NewEncoder(os.Stdout).Encode(r)
}

func encode(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}

type mismatchError struct {
	theirs uint16
	ours   uint16
}

func (e *mismatchError) Error() string {
	return fmt.Sprintf("%s: peer=%d self=%d", errProtocolMismatch, e.theirs, e.ours)
}

func (*mismatchError) Unwrap() error { return errProtocolMismatch }

func writeFrame(conn net.Conn, f frame) error {
	body := make([]byte, headerSize+len(f.payload))
	copy(body[:4], frameMagic[:])
	binary.BigEndian.PutUint16(body[4:6], protocol)
	body[6] = f.kind
	body[7] = f.flags
	binary.BigEndian.PutUint64(body[8:16], f.id)
	copy(body[headerSize:], f.payload)

	packet := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(body)))
	copy(packet[4:], body)
	if _, err := conn.Write(packet); err != nil {
		return fmt.Errorf("cut: write frame: %w", err)
	}
	return nil
}

func readFrame(conn net.Conn) (frame, error) {
	var lead [2]byte
	if _, err := io.ReadFull(conn, lead[:]); err != nil {
		return frame{}, err
	}
	return readFrameAfterLead(conn, lead)
}

func readFrameAfterLead(conn net.Conn, lead [2]byte) (frame, error) {
	var rest [2]byte
	if _, err := io.ReadFull(conn, rest[:]); err != nil {
		return frame{}, err
	}
	length := binary.BigEndian.Uint32([]byte{lead[0], lead[1], rest[0], rest[1]})
	if length < headerSize || length > maxFrame {
		return frame{}, fmt.Errorf("%w: body length %d", errMalformedFrame, length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		return frame{}, err
	}
	if [4]byte(body[:4]) != frameMagic {
		return frame{}, fmt.Errorf("%w: magic %q", errMalformedFrame, body[:4])
	}
	theirs := binary.BigEndian.Uint16(body[4:6])
	if theirs != protocol {
		return frame{}, &mismatchError{theirs: theirs, ours: protocol}
	}
	return frame{
		protocol: theirs,
		kind:     body[6],
		flags:    body[7],
		id:       binary.BigEndian.Uint64(body[8:16]),
		payload:  body[headerSize:],
	}, nil
}
