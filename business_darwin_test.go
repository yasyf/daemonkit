package daemonkit

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/paths"
)

// TestBusinessAttachWritesNothingToAnUnjudgedSquatter is
// internal/wire's TestAuthorizeRefusalPreemptsAForgedDrain driven from the
// consumer's own entry point. A same-UID process unlinks the daemon's socket,
// binds the path first, and answers with a forged drain preamble; the client
// that attaches must write it nothing at all — the wire hello included — and
// must surface the authorize refusal rather than any daemon state the squatter
// offered it.
func TestBusinessAttachWritesNothingToAnUnjudgedSquatter(t *testing.T) {
	shortHome(t)
	const label = "dksquat"
	socket, err := paths.Socket(label)
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatalf("mkdir socket dir: %v", err)
	}
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unlink socket: %v", err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("squat listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	type squat struct {
		accepted bool
		received []byte
	}
	observed := make(chan squat, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			observed <- squat{}
			return
		}
		defer conn.Close()
		_, _ = conn.Write(forgedDrainPreamble)
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 64)
		n, _ := conn.Read(buf)
		observed <- squat{accepted: true, received: buf[:n]}
	}()

	client, err := Open(Daemon{
		Label:   label,
		Schemas: []Schema{businessSchema},
		Trust: Trust{Serving: ServingSigned(Requirement{
			TeamID:            "SXKCTF23Q2",
			SigningIdentifier: "com.yasyf.daemonkit.not-this-binary",
		})},
	})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = client.Business().Call(ctx, echoOp, []byte("never"))
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Call() = %v, want ErrUntrusted for a peer that cannot prove the deployed identity", err)
	}
	forgeable := []struct {
		name string
		err  error
	}{
		{"ErrDraining", ErrDraining},
		{"wire.ErrBuildMismatch", wire.ErrBuildMismatch},
		{"wire.ErrHandshake", wire.ErrHandshake},
		{"ErrAbsent", ErrAbsent},
		{"ErrNotReady", ErrNotReady},
	}
	for _, forged := range forgeable {
		if errors.Is(err, forged.err) {
			t.Errorf("Call() = %v, want no %s a squatter that was never judged could forge", err, forged.name)
		}
	}
	if !Undispatched(err) {
		t.Error("Undispatched() = false for a request no byte of which was written")
	}
	squatter := <-observed
	if !squatter.accepted {
		t.Fatal("the client never reached the squatter holding the socket path")
	}
	if len(squatter.received) != 0 {
		t.Fatalf("the client wrote %#x to an unjudged peer, want nothing", squatter.received)
	}
}

// TestBusinessTrustSetRefusesAPeerNoDisjunctNames is Trust.Business reaching
// admission as a set rather than as a first element: a real daemon states two
// bundles, and a client neither names is refused as untrusted — the same
// verdict internal/trust's TestAnyOfIsADisjunctionOverFullRequirements pins
// element by element, observed from the consumer's Call.
func TestBusinessTrustSetRefusesAPeerNoDisjunctNames(t *testing.T) {
	shortHome(t)
	host, extension := hostAndExtension()
	d := Daemon{
		Label:    "dkbizset",
		Schemas:  []Schema{businessSchema},
		Shutdown: Grace(5 * time.Second),
		Trust:    Trust{Business: Requirements{host, extension}},
	}
	if (Requirements{host, extension}).Digest() != (Requirements{extension, host}).Digest() {
		t.Fatal("the stated set and its reverse are two policies")
	}
	done := make(chan error, 1)
	go func() {
		_, err := Serve(context.Background(), d, func(Ctx) (Product, error) { return &stubProduct{}, nil })
		done <- err
	}()
	socket, err := paths.Socket(string(d.Label))
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	_, err = openClient(t, d).Business().Call(ctx, echoOp, []byte("never"))
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Call() = %v, want ErrUntrusted from a set no disjunct of which names this peer", err)
	}
	if !Undispatched(err) {
		t.Error("Undispatched() = false for a handshake the server rejected")
	}
	if _, err := session.Drain(ctx); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() = %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not return after the drain verb")
	}
}

// forgedDrainPreamble is the two bytes a draining server emits instead of a
// hello ack. Any same-UID process can write them; nothing above the trust gate
// may believe them.
var forgedDrainPreamble = []byte{0x44, 0x52}
