package daemonkit

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
	"github.com/yasyf/daemonkit/internal/wire"
)

const spawnChannelSchema = "chan.e2e.v1"

func testSpawnChannelPair(t *testing.T) (parent, child *net.UnixConn) {
	t.Helper()
	parentFile, childFile, err := proc.SocketpairFiles()
	if err != nil {
		t.Fatalf("SocketpairFiles() = %v", err)
	}
	parentConn, err := net.FileConn(parentFile)
	if err != nil {
		t.Fatalf("FileConn(parent) = %v", err)
	}
	childConn, err := net.FileConn(childFile)
	if err != nil {
		t.Fatalf("FileConn(child) = %v", err)
	}
	_ = parentFile.Close()
	_ = childFile.Close()
	parent = parentConn.(*net.UnixConn)
	child = childConn.(*net.UnixConn)
	t.Cleanup(func() {
		_ = parent.Close()
		_ = child.Close()
	})
	return parent, child
}

func testSpawnNonce(t *testing.T) []byte {
	t.Helper()
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	return nonce
}

func writeSpawnChannelRequest(t *testing.T, channel *net.UnixConn, op string, nonce []byte, fd int) {
	t.Helper()
	payload, err := json.Marshal(wire.SpawnChannelRequest{Op: op, Nonce: nonce})
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	if err := wire.WriteSpawnChannelFrame(channel, payload, fd); err != nil {
		t.Fatalf("write request: %v", err)
	}
}

func TestServeSpawnChannelMintsAPairAdmittedUnderAFreshNonce(t *testing.T) {
	parentEnd, childEnd := testSpawnChannelPair(t)
	spawnNonce := testSpawnNonce(t)
	type minted struct {
		conn  *net.UnixConn
		nonce []byte
	}
	admitted := make(chan minted, 1)
	served := make(chan error, 1)
	go func() {
		served <- serveSpawnChannel(
			t.Context(), parentEnd, spawnNonce, trust.Peer{UID: os.Getuid()},
			func(conn *net.UnixConn, _ trust.Peer, nonce []byte) error {
				admitted <- minted{conn: conn, nonce: append([]byte(nil), nonce...)}
				return nil
			},
			func(conn *net.UnixConn) error {
				_ = conn.Close()
				return errors.New("unexpected adopt")
			},
		)
	}()

	writeSpawnChannelRequest(t, childEnd, wire.SpawnChannelOpMint, spawnNonce, -1)
	payload, fd, err := wire.ReadSpawnChannelFrame(childEnd, true)
	if err != nil {
		t.Fatalf("read mint response: %v", err)
	}
	if fd < 0 {
		t.Fatal("mint response carried no descriptor")
	}
	var response wire.SpawnChannelMinted
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode mint response: %v", err)
	}
	got := <-admitted
	defer got.conn.Close()
	if len(response.Nonce) != 32 || !bytes.Equal(response.Nonce, got.nonce) {
		t.Fatalf("minted nonce = %x, admitted %x; want one fresh 32-byte nonce on both ends", response.Nonce, got.nonce)
	}
	if bytes.Equal(response.Nonce, spawnNonce) {
		t.Fatal("the mint reused the spawn nonce instead of minting a fresh one")
	}

	mintedFile := os.NewFile(uintptr(fd), "minted")
	mintedConn, err := net.FileConn(mintedFile)
	if err != nil {
		t.Fatalf("adopt minted end: %v", err)
	}
	_ = mintedFile.Close()
	defer mintedConn.Close()
	if _, err := mintedConn.Write([]byte("ping")); err != nil {
		t.Fatalf("write minted end: %v", err)
	}
	probe := make([]byte, 4)
	if err := got.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	if _, err := io.ReadFull(got.conn, probe); err != nil || string(probe) != "ping" {
		t.Fatalf("admitted end read %q (%v), want the minted pair connected end to end", probe, err)
	}

	_ = childEnd.Close()
	if err := <-served; err != nil {
		t.Fatalf("serveSpawnChannel() = %v, want nil after a clean child close", err)
	}
}

func TestServeSpawnChannelDeliversAndAnswersAdoptions(t *testing.T) {
	parentEnd, childEnd := testSpawnChannelPair(t)
	spawnNonce := testSpawnNonce(t)
	refusal := errors.New("daemon at capacity")
	adoptions := make(chan *net.UnixConn, 2)
	verdicts := []error{nil, refusal}
	served := make(chan error, 1)
	go func() {
		served <- serveSpawnChannel(
			t.Context(), parentEnd, spawnNonce, trust.Peer{UID: os.Getuid()},
			func(conn *net.UnixConn, _ trust.Peer, _ []byte) error {
				_ = conn.Close()
				return errors.New("unexpected mint")
			},
			func(conn *net.UnixConn) error {
				adoptions <- conn
				verdict := verdicts[0]
				verdicts = verdicts[1:]
				return verdict
			},
		)
	}()

	delegated, kept := testSpawnChannelPair(t)
	delegatedFile, err := delegated.File()
	if err != nil {
		t.Fatalf("File() = %v", err)
	}
	writeSpawnChannelRequest(t, childEnd, wire.SpawnChannelOpAdopt, spawnNonce, int(delegatedFile.Fd()))
	_ = delegatedFile.Close()
	payload, fd, err := wire.ReadSpawnChannelFrame(childEnd, false)
	if err != nil || fd != -1 {
		t.Fatalf("adopt response = (%d, %v), want a bare frame", fd, err)
	}
	var response wire.SpawnChannelAdopted
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode adopt response: %v", err)
	}
	if !response.Adopted || response.Reason != "" {
		t.Fatalf("adopt response = %+v, want adopted", response)
	}
	adopted := <-adoptions
	defer adopted.Close()
	if _, err := adopted.Write([]byte("pong")); err != nil {
		t.Fatalf("write adopted end: %v", err)
	}
	probe := make([]byte, 4)
	if err := kept.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	if _, err := io.ReadFull(kept, probe); err != nil || string(probe) != "pong" {
		t.Fatalf("kept end read %q (%v), want the delegated conn adopted end to end", probe, err)
	}

	again, keptAgain := testSpawnChannelPair(t)
	againFile, err := again.File()
	if err != nil {
		t.Fatalf("File() = %v", err)
	}
	writeSpawnChannelRequest(t, childEnd, wire.SpawnChannelOpAdopt, spawnNonce, int(againFile.Fd()))
	_ = againFile.Close()
	payload, _, err = wire.ReadSpawnChannelFrame(childEnd, false)
	if err != nil {
		t.Fatalf("read refusal response: %v", err)
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode refusal response: %v", err)
	}
	if response.Adopted || !strings.Contains(response.Reason, "capacity") {
		t.Fatalf("refusal response = %+v, want the named refusal", response)
	}
	_ = (<-adoptions).Close()
	_ = keptAgain.Close()

	_ = childEnd.Close()
	if err := <-served; err != nil {
		t.Fatalf("serveSpawnChannel() = %v, want nil: a refused adoption is an answer, not a violation", err)
	}
}

func TestServeSpawnChannelClosesOnEveryProtocolViolation(t *testing.T) {
	tests := []struct {
		name string
		send func(t *testing.T, channel *net.UnixConn, spawnNonce []byte)
		want string
	}{
		{
			name: "wrong nonce",
			send: func(t *testing.T, channel *net.UnixConn, _ []byte) {
				writeSpawnChannelRequest(t, channel, wire.SpawnChannelOpMint, make([]byte, 32), -1)
			},
			want: "nonce",
		},
		{
			name: "unknown op",
			send: func(t *testing.T, channel *net.UnixConn, spawnNonce []byte) {
				writeSpawnChannelRequest(t, channel, "drain", spawnNonce, -1)
			},
			want: "op",
		},
		{
			name: "mint with a descriptor",
			send: func(t *testing.T, channel *net.UnixConn, spawnNonce []byte) {
				extra, _ := testSpawnChannelPair(t)
				file, err := extra.File()
				if err != nil {
					t.Fatalf("File() = %v", err)
				}
				defer file.Close()
				writeSpawnChannelRequest(t, channel, wire.SpawnChannelOpMint, spawnNonce, int(file.Fd()))
			},
			want: "descriptor",
		},
		{
			name: "adopt without a descriptor",
			send: func(t *testing.T, channel *net.UnixConn, spawnNonce []byte) {
				writeSpawnChannelRequest(t, channel, wire.SpawnChannelOpAdopt, spawnNonce, -1)
			},
			want: "descriptor",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentEnd, childEnd := testSpawnChannelPair(t)
			spawnNonce := testSpawnNonce(t)
			served := make(chan error, 1)
			go func() {
				served <- serveSpawnChannel(
					t.Context(), parentEnd, spawnNonce, trust.Peer{UID: os.Getuid()},
					func(conn *net.UnixConn, _ trust.Peer, _ []byte) error { _ = conn.Close(); return nil },
					func(conn *net.UnixConn) error { _ = conn.Close(); return nil },
				)
			}()
			tt.send(t, childEnd, spawnNonce)
			err := <-served
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("serveSpawnChannel() = %v, want a refusal naming %q", err, tt.want)
			}
			if err := childEnd.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatalf("deadline: %v", err)
			}
			if _, _, frameErr := wire.ReadSpawnChannelFrame(childEnd, true); !errors.Is(frameErr, io.EOF) {
				t.Fatalf("channel after violation = %v, want closed", frameErr)
			}
		})
	}
}

func TestServeSpawnChannelEndsWithItsContext(t *testing.T) {
	parentEnd, _ := testSpawnChannelPair(t)
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() {
		served <- serveSpawnChannel(
			ctx, parentEnd, testSpawnNonce(t), trust.Peer{UID: os.Getuid()},
			func(conn *net.UnixConn, _ trust.Peer, _ []byte) error { _ = conn.Close(); return nil },
			func(conn *net.UnixConn) error { _ = conn.Close(); return nil },
		)
	}()
	cancel()
	if err := <-served; !errors.Is(err, context.Canceled) {
		t.Fatalf("serveSpawnChannel() = %v, want context.Canceled", err)
	}
}

func TestServeChannelServesTheSpawnSuspendedIdentityAfterSettlement(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	owned, err := OwnProcesses(ctx, filepath.Join(t.TempDir(), "daemon.records"))
	if err != nil {
		t.Fatalf("OwnProcesses() = %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer closeCancel()
		_ = owned.Close(closeCtx)
	})
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable() = %v", err)
	}
	child, err := owned.Spawn(ctx, Cmd{
		Path:    executable,
		Env:     append(os.Environ(), childRoleEnv+"=exit-clean"),
		Session: true,
		Exec:    ServingSameUser(),
	}, ChannelHandoff, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	exit := <-child.Done()
	if exit.Code != 0 || exit.Signal != 0 {
		t.Fatalf("child exit = %+v, want clean", exit)
	}
	if !child.token.Valid() || child.token.PID() != child.PID() {
		t.Fatalf("pinned token = pid %d valid %t, want the spawned pid %d",
			child.token.PID(), child.token.Valid(), child.PID())
	}
	c := Ctx{
		Context: ctx,
		owner:   owned,
		adoptMinted: func(conn *net.UnixConn, _ trust.Peer, _ []byte) error {
			_ = conn.Close()
			return nil
		},
		adoptHandoff: func(conn *net.UnixConn) error {
			_ = conn.Close()
			return nil
		},
	}
	if err := c.ServeChannel(ctx, child); err != nil {
		t.Fatalf("ServeChannel() = %v, want nil: identity was pinned at the suspended spawn, not read from the PID table", err)
	}
}

func TestServeChannelRefusesOutsideAServingDaemon(t *testing.T) {
	if err := (Ctx{}).ServeChannel(t.Context(), nil); err == nil ||
		!strings.Contains(err.Error(), "serving daemon") {
		t.Fatalf("ServeChannel() = %v, want the serving-daemon refusal", err)
	}
}

func TestServeChannelMintsAndAdoptsForARealSpawnedChild(t *testing.T) {
	shortHome(t)
	d := Daemon{
		Label:    "dkchan",
		Schemas:  []Schema{spawnChannelSchema},
		Shutdown: Grace(10 * time.Second),
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable() = %v", err)
	}
	stderr := &lockedBuffer{}
	product := &spawnChannelEchoProduct{}
	childPID := make(chan int, 1)
	childExit := make(chan Exit, 1)
	serveDone := make(chan error, 1)
	go func() {
		_, err := Serve(context.Background(), d, func(c Ctx) (Product, error) {
			spawnCtx, cancel := context.WithTimeout(c.Context, 30*time.Second)
			child, spawnErr := c.Spawn(spawnCtx, Cmd{
				Path:    executable,
				Env:     append(os.Environ(), childRoleEnv+"=spawn-channel-client"),
				Session: true,
				Exec:    ServingSameUser(),
			}, ChannelHandoff, stderr)
			cancel()
			if spawnErr != nil {
				return nil, spawnErr
			}
			childPID <- child.PID()
			go func() { _ = c.ServeChannel(c.Context, child) }()
			go func() {
				exit := <-child.Done()
				childExit <- exit
				c.Stop(nil)
			}()
			return product, nil
		})
		serveDone <- err
	}()

	select {
	case exit := <-childExit:
		if exit.Code != 0 || exit.Signal != 0 {
			t.Errorf("child exit = %+v, want clean; stderr:\n%s", exit, stderr.String())
		}
	case <-time.After(60 * time.Second):
		t.Error("the spawned channel client never settled")
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve() = %v; child stderr:\n%s", err, stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Serve did not return after the product stop")
	}
	pid := <-childPID
	callers := product.snapshot()
	if len(callers) != 2 {
		t.Fatalf("callers = %+v, want the minted and adopted lanes' two requests", callers)
	}
	for _, caller := range callers {
		if caller.PID != pid || caller.UID != uint32(os.Getuid()) { //nolint:gosec // kernel UIDs are non-negative
			t.Fatalf("caller = %+v, want the spawned child pid %d at this uid", caller, pid)
		}
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type spawnChannelEchoProduct struct {
	mu      sync.Mutex
	callers []Caller
}

func (p *spawnChannelEchoProduct) Handle(_ context.Context, req Request) (Reply, error) {
	p.mu.Lock()
	p.callers = append(p.callers, req.Caller)
	p.mu.Unlock()
	return Reply{Body: req.Body}, nil
}

func (p *spawnChannelEchoProduct) snapshot() []Caller {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Caller(nil), p.callers...)
}

func (*spawnChannelEchoProduct) Drain(Budget) error { return nil }

func (*spawnChannelEchoProduct) Close(Budget) error { return nil }

func childSpawnChannelClient() int {
	handoff, err := claimSpawnedHandoff()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: claim: %v\n", err)
		return 65
	}
	channel, ok := handoff.conn.(*net.UnixConn)
	if !ok {
		fmt.Fprintf(os.Stderr, "child: channel is %T\n", handoff.conn)
		return 65
	}
	defer channel.Close()

	if code := childMintAndCall(channel, handoff.nonce); code != 0 {
		return code
	}
	if code := childDelegateAndCall(channel, handoff.nonce); code != 0 {
		return code
	}
	return 0
}

func childMintAndCall(channel *net.UnixConn, spawnNonce []byte) int {
	payload, err := json.Marshal(wire.SpawnChannelRequest{Op: wire.SpawnChannelOpMint, Nonce: spawnNonce})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: encode mint: %v\n", err)
		return 66
	}
	if err := wire.WriteSpawnChannelFrame(channel, payload, -1); err != nil {
		fmt.Fprintf(os.Stderr, "child: write mint: %v\n", err)
		return 66
	}
	response, fd, err := wire.ReadSpawnChannelFrame(channel, true)
	if err != nil || fd < 0 {
		fmt.Fprintf(os.Stderr, "child: mint response fd=%d: %v\n", fd, err)
		return 66
	}
	var mint wire.SpawnChannelMinted
	if err := json.Unmarshal(response, &mint); err != nil {
		fmt.Fprintf(os.Stderr, "child: decode mint: %v\n", err)
		return 66
	}
	mintedFile := os.NewFile(uintptr(fd), "minted")
	mintedConn, err := net.FileConn(mintedFile)
	_ = mintedFile.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: adopt minted end: %v\n", err)
		return 66
	}
	return childCallEcho(mintedConn, mint.Nonce, "minted")
}

// childDelegateAndCall speaks on the kept end concurrently with the adopt
// round trip, as a real delegated peer does: its hello is already in flight
// when the daemon admits the delivered descriptor.
func childDelegateAndCall(channel *net.UnixConn, spawnNonce []byte) int {
	keptFile, delegatedFile, err := proc.SocketpairFiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: delegated pair: %v\n", err)
		return 67
	}
	keptConn, err := net.FileConn(keptFile)
	_ = keptFile.Close()
	if err != nil {
		_ = delegatedFile.Close()
		fmt.Fprintf(os.Stderr, "child: kept end: %v\n", err)
		return 67
	}
	called := make(chan int, 1)
	go func() { called <- childCallEcho(keptConn, nil, "adopted") }()
	payload, err := json.Marshal(wire.SpawnChannelRequest{Op: wire.SpawnChannelOpAdopt, Nonce: spawnNonce})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: encode adopt: %v\n", err)
		return 67
	}
	writeErr := wire.WriteSpawnChannelFrame(channel, payload, int(delegatedFile.Fd()))
	_ = delegatedFile.Close()
	if writeErr != nil {
		fmt.Fprintf(os.Stderr, "child: write adopt: %v\n", writeErr)
		return 67
	}
	response, _, err := wire.ReadSpawnChannelFrame(channel, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: adopt response: %v\n", err)
		return 67
	}
	var adopted wire.SpawnChannelAdopted
	if err := json.Unmarshal(response, &adopted); err != nil || !adopted.Adopted {
		fmt.Fprintf(os.Stderr, "child: adoption = %+v: %v\n", adopted, err)
		return 67
	}
	return <-called
}

func childCallEcho(conn net.Conn, nonce []byte, lane string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial:      func(context.Context) (net.Conn, error) { return conn, nil },
		Authorize: func(net.Conn) error { return nil },
		Lane:      wire.LaneBusiness,
		Schema:    spawnChannelSchema,
		Nonce:     nonce,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: %s attach: %v\n", lane, err)
		return 68
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = client.Close(closeCtx)
	}()
	if err := client.WaitReady(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "child: %s ready: %v\n", lane, err)
		return 68
	}
	body := []byte(`{"probe":"` + lane + `"}`)
	result, err := client.Call(ctx, "echo", body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: %s call: %v\n", lane, err)
		return 68
	}
	var envelope struct {
		Body []byte `json:"Body"`
	}
	if err := json.Unmarshal(result.Response.Payload, &envelope); err != nil {
		fmt.Fprintf(os.Stderr, "child: %s envelope: %v\n", lane, err)
		return 68
	}
	if result.Outcome != wire.Delivered || result.Response.Rejected || !bytes.Equal(envelope.Body, body) {
		fmt.Fprintf(os.Stderr, "child: %s result = %+v\n", lane, result)
		return 68
	}
	return 0
}
