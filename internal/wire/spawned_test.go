package wire_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
)

const spawnedChildEnv = "DAEMONKIT_WIRE_SPAWNED_CHILD"

// TestMain branches on the child marker before m.Run so a spawned copy of this
// binary serves its session and exits instead of re-entering the suite — the
// re-exec fork-bomb guard scripts/test.sh backstops.
func TestMain(m *testing.M) {
	if os.Getenv(spawnedChildEnv) == "1" {
		runSpawnedChild()
	}
	os.Exit(m.Run())
}

func runSpawnedChild() {
	nonce, err := hex.DecodeString(os.Getenv(wire.SpawnedNonceEnv))
	if err != nil {
		fmt.Fprintf(os.Stderr, "spawned child: decode nonce: %v\n", err)
		os.Exit(72)
	}
	err = wire.RunSpawnedSession(context.Background(), wire.SpawnedSessionConfig{
		Nonce:  nonce,
		Schema: "spawned.v1",
		Handlers: []wire.HandlerSpec{{
			Op:         "child.echo.v1",
			Concurrent: true,
			Handler: func(_ context.Context, req wire.Request) (any, error) {
				return json.RawMessage(req.Payload), nil
			},
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "spawned child: %v\n", err)
		os.Exit(71)
	}
	os.Exit(0)
}

func spawnChild(t *testing.T, envNonce []byte) *proc.Child {
	t.Helper()
	openCtx, cancelOpen := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelOpen()
	store, err := proc.OpenStore(openCtx, filepath.Join(t.TempDir(), "records.dkstate"))
	if err != nil {
		t.Fatalf("OpenStore() = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() = %v", err)
	}
	child, err := store.Spawn(t.Context(), proc.Cmd{
		Path:    executable,
		Channel: proc.ChannelHandoff,
		Env: append(
			os.Environ(),
			spawnedChildEnv+"=1",
			wire.SpawnedNonceEnv+"="+hex.EncodeToString(envNonce),
		),
	}, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	return child
}

func handoffConn(t *testing.T, child *proc.Child) *net.UnixConn {
	t.Helper()
	conn, err := child.TakeChannel()
	if err != nil {
		t.Fatalf("TakeChannel() = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn.(*net.UnixConn)
}

func awaitExit(t *testing.T, child *proc.Child) proc.Exit {
	t.Helper()
	select {
	case exit := <-child.Done():
		return exit
	case <-time.After(15 * time.Second):
		t.Fatal("spawned child never settled")
		return proc.Exit{}
	}
}

func TestSpawnedSessionRoundTripOverCmdHandoff(t *testing.T) {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	child := spawnChild(t, nonce)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := wire.NewSpawnedClient(ctx, wire.SpawnedClientConfig{
		Conn: handoffConn(t, child), Nonce: nonce, Schema: "spawned.v1",
	})
	if err != nil {
		t.Fatalf("NewSpawnedClient() = %v", err)
	}
	if client.WireBuild() != "spawned.v1" {
		t.Fatalf("WireBuild() = %q", client.WireBuild())
	}
	result, err := client.Call(ctx, "child.echo.v1", "", []byte(`{"hello":"child"}`))
	if err != nil {
		t.Fatalf("Call() = %v", err)
	}
	if result.Outcome != wire.Delivered || string(result.Response.Payload) != `{"hello":"child"}` {
		t.Fatalf("result = %v %s", result.Outcome, result.Response.Payload)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if exit := awaitExit(t, child); exit.Code != 0 {
		t.Fatalf("child exit = %+v, want code 0", exit)
	}
}

func TestSpawnedSessionRejectsWrongNonce(t *testing.T) {
	childNonce := make([]byte, 32)
	parentNonce := make([]byte, 32)
	parentNonce[0] = 1
	child := spawnChild(t, childNonce)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := wire.NewSpawnedClient(ctx, wire.SpawnedClientConfig{
		Conn: handoffConn(t, child), Nonce: parentNonce, Schema: "spawned.v1",
	})
	if err == nil {
		_ = client.Abort(nil)
		t.Fatal("NewSpawnedClient() with a wrong nonce succeeded")
	}
	if !errors.Is(err, wire.ErrHandshake) {
		t.Fatalf("NewSpawnedClient() = %v, want a handshake failure", err)
	}
	if exit := awaitExit(t, child); exit.Code != 71 {
		t.Fatalf("child exit = %+v, want the nonce-refusal code 71", exit)
	}
}
