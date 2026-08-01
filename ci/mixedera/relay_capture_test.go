//go:build mixedera

package mixedera

import (
	"bytes"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/ci/mixedera/coverage"
)

func echoUpstream(t *testing.T) string {
	t.Helper()
	path := filepath.Join(socketDir(t), "up.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return path
}

func speak(t *testing.T, path string, payload []byte) {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(quiesceWait)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	echoed := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, echoed); err != nil {
		t.Fatal(err)
	}
}

func TestRelayCapturesEveryConnectionSeparately(t *testing.T) {
	front := newRelay(t, echoUpstream(t))
	spoken := [][]byte{
		[]byte("SACRIFICIAL-----"),
		[]byte("MUTATED---------"),
		[]byte("THIRD-----------"),
	}
	for _, payload := range spoken {
		speak(t, front.path, payload)
	}
	crossings := front.quiesce(t)
	if len(crossings) != len(spoken) {
		t.Fatalf("the relay reported %d of %d connections", len(crossings), len(spoken))
	}
	for i, want := range spoken {
		if !bytes.Equal(crossings[i].opened, want) {
			t.Errorf("connection %d opened with %q, want %q", i+1, crossings[i].opened, want)
		}
		if !bytes.Equal(crossings[i].answered, want) {
			t.Errorf("connection %d was answered with %q, want %q", i+1, crossings[i].answered, want)
		}
	}
	coverage.Observe(t)
}
