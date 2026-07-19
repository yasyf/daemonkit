package wire_test

import (
	"net"
	"os"
	"runtime"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/wiretest"
)

func TestPeerFromConnSelf(t *testing.T) {
	client, server := wiretest.Pair(t)
	ends := []struct {
		name string
		conn *net.UnixConn
	}{
		{"server end", server},
		{"client end", client},
	}
	for _, tt := range ends {
		t.Run(tt.name, func(t *testing.T) {
			p, err := wire.PeerFromConn(tt.conn)
			if err != nil {
				t.Fatalf("PeerFromConn: %v", err)
			}
			if p.UID != os.Getuid() {
				t.Errorf("UID = %d, want %d", p.UID, os.Getuid())
			}
			if p.PID != os.Getpid() {
				t.Errorf("PID = %d, want %d", p.PID, os.Getpid())
			}
			if p.StartTime == "" || p.Comm == "" || p.Boot == "" {
				t.Errorf("process identity = %+v, want complete kernel identity", p.ProcessIdentity())
			}
			if !p.MatchesProcess(proc.Record{PID: p.PID, StartTime: p.StartTime}) {
				t.Fatal("peer did not match its exact process record")
			}
			if p.MatchesProcess(proc.Record{PID: p.PID + 1, StartTime: p.StartTime}) {
				t.Fatal("peer matched a different pid")
			}
			if p.MatchesProcess(proc.Record{PID: p.PID, StartTime: p.StartTime + "-reused"}) {
				t.Fatal("peer matched a reused pid with a different start identity")
			}
			if runtime.GOOS == "darwin" {
				if len(p.Audit) != 32 {
					t.Errorf("Audit len = %d, want 32 (audit_token_t)", len(p.Audit))
				}
			} else if p.Audit != nil {
				t.Errorf("Audit = %v, want nil on %s", p.Audit, runtime.GOOS)
			}
		})
	}
}
