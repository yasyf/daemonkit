// Package peer defines the OS-authenticated identity shared by transport and trust.
package peer

import "github.com/yasyf/daemonkit/proc"

// Identity is the exact process identity captured from a Unix socket peer.
type Identity struct {
	PID        int
	UID        int
	StartTime  string
	Comm       string
	Boot       string
	Executable string
	Audit      []byte
}

// ProcessIdentity returns the peer's kernel process identity captured at accept.
func (p Identity) ProcessIdentity() proc.Identity {
	identity := proc.Identity{PID: p.PID, StartTime: p.StartTime, Comm: p.Comm, Boot: p.Boot, Executable: p.Executable}
	if token, err := proc.AuditTokenFromBytes(p.Audit); err == nil {
		identity.AuditToken = token
	}
	return identity
}

// MatchesProcess reports whether rec names the exact process instance.
func (p Identity) MatchesProcess(rec proc.Record) bool {
	return p.PID == rec.PID && p.StartTime != "" && p.StartTime == rec.StartTime &&
		p.Boot != "" && p.Boot == rec.Boot
}
