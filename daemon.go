package daemonkit

import (
	"fmt"
	"time"
)

// Daemon is one daemon's whole identity. The process that serves it and the
// process that launches it read this same value, so no fact is declared twice
// and none can disagree with another. Socket, lock, state dir, record file,
// and launchd job all derive from Label through paths.
type Daemon struct {
	Label       Label
	Program     Program // the executable launchd runs
	Args        []string
	Schemas     []Schema // [0] is what this build speaks; the rest are prior eras still accepted
	Trust       Trust    // lane requirements; the same-EUID floor is not here and cannot be turned off
	Restart     Restart
	Shutdown    Grace  // the whole drain budget AND the plist's ExitTimeOut; 30s when zero
	Handshake   Grace  // the whole admission budget; 10s when zero
	Log         string // launchd's stderr sink
	MaxFrame    Bytes  // 4 MiB when zero
	Concurrency int    // in-flight requests; every queue depth derives; 8 when zero
}

// Label names one daemon; every path, lock, record file, and launchd job
// derives from it.
type Label string

// Schema names one application-protocol era a build speaks or still accepts.
type Schema string

// Bytes is a size in bytes.
type Bytes int64

// Grace is a whole operation budget and the only duration this API accepts.
// Shutdown and Handshake bound disjoint operations; every bound inside either
// is a Share or Reserve of it, so a sibling literal has no parameter to take.
type Grace time.Duration

const maxGrace = Grace(24 * time.Hour)

// ValidateForServe is the config-boundary check Serve runs once before any
// Budget is minted: every Grace must lie in (0, 24h], zero meaning its
// documented default. Rejecting the range here is what keeps every deadline
// downstream of mint within budget arithmetic's exact domain.
func (d Daemon) ValidateForServe() error {
	for _, g := range []struct {
		field string
		grace Grace
	}{
		{"Shutdown", d.Shutdown},
		{"Handshake", d.Handshake},
	} {
		if g.grace < 0 || g.grace > maxGrace {
			return fmt.Errorf("daemonkit: %s grace %v is outside (0, 24h]", g.field, time.Duration(g.grace))
		}
	}
	return nil
}

// Trust names what each lane must additionally prove. The same-EUID floor
// runs unconditionally in the acceptor before Trust is consulted; no Trust
// value can express its absence.
type Trust struct {
	Control  *Requirement // nil: floor alone. Gates Control and the wire Drain verb.
	Business *Requirement // nil: floor alone.
}

// Restart is launchd's relaunch policy for the daemon's job.
type Restart uint8

const (
	// RestartNever leaves the daemon down once it exits.
	RestartNever Restart = iota
	// RestartOnFailure relaunches only after an unclean exit.
	RestartOnFailure
	// RestartAlways relaunches after every exit.
	RestartAlways
)
