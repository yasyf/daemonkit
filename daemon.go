package daemonkit

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/daemonkit/paths"
)

// Daemon is one daemon's whole identity. The process that serves it and the
// process that launches it read this same value, so no fact is declared twice
// and none can disagree with another. Socket, lock, state dir, record file,
// and launchd job all derive from Label through paths.
type Daemon struct {
	Label       Label
	Program     Program // the executable launchd runs; Ensure places it
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

// element is a Label the rule accepted. Every path this package joins a Label
// into is joined from an element and never from a Label, and Label.element is
// the only thing that makes one, so a path added later inherits the rule
// instead of restating a weaker one beside it. The unexported field is what
// holds that: a Label cannot be converted into an element, only accepted into
// one.
type element struct{ label string }

// element refuses a Label that is not a launchd job label. The rule lives in
// launchd and is read from there rather than copied: the Label names a
// LaunchAgent before it names anything else, so the strictest reading of it is
// the only one, and a second rule at any of the paths that derive from it is a
// rule that disagrees.
func (l Label) element() (element, error) {
	if err := launchd.ValidateLabel(string(l)); err != nil {
		return element{}, fmt.Errorf("daemonkit: %w", err)
	}
	return element{label: string(l)}, nil
}

func (e element) state() paths.Paths { return paths.Agent(e.label) }

func (e element) socket() (string, error) { return paths.Socket(e.label) }

func (e element) record() string { return filepath.Join(e.state().StateDir(), "daemon.records") }

// RecordPath is where Serve persists this daemon's durable owner record: the
// {pid, start, boot, generation, build} core it writes behind the flock before
// it binds. An inventory gate outside this package reads it to correlate a
// live process nothing could name against the identity this daemon recorded —
// the only thing that says whose husk it is.
//
// It is the one derivation that states the layout without running the rule
// itself, because it does no I/O and has no refusal to return. What carries the
// rule instead is the boundary: every entry point that takes a Daemon — Serve,
// deploy.Open, and each Client verb — refuses a Label launchd would, so a
// Daemon that reached any code in this module has been accepted and the path is
// inside its own state directory. A Daemon that crossed no boundary gets an
// unchecked join here, and the file it names is not one to read.
func (d Daemon) RecordPath() string { return element{label: string(d.Label)}.record() }

func (d Daemon) shutdownGrace() Grace {
	if d.Shutdown == 0 {
		return Grace(30 * time.Second)
	}
	return d.Shutdown
}

// ownSchema is the application protocol this build speaks, empty when the
// daemon declares none; the business lane refuses that at its config boundary
// rather than attaching under a schema no daemon accepts.
func (d Daemon) ownSchema() Schema {
	if len(d.Schemas) == 0 {
		return ""
	}
	return d.Schemas[0]
}

func (d Daemon) wireSchemas() wire.Schemas {
	schemas := make(wire.Schemas, len(d.Schemas))
	for i, schema := range d.Schemas {
		schemas[i] = string(schema)
	}
	return schemas
}

// Schema names one application-protocol era a build speaks or still accepts.
type Schema string

// Bytes is a size in bytes.
type Bytes int64

// Grace is a whole operation budget and the only duration this API accepts.
// Shutdown and Handshake bound disjoint operations; every bound inside either
// is a Share or Reserve of it, so a sibling literal has no parameter to take.
type Grace time.Duration

const maxGrace = Grace(24 * time.Hour)

// ValidateForServe is the config-boundary check every entry point taking a
// Daemon runs before the value reaches a derivation — Serve on its argument,
// deploy.Open on Config.Daemon — and it runs before any Budget is minted. The
// Label must be one launchd itself would accept, since every path, lock,
// socket, and job name is joined from it; every Grace must lie in (0, 24h],
// zero meaning its documented default. Rejecting the range here is what keeps
// every deadline downstream of mint within budget arithmetic's exact domain.
// Trust.Business, when stated at all, must name at least one requirement: a
// disjunction over nothing admits nobody, and reading as the unset field
// instead would open the lane to every same-UID peer.
func (d Daemon) ValidateForServe() error {
	if _, err := d.Label.element(); err != nil {
		return err
	}
	if err := d.Shutdown.validate("Shutdown"); err != nil {
		return err
	}
	if err := d.Handshake.validate("Handshake"); err != nil {
		return err
	}
	if d.Trust.Business != nil && len(d.Trust.Business) == 0 {
		return errors.New("daemonkit: Trust.Business is stated but empty; leave it nil for the same-EUID floor alone")
	}
	return nil
}

// ValidateForClient is the config-boundary check Open runs once: every Grace a
// client consumes lies in (0, 24h], Label is present, and Trust.Serving is
// stated. It mirrors ValidateForServe on the client's half of the Daemon
// value — Handshake bounds admission on the serving side and no client verb
// reads it, so it is not judged here.
func (d Daemon) ValidateForClient() error {
	if _, err := d.Label.element(); err != nil {
		return err
	}
	if err := d.Shutdown.validate("Shutdown"); err != nil {
		return err
	}
	return d.Trust.Serving.validate("Trust.Serving")
}

func (g Grace) validate(field string) error {
	if g < 0 || g > maxGrace {
		return fmt.Errorf("daemonkit: %s grace %v is outside (0, 24h]", field, time.Duration(g))
	}
	return nil
}

// Trust names what each lane must additionally prove. The same-EUID floor
// runs unconditionally on both ends before Trust is consulted; no Trust value
// can express its absence.
type Trust struct {
	Control  *Requirement // nil: floor alone. Gates Control and the wire Drain verb.
	Business Requirements // nil: floor alone. Any element admits.
	// Serving is what the process answering on the socket must prove to a
	// client attaching to it; the daemon never reads it.
	Serving Serving
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
