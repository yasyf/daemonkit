// Command e2ehelper is the real daemon the launchd end-to-end suite deploys:
// launchd execs it, it serves one daemonkit Daemon, and it appends a timestamped
// line to the shared event log at every lifecycle edge so the test can time the
// drain ladder from the daemon's own side.
//
// Argv is [home, label]. The home is re-exported as DAEMONKIT_HOME before Serve
// runs, because Daemon.agent() renders no EnvironmentVariables of its own and a
// launchd-spawned daemon would otherwise resolve its socket out of the passwd
// home while the launcher resolves it out of the sandbox.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/realhome"
)

// variant is stamped through -ldflags so two builds of this same source differ
// in every byte the digest reads.
var variant = "unset"

// EventLog is the shared append-only lifecycle log, relative to the sandbox home.
const EventLog = "events.log"

// behavior is the per-run knob file the daemon re-reads at every start, so a
// test changes what Drain and Close do without changing the plist.
type behavior struct {
	Drain string `json:"drain"`
	Close string `json:"close"`
	Start string `json:"start"`
}

func (b behavior) durations() (start, drain, closing time.Duration, err error) {
	for _, field := range []struct {
		raw  string
		into *time.Duration
	}{{b.Start, &start}, {b.Drain, &drain}, {b.Close, &closing}} {
		if field.raw == "" {
			continue
		}
		parsed, parseErr := time.ParseDuration(field.raw)
		if parseErr != nil {
			return 0, 0, 0, parseErr
		}
		*field.into = parsed
	}
	return start, drain, closing, nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "e2ehelper: want [home label], got %v\n", os.Args[1:])
		os.Exit(64)
	}
	home, label := os.Args[1], os.Args[2]
	if err := os.Setenv(realhome.EnvOverride, home); err != nil {
		fmt.Fprintf(os.Stderr, "e2ehelper: %v\n", err)
		os.Exit(65)
	}
	start, drain, closing, err := readBehavior(home).durations()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2ehelper: behavior: %v\n", err)
		os.Exit(66)
	}
	event(home, "serve.enter pid=%d variant=%s ppid=%d", os.Getpid(), variant, os.Getppid())
	drained, err := daemonkit.Serve(
		context.Background(),
		daemonkit.Daemon{
			Label:    daemonkit.Label(label),
			Schemas:  []daemonkit.Schema{"daemonkit.e2e.v1"},
			Shutdown: daemonkit.Grace(shutdownGrace),
		},
		func(daemonkit.Ctx) (daemonkit.Product, error) {
			time.Sleep(start)
			event(home, "start.ready pid=%d", os.Getpid())
			return product{home: home, drain: drain, closing: closing}, nil
		},
	)
	event(home, "serve.return pid=%d settled=%v abandoned=%v err=%v",
		os.Getpid(), drained.Settled, drained.Abandoned, err)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2ehelper: %v\n", err)
		os.Exit(71)
	}
	os.Exit(0)
}

// shutdownGrace is both the whole drain budget and the plist's ExitTimeOut, so
// the ladder's total and launchd's SIGKILL backstop are the same number by
// construction — the coincidence the wedge case exists to measure.
const shutdownGrace = 6 * time.Second

func readBehavior(home string) behavior {
	data, err := os.ReadFile(filepath.Join(home, "behavior.json")) //nolint:gosec // home is this daemon's own argv-supplied sandbox root
	if err != nil {
		return behavior{}
	}
	var b behavior
	if err := json.Unmarshal(data, &b); err != nil {
		return behavior{}
	}
	return b
}

type product struct {
	home    string
	drain   time.Duration
	closing time.Duration
}

func (product) Handle(context.Context, daemonkit.Request) (daemonkit.Reply, error) {
	return daemonkit.Reply{}, errors.New("e2ehelper: no business surface")
}

func (p product) Drain(daemonkit.Budget) error {
	event(p.home, "drain.enter pid=%d", os.Getpid())
	time.Sleep(p.drain)
	event(p.home, "drain.exit pid=%d", os.Getpid())
	return nil
}

func (p product) Close(daemonkit.Budget) error {
	event(p.home, "close.enter pid=%d", os.Getpid())
	time.Sleep(p.closing)
	event(p.home, "close.exit pid=%d", os.Getpid())
	return nil
}

// event appends one durably-flushed line, so the record of a process SIGKILLed
// mid-ladder is exactly the edges it reached.
func event(home, format string, args ...any) {
	file, err := os.OpenFile(filepath.Join(home, EventLog), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // home is this daemon's own argv-supplied sandbox root
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(file, "%d %s\n", time.Now().UnixNano(), fmt.Sprintf(format, args...))
	_ = file.Sync()
	_ = file.Close()
}
