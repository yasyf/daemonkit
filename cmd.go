package daemonkit

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
)

// Cmd is one command run under durable process ownership. It mirrors the
// internal spawn contract field for field.
type Cmd struct {
	// Path is the exact executable: absolute and cleaned. Required. Spawn
	// never copies, stages, or rewrites Path — the recorded image, the
	// verified image, and the running image are one file. This is a named
	// cross-repo invariant, not an implementation accident: App Group
	// entitlement grants attach to the deployed path, and a staged copy would
	// run without them.
	Path string
	// Args follow Path in argv.
	Args []string
	// Env nil inherits the parent environment; non-nil is the child's exact
	// environment, except that a ChannelHandoff spawn appends exactly the
	// daemonkit-owned DAEMONKIT_SPAWNED_* variables after it. Duplicate keys
	// are a boundary refusal: posix_spawn passes envp verbatim and both macOS
	// __findenv and Go's syscall.copyenv keep the FIRST occurrence, so a
	// build-by-append that relied on last-wins would silently run the child on
	// the wrong value. Deduplicate before the boundary. A caller-supplied
	// DAEMONKIT_SPAWNED_* key is refused; an inherited one is stripped.
	Env []string
	// Dir is the working directory; empty inherits the caller's.
	Dir string
	// Stdin is delivered once, then the child's stdin closes; empty means
	// /dev/null. A Spawn on ChannelStdio refuses it — stdin is the channel.
	Stdin []byte
	// Session gives the child its own session and process group; settlement
	// and reclaim then cover the descendants still inside that session, not
	// just the child. A descendant that setsid()s out of it leaves the only
	// scope the kernel offers and is neither signalled nor counted. Spawn's
	// choice alone — a long-lived co-process may legitimately want the
	// caller's session or its own — and refused by Run, which always spawns
	// into a dedicated session because a disposable command that leaves
	// descendants behind is a leak, not a posture.
	Session bool
	// Exec is the trust posture the executable behind Path must prove before
	// the child runs its first instruction. Required: the zero Serving refuses
	// at the boundary, so every Run and Spawn site states its posture
	// greppably. ServingSigned(r) verifies the suspended child's kernel-held
	// code identity — the image the kernel established at exec, in place,
	// never a staged copy — and a failed verify aborts the spawn before
	// release. ServingSameUser() is the named waiver.
	Exec Serving
	// Limits is the spawned session's resource declaration, ChannelHandoff
	// only: Spawn conveys it to the child alongside the nonce, and both ends
	// adopt it, so the two ends of one handoff cannot skew. Refused non-zero
	// on any other channel and on Run.
	Limits Limits
	// MaxOutput caps each retained Run stream; 4 MiB when zero. Run only:
	// Spawn refuses a Cmd naming it.
	MaxOutput Bytes
}

// Limits is one exact spawned-session resource declaration.
type Limits struct {
	MaxFrame    Bytes // 4 MiB when zero
	Concurrency int   // in-flight requests; 8 when zero
}

// Channel is the duplex channel a Spawn establishes before the child can run.
// A value outside the three named constants is refused at the spawn boundary.
type Channel uint8

const (
	// ChannelNone spawns with no channel; stdout goes to /dev/null.
	ChannelNone Channel = iota
	// ChannelHandoff duplicates a pre-connected unix socketpair end to the
	// child at fd 3; Child.Conn returns the parent end. The channel exists
	// before the first child instruction, and the kernel pair has no path a
	// squatter could bind.
	ChannelHandoff
	// ChannelStdio joins the child's stdin and stdout into one deadline-aware
	// net.Conn, for foreign executables — ssh, a Python worker — that cannot
	// take fd 3.
	ChannelStdio

	channelLimit
)

// RunResult is one bounded Run. Stdout and Stderr are returned alongside any
// error, never discarded on failure.
type RunResult struct {
	Exit   Exit
	Stdout []byte
	Stderr []byte
}

// Exit is a child's terminal value.
type Exit struct {
	// Code is the exit status; -1 when the child died by signal.
	Code int
	// Signal is the fatal signal; zero when the child exited.
	Signal syscall.Signal
	// Reap is how the exit was proven (shared with Stopped.Reap).
	Reap Reap
}

func (e Exit) clean() bool { return e.Code == 0 && e.Signal == 0 }

// ExitError reports a command that ran and settled unsuccessfully. Recover it
// with errors.As to branch on Exit.Code — a nonzero exit, a signal death, and
// a deadline are three different errors.
type ExitError struct{ Exit Exit }

func (e *ExitError) Error() string {
	if e.Exit.Signal != 0 {
		return fmt.Sprintf("daemonkit: command died on signal %s", e.Exit.Signal)
	}
	return fmt.Sprintf("daemonkit: command exited with status %d", e.Exit.Code)
}

// ErrTruncated means the retained bytes are not the whole stream: either it
// exceeded Cmd.MaxOutput, or the drain was severed at the settlement deadline
// with a descendant still holding the pipe. The RunResult carries what was
// retained and the exit, and the shortfall is an error by default: a silently
// shortened stream a caller parses is corruption, not data. Callers that
// accept it match it and proceed.
var ErrTruncated = errors.New("daemonkit: run output is not the whole stream")

// spawnLimitsEnv conveys Cmd.Limits to a ChannelHandoff child, so parent and
// child adopt one declaration and cannot skew. Its value is
// "<maxframe>,<concurrency>" in decimal.
const spawnLimitsEnv = "DAEMONKIT_SPAWNED_LIMITS"

// spawnEnvPrefix is daemonkit's spawn-owned environment namespace: a caller
// naming a key inside it is refused, and an inherited one is stripped, so the
// variables Spawn appends cannot collide with the caller's by construction.
const spawnEnvPrefix = "DAEMONKIT_SPAWNED_"

// validate is the public spawn boundary's contract on a command, restating in
// this package's register what the internal spawn boundary would refuse
// later, plus the postures and the combinations only one verb accepts.
func (c Cmd) validate(verb string, channel Channel) error {
	if c.Path == "" {
		return errors.New("daemonkit: Cmd.Path is required")
	}
	if !filepath.IsAbs(c.Path) || filepath.Clean(c.Path) != c.Path {
		return fmt.Errorf("daemonkit: Cmd.Path %q is not absolute and clean", c.Path)
	}
	if c.Dir != "" && (!filepath.IsAbs(c.Dir) || filepath.Clean(c.Dir) != c.Dir) {
		return fmt.Errorf("daemonkit: Cmd.Dir %q is not absolute and clean", c.Dir)
	}
	if !c.Exec.stated() {
		return fmt.Errorf("daemonkit: %s requires a stated Cmd.Exec posture (ServingSigned or ServingSameUser)", verb)
	}
	if channel >= channelLimit {
		return fmt.Errorf("daemonkit: channel %d is not one of ChannelNone, ChannelHandoff, ChannelStdio", channel)
	}
	if channel == ChannelStdio && len(c.Stdin) > 0 {
		return errors.New("daemonkit: Cmd.Stdin is refused on ChannelStdio — stdin is the channel")
	}
	if c.Limits != (Limits{}) && channel != ChannelHandoff {
		return fmt.Errorf("daemonkit: Cmd.Limits is the handoff session's declaration and is refused by %s on this channel", verb)
	}
	if c.MaxOutput != 0 && verb != "Run" {
		return fmt.Errorf("daemonkit: Cmd.MaxOutput bounds a Run's retained streams and is refused by %s", verb)
	}
	if c.Session && verb == "Run" {
		return errors.New("daemonkit: Cmd.Session is Spawn's posture; Run always gives its child a dedicated session and refuses the field")
	}
	if c.MaxOutput < 0 {
		return fmt.Errorf("daemonkit: Cmd.MaxOutput %d is negative", c.MaxOutput)
	}
	if c.Limits.MaxFrame < 0 || c.Limits.Concurrency < 0 {
		return fmt.Errorf("daemonkit: Cmd.Limits %+v is negative", c.Limits)
	}
	return validateEnv(c.Env)
}

func validateEnv(env []string) error {
	if env == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, spawnEnvPrefix) {
			return fmt.Errorf("daemonkit: Cmd.Env names %s, which is daemonkit's own spawn namespace", key)
		}
		if _, repeated := seen[key]; repeated {
			return fmt.Errorf(
				"daemonkit: Cmd.Env repeats %s; posix_spawn passes envp verbatim and the first occurrence wins, so deduplicate before the boundary",
				key,
			)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// childEnv is the child's exact environment: the caller's, or the inherited
// one, with daemonkit's spawn namespace stripped either way, then the exact
// variables this channel conveys appended.
func childEnv(c Cmd, channel Channel, nonce []byte) []string {
	source := c.Env
	if source == nil {
		source = os.Environ()
	}
	env := make([]string, 0, len(source)+2)
	for _, entry := range source {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, spawnEnvPrefix) {
			continue
		}
		env = append(env, entry)
	}
	if channel != ChannelHandoff {
		return env
	}
	return append(env,
		fmt.Sprintf("%s=%x", wire.SpawnedNonceEnv, nonce),
		fmt.Sprintf("%s=%d,%d", spawnLimitsEnv, c.Limits.MaxFrame, c.Limits.Concurrency),
	)
}

func parseSpawnLimits(value string) (Limits, error) {
	frame, concurrency, ok := strings.Cut(value, ",")
	if !ok {
		return Limits{}, fmt.Errorf("daemonkit: %s value %q is not <maxframe>,<concurrency>", spawnLimitsEnv, value)
	}
	maxFrame, err := strconv.ParseInt(frame, 10, 64)
	if err != nil {
		return Limits{}, fmt.Errorf("daemonkit: %s max frame: %w", spawnLimitsEnv, err)
	}
	inFlight, err := strconv.Atoi(concurrency)
	if err != nil {
		return Limits{}, fmt.Errorf("daemonkit: %s concurrency: %w", spawnLimitsEnv, err)
	}
	return Limits{MaxFrame: Bytes(maxFrame), Concurrency: inFlight}, nil
}

// command compiles a validated Cmd into the internal spawn contract, with the
// exec posture compiled into the release gate that runs against the suspended
// child.
func command(c Cmd, channel Channel, nonce []byte) proc.Cmd {
	exec := c.Exec
	return proc.Cmd{
		Path:      c.Path,
		Dir:       c.Dir,
		Args:      c.Args,
		Env:       childEnv(c, channel, nonce),
		Stdin:     c.Stdin,
		MaxOutput: proc.Bytes(c.MaxOutput),
		Session:   c.Session,
		Channel:   procChannel(channel),
		Verify:    func(pid int) error { return exec.verifyProcess(pid) },
	}
}

func procChannel(channel Channel) proc.Channel {
	switch channel {
	case ChannelHandoff:
		return proc.ChannelHandoff
	case ChannelStdio:
		return proc.ChannelStdio
	}
	return proc.ChannelNone
}

func exitOf(exit proc.Exit) Exit {
	return Exit{Code: exit.Code, Signal: exit.Signal, Reap: Reap(exit.Reap)}
}
