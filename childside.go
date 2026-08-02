package daemonkit

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
	"golang.org/x/sys/unix"
)

// spawnedHandoff is everything a ChannelHandoff spawn conveyed to this child:
// the proven descriptor and the two variables that rode the exec beside it.
type spawnedHandoff struct {
	conn   net.Conn
	nonce  []byte
	limits Limits
}

// errHandoffClaimed is D8's child-side single-take: one descriptor, one claim,
// whichever entry point makes it.
var errHandoffClaimed = errors.New("daemonkit: the spawned handoff was already claimed")

var (
	claimMu    sync.Mutex
	claimTaken bool
)

// ClaimHandoff claims the ChannelHandoff descriptor a daemonkit parent placed
// at fd 3, single-take, verified to be a unix socketpair end whose creator is
// this process's parent (creds.pid == getppid, creds.uid == getuid). The Go
// child half of the handoff contract; a non-Go child just uses fd 3. A second
// claim — direct or via ServeSpawned, in either order — is a named refusal,
// mirroring the parent-side Conn/Business single-take.
func ClaimHandoff() (net.Conn, error) {
	handoff, err := claimSpawnedHandoff()
	if err != nil {
		return nil, err
	}
	return handoff.conn, nil
}

// Handler serves one admitted request on a spawned session.
type Handler func(ctx context.Context, req Request) (Reply, error)

// ServeSpawned is the child half of a ChannelHandoff spawn: it claims fd 3 —
// ClaimHandoff's structural check, so the pair's creator is proven to be this
// process's parent at the same UID — adopts the session limits the parent
// conveyed alongside the nonce, echoes the nonce, and serves exactly one
// session until ctx ends or the parent closes the channel. contract's limits
// must be zero or equal to the conveyed ones; disagreement refuses before
// serving. The claim is the same single take ClaimHandoff makes, so one after
// the other, in either order, is a named refusal. A spawned server is not a
// daemon — no flock, no owner record, no launchd job, no drain ladder.
func ServeSpawned(ctx context.Context, contract Contract, handle Handler) error {
	if err := contract.validate(); err != nil {
		return err
	}
	if handle == nil {
		return errors.New("daemonkit: ServeSpawned requires a handler")
	}
	handoff, err := claimSpawnedHandoff()
	if err != nil {
		return err
	}
	limits, err := contract.adopt(handoff.limits)
	if err != nil {
		_ = handoff.conn.Close()
		return err
	}
	return wire.RunSpawnedSession(ctx, wire.SpawnedSessionConfig{
		Conn:    handoff.conn,
		Nonce:   handoff.nonce,
		Schema:  string(contract.Schema),
		Limits:  sessionLimits(limits),
		Runtime: spawnedRuntime{handle: handle},
	})
}

// spawnedRuntime is the child half's wire.Runtime: one session that is ready
// the moment it is served, with no lifecycle to publish and no drain ladder to
// run behind it.
type spawnedRuntime struct{ handle Handler }

func (r spawnedRuntime) Handle(ctx context.Context, req wire.Request) (any, error) {
	return handleBusiness(ctx, req, r.handle)
}

func (spawnedRuntime) Phase() wire.PhaseSnapshot {
	return wire.PhaseSnapshot{Sequence: 1, Phase: wire.PhaseReady}
}

func (r spawnedRuntime) WaitPhase(ctx context.Context, after uint64) (wire.PhaseSnapshot, error) {
	if after == 0 {
		return r.Phase(), nil
	}
	<-ctx.Done()
	return wire.PhaseSnapshot{}, ctx.Err()
}

func (spawnedRuntime) Drain() {}

// claimSpawnedHandoff performs the one claim both child-side entry points
// share. The DAEMONKIT_SPAWNED_* variables are read once and unset here: they
// are not secrets — any same-UID process reads a peer's environment via
// KERN_PROCARGS2 — but a variable left behind would ride the next exec this
// child makes and name a descriptor that child never inherited.
func claimSpawnedHandoff() (spawnedHandoff, error) {
	claimMu.Lock()
	defer claimMu.Unlock()
	if claimTaken {
		return spawnedHandoff{}, errHandoffClaimed
	}
	claimTaken = true
	return readSpawnedHandoff()
}

func readSpawnedHandoff() (spawnedHandoff, error) {
	encoded, nonceSet := os.LookupEnv(wire.SpawnedNonceEnv)
	limits, limitsSet := os.LookupEnv(spawnLimitsEnv)
	if err := errors.Join(os.Unsetenv(wire.SpawnedNonceEnv), os.Unsetenv(spawnLimitsEnv)); err != nil {
		return spawnedHandoff{}, fmt.Errorf("daemonkit: unset the spawn conveyance: %w", err)
	}
	if !nonceSet || !limitsSet {
		return spawnedHandoff{}, fmt.Errorf(
			"daemonkit: %s and %s are absent; this process was not spawned on ChannelHandoff",
			wire.SpawnedNonceEnv, spawnLimitsEnv,
		)
	}
	nonce, err := hex.DecodeString(encoded)
	if err != nil {
		return spawnedHandoff{}, fmt.Errorf("daemonkit: decode %s: %w", wire.SpawnedNonceEnv, err)
	}
	declared, err := parseSpawnLimits(limits)
	if err != nil {
		return spawnedHandoff{}, err
	}
	file, err := proc.ClaimHandoff()
	if err != nil {
		return spawnedHandoff{}, fmt.Errorf("daemonkit: claim the spawned handoff: %w", err)
	}
	conn, err := net.FileConn(file)
	closeErr := file.Close()
	if err != nil {
		return spawnedHandoff{}, errors.Join(fmt.Errorf("daemonkit: adopt the spawned handoff: %w", err), closeErr)
	}
	if closeErr != nil {
		_ = conn.Close()
		return spawnedHandoff{}, closeErr
	}
	return spawnedHandoff{conn: conn, nonce: nonce, limits: declared}, nil
}

// CloseInheritedFDs closes every inherited non-CLOEXEC descriptor above
// stderr. A daemon calls it as its first act so no parent lease fd stays
// pinned for the process's life.
func CloseInheritedFDs() error {
	directory, err := os.Open("/dev/fd")
	if err != nil {
		return fmt.Errorf("daemonkit: list open descriptors: %w", err)
	}
	names, readErr := directory.Readdirnames(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		return fmt.Errorf("daemonkit: list open descriptors: %w", errors.Join(readErr, closeErr))
	}
	for _, name := range names {
		fd, err := strconv.Atoi(name)
		if err != nil || fd < 3 {
			continue
		}
		// The closed enumeration descriptor reads EBADF here and is skipped.
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0) //nolint:gosec // /dev/fd entries are non-negative descriptors
		if err != nil || flags&unix.FD_CLOEXEC != 0 {
			continue
		}
		_ = unix.Close(fd)
	}
	return nil
}
