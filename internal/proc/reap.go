package proc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"syscall"
	"time"
)

const settlementPollInterval = 10 * time.Millisecond

// Recover settles every prior-generation record through the reap ladder,
// sweeps any legacy bbolt files, and reports what it reclaimed and where an
// unreadable record file was archived. ctx must carry a deadline; every
// ladder bound derives from it.
func (s *Store) Recover(ctx context.Context, legacy []string) ([]Reclaimed, string, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, "", errors.New("proc: recover requires a context deadline")
	}
	var reclaimed []Reclaimed
	var errs []error
	var archives []string
	if s.archived != "" {
		archives = append(archives, s.archived)
	}
	for _, rec := range s.snapshot() {
		if rec.Generation == s.generation {
			continue
		}
		reclaimed, errs = s.recoverOne(ctx, rec.id(), rec.Session, reclaimed, errs)
	}
	for _, path := range legacy {
		archived, swept, err := s.sweepLegacy(ctx, path)
		reclaimed = append(reclaimed, swept...)
		if err != nil {
			errs = append(errs, err)
		}
		if archived != "" {
			archives = append(archives, archived)
		}
	}
	return reclaimed, strings.Join(archives, ", "), errors.Join(errs...)
}

func (s *Store) recoverOne(
	ctx context.Context,
	id identity,
	session int,
	reclaimed []Reclaimed,
	errs []error,
) ([]Reclaimed, []error) {
	outcome, err := s.reapIdentity(ctx, id, session)
	if err != nil {
		return reclaimed, append(errs, fmt.Errorf("reap child %d: %w", id.pid, err))
	}
	fate := RecordAbandoned
	select {
	case fate = <-s.retire(id):
	case <-ctx.Done():
	}
	return append(reclaimed, Reclaimed{PID: id.pid, Exit: Exit{Code: -1, Reap: outcome, Record: fate}}), errs
}

// An undecided outcome always keeps the record, so a probe failure can never
// read as dead.
func (s *Store) reapIdentity(ctx context.Context, id identity, session int) (Reap, error) {
	boot, err := s.prober.boot()
	if err != nil {
		return reapUndetermined, fmt.Errorf("load current boot identity: %w", err)
	}
	if id.crossBoot(boot) {
		return ReapCrossBoot, nil
	}
	if id.unsafe() {
		return reapUndetermined, fmt.Errorf("refusing unsafe process identity %d", id.pid)
	}
	info, err := s.prober.probe(id.pid)
	if session != 0 {
		return s.reapSession(ctx, id, session, info, err, boot)
	}
	switch {
	case errors.Is(err, errNoProc):
		return ReapAbsent, nil
	case err != nil:
		return reapUndetermined, err
	case !id.matches(identity{pid: id.pid, start: info.start, boot: boot}):
		return ReapReused, nil
	case info.zombie:
		return ReapAbsent, nil
	}
	return s.reapOrphan(ctx, id, boot)
}

// reapOrphan delivers SIGTERM, re-verifies identity through every poll of the
// grace share, then SIGKILLs and settles on observed absence. ESRCH anywhere
// is success; a PID reused during grace is never SIGKILLed.
func (s *Store) reapOrphan(ctx context.Context, id identity, boot uint64) (Reap, error) {
	gone, err := s.signalGone(id.pid, syscall.SIGTERM)
	if err != nil {
		return reapUndetermined, err
	}
	if gone {
		return ReapAbsent, nil
	}
	clk := clockOrReal(s.clock)
	grace := clk.Now().Add(graceShare(ctx, clk))
	for {
		select {
		case <-ctx.Done():
			return reapUndetermined, ctx.Err()
		case <-clk.After(min(settlementPollInterval, grace.Sub(clk.Now()))):
		}
		info, err := s.prober.probe(id.pid)
		switch {
		case errors.Is(err, errNoProc):
			return ReapTerminated, nil
		case err != nil:
			return reapUndetermined, err
		case !id.matches(identity{pid: id.pid, start: info.start, boot: boot}):
			return ReapReused, nil
		case info.zombie:
			return ReapTerminated, nil
		}
		if !clk.Now().Before(grace) {
			break
		}
	}
	gone, err = s.signalGone(id.pid, syscall.SIGKILL)
	if err != nil {
		return reapUndetermined, err
	}
	if gone {
		return ReapTerminated, nil
	}
	return s.awaitSettlement(ctx, id, boot)
}

func (s *Store) awaitSettlement(ctx context.Context, id identity, boot uint64) (Reap, error) {
	clk := clockOrReal(s.clock)
	deadline, _ := ctx.Deadline()
	for {
		info, err := s.prober.probe(id.pid)
		switch {
		case errors.Is(err, errNoProc):
			return ReapTerminated, nil
		case err != nil:
			return reapUndetermined, fmt.Errorf("prove killed process %d settled: %w", id.pid, err)
		case !id.matches(identity{pid: id.pid, start: info.start, boot: boot}), info.zombie:
			return ReapTerminated, nil
		case !clk.Now().Before(deadline):
			return reapUndetermined, errors.New("killed process remained live through settlement deadline")
		}
		select {
		case <-ctx.Done():
			return reapUndetermined, ctx.Err()
		case <-clk.After(settlementPollInterval):
		}
	}
}

// reapSession settles a dedicated-session record. A reaped leader PID can be
// reused within the same start-time tick; that mismatched process is never
// signaled — only members still in the durably recorded session settle.
func (s *Store) reapSession(
	ctx context.Context,
	id identity,
	session int,
	leader procInfo,
	leaderErr error,
	boot uint64,
) (Reap, error) {
	if session <= 1 || session != id.pid {
		return reapUndetermined, errors.New("session record has no durable dedicated-session identity")
	}
	switch {
	case leaderErr == nil && !id.matches(identity{pid: id.pid, start: leader.start, boot: boot}):
		return ReapReused, nil
	case leaderErr != nil && !errors.Is(leaderErr, errNoProc):
		return reapUndetermined, leaderErr
	}
	return s.settleSession(ctx, session, boot)
}

// settleSession terminates every verified member of the dedicated session:
// SIGTERM per process group, a grace share of re-verified polls, then SIGKILL
// to the ctx deadline.
func (s *Store) settleSession(ctx context.Context, session int, boot uint64) (Reap, error) {
	members, err := s.verifiedMembers(session, boot)
	if err != nil {
		return reapUndetermined, err
	}
	if len(members) == 0 {
		return ReapAbsent, nil
	}
	settled, err := s.signalSessionGroups(session, members, syscall.SIGTERM, boot)
	if err != nil {
		return reapUndetermined, err
	}
	if settled {
		return ReapAbsent, nil
	}
	clk := clockOrReal(s.clock)
	grace := clk.Now().Add(graceShare(ctx, clk))
	for {
		select {
		case <-ctx.Done():
			return reapUndetermined, ctx.Err()
		case <-clk.After(min(settlementPollInterval, grace.Sub(clk.Now()))):
		}
		members, err = s.verifiedMembers(session, boot)
		if err != nil {
			return reapUndetermined, err
		}
		if len(members) == 0 {
			return ReapTerminated, nil
		}
		if !clk.Now().Before(grace) {
			break
		}
	}
	return s.awaitSessionSettlement(ctx, session, boot)
}

func (s *Store) awaitSessionSettlement(ctx context.Context, session int, boot uint64) (Reap, error) {
	clk := clockOrReal(s.clock)
	deadline, _ := ctx.Deadline()
	for {
		members, err := s.verifiedMembers(session, boot)
		if err != nil {
			return reapUndetermined, fmt.Errorf("prove killed session %d settled: %w", session, err)
		}
		if len(members) == 0 {
			return ReapTerminated, nil
		}
		if !clk.Now().Before(deadline) {
			return reapUndetermined, errors.New("killed session remained live through settlement deadline")
		}
		settled, err := s.signalSessionGroups(session, members, syscall.SIGKILL, boot)
		if err != nil {
			return reapUndetermined, err
		}
		if settled {
			return ReapTerminated, nil
		}
		select {
		case <-ctx.Done():
			return reapUndetermined, ctx.Err()
		case <-clk.After(settlementPollInterval):
		}
	}
}

// verifiedMembers re-verifies every enumerated member immediately before it
// can be signaled: identity re-probed through matches, session membership
// still the recorded one, zombies excluded.
func (s *Store) verifiedMembers(session int, boot uint64) ([]groupMember, error) {
	members, err := s.prober.groupMembers(session)
	if err != nil {
		return nil, fmt.Errorf("enumerate dedicated session %d: %w", session, err)
	}
	stable := make([]groupMember, 0, len(members))
	for _, member := range members {
		info, err := s.prober.probe(member.pid)
		enumerated := identity{pid: member.pid, start: member.info.start, boot: boot}
		switch {
		case errors.Is(err, errNoProc):
		case err != nil:
			return nil, fmt.Errorf("revalidate session member %d: %w", member.pid, err)
		case !enumerated.matches(identity{pid: member.pid, start: info.start, boot: boot}):
		case info.session != session:
		case info.zombie:
		default:
			stable = append(stable, groupMember{pid: member.pid, info: info})
		}
	}
	return stable, nil
}

// signalSessionGroups signals each distinct process group of the verified
// members. Darwin may deny killpg after the verified group exits; only a
// fresh exact absence proof settles that denial.
func (s *Store) signalSessionGroups(
	session int,
	members []groupMember,
	sig syscall.Signal,
	boot uint64,
) (bool, error) {
	groups := make([]int, 0, len(members))
	seen := make(map[int]struct{}, len(members))
	for _, member := range members {
		if member.info.group <= 1 {
			return false, errors.New("dedicated-session member has an invalid process group")
		}
		if _, ok := seen[member.info.group]; ok {
			continue
		}
		seen[member.info.group] = struct{}{}
		groups = append(groups, member.info.group)
	}
	slices.Sort(groups)
	allGone := true
	for _, group := range groups {
		gone, err := s.signalGone(-group, sig)
		if err != nil {
			if !errors.Is(err, syscall.EPERM) {
				return false, err
			}
			remaining, verifyErr := s.verifiedMembers(session, boot)
			if verifyErr != nil {
				return false, errors.Join(err, fmt.Errorf("revalidate dedicated session after denied signal: %w", verifyErr))
			}
			if len(remaining) != 0 {
				return false, err
			}
			return true, nil
		}
		allGone = allGone && gone
	}
	return allGone, nil
}

// signalGone delivers sig to pid, mapping ESRCH (already gone) to gone=true.
func (s *Store) signalGone(pid int, sig syscall.Signal) (bool, error) {
	if err := s.signaler.signal(pid, sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// graceShare is the TERM grace: a named fraction of the time remaining on ctx
// at entry, with SIGKILL and settlement spending the rest.
func graceShare(ctx context.Context, clk clock) time.Duration {
	deadline, _ := ctx.Deadline()
	return fractionOf(deadline.Sub(clk.Now()), termShare)
}
