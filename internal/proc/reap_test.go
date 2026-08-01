package proc

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
)

func ladderContext(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestReapDropsCrossBootRecordWithoutProbeOrSignal(t *testing.T) {
	s, _ := newTestStore(t)
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		t.Fatal("cross-boot record was probed")
		return procInfo{}, nil
	}}
	signaler := &funcSignaler{}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: 4242, start: 1, boot: testBoot + 1}, 0)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v", err)
	}
	if got != ReapCrossBoot {
		t.Fatalf("reapIdentity() = %d, want ReapCrossBoot", got)
	}
	if len(signaler.signals()) != 0 {
		t.Fatalf("cross-boot record was signaled: %v", signaler.signals())
	}
}

func TestReapRefusesSelfAndPID1(t *testing.T) {
	tests := []struct {
		name string
		pid  int
	}{
		{"init", 1},
		{"self", syscall.Getpid()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestStore(t)
			prober := &funcProber{probeFn: func(int) (procInfo, error) {
				t.Fatal("unsafe identity was probed")
				return procInfo{}, nil
			}}
			signaler := &funcSignaler{}
			s.prober, s.signaler = prober, signaler

			_, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: tt.pid, start: 1, boot: testBoot}, 0)
			if err == nil {
				t.Fatal("reapIdentity() accepted an unsafe identity")
			}
			if len(signaler.signals()) != 0 {
				t.Fatalf("unsafe identity was signaled: %v", signaler.signals())
			}
		})
	}
}

func TestReapPIDReuseNeverSignaled(t *testing.T) {
	s, _ := newTestStore(t)
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		return procInfo{start: 999}, nil
	}}
	signaler := &funcSignaler{}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: 4242, start: 1, boot: testBoot}, 0)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v", err)
	}
	if got != ReapReused {
		t.Fatalf("reapIdentity() = %d, want ReapReused", got)
	}
	if len(signaler.signals()) != 0 {
		t.Fatalf("reused PID was signaled: %v", signaler.signals())
	}
}

func TestReapPIDReuseDuringGraceIsNeverKilled(t *testing.T) {
	s, _ := newTestStore(t)
	termed := false
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		if termed {
			return procInfo{start: 999}, nil
		}
		return procInfo{start: 1}, nil
	}}
	signaler := &funcSignaler{fn: func(_ int, sig syscall.Signal) error {
		if sig == syscall.SIGTERM {
			termed = true
		}
		return nil
	}}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, 500*time.Millisecond), identity{pid: 4242, start: 1, boot: testBoot}, 0)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v", err)
	}
	if got != ReapReused {
		t.Fatalf("reapIdentity() = %d, want ReapReused", got)
	}
	for _, sent := range signaler.signals() {
		if sent.sig == syscall.SIGKILL {
			t.Fatalf("PID reused during grace was SIGKILLed: %v", signaler.signals())
		}
	}
}

func TestReapProbeErrorFailsClosedAndKeepsRecord(t *testing.T) {
	s, path := newTestStore(t)
	rec := record{PID: 4242, Start: 1, Boot: testBoot, Generation: s.generation + 1}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatalf("add() = %v", err)
	}
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		return procInfo{}, errors.New("sysctl exploded")
	}}
	signaler := &funcSignaler{}
	s.prober, s.signaler = prober, signaler

	reclaimed, _, err := s.Recover(ladderContext(t, time.Second), nil)
	if err == nil {
		t.Fatal("Recover() succeeded despite an undetermined probe")
	}
	if len(reclaimed) != 0 {
		t.Fatalf("Recover() published %v for an undetermined probe", reclaimed)
	}
	if len(signaler.signals()) != 0 {
		t.Fatalf("undetermined identity was signaled: %v", signaler.signals())
	}
	if !storeHolds(t, path, rec.id()) {
		t.Fatal("undetermined probe did not keep the record")
	}
}

func TestReapZombieIsAbsent(t *testing.T) {
	s, _ := newTestStore(t)
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		return procInfo{start: 1, zombie: true}, nil
	}}
	signaler := &funcSignaler{}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: 4242, start: 1, boot: testBoot}, 0)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v", err)
	}
	if got != ReapAbsent {
		t.Fatalf("reapIdentity() = %d, want ReapAbsent", got)
	}
	if len(signaler.signals()) != 0 {
		t.Fatalf("zombie was signaled: %v", signaler.signals())
	}
}

func TestReapESRCHOnSignalIsSuccess(t *testing.T) {
	s, _ := newTestStore(t)
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		return procInfo{start: 1}, nil
	}}
	signaler := &funcSignaler{fn: func(int, syscall.Signal) error { return syscall.ESRCH }}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: 4242, start: 1, boot: testBoot}, 0)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v", err)
	}
	if got != ReapAbsent {
		t.Fatalf("reapIdentity() = %d, want ReapAbsent", got)
	}
}

func TestReapSIGKILLProvenAbsenceIsTerminated(t *testing.T) {
	s, _ := newTestStore(t)
	killed := false
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		if killed {
			return procInfo{}, errors.New("sysctl transient")
		}
		return procInfo{start: 1}, nil
	}}
	signaler := &funcSignaler{fn: func(_ int, sig syscall.Signal) error {
		if sig == syscall.SIGKILL {
			killed = true
			return syscall.ESRCH
		}
		return nil
	}}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, 300*time.Millisecond), identity{pid: 4242, start: 1, boot: testBoot}, 0)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v; SIGKILL->ESRCH proved absence yet a later probe error failed recovery", err)
	}
	if got != ReapTerminated {
		t.Fatalf("reapIdentity() = %d, want ReapTerminated from SIGKILL's proven absence", got)
	}
}

func TestReapRemovesRecordOnlyAfterPostKillAbsence(t *testing.T) {
	s, path := newTestStore(t)
	rec := record{PID: 4242, Start: 1, Boot: testBoot, Generation: s.generation + 1}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatalf("add() = %v", err)
	}
	killed := false
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		if killed {
			return procInfo{}, errNoProc
		}
		return procInfo{start: 1}, nil
	}}
	signaler := &funcSignaler{fn: func(_ int, sig syscall.Signal) error {
		if sig == syscall.SIGKILL {
			killed = true
		}
		return nil
	}}
	s.prober, s.signaler = prober, signaler

	reclaimed, _, err := s.Recover(ladderContext(t, time.Second), nil)
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].PID != rec.PID {
		t.Fatalf("Recover() = %v, want one reclaim of pid %d", reclaimed, rec.PID)
	}
	if reclaimed[0].Exit.Reap != ReapTerminated {
		t.Fatalf("Exit.Reap = %d, want ReapTerminated", reclaimed[0].Exit.Reap)
	}
	if reclaimed[0].Exit.Record != RecordRemoved {
		t.Fatalf("Exit.Record = %d, want RecordRemoved", reclaimed[0].Exit.Record)
	}
	if storeHolds(t, path, rec.id()) {
		t.Fatal("settled record was not removed")
	}
}

func TestReapRetainsRecordWhenKilledProcessNeverSettles(t *testing.T) {
	s, path := newTestStore(t)
	rec := record{PID: 4242, Start: 1, Boot: testBoot, Generation: s.generation + 1}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatalf("add() = %v", err)
	}
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		return procInfo{start: 1}, nil
	}}
	signaler := &funcSignaler{}
	s.prober, s.signaler = prober, signaler

	reclaimed, _, err := s.Recover(ladderContext(t, 400*time.Millisecond), nil)
	if err == nil {
		t.Fatal("Recover() succeeded although the killed process never settled")
	}
	if len(reclaimed) != 0 {
		t.Fatalf("Recover() published %v for an unsettled kill", reclaimed)
	}
	if !storeHolds(t, path, rec.id()) {
		t.Fatal("unsettled record was removed")
	}
}

func TestReapLeaderlessSessionUsesDurableSessionMembers(t *testing.T) {
	s, _ := newTestStore(t)
	const session = 4242
	member := groupMember{pid: 5000, info: procInfo{start: 50, group: 5000, session: session}}
	gone := false
	prober := &funcProber{
		probeFn: func(pid int) (procInfo, error) {
			if pid == session {
				return procInfo{}, errNoProc
			}
			if gone {
				return procInfo{}, errNoProc
			}
			return member.info, nil
		},
		membersFn: func(sessionID int) ([]groupMember, error) {
			if sessionID != session {
				t.Fatalf("enumerated session %d, want %d", sessionID, session)
			}
			if gone {
				return nil, nil
			}
			return []groupMember{member}, nil
		},
	}
	signaler := &funcSignaler{fn: func(pid int, sig syscall.Signal) error {
		if pid != -member.info.group {
			t.Fatalf("signaled %d, want process group %d", pid, -member.info.group)
		}
		if sig == syscall.SIGTERM {
			gone = true
		}
		return nil
	}}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: session, start: 1, boot: testBoot}, session)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v", err)
	}
	if got != ReapTerminated {
		t.Fatalf("reapIdentity() = %d, want ReapTerminated", got)
	}
}

func TestReapIgnoresSameTickReusedLeaderAndSettlesRecordedSession(t *testing.T) {
	s, _ := newTestStore(t)
	const session = 4242
	member := groupMember{pid: 5000, info: procInfo{start: 50, group: 5000, session: session}}
	memberGone := false
	prober := &funcProber{
		probeFn: func(pid int) (procInfo, error) {
			if pid == session {
				return procInfo{start: 1, group: session, session: 9999}, nil
			}
			if memberGone {
				return procInfo{}, errNoProc
			}
			return member.info, nil
		},
		membersFn: func(int) ([]groupMember, error) {
			if memberGone {
				return nil, nil
			}
			return []groupMember{member}, nil
		},
	}
	signaler := &funcSignaler{fn: func(pid int, sig syscall.Signal) error {
		if pid == session || pid == -session {
			t.Fatalf("same-tick reused leader was signaled with %v", sig)
		}
		if sig == syscall.SIGTERM {
			memberGone = true
		}
		return nil
	}}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: session, start: 1, boot: testBoot}, session)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v", err)
	}
	if got != ReapTerminated {
		t.Fatalf("reapIdentity() = %d, want ReapTerminated", got)
	}
}

func TestReapAcceptsDeniedGroupSignalOnlyAfterExactSessionAbsence(t *testing.T) {
	s, _ := newTestStore(t)
	const session = 4242
	member := groupMember{pid: 5000, info: procInfo{start: 50, group: 5000, session: session}}
	enumerations := 0
	prober := &funcProber{
		probeFn: func(pid int) (procInfo, error) {
			if pid == session {
				return procInfo{}, errNoProc
			}
			return member.info, nil
		},
		membersFn: func(int) ([]groupMember, error) {
			enumerations++
			if enumerations > 1 {
				return nil, nil
			}
			return []groupMember{member}, nil
		},
	}
	signaler := &funcSignaler{fn: func(int, syscall.Signal) error { return syscall.EPERM }}
	s.prober, s.signaler = prober, signaler

	got, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: session, start: 1, boot: testBoot}, session)
	if err != nil {
		t.Fatalf("reapIdentity() error = %v", err)
	}
	if got != ReapAbsent {
		t.Fatalf("reapIdentity() = %d, want ReapAbsent", got)
	}
}

func TestReapDeniedGroupSignalRetainsLiveSessionAuthority(t *testing.T) {
	s, _ := newTestStore(t)
	const session = 4242
	member := groupMember{pid: 5000, info: procInfo{start: 50, group: 5000, session: session}}
	prober := &funcProber{
		probeFn: func(pid int) (procInfo, error) {
			if pid == session {
				return procInfo{}, errNoProc
			}
			return member.info, nil
		},
		membersFn: func(int) ([]groupMember, error) {
			return []groupMember{member}, nil
		},
	}
	signaler := &funcSignaler{fn: func(int, syscall.Signal) error { return syscall.EPERM }}
	s.prober, s.signaler = prober, signaler

	_, err := s.reapIdentity(ladderContext(t, time.Second), identity{pid: session, start: 1, boot: testBoot}, session)
	if !errors.Is(err, syscall.EPERM) {
		t.Fatalf("reapIdentity() error = %v, want the denied signal", err)
	}
}
