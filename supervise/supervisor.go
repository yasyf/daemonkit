//go:build linux

package supervise

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
)

const (
	// throttleInterval is the least time between two starts of one service
	// that the restart policy makes, the ThrottleInterval daemonkit's
	// LaunchAgents render. A start a verb asks for is never throttled.
	throttleInterval = 10 * time.Second
	// requestGrace bounds how long an accepted connection may take to send its
	// verb, so a peer that connects and says nothing cannot hold a handler.
	requestGrace = 10 * time.Second
)

// ErrBusy means another supervisor already holds the label.
var ErrBusy = errors.New("supervise: another supervisor holds this label")

// Run is the supervisor for one label, and returns when ctx ends. It takes the
// label's exclusive lock, settles whatever a previous supervisor left running,
// resumes the persisted service if one was applied, and then serves [Apply],
// [Verify], and [Remove] until ctx is cancelled — at which point it stops the
// service within its exit timeout and returns. The applied specification is
// kept, so the next Run resumes it.
//
// Run is a foreground call in a process the workspace owns. It never
// daemonizes and never outlives its caller, and a second Run for a label one
// already holds is ErrBusy.
func Run(ctx context.Context, name string) error {
	return run(ctx, name, throttleInterval)
}

func run(ctx context.Context, name string, throttle time.Duration) error {
	where, err := layoutFor(name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(where.dir, 0o700); err != nil {
		return fmt.Errorf("supervise: create state dir: %w", err)
	}
	if err := where.private(); err != nil {
		return err
	}
	openCtx, cancelOpen := context.WithTimeout(ctx, proc.SettleGrace)
	store, err := proc.OpenStore(openCtx, where.records())
	cancelOpen()
	if errors.Is(err, durable.ErrLockBusy) {
		return fmt.Errorf("%w: %q: %w", ErrBusy, name, err)
	}
	if err != nil {
		return fmt.Errorf("supervise: open record store: %w", err)
	}
	defer func() { _ = store.Close() }()

	s := &supervisor{label: name, where: where, store: store, throttle: throttle}
	if err := s.load(); err != nil {
		return err
	}
	if err := s.reclaim(ctx); err != nil {
		return err
	}
	_ = os.Remove(where.socket())
	listener, err := net.Listen("unix", where.socket())
	if err != nil {
		return fmt.Errorf("supervise: bind control socket: %w", err)
	}
	defer func() { _ = os.Remove(where.socket()) }()
	defer func() { _ = listener.Close() }()
	if err := os.Chmod(where.socket(), 0o600); err != nil {
		return fmt.Errorf("supervise: chmod control socket: %w", err)
	}

	verbs := make(chan verb)
	done := make(chan struct{})
	defer close(done)
	go accept(listener, verbs, done)

	if s.desired != nil {
		if err := s.start(ctx); err != nil {
			slog.Error("supervise: resume the applied service", "label", name, "err", err)
			s.retry()
		}
	}
	return s.serve(ctx, verbs)
}

// serve runs the loop until ctx ends or a child's reap goes unproven. That
// child's record stays in the store, so the supervisor returns rather than
// start anything beside it: the next Run reclaims it or refuses the label.
func (s *supervisor) serve(ctx context.Context, verbs <-chan verb) error {
	for {
		select {
		case <-ctx.Done():
			return s.stop()
		case asked := <-verbs:
			asked.reply <- s.handle(ctx, asked.request)
		case exit := <-s.exited:
			s.settled(exit)
		case <-s.restart:
			s.restart = nil
			if err := s.start(ctx); err != nil {
				slog.Error("supervise: restart the service", "label", s.label, "err", err)
				s.retry()
			}
		}
		if s.unsettled != nil {
			return s.unsettled
		}
	}
}

// verb is one request awaiting the supervisor loop's answer.
type verb struct {
	request request
	reply   chan response
}

// supervisor is one label's whole state, owned by the one goroutine that runs
// the loop: nothing here is shared, so nothing here is locked.
type supervisor struct {
	label    string
	where    layout
	store    *proc.Store
	throttle time.Duration

	// desired is the applied service, nil when none is.
	desired *Service
	// running is the specification the live child was started from.
	running Service
	child   *proc.Child
	exited  <-chan proc.Exit
	started time.Time
	restart <-chan time.Time
	// unsettled is the child whose reap was never proven. It is terminal:
	// nothing starts under this supervisor once it is set.
	unsettled error
}

func (s *supervisor) load() error {
	service, err := durable.ReadFile[Service](s.where.spec())
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("supervise: read the applied service: %w", err)
	}
	if service.Label != s.label {
		return fmt.Errorf("supervise: the service applied under %q names label %q", s.label, service.Label)
	}
	s.desired = &service
	return nil
}

// reclaim settles the child a previous supervisor recorded and did not stop.
// It is refused rather than skipped when that child cannot be proven gone: a
// second instance started over it would only meet the first one's lock.
func (s *supervisor) reclaim(ctx context.Context) error {
	grace := defaultExitTimeOut
	if s.desired != nil {
		grace = s.desired.exitTimeOut()
	}
	reclaimCtx, cancel := context.WithTimeout(ctx, proc.KillAfter(grace)+proc.SettleGrace)
	defer cancel()
	reclaimed, _, err := s.store.Recover(reclaimCtx)
	for _, child := range reclaimed {
		slog.Warn("supervise: reclaimed a child a previous supervisor left", "label", s.label, "pid", child.PID)
	}
	if err != nil {
		return fmt.Errorf("supervise: reclaim the previous supervisor's child: %w", err)
	}
	return nil
}

func (s *supervisor) handle(ctx context.Context, asked request) response {
	switch asked.Op {
	case opApply:
		if err := s.apply(ctx, *asked.Service); err != nil {
			return response{Error: err.Error()}
		}
	case opRemove:
		if err := s.remove(); err != nil {
			return response{Error: err.Error()}
		}
	}
	return response{Service: s.desired}
}

func (s *supervisor) apply(ctx context.Context, service Service) error {
	if s.unsettled != nil {
		return s.unsettled
	}
	if service.Label != s.label {
		return fmt.Errorf("supervise: this supervisor holds %q, not %q", s.label, service.Label)
	}
	if err := validateProgram(service.Program); err != nil {
		return err
	}
	data, err := durable.Marshal(service)
	if err != nil {
		return err
	}
	if s.child != nil {
		live, err := s.live(ctx)
		if err != nil {
			return err
		}
		if !live || !s.running.equal(service) {
			if err := s.stop(); err != nil {
				return err
			}
		}
	}
	if err := durable.WriteFile(s.where.spec(), data, 0o600); err != nil {
		return fmt.Errorf("supervise: persist the applied service: %w", err)
	}
	s.desired = &service
	s.restart = nil
	if s.child != nil {
		return nil
	}
	return s.start(ctx)
}

func (s *supervisor) remove() error {
	if err := s.stop(); err != nil {
		return err
	}
	if err := forget(s.where); err != nil {
		return err
	}
	s.desired = nil
	s.restart = nil
	return nil
}

// start spawns the applied service in a session of its own, so stopping it
// settles whatever it forked as well. The child is recorded durably before it
// runs an instruction, which is what lets the next supervisor reclaim it if
// this one dies first.
func (s *supervisor) start(ctx context.Context) error {
	if s.unsettled != nil {
		return s.unsettled
	}
	service := *s.desired
	s.started = time.Now()
	if err := validateProgram(service.Program); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(service.LogPath), 0o700); err != nil {
		return fmt.Errorf("supervise: create log directory: %w", err)
	}
	spawnCtx, cancel := context.WithTimeout(ctx, proc.SettleGrace)
	defer cancel()
	child, err := s.store.SpawnLogged(spawnCtx, proc.Cmd{
		Path:    service.Program,
		Args:    service.Args,
		Env:     service.environment(),
		Dir:     "/",
		Session: true,
	}, service.LogPath)
	if err != nil {
		return fmt.Errorf("supervise: start %q: %w", service.Program, err)
	}
	s.child, s.exited, s.running = child, child.Done(), service
	slog.Info("supervise: service started", "label", s.label, "pid", child.PID())
	return nil
}

// live reports whether the child's own process is still running. A service
// that has just exited stays this supervisor's child until its driver has
// settled the session it led, and a verb that lands inside that window must
// not read it as running: the caller that drained a daemon and applies its
// service the next instant is owed a fresh start, not the corpse.
func (s *supervisor) live(ctx context.Context) (bool, error) {
	scope, err := s.child.Observe(ctx)
	if err != nil {
		return false, fmt.Errorf("supervise: observe the service: %w", err)
	}
	return slices.ContainsFunc(scope.Members, func(member proc.Identity) bool {
		return proc.SameInstance(member, scope.Identity)
	}), nil
}

// stop terminates the live child and proves it gone: SIGTERM, the service's
// whole exit timeout, then SIGKILL. A child that outlives the ladder stays
// recorded and is an error, never a quiet success.
func (s *supervisor) stop() error {
	if s.unsettled != nil {
		return s.unsettled
	}
	if s.child == nil {
		return nil
	}
	s.child.TerminateBy(time.Now().Add(proc.KillAfter(s.running.exitTimeOut())))
	pid := s.child.PID()
	if err := s.release(<-s.exited); err != nil {
		return err
	}
	slog.Info("supervise: service stopped", "label", s.label, "pid", pid)
	return nil
}

// release gives up the exited child only when its reap was proven. An unproven
// one still has a record, and possibly a process, that a start would run
// beside.
func (s *supervisor) release(exit proc.Exit) error {
	pid := s.child.PID()
	s.child, s.exited = nil, nil
	if !exit.Reap.Proven() {
		s.unsettled = fmt.Errorf("%w: service pid %d", proc.ErrUnsettled, pid)
		slog.Error("supervise: the service's exit was not proven", "label", s.label, "pid", pid)
	}
	return s.unsettled
}

// settled applies the restart policy to a child that exited by itself.
func (s *supervisor) settled(exit proc.Exit) {
	slog.Info("supervise: service exited", "label", s.label, "pid", s.child.PID(), "code", exit.Code, "signal", exit.Signal)
	if s.release(exit) != nil || s.desired == nil {
		return
	}
	failed := exit.Code != 0 || exit.Signal != 0
	if s.desired.RestartPolicy == RestartAlways || (s.desired.RestartPolicy == RestartOnFailure && failed) {
		s.retry()
	}
}

// retry schedules the next policy start no sooner than one throttle interval
// after the last attempt, failed ones included.
func (s *supervisor) retry() {
	if s.desired == nil || s.desired.RestartPolicy == NoRestart {
		return
	}
	s.restart = time.After(max(0, s.throttle-time.Since(s.started)))
}

func accept(listener net.Listener, verbs chan<- verb, done <-chan struct{}) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go answer(conn.(*net.UnixConn), verbs, done)
	}
}

// answer serves one connection one verb. The peer is held to the
// same-effective-UID floor before anything it sent is read.
func answer(conn *net.UnixConn, verbs chan<- verb, done <-chan struct{}) {
	defer func() { _ = conn.Close() }()
	peer, err := trust.PeerCredentials(conn)
	if err != nil {
		slog.Warn("supervise: read control peer credentials", "err", err)
		return
	}
	if err := trust.Floor(peer.UID); err != nil {
		slog.Warn("supervise: refused a control peer", "err", err)
		return
	}
	if err := conn.SetReadDeadline(time.Now().Add(requestGrace)); err != nil {
		return
	}
	asked, err := receive[request](conn)
	if err != nil {
		_ = send(conn, response{Error: err.Error()})
		return
	}
	reply := make(chan response, 1)
	select {
	case verbs <- verb{request: asked, reply: reply}:
	case <-done:
		return
	}
	select {
	case answered := <-reply:
		_ = send(conn, answered)
	case <-done:
		select {
		case answered := <-reply:
			_ = send(conn, answered)
		default:
		}
	}
}
