//go:build linux

package supervise

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
)

const (
	opApply  = "apply"
	opPrint  = "print"
	opRemove = "remove"

	// maxMessage bounds one control message in either direction.
	maxMessage = 1 << 20
	// retriesPerBudget derives Remove's re-observation cadence from the
	// caller's own deadline, capped by maxRetryCadence.
	retriesPerBudget = 64
	maxRetryCadence  = 250 * time.Millisecond
)

// ErrNoSupervisor means no supervisor is answering for the label. Nothing
// here starts one: the supervisor is a foreground process the workspace owns,
// so its absence is reported rather than repaired.
var ErrNoSupervisor = errors.New("supervise: no supervisor is running for this label")

// request is one verb sent to a supervisor.
type request struct {
	Op      string   `json:"op"`
	Service *Service `json:"service,omitempty"`
}

// Validate admits the three verbs, and a service only beside the one that
// applies it.
func (r request) Validate() error {
	switch r.Op {
	case opApply:
		if r.Service == nil {
			return errors.New("supervise: apply carries no service")
		}
		return r.Service.Validate()
	case opPrint, opRemove:
		if r.Service != nil {
			return fmt.Errorf("supervise: %s carries a service", r.Op)
		}
		return nil
	}
	return fmt.Errorf("supervise: unknown verb %q", r.Op)
}

// response is a supervisor's answer: the refusal, or the service it holds
// applied once the verb has taken effect.
type response struct {
	Error   string   `json:"error,omitempty"`
	Service *Service `json:"service,omitempty"`
}

// Validate admits an answer whose service, when it names one, is one a
// supervisor could be running.
func (r response) Validate() error {
	if r.Service == nil {
		return nil
	}
	return r.Service.Validate()
}

// Apply installs or repairs exactly the one named service and always starts
// it. A supervisor already running the identical service leaves it running; one
// running another specification under the label stops that child, within its
// own exit timeout, before it starts this one. Apply returns once the service's
// process is recorded and released, and ErrNoSupervisor when no supervisor
// answers for the label. ctx must carry a deadline.
func Apply(ctx context.Context, service Service) error {
	desired, err := service.canonical()
	if err != nil {
		return err
	}
	_, err = call(ctx, desired.Label, request{Op: opApply, Service: &desired})
	return err
}

// Verify reports whether the one named service is already exactly applied: a
// supervisor is answering for the label and holds this specification. It is
// [Apply]'s own observation and mutates nothing. A supervisor that could not
// be asked at all is ErrNoSupervisor, never a plain "not applied": deciding to
// replace a running service needs a supervisor there to start the next one.
// ctx must carry a deadline.
func Verify(ctx context.Context, service Service) (bool, error) {
	desired, err := service.canonical()
	if err != nil {
		return false, err
	}
	answer, err := call(ctx, desired.Label, request{Op: opPrint})
	if err != nil {
		return false, err
	}
	return answer.Service != nil && answer.Service.equal(desired), nil
}

// Remove stops and forgets the service at the named label. A supervisor that
// answers stops its child within the service's exit timeout, forgets the
// specification, and stays up to take the next Apply. With no supervisor
// answering, the persisted specification is deleted under the supervisor's own
// lock, so one starting up can never load what this call removed. A label
// nothing was applied to is success, so a repeated Remove is a no-op. ctx must
// carry a deadline.
func Remove(ctx context.Context, name string) error {
	where, err := layoutFor(name)
	if err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("supervise: Remove requires a context deadline")
	}
	cadence := min(time.Until(deadline)/retriesPerBudget, maxRetryCadence)
	timer := time.NewTimer(cadence)
	defer timer.Stop()
	for {
		_, err := call(ctx, name, request{Op: opRemove})
		if !errors.Is(err, ErrNoSupervisor) {
			return err
		}
		removed, err := removeUnsupervised(where)
		if err != nil || removed {
			return err
		}
		timer.Reset(cadence)
		select {
		case <-ctx.Done():
			return fmt.Errorf("supervise: a supervisor holds label %q and never answered: %w", name, ctx.Err())
		case <-timer.C:
		}
	}
}

// removeUnsupervised deletes the persisted specification while holding the
// lock a supervisor takes before it reads one. removed is false when a
// supervisor holds that lock: it is starting up or shutting down, and the
// verb belongs on its socket.
func removeUnsupervised(where layout) (removed bool, err error) {
	if _, err := os.Lstat(where.spec()); errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	lock, err := flock.Spec{
		Path:     proc.LockPath(where.records()),
		Mode:     flock.Exclusive,
		Deadline: time.Nanosecond,
	}.TryAcquire()
	if errors.Is(err, flock.ErrLockBusy) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("supervise: lock the service state: %w", err)
	}
	defer func() { _ = lock.Close() }()
	if err := forget(where); err != nil {
		return false, err
	}
	return true, nil
}

func forget(where layout) error {
	if err := durable.Remove(where.spec()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("supervise: remove the applied service: %w", err)
	}
	return nil
}

// call sends one verb to the label's supervisor and returns its answer. The
// peer accepting on the socket is held to the same-effective-UID floor before
// a byte is written.
func call(ctx context.Context, name string, verb request) (response, error) {
	where, err := layoutFor(name)
	if err != nil {
		return response{}, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return response{}, errors.New("supervise: a supervisor verb requires a context deadline")
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", where.socket())
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
		return response{}, fmt.Errorf("%w: %q", ErrNoSupervisor, name)
	}
	if err != nil {
		return response{}, fmt.Errorf("supervise: reach the supervisor for %q: %w", name, err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(deadline); err != nil {
		return response{}, fmt.Errorf("supervise: bound the supervisor exchange: %w", err)
	}
	peer, err := trust.PeerCredentials(conn.(*net.UnixConn))
	if err != nil {
		return response{}, err
	}
	if err := trust.Floor(peer.UID); err != nil {
		return response{}, fmt.Errorf("supervise: the process answering for %q: %w", name, err)
	}
	if err := send(conn, verb); err != nil {
		return response{}, err
	}
	answer, err := receive[response](conn)
	if err != nil {
		return response{}, fmt.Errorf("supervise: read the supervisor's answer: %w", err)
	}
	if answer.Error != "" {
		return response{}, fmt.Errorf("supervise: the supervisor for %q refused %s: %s", name, verb.Op, answer.Error)
	}
	return answer, nil
}

func send[T durable.Validating](conn net.Conn, message T) error {
	data, err := durable.Marshal(message)
	if err != nil {
		return err
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("supervise: write the control message: %w", err)
	}
	return nil
}

// receive reads one newline-terminated message, bounded by maxMessage.
func receive[T durable.Validating](conn net.Conn) (T, error) {
	line, err := bufio.NewReader(io.LimitReader(conn, maxMessage)).ReadBytes('\n')
	if err != nil {
		var zero T
		return zero, err
	}
	return durable.Unmarshal[T](line)
}
