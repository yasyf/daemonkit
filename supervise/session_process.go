package supervise

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/yasyf/daemonkit/internal/duplexconn"
	"github.com/yasyf/daemonkit/proc"
)

// SessionProcessSpec describes one durably tracked child with a private duplex
// session. Standard output is reserved for the session; diagnostics use Stderr.
type SessionProcessSpec struct {
	RecoveryClass    proc.RecoveryClass
	Path             string
	Args             []string
	Dir              string
	Env              []string
	Stderr           io.Writer
	Ready            func(context.Context, proc.Record, net.Conn) error
	Recorded         func(context.Context, proc.Record) error
	ReadinessTimeout time.Duration
}

// SessionProcess is a long-lived child and its daemonkit-owned duplex session.
type SessionProcess struct {
	process *Process
	conn    net.Conn
}

// StartSession launches a managed child with stdin and stdout joined into one
// backpressured duplex connection. The connection closes when the child exits.
func (p *Pool) StartSession(startup context.Context, spec SessionProcessSpec) (*SessionProcess, error) {
	childInput, parentInput, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("supervise: managed session input pipe: %w", err)
	}
	parentOutput, childOutput, err := os.Pipe()
	if err != nil {
		_ = childInput.Close()
		_ = parentInput.Close()
		return nil, fmt.Errorf("supervise: managed session output pipe: %w", err)
	}
	conn, err := duplexconn.New(parentOutput, parentInput)
	if err != nil {
		_ = childInput.Close()
		_ = childOutput.Close()
		_ = parentOutput.Close()
		_ = parentInput.Close()
		return nil, fmt.Errorf("supervise: managed session connection: %w", err)
	}
	processSpec := ProcessSpec{
		RecoveryClass:    spec.RecoveryClass,
		Path:             spec.Path,
		Args:             spec.Args,
		Dir:              spec.Dir,
		Env:              spec.Env,
		Stdout:           childOutput,
		Stderr:           spec.Stderr,
		stdin:            childInput,
		Recorded:         spec.Recorded,
		ReadinessTimeout: spec.ReadinessTimeout,
	}
	if spec.Ready != nil {
		processSpec.Ready = func(ctx context.Context, record proc.Record) error {
			return spec.Ready(ctx, record, conn)
		}
	}
	process, startErr := p.Start(startup, processSpec)
	closeErr := errors.Join(childInput.Close(), childOutput.Close())
	if startErr != nil {
		return nil, errors.Join(startErr, closeErr, conn.Close())
	}
	if closeErr != nil {
		return nil, errors.Join(closeErr, process.Stop(context.WithoutCancel(startup)), conn.Close())
	}
	session := &SessionProcess{process: process, conn: conn}
	go session.closeOnExit()
	return session, nil
}

// Conn returns the child session connection.
func (s *SessionProcess) Conn() net.Conn { return s.conn }

// Record returns the child's immutable process identity.
func (s *SessionProcess) Record() proc.Record { return s.process.Record() }

// Wait waits for the child to exit and closes its session before returning, or
// returns when ctx expires while the background exit close remains armed.
func (s *SessionProcess) Wait(ctx context.Context) error {
	if err := s.process.Wait(ctx); err != nil {
		return err
	}
	return s.conn.Close()
}

// Stop closes the session, terminates the process group, reaps it, and removes
// its durable identity before returning.
func (s *SessionProcess) Stop(ctx context.Context) error {
	closeErr := s.conn.Close()
	if errors.Is(closeErr, os.ErrClosed) || errors.Is(closeErr, io.ErrClosedPipe) {
		closeErr = nil
	}
	return errors.Join(closeErr, s.process.Stop(ctx))
}

func (s *SessionProcess) closeOnExit() {
	<-s.process.done
	_ = s.conn.Close()
}
