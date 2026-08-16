package proc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/duplexconn"
)

// Bytes is a byte-count limit; zero means the field's documented default.
type Bytes int64

// Channel is the duplex channel a spawn establishes before the child can run.
type Channel uint8

const (
	// ChannelNone spawns with no channel; stdout goes to /dev/null.
	ChannelNone Channel = iota
	// ChannelHandoff duplicates a pre-connected unix socketpair end to the
	// child at fd 3 and keeps the parent end.
	ChannelHandoff
	// ChannelStdio joins the child's stdin and stdout into one deadline-aware
	// conn, for foreign executables that cannot take fd 3.
	ChannelStdio

	channelLimit
)

// Cmd is one command run under daemonkit process ownership. Path is the exact
// executable path; argv is Path followed by Args; a nil Env inherits.
type Cmd struct {
	Path, Dir string
	Args, Env []string
	Stdin     []byte
	MaxOutput Bytes // Run only; 4 MiB when zero
	Session   bool  // dedicated session+group; its id is durable kill identity
	Channel   Channel
	// Verify is the exec-posture gate. It runs against the suspended child
	// between its durable record and its release, and a non-nil error aborts
	// the spawn through the same path a failed record takes, so the child
	// never executes an instruction.
	Verify func(pid int) error
}

type spawnFiles struct {
	stdin, stdout, stderr *os.File
	handoff               *os.File
}

// Spawn starts one owned child whose record is durable before its first
// instruction runs: spawned suspended, continued only after the fsynced write
// is re-read and Cmd.Verify has passed. A non-nil stderr receives the child's
// stderr for its whole life; nil sends it to /dev/null. ctx bounds the record
// write, the probe, and the verification — never the child's life.
func (s *Store) Spawn(ctx context.Context, c Cmd, stderr io.Writer) (*Child, error) {
	if stderr == nil {
		return s.spawn(ctx, c, nil, nil)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("proc: create stderr pipe: %w", err)
	}
	drain := startStderrCopy(reader, stderr)
	child, err := s.spawn(ctx, c, nil, writer)
	if err != nil {
		drain.abort()
		return nil, err
	}
	child.stderr = drain
	return child, nil
}

func (s *Store) spawn(ctx context.Context, c Cmd, childOut, childErr *os.File) (*Child, error) {
	if err := validateCmd(c); err != nil {
		return nil, err
	}
	files, parentEnd, stdinDelivered, closeChildFiles, err := s.plumb(c, childOut, childErr)
	if err != nil {
		return nil, err
	}
	pid, err := startChild(c, files)
	closeChildFiles()
	if err != nil {
		closeIfOpen(parentEnd)
		return nil, err
	}
	boot, err := s.prober.boot()
	if err != nil {
		return nil, s.abortSpawn(pid, parentEnd, fmt.Errorf("snapshot boot identity: %w", err))
	}
	info, err := s.prober.probe(pid)
	if err != nil {
		return nil, s.abortSpawn(pid, parentEnd, fmt.Errorf("snapshot pid %d: %w", pid, err))
	}
	if !info.stopped {
		return nil, s.abortSpawn(pid, parentEnd, fmt.Errorf("proc: spawned pid %d is not suspended", pid))
	}
	session := 0
	if c.Session {
		if info.session != pid {
			return nil, s.abortSpawn(pid, parentEnd, fmt.Errorf("proc: pid %d has session %d, want a dedicated session leader", pid, info.session))
		}
		session = pid
	}
	id := identity{pid: pid, start: info.start, boot: boot}
	rec := record{PID: pid, Start: info.start, Boot: boot, Generation: s.generation, Session: session, Comm: info.comm}
	if err := s.add(ctx, rec); err != nil {
		return nil, s.abortSpawn(pid, parentEnd, err)
	}
	if c.Verify != nil {
		if err := c.Verify(pid); err != nil {
			aborted := s.abortSpawn(pid, parentEnd, err)
			<-s.retire(id)
			return nil, aborted
		}
	}
	if err := releaseChild(pid); err != nil {
		aborted := s.abortSpawn(pid, parentEnd, fmt.Errorf("release suspended pid %d: %w", pid, err))
		<-s.retire(id)
		return nil, aborted
	}
	child := &Child{
		pid:     pid,
		demand:  make(chan time.Time, 1),
		stdin:   stdinDelivered,
		settled: make(chan struct{}),
		channel: parentEnd,
	}
	go s.drive(child, id, session) //nolint:contextcheck // the driver outlives the caller's ctx by design: ctx bounds the record, the probe, and the verify — never the child's life
	return child, nil
}

// validateCmd is the spawn boundary's contract on the command: an exact,
// absolute, already-cleaned executable path, the same for a set working
// directory, and a channel this package establishes. It runs before any file
// action or child so an inexact command is rejected without spawning.
func validateCmd(c Cmd) error {
	if c.Path == "" {
		return errors.New("proc: cmd path is required")
	}
	if !filepath.IsAbs(c.Path) || filepath.Clean(c.Path) != c.Path {
		return fmt.Errorf("proc: cmd path %q is not absolute and clean", c.Path)
	}
	if c.Dir != "" && (!filepath.IsAbs(c.Dir) || filepath.Clean(c.Dir) != c.Dir) {
		return fmt.Errorf("proc: cmd dir %q is not absolute and clean", c.Dir)
	}
	if c.Channel >= channelLimit {
		return fmt.Errorf("proc: cmd channel %d is not one of the established channels", c.Channel)
	}
	if c.Channel == ChannelStdio && len(c.Stdin) > 0 {
		return errors.New("proc: cmd stdin is the stdio channel and cannot also be delivered")
	}
	return nil
}

// SIGKILL reaches suspended processes, and the wait4 reaps the zombie so the
// pid cannot linger.
func (s *Store) abortSpawn(pid int, parentEnd net.Conn, cause error) error {
	_ = syscall.Kill(pid, syscall.SIGKILL)
	awaitExit(pid)
	closeIfOpen(parentEnd)
	return cause
}

func (s *Store) plumb(c Cmd, childOut, childErr *os.File) (spawnFiles, net.Conn, <-chan error, func(), error) {
	var owned []*os.File
	closeOwned := func() {
		for _, f := range owned {
			_ = f.Close()
		}
	}
	fail := func(err error) (spawnFiles, net.Conn, <-chan error, func(), error) {
		closeOwned()
		return spawnFiles{}, nil, nil, nil, err
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fail(fmt.Errorf("proc: open %s: %w", os.DevNull, err))
	}
	owned = append(owned, devNull)
	files := spawnFiles{stdin: devNull, stdout: devNull, stderr: devNull}
	if childOut != nil {
		files.stdout = childOut
		owned = append(owned, childOut)
	}
	if childErr != nil {
		files.stderr = childErr
		owned = append(owned, childErr)
	}
	var stdinDelivered <-chan error
	if len(c.Stdin) > 0 {
		reader, writer, err := os.Pipe()
		if err != nil {
			return fail(fmt.Errorf("proc: create stdin pipe: %w", err))
		}
		owned = append(owned, reader)
		files.stdin = reader
		stdinDelivered = deliverStdin(writer, c.Stdin)
	}
	var parentEnd net.Conn
	switch c.Channel {
	case ChannelHandoff:
		parent, child, err := SocketpairFiles()
		if err != nil {
			return fail(err)
		}
		owned = append(owned, child)
		files.handoff = child
		conn, err := net.FileConn(parent)
		closeErr := parent.Close()
		if err != nil {
			return fail(errors.Join(fmt.Errorf("proc: adopt handoff parent end: %w", err), closeErr))
		}
		if closeErr != nil {
			_ = conn.Close()
			return fail(closeErr)
		}
		parentEnd = conn
	case ChannelStdio:
		inbound, toChild, err := os.Pipe()
		if err != nil {
			return fail(fmt.Errorf("proc: create stdio input pipe: %w", err))
		}
		owned = append(owned, inbound)
		fromChild, outbound, err := os.Pipe()
		if err != nil {
			_ = toChild.Close()
			return fail(fmt.Errorf("proc: create stdio output pipe: %w", err))
		}
		owned = append(owned, outbound)
		files.stdin = inbound
		files.stdout = outbound
		conn, err := duplexconn.New(fromChild, toChild)
		if err != nil {
			_ = fromChild.Close()
			_ = toChild.Close()
			return fail(fmt.Errorf("proc: join stdio channel: %w", err))
		}
		parentEnd = conn
	case ChannelNone:
	}
	return files, parentEnd, stdinDelivered, closeOwned, nil
}

func deliverStdin(writer *os.File, payload []byte) <-chan error {
	delivered := make(chan error, 1)
	go func() {
		n, err := writer.Write(payload)
		_ = writer.Close()
		switch {
		case errors.Is(err, syscall.EPIPE):
			err = nil
		case err == nil && n < len(payload):
			err = io.ErrShortWrite
		}
		delivered <- err
	}()
	return delivered
}

func closeIfOpen(c io.Closer) {
	if c != nil {
		_ = c.Close()
	}
}
