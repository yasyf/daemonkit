package proc

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

// Bytes is a byte-count limit; zero means the field's documented default.
type Bytes int64

// Cmd is one command run under daemonkit process ownership. Path is the exact
// executable path; argv is Path followed by Args; a nil Env inherits.
type Cmd struct {
	Path, Dir            string
	Args, Env            []string
	Stdin                []byte
	MaxStdout, MaxStderr Bytes // Run only; 1 MiB when zero
	Session              bool  // dedicated session+group; its id is durable kill identity
	Handoff              bool  // dup a pre-connected unix socketpair end to the child at fd 3
}

type spawnFiles struct {
	stdin, stdout, stderr *os.File
	handoff               *os.File
}

// Spawn starts one owned child whose record is durable before its first
// instruction runs: spawned suspended, continued only after the fsynced write
// is re-read (darwin; spawn_other.go names the residue).
func (s *Store) Spawn(c Cmd) (*Child, error) {
	return s.spawn(c, nil, nil)
}

func (s *Store) spawn(c Cmd, childOut, childErr *os.File) (*Child, error) {
	if c.Path == "" {
		return nil, errors.New("proc: cmd path is required")
	}
	files, parentEnd, closeChildFiles, err := s.plumb(c, childOut, childErr)
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
	if spawnSuspends && !info.stopped {
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
	if err := s.add(rec); err != nil {
		return nil, s.abortSpawn(pid, parentEnd, err)
	}
	if s.beforeRelease != nil {
		s.beforeRelease(pid)
	}
	if err := releaseChild(pid); err != nil {
		aborted := s.abortSpawn(pid, parentEnd, fmt.Errorf("release suspended pid %d: %w", pid, err))
		<-s.retire(id)
		return nil, aborted
	}
	child := &Child{
		pid:     pid,
		demand:  make(chan time.Time, 1),
		settled: make(chan struct{}),
		handoff: parentEnd,
	}
	go s.drive(child, id, session)
	return child, nil
}

// SIGKILL reaches suspended processes, and the wait4 reaps the zombie so the
// pid cannot linger.
func (s *Store) abortSpawn(pid int, parentEnd *os.File, cause error) error {
	_ = syscall.Kill(pid, syscall.SIGKILL)
	awaitExit(pid)
	closeIfOpen(parentEnd)
	return cause
}

func (s *Store) plumb(c Cmd, childOut, childErr *os.File) (spawnFiles, *os.File, func(), error) {
	var owned []*os.File
	closeOwned := func() {
		for _, f := range owned {
			_ = f.Close()
		}
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return spawnFiles{}, nil, nil, fmt.Errorf("proc: open %s: %w", os.DevNull, err)
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
	if len(c.Stdin) > 0 {
		r, w, err := os.Pipe()
		if err != nil {
			closeOwned()
			return spawnFiles{}, nil, nil, fmt.Errorf("proc: create stdin pipe: %w", err)
		}
		owned = append(owned, r)
		stdin := c.Stdin
		go func() {
			_, _ = w.Write(stdin)
			_ = w.Close()
		}()
		files.stdin = r
	}
	var parentEnd *os.File
	if c.Handoff {
		parent, child, err := socketpairFiles()
		if err != nil {
			closeOwned()
			return spawnFiles{}, nil, nil, err
		}
		owned = append(owned, child)
		files.handoff = child
		parentEnd = parent
	}
	return files, parentEnd, closeOwned, nil
}

func closeIfOpen(f *os.File) {
	if f != nil {
		_ = f.Close()
	}
}
