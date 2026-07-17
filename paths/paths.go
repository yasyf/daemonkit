// Package paths owns the canonical state-directory layout under the user's home
// directory: the daemon's unix socket, the http handshake file, per-subject
// artifacts, and the lazy-start lock dir. The directory basename is fixed by
// Paths.App (e.g. ".cc-review") and is never relocated by an environment
// variable, so state stays stable regardless of CLAUDE_CONFIG_DIR.
package paths

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// Paths produces the state-directory layout for an application whose private
// state lives at ~/<App>.
type Paths struct {
	// App is the state-dir basename, e.g. ".cc-review".
	App string
}

func mustHome() string {
	h, err := os.UserHomeDir()
	if err != nil {
		panic(fmt.Sprintf("resolve home dir: %v", err))
	}
	return h
}

// StateDir is the application's private state directory (~/<App>).
func (p Paths) StateDir() string { return filepath.Join(mustHome(), p.App) }

// DBPath is the sqlite database path.
func (p Paths) DBPath() string { return filepath.Join(p.StateDir(), "state.db") }

// SocketPath is the daemon control-plane unix socket.
func (p Paths) SocketPath() string { return filepath.Join(p.StateDir(), "daemon.sock") }

// LogPath is the daemon log path.
func (p Paths) LogPath() string { return filepath.Join(p.StateDir(), "daemon.log") }

// HTTPInfoPath is where the daemon publishes its ephemeral port so the CLI
// and the SSE consumers can reach the HTTP plane.
func (p Paths) HTTPInfoPath() string { return filepath.Join(p.StateDir(), "http.json") }

// ChannelSetupMarkerPath is the channel-setup offer marker path.
func (p Paths) ChannelSetupMarkerPath() string {
	return filepath.Join(p.StateDir(), "channels-setup.json")
}

// LockDir holds the lazy-start flock file.
func (p Paths) LockDir() string { return filepath.Join(p.StateDir(), "locks") }

// StartLockPath is the flock file serializing lazy daemon starts.
func (p Paths) StartLockPath() string { return filepath.Join(p.LockDir(), "start.lock") }

// SubjectsDir is the parent of all per-subject on-disk artifacts.
func (p Paths) SubjectsDir() string { return filepath.Join(p.StateDir(), "subjects") }

// SubjectDir is the on-disk artifact directory for a single subject.
func (p Paths) SubjectDir(id string) string { return filepath.Join(p.SubjectsDir(), id) }

// ConsumerCursorPath is where a stream consumer (e.g. "watch" or "channel")
// persists its last-delivered event seq, so a restart resumes without
// re-delivering. Each consumer keeps its own cursor, so they never contend.
func (p Paths) ConsumerCursorPath(subjectID, consumer string) string {
	return filepath.Join(p.SubjectDir(subjectID), consumer+".cursor")
}

// TurnsDir is the parent of all per-repo turn-snapshot scratch dirs.
func (p Paths) TurnsDir() string { return filepath.Join(p.StateDir(), "turns") }

// RepoTurnsDir is the turn-snapshot scratch dir for a single repo, keyed by a
// hash of its root so absolute paths never leak into directory names.
func (p Paths) RepoTurnsDir(repoRoot string) string {
	sum := sha256.Sum256([]byte(repoRoot))
	return filepath.Join(p.TurnsDir(), hex.EncodeToString(sum[:8]))
}

// EnsureStateDir creates the state dir (0700) if missing.
func (p Paths) EnsureStateDir() error { return os.MkdirAll(p.StateDir(), 0o700) }

// EnsureLockDir creates the lock dir (0700) if missing.
func (p Paths) EnsureLockDir() error { return os.MkdirAll(p.LockDir(), 0o700) }

// EnsureSubjectDir creates a subject's artifact dir (0700) if missing.
func (p Paths) EnsureSubjectDir(id string) error { return os.MkdirAll(p.SubjectDir(id), 0o700) }

// EnsureRepoTurnsDir creates a repo's turn-snapshot scratch dir (0700) if
// missing and returns it.
func (p Paths) EnsureRepoTurnsDir(repoRoot string) (string, error) {
	dir := p.RepoTurnsDir(repoRoot)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create turns dir: %w", err)
	}
	return dir, nil
}
