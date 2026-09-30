//go:build linux

package supervise

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/internal/label"
	"github.com/yasyf/daemonkit/paths"
)

const (
	specName    = "service.json"
	recordsName = "supervisor.records"
	// socketName is shorter than the daemon's own socket leaf, so a label whose
	// daemon socket fits sun_path has a supervisor socket that fits too.
	socketName = "sv.sock"

	// defaultExitTimeOut is launchd's own default for a job that states none.
	defaultExitTimeOut = 20 * time.Second
)

// RestartPolicy defines when the supervisor restarts a service after it exits.
type RestartPolicy uint8

const (
	restartPolicyUnset RestartPolicy = iota
	// RestartAlways restarts the service after every exit.
	RestartAlways
	// RestartOnFailure restarts the service only after an unsuccessful exit.
	RestartOnFailure
	// NoRestart leaves the service stopped after it exits.
	NoRestart
)

// Service is one exact desired supervised service specification.
type Service struct {
	// Label names the service, its state directory, and its supervisor.
	// Required.
	Label string `json:"label"`
	// Program is the exact absolute path the supervisor execs. Required.
	Program string `json:"program"`
	// Args are the arguments passed after Program.
	Args []string `json:"args,omitempty"`
	// LogPath is the file the service's stdout and stderr append to; its
	// parent directory is created 0700. Required.
	LogPath string `json:"log_path"`
	// Env overrides entries of the supervisor's own environment, which the
	// service otherwise inherits.
	Env map[string]string `json:"env,omitempty"`
	// RestartPolicy defines when the supervisor restarts the service. Required.
	RestartPolicy RestartPolicy `json:"restart_policy"`
	// ExitTimeOut is the grace between the SIGTERM a stopped service is sent
	// and the SIGKILL that backstops a drain that wedges. Zero is 20 seconds.
	ExitTimeOut time.Duration `json:"exit_timeout,omitempty"`
}

// ValidateLabel is the one rule a service label must pass, and the rule a
// launchd job label passes on darwin: exactly one path component, no leading
// or trailing dot, no "..", and nothing outside [A-Za-z0-9.-].
func ValidateLabel(name string) error {
	if err := label.Validate(name); err != nil {
		return fmt.Errorf("supervise: service %w", err)
	}
	return nil
}

// Validate refuses a specification the supervisor could not run exactly as
// written.
func (s Service) Validate() error {
	if err := ValidateLabel(s.Label); err != nil {
		return err
	}
	if s.Program == "" || !filepath.IsAbs(s.Program) || filepath.Clean(s.Program) != s.Program {
		return fmt.Errorf("supervise: program path %q is not exact and absolute", s.Program)
	}
	if !filepath.IsAbs(s.LogPath) || filepath.Clean(s.LogPath) != s.LogPath {
		return fmt.Errorf("supervise: log path %q is not exact and absolute", s.LogPath)
	}
	switch s.RestartPolicy {
	case RestartAlways, RestartOnFailure, NoRestart:
	case restartPolicyUnset:
		return errors.New("supervise: restart policy is required")
	default:
		return fmt.Errorf("supervise: invalid restart policy %d", s.RestartPolicy)
	}
	if s.ExitTimeOut < 0 {
		return fmt.Errorf("supervise: exit timeout %v is negative", s.ExitTimeOut)
	}
	if strings.ContainsRune(s.Program, 0) || strings.ContainsRune(s.LogPath, 0) {
		return errors.New("supervise: a path contains a NUL byte")
	}
	for _, arg := range s.Args {
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("supervise: argument %q contains a NUL byte", arg)
		}
	}
	for key, value := range s.Env {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, 0) {
			return fmt.Errorf("supervise: environment variable %q is not a valid assignment", key)
		}
	}
	return nil
}

// canonical validates s and returns it with every reference-typed field
// copied, and an empty one normalized to nil, so two specifications that run
// the same service compare equal whichever side of the socket built them.
func (s Service) canonical() (Service, error) {
	if err := s.Validate(); err != nil {
		return Service{}, err
	}
	s.Args = slices.Clone(s.Args)
	if len(s.Args) == 0 {
		s.Args = nil
	}
	s.Env = maps.Clone(s.Env)
	if len(s.Env) == 0 {
		s.Env = nil
	}
	return s, nil
}

func (s Service) equal(other Service) bool {
	return s.Label == other.Label && s.Program == other.Program && s.LogPath == other.LogPath &&
		s.RestartPolicy == other.RestartPolicy && s.ExitTimeOut == other.ExitTimeOut &&
		slices.Equal(s.Args, other.Args) && maps.Equal(s.Env, other.Env)
}

func (s Service) exitTimeOut() time.Duration {
	if s.ExitTimeOut == 0 {
		return defaultExitTimeOut
	}
	return s.ExitTimeOut
}

// environment is the supervisor's own environment with the service's entries
// laid over it, one assignment per key.
func (s Service) environment() []string {
	merged := make(map[string]string, len(s.Env))
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		merged[key] = value
	}
	maps.Copy(merged, s.Env)
	env := make([]string, 0, len(merged))
	for _, key := range slices.Sorted(maps.Keys(merged)) {
		env = append(env, key+"="+merged[key])
	}
	return env
}

// validateProgram is a live-filesystem check that the final component is a
// regular executable file, so a supervisor never records a child it could not
// have executed.
func validateProgram(program string) error {
	info, err := os.Lstat(program)
	if err != nil {
		return fmt.Errorf("supervise: inspect program: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("supervise: program %q is not a regular file", program)
	}
	if info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("supervise: program %q is not executable", program)
	}
	return nil
}

// layout is where one label's service state lives: inside the private state
// directory the daemon itself serves out of.
type layout struct{ dir string }

func layoutFor(name string) (layout, error) {
	if err := ValidateLabel(name); err != nil {
		return layout{}, err
	}
	return layout{dir: paths.Agent(name).StateDir()}, nil
}

func (l layout) spec() string    { return filepath.Join(l.dir, specName) }
func (l layout) records() string { return filepath.Join(l.dir, recordsName) }
func (l layout) socket() string  { return filepath.Join(l.dir, socketName) }
