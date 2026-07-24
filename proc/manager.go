package proc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/fenceauth"
)

const childWrapper = `
trap ':' TERM
printf r >&3
exec 3>&-
if ! IFS= read -r marker <&4 || [ "$marker" != start ]; then exit 125; fi
exec 4<&-
trap - TERM
exec "$@"
`

var (
	// ErrManagerClosed means child admission is permanently closed.
	ErrManagerClosed = errors.New("proc: manager closed")
	// ErrChildStarted means Start was called more than once.
	ErrChildStarted = errors.New("proc: prepared child already started")
	// ErrChildStopped means Stop won the dispatch race.
	ErrChildStopped = errors.New("proc: prepared child stopped before dispatch")
	// ErrChildSettlementIncomplete means exact reap or durable untracking did not settle.
	ErrChildSettlementIncomplete = errors.New("proc: child settlement incomplete")
	// ErrFenceRequired means a peer-fenced child cannot be directly dispatched.
	ErrFenceRequired = errors.New("proc: child requires a ready-only peer fence")
)

const preparedChildTerminationGrace = 500 * time.Millisecond

// ErrPipeUnavailable means a configured prepared-child pipe is absent, transferred, or dispatched.
var ErrPipeUnavailable = errors.New("proc: prepared child pipe is unavailable")

// StdioMode selects one sealed child stream topology.
type StdioMode uint8

const (
	// StdioNull connects the stream to /dev/null.
	StdioNull StdioMode = iota + 1
	// StdioPipe creates one daemonkit-owned bounded-lifetime pipe endpoint.
	StdioPipe
)

// SignatureDigest is an opaque exact signed-target policy identity.
type SignatureDigest [32]byte

// NewSignatureDigest constructs one nonzero opaque signed-target policy identity.
func NewSignatureDigest(value [32]byte) (SignatureDigest, error) {
	digest := SignatureDigest(value)
	if digest == (SignatureDigest{}) {
		return SignatureDigest{}, errors.New("proc: signature digest is zero")
	}
	return digest, nil
}

// SpawnRequestDigest is the canonical immutable identity of one spawn request.
type SpawnRequestDigest [sha256.Size]byte

// SpawnConfig is copied and compiled by NewSpawnRequest.
type SpawnConfig struct {
	RecoveryClass     RecoveryClass
	Executable        string
	Args              []string
	Dir               string
	Env               []string
	Stdin             StdioMode
	Stdout            StdioMode
	Stderr            StdioMode
	RequiresPeerFence bool
	SpawnedSession    bool
	ExpectedSignature *SignatureDigest
}

// SpawnRequest is an immutable validated child launch request.
type SpawnRequest struct {
	recoveryClass  RecoveryClass
	executable     string
	args           []string
	dir            string
	env            []string
	stdin          StdioMode
	stdout         StdioMode
	stderr         StdioMode
	requiresFence  bool
	spawnedSession bool
	signature      SignatureDigest
	hasSignature   bool
	digest         SpawnRequestDigest
}

// NewSpawnRequest validates and deep-copies one exact launch request.
func NewSpawnRequest(config SpawnConfig) (SpawnRequest, error) {
	if err := config.RecoveryClass.Validate(); err != nil {
		return SpawnRequest{}, fmt.Errorf("proc: spawn recovery class: %w", err)
	}
	if strings.ContainsRune(config.Executable, '\x00') || !filepath.IsAbs(config.Executable) || filepath.Clean(config.Executable) != config.Executable {
		return SpawnRequest{}, errors.New("proc: spawn executable must be exact and absolute")
	}
	if config.Dir != "" && (strings.ContainsRune(config.Dir, '\x00') || !filepath.IsAbs(config.Dir) || filepath.Clean(config.Dir) != config.Dir) {
		return SpawnRequest{}, errors.New("proc: spawn directory must be exact and absolute")
	}
	for _, argument := range config.Args {
		if strings.ContainsRune(argument, '\x00') {
			return SpawnRequest{}, errors.New("proc: spawn argument contains NUL")
		}
	}
	if config.ExpectedSignature != nil && *config.ExpectedSignature == (SignatureDigest{}) {
		return SpawnRequest{}, errors.New("proc: expected signature digest is zero")
	}
	if config.RequiresPeerFence && config.ExpectedSignature == nil {
		return SpawnRequest{}, errors.New("proc: peer-fenced spawn requires an expected signature")
	}
	if config.SpawnedSession && (config.ExpectedSignature == nil || config.RequiresPeerFence ||
		config.Stdin != StdioNull || config.Stdout != StdioNull) {
		return SpawnRequest{}, errors.New("proc: spawned session requires an exact signature, null stdin/stdout, and no peer fence")
	}
	for _, mode := range []StdioMode{config.Stdin, config.Stdout, config.Stderr} {
		if mode != StdioNull && mode != StdioPipe {
			return SpawnRequest{}, errors.New("proc: every stdio mode must be explicit")
		}
	}
	seen := map[string]struct{}{"PATH": {}, "LANG": {}}
	for _, variable := range config.Env {
		key, _, ok := strings.Cut(variable, "=")
		if !ok || key == "" || strings.ContainsRune(key, '\x00') || strings.ContainsRune(variable, '\x00') {
			return SpawnRequest{}, errors.New("proc: spawn environment entry is invalid")
		}
		if _, exists := seen[key]; exists {
			return SpawnRequest{}, fmt.Errorf("proc: duplicate spawn environment key %q", key)
		}
		seen[key] = struct{}{}
	}
	environment := append([]string(nil), config.Env...)
	sort.Strings(environment)
	request := SpawnRequest{
		recoveryClass: config.RecoveryClass, executable: config.Executable,
		args: append([]string(nil), config.Args...), dir: config.Dir,
		env: environment, stdin: config.Stdin, stdout: config.Stdout, stderr: config.Stderr,
		requiresFence: config.RequiresPeerFence, spawnedSession: config.SpawnedSession,
	}
	if config.ExpectedSignature != nil {
		request.signature = *config.ExpectedSignature
		request.hasSignature = true
	}
	request.digest = digestSpawnRequest(request)
	return request, nil
}

func digestSpawnRequest(request SpawnRequest) SpawnRequestDigest {
	h := sha256.New()
	writeSpawnDigestBytes(h, []byte("daemonkit.proc.spawn-request.v1"))
	writeSpawnDigestBytes(h, []byte{byte(request.recoveryClass)})
	writeSpawnDigestBytes(h, []byte(request.executable))
	writeSpawnDigestBytes(h, []byte(request.dir))
	writeSpawnDigestStrings(h, request.args)
	writeSpawnDigestStrings(h, request.env)
	for _, mode := range []StdioMode{request.stdin, request.stdout, request.stderr} {
		writeSpawnDigestBytes(h, []byte{byte(mode)})
	}
	writeSpawnDigestBytes(h, []byte{boolByte(request.requiresFence)})
	writeSpawnDigestBytes(h, []byte{boolByte(request.spawnedSession)})
	writeSpawnDigestBytes(h, []byte{boolByte(request.hasSignature)})
	if request.hasSignature {
		writeSpawnDigestBytes(h, request.signature[:])
	}
	var digest SpawnRequestDigest
	copy(digest[:], h.Sum(nil))
	return digest
}

func writeSpawnDigestStrings(h interface{ Write([]byte) (int, error) }, values []string) {
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(values)))
	_, _ = h.Write(count[:])
	for _, value := range values {
		writeSpawnDigestBytes(h, []byte(value))
	}
}

func writeSpawnDigestBytes(h interface{ Write([]byte) (int, error) }, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}

func boolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}

// ProcessReceipt binds the prepared process instance to its expected target.
type ProcessReceipt struct {
	process        Identity
	executable     string
	signature      SignatureDigest
	hasSignature   bool
	requestDigest  SpawnRequestDigest
	requiresFence  bool
	spawnedSession bool
	generation     string
	state          *preparedReceiptState
	owner          *managerToken
}

type preparedReceiptState struct {
	mu       sync.Mutex
	prepared bool
}

// ProcessIdentity returns the immutable prepared wrapper process instance.
func (r ProcessReceipt) ProcessIdentity() Identity { return r.process }

// ExpectedExecutable returns the exact post-gate target executable.
func (r ProcessReceipt) ExpectedExecutable() string { return r.executable }

// ExpectedSignature returns the exact signed-target policy identity.
func (r ProcessReceipt) ExpectedSignature() (SignatureDigest, bool) {
	return r.signature, r.hasSignature
}

// RequestDigest returns the immutable canonical spawn-request identity.
func (r ProcessReceipt) RequestDigest() SpawnRequestDigest { return r.requestDigest }

// RequiresPeerFence reports whether direct Start is forbidden.
func (r ProcessReceipt) RequiresPeerFence() bool { return r.requiresFence }

// HasSpawnedSession reports whether the exact spawn owns a sealed session.
func (r ProcessReceipt) HasSpawnedSession() bool { return r.spawnedSession }

// OwnerGeneration returns the exact daemon generation that durably owns the child.
func (r ProcessReceipt) OwnerGeneration() string { return r.generation }

// Prepared reports whether the exact child is still owned and undispatched or live.
func (r ProcessReceipt) Prepared() bool {
	if r.state == nil {
		return false
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return r.state.prepared
}

// ProcessExit is one immutable child completion observation.
type ProcessExit struct {
	Code    int
	Stopped bool
	Error   string
}

// Manager bounds and owns prepared and started child process groups.
type Manager struct {
	reaper *Reaper
	limit  chan struct{}
	token  *managerToken

	mu        sync.Mutex
	state     managerState
	preparing int
	children  map[*PreparedChild]struct{}
	untracked map[*untrackedChild]struct{}
	changed   chan struct{}
}

type managerToken struct{}

type managerState uint8

const (
	managerUnclaimed managerState = iota
	managerClaimedUnrecovered
	managerRecovering
	managerRecovered
	managerActivated
	managerClosed
)

// ClaimRuntime permanently assigns an idle manager to one pending Runtime.
func (m *Manager) ClaimRuntime() error {
	if m == nil {
		return errors.New("proc: manager is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != managerUnclaimed || m.preparing != 0 || len(m.children) != 0 || len(m.untracked) != 0 {
		return errors.New("proc: manager is already used or runtime-owned")
	}
	m.state = managerClaimedUnrecovered
	return nil
}

// ReleaseRuntime releases a claim that never admitted a child.
func (m *Manager) ReleaseRuntime() error {
	if m == nil {
		return errors.New("proc: manager is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if (m.state != managerClaimedUnrecovered && m.state != managerRecovered) || m.preparing != 0 || len(m.children) != 0 || len(m.untracked) != 0 {
		return errors.New("proc: manager claim cannot be released")
	}
	m.state = managerUnclaimed
	return nil
}

// NewManager constructs a child owner over one concrete durable reaper.
func NewManager(limit int, reaper *Reaper) (*Manager, error) {
	if limit <= 0 {
		return nil, errors.New("proc: manager limit must be positive")
	}
	if reaper == nil || reaper.Store == nil || reaper.Generation == "" {
		return nil, errors.New("proc: manager reaper is incomplete")
	}
	return &Manager{
		reaper: reaper, limit: make(chan struct{}, limit), token: &managerToken{},
		children: make(map[*PreparedChild]struct{}), untracked: make(map[*untrackedChild]struct{}), changed: make(chan struct{}),
	}, nil
}

// PreparedChild is a durably recorded child held before target exec.
type PreparedChild struct {
	manager *Manager
	record  Record
	receipt ProcessReceipt
	gate    *os.File
	waited  <-chan error
	stdin   *os.File
	stdout  *os.File
	stderr  *os.File
	session *spawnedSessionParent

	mu             sync.Mutex
	state          preparedChildState
	observed       chan struct{}
	done           chan struct{}
	waitErr        error
	terminated     bool
	attempting     bool
	attemptDone    chan struct{}
	attemptErr     error
	exit           ProcessExit
	requiresFence  bool
	spawnedSession bool
	requestDigest  SpawnRequestDigest
}

type preparedChildState uint8

const (
	preparedChildPending preparedChildState = iota + 1
	preparedChildStarted
	preparedChildStopping
	preparedChildSettled
)

// Prepare starts only the gated daemonkit wrapper, durably records its exact
// identity, and returns before the target executable can run.
//
//nolint:contextcheck // Failure cleanup uses a fresh manager-owned settlement budget.
func (m *Manager) Prepare(ctx context.Context, request SpawnRequest) (*PreparedChild, ProcessReceipt, error) {
	if request.executable == "" {
		return nil, ProcessReceipt{}, errors.New("proc: spawn request was not constructed")
	}
	if err := m.acquirePrepare(ctx); err != nil {
		return nil, ProcessReceipt{}, err
	}

	command, pipes, err := prepareCommand(request)
	if err != nil {
		m.releasePreparing()
		return nil, ProcessReceipt{}, err
	}
	if err := withChildNprocCap(command.Start); err != nil {
		pipes.closeAll()
		m.releasePreparing()
		return nil, ProcessReceipt{}, fmt.Errorf("proc: start prepared wrapper: %w", err)
	}
	pipes.closeChildEnds()
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	preparedIdentity, err := Probe(command.Process.Pid)
	if err != nil {
		cleanupErr := m.cleanupUntracked(pipes, Identity{}, waited, true)
		return nil, ProcessReceipt{}, errors.Join(fmt.Errorf("proc: snapshot prepared wrapper: %w", err), cleanupErr)
	}
	record, err := m.reaper.TrackGroup(ctx, command.Process.Pid, request.recoveryClass)
	if err != nil {
		waitErr := m.cleanupUntracked(pipes, preparedIdentity, waited, true)
		return nil, ProcessReceipt{}, errors.Join(fmt.Errorf("proc: track prepared child: %w", err), waitErr)
	}
	receiptState := &preparedReceiptState{prepared: true}
	receipt := ProcessReceipt{
		process:    Identity{PID: record.PID, StartTime: record.StartTime, Boot: record.Boot, Comm: record.Comm, Executable: record.Executable},
		executable: request.executable, signature: request.signature, hasSignature: request.hasSignature,
		requestDigest: request.digest, requiresFence: request.requiresFence,
		spawnedSession: request.spawnedSession,
		generation:     record.Generation, state: receiptState, owner: m.token,
	}
	child := &PreparedChild{
		manager: m, record: record, receipt: receipt, gate: pipes.gateWrite, waited: waited,
		stdin: pipes.stdinParent, stdout: pipes.stdoutParent, stderr: pipes.stderrParent,
		session: newSpawnedSessionParent(pipes.takeSessionParent()),
		state:   preparedChildPending, observed: make(chan struct{}), done: make(chan struct{}),
		requiresFence: request.requiresFence, spawnedSession: request.spawnedSession,
		requestDigest: request.digest,
	}
	m.mu.Lock()
	m.preparing--
	m.children[child] = struct{}{}
	m.notifyLocked()
	m.mu.Unlock()
	go child.observe() //nolint:gosec // Observation owns process lifetime, not request lifetime.
	if err := awaitPrepared(ctx, pipes.readyRead); err != nil {
		return nil, ProcessReceipt{}, errors.Join(err, child.stopWithinManagerBudget())
	}
	m.mu.Lock()
	closed := m.state == managerClosed
	m.mu.Unlock()
	if closed {
		return nil, ProcessReceipt{}, errors.Join(ErrManagerClosed, child.stopWithinManagerBudget())
	}
	return child, receipt, nil
}

func (m *Manager) acquirePrepare(ctx context.Context) error {
	for {
		m.mu.Lock()
		if m.state != managerRecovered && m.state != managerActivated {
			m.mu.Unlock()
			return ErrManagerClosed
		}
		select {
		case m.limit <- struct{}{}:
			m.state = managerActivated
			m.preparing++
			m.notifyLocked()
			m.mu.Unlock()
			return nil
		default:
			changed := m.changed
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
		}
	}
}

func (m *Manager) releasePreparing() {
	m.mu.Lock()
	m.preparing--
	<-m.limit
	m.notifyLocked()
	m.mu.Unlock()
}

func (m *Manager) notifyLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

// Start releases the target exec gate exactly once.
func (c *PreparedChild) Start(ctx context.Context) error {
	if c == nil {
		return errors.New("proc: prepared child is required")
	}
	if c.requiresFence {
		return ErrFenceRequired
	}
	return c.start(ctx)
}

// StartFenced releases a peer-fenced child only for daemonkit's internal ready-only authority.
func (c *PreparedChild) StartFenced(ctx context.Context, receipt ProcessReceipt, authority fenceauth.Authority) error {
	if c == nil || !authority.Valid() || !c.requiresFence || !c.matchesReceipt(receipt) {
		return ErrFenceRequired
	}
	return c.start(ctx)
}

func (c *PreparedChild) matchesReceipt(receipt ProcessReceipt) bool {
	return receipt.state != nil && receipt.state == c.receipt.state && receipt.Prepared() &&
		receipt.process == c.receipt.process && receipt.generation == c.receipt.generation &&
		receipt.requestDigest == c.requestDigest && receipt.requiresFence == c.requiresFence &&
		receipt.executable == c.receipt.executable && receipt.signature == c.receipt.signature &&
		receipt.hasSignature == c.receipt.hasSignature && receipt.spawnedSession == c.spawnedSession
}

// ClaimSpawnedSession consumes the receipt-bound parent endpoint after dispatch.
func (c *PreparedChild) ClaimSpawnedSession(
	ctx context.Context,
	receipt ProcessReceipt,
) (SpawnedSessionEndpoint, error) {
	if c == nil {
		return SpawnedSessionEndpoint{}, ErrSpawnedSessionUnavailable
	}
	if err := ctx.Err(); err != nil {
		return SpawnedSessionEndpoint{}, err
	}
	c.mu.Lock()
	if c.state != preparedChildStarted || !c.spawnedSession || c.session == nil || !c.matchesReceipt(receipt) {
		c.mu.Unlock()
		return SpawnedSessionEndpoint{}, ErrSpawnedSessionUnavailable
	}
	session := c.session
	c.mu.Unlock()
	parent, err := spawnedCurrentIdentity()
	if err != nil {
		return SpawnedSessionEndpoint{}, fmt.Errorf("proc: snapshot spawned session parent: %w", err)
	}
	return session.claim(ctx, receipt, parent)
}

//nolint:contextcheck // Failed dispatch settles under the manager-owned termination budget.
func (c *PreparedChild) start(ctx context.Context) error {
	c.mu.Lock()
	switch c.state {
	case preparedChildStarted:
		c.mu.Unlock()
		return ErrChildStarted
	case preparedChildStopping, preparedChildSettled:
		c.mu.Unlock()
		return ErrChildStopped
	case preparedChildPending:
	default:
		c.mu.Unlock()
		return errors.New("proc: invalid prepared child state")
	}
	if err := ctx.Err(); err != nil {
		c.state = preparedChildStopping
		gate := c.gate
		c.gate = nil
		c.mu.Unlock()
		var closeErr error
		if gate != nil {
			closeErr = gate.Close()
		}
		return errors.Join(err, closeErr, c.stopWithinManagerBudget())
	}
	written, startErr := io.WriteString(c.gate, "start\n")
	if startErr == nil && written != len("start\n") {
		startErr = io.ErrShortWrite
	}
	closeErr := c.gate.Close()
	startErr = errors.Join(startErr, closeErr)
	if startErr != nil {
		c.state = preparedChildStopping
		c.mu.Unlock()
		return errors.Join(fmt.Errorf("proc: release prepared child: %w", startErr), c.stopWithinManagerBudget())
	}
	c.state = preparedChildStarted
	c.mu.Unlock()
	return nil
}

//nolint:contextcheck // The fixed settlement reserve cannot inherit a canceled caller.
func (c *PreparedChild) stopWithinManagerBudget() error {
	ctx, cancel := context.WithTimeout(
		context.Background(), preparedChildTerminationGrace+c.manager.reaper.settlementDur(),
	)
	defer cancel()
	return c.Stop(ctx)
}

// TakeStdin transfers exclusive ownership of the configured stdin endpoint before dispatch.
func (c *PreparedChild) TakeStdin() (*os.File, error) {
	if c == nil {
		return nil, ErrPipeUnavailable
	}
	return c.takePipe(&c.stdin)
}

// TakeStdout transfers exclusive ownership of the configured stdout endpoint before dispatch.
func (c *PreparedChild) TakeStdout() (*os.File, error) {
	if c == nil {
		return nil, ErrPipeUnavailable
	}
	return c.takePipe(&c.stdout)
}

// TakeStderr transfers exclusive ownership of the configured stderr endpoint before dispatch.
func (c *PreparedChild) TakeStderr() (*os.File, error) {
	if c == nil {
		return nil, ErrPipeUnavailable
	}
	return c.takePipe(&c.stderr)
}

func (c *PreparedChild) takePipe(endpoint **os.File) (*os.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != preparedChildPending || *endpoint == nil {
		return nil, ErrPipeUnavailable
	}
	result := *endpoint
	*endpoint = nil
	return result, nil
}

// Done closes only after the exact process group is reaped and untracked.
func (c *PreparedChild) Done() <-chan struct{} { return c.done }

// Exit returns the immutable completion once Done is closed.
func (c *PreparedChild) Exit() (ProcessExit, bool) {
	select {
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.exit, true
	default:
		return ProcessExit{}, false
	}
}

// Stop synchronously TERM/KILLs, reaps, and untracks the exact child group.
func (c *PreparedChild) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	for {
		c.mu.Lock()
		if c.state == preparedChildSettled {
			c.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return errors.Join(ErrChildSettlementIncomplete, err)
		}
		gate := c.gate
		c.gate = nil
		c.state = preparedChildStopping
		var done chan struct{}
		if !c.attempting {
			c.attempting = true
			c.attemptDone = make(chan struct{})
			c.attemptErr = nil
			done = c.attemptDone
			c.mu.Unlock()
			if gate != nil {
				_ = gate.Close()
			}
			go c.runSettlementAttempt(ctx, true, done)
		} else {
			done := c.attemptDone
			c.mu.Unlock()
			if gate != nil {
				_ = gate.Close()
			}
			select {
			case <-ctx.Done():
				return errors.Join(ErrChildSettlementIncomplete, ctx.Err())
			case <-done:
			}
			c.mu.Lock()
			settled, attemptErr := c.state == preparedChildSettled, c.attemptErr
			c.mu.Unlock()
			if settled {
				return nil
			}
			if attemptErr != nil {
				continue
			}
			continue
		}

		select {
		case <-ctx.Done():
			return errors.Join(ErrChildSettlementIncomplete, ctx.Err())
		case <-done:
		}
		c.mu.Lock()
		settled, attemptErr := c.state == preparedChildSettled, c.attemptErr
		c.mu.Unlock()
		if settled {
			return nil
		}
		if attemptErr != nil {
			return attemptErr
		}
	}
}

func (c *PreparedChild) runSettlementAttempt(ctx context.Context, stopped bool, done chan struct{}) {
	err := c.settleAttempt(ctx, stopped)
	c.mu.Lock()
	c.attemptErr = err
	c.attempting = false
	close(done)
	c.mu.Unlock()
}

func (c *PreparedChild) settleAttempt(ctx context.Context, stopped bool) error {
	c.mu.Lock()
	terminated := c.terminated
	c.mu.Unlock()
	if !terminated {
		if err := c.manager.reaper.TerminateWithin(ctx, c.record, preparedChildTerminationGrace); err != nil {
			return errors.Join(ErrChildSettlementIncomplete, err)
		}
		c.mu.Lock()
		c.terminated = true
		c.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		return errors.Join(ErrChildSettlementIncomplete, ctx.Err())
	case <-c.observed:
	}
	c.settle(stopped, nil)
	return nil
}

// Shutdown closes admission and synchronously settles every owned child.
func (m *Manager) Shutdown(ctx context.Context) error {
	if m == nil {
		return errors.New("proc: manager is required")
	}
	m.mu.Lock()
	for m.state == managerRecovering {
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return errors.Join(ErrChildSettlementIncomplete, ctx.Err())
		case <-changed:
		}
		m.mu.Lock()
	}
	m.state = managerClosed
	m.notifyLocked()
	for m.preparing != 0 {
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return errors.Join(ErrChildSettlementIncomplete, ctx.Err())
		case <-changed:
		}
		m.mu.Lock()
	}
	children := make([]*PreparedChild, 0, len(m.children))
	for child := range m.children {
		children = append(children, child)
	}
	untracked := make([]*untrackedChild, 0, len(m.untracked))
	for child := range m.untracked {
		untracked = append(untracked, child)
	}
	m.mu.Unlock()
	var errs []error
	for _, child := range children {
		errs = append(errs, child.Stop(ctx))
	}
	for _, child := range untracked {
		errs = append(errs, child.Stop(ctx))
	}
	return errors.Join(errs...)
}

// Recover settles every exact prior-generation child before listener acquisition.
func (m *Manager) Recover(ctx context.Context) error {
	if m == nil {
		return errors.New("proc: manager is required")
	}
	m.mu.Lock()
	if m.state != managerClaimedUnrecovered || m.preparing != 0 || len(m.children) != 0 || len(m.untracked) != 0 {
		m.mu.Unlock()
		return errors.New("proc: manager recovery requires one idle runtime claim")
	}
	m.state = managerRecovering
	m.notifyLocked()
	m.mu.Unlock()
	err := m.reaper.Reap(ctx)
	m.mu.Lock()
	if err == nil {
		m.state = managerRecovered
	} else {
		m.state = managerClaimedUnrecovered
	}
	m.notifyLocked()
	m.mu.Unlock()
	return err
}

// OwnsReceipt reports whether this manager minted and still owns receipt.
func (m *Manager) OwnsReceipt(receipt ProcessReceipt) bool {
	if m == nil || receipt.owner == nil || receipt.owner != m.token {
		return false
	}
	m.mu.Lock()
	active := m.state == managerActivated
	m.mu.Unlock()
	return active && receipt.Prepared()
}

// Active returns the exact currently owned child count.
func (m *Manager) Active() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.preparing + len(m.children) + len(m.untracked)
}

//nolint:contextcheck // Untracked cleanup must complete independently of the failed request.
func (m *Manager) cleanupUntracked(pipes *preparedPipes, identity Identity, waited <-chan error, transferPreparing bool) error {
	child := &untrackedChild{
		manager: m, identity: identity, gate: pipes.gateWrite, waited: waited,
		observed: make(chan struct{}), done: make(chan struct{}),
		files: []*os.File{
			pipes.readyRead, pipes.stdinParent, pipes.stdoutParent, pipes.stderrParent,
			pipes.sessionParent,
		},
	}
	m.mu.Lock()
	if transferPreparing {
		m.preparing--
	}
	m.untracked[child] = struct{}{}
	m.notifyLocked()
	m.mu.Unlock()
	go child.observe()
	ctx, cancel := context.WithTimeout(context.Background(), preparedChildTerminationGrace+m.reaper.settlementDur())
	defer cancel()
	return child.Stop(ctx)
}

type untrackedChild struct {
	manager  *Manager
	identity Identity
	gate     *os.File
	waited   <-chan error
	observed chan struct{}
	done     chan struct{}
	files    []*os.File

	mu         sync.Mutex
	terminated bool
	settled    bool
}

func (c *untrackedChild) observe() {
	<-c.waited
	close(c.observed)
	c.settle()
}

func (c *untrackedChild) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.settled {
		c.mu.Unlock()
		return nil
	}
	gate := c.gate
	c.gate = nil
	terminated := c.terminated
	c.mu.Unlock()
	if gate != nil {
		_ = gate.Close()
	}
	if !terminated && c.identity.PID != 0 {
		if err := c.manager.reaper.TerminateIdentityWithin(ctx, c.identity, preparedChildTerminationGrace); err != nil {
			return errors.Join(ErrChildSettlementIncomplete, err)
		}
		c.mu.Lock()
		c.terminated = true
		c.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		return errors.Join(ErrChildSettlementIncomplete, ctx.Err())
	case <-c.done:
		return nil
	}
}

func (c *untrackedChild) settle() {
	c.mu.Lock()
	if c.settled {
		c.mu.Unlock()
		return
	}
	c.settled = true
	gate := c.gate
	c.gate = nil
	files := c.files
	c.files = nil
	c.mu.Unlock()
	if gate != nil {
		_ = gate.Close()
	}
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
	c.manager.mu.Lock()
	delete(c.manager.untracked, c)
	<-c.manager.limit
	c.manager.notifyLocked()
	c.manager.mu.Unlock()
	close(c.done)
}

func (c *PreparedChild) observe() {
	waitErr := <-c.waited
	c.mu.Lock()
	c.waitErr = waitErr
	close(c.observed)
	c.mu.Unlock()

	c.mu.Lock()
	if c.state == preparedChildStopping || c.state == preparedChildSettled || c.attempting {
		c.mu.Unlock()
		return
	}
	c.state = preparedChildStopping
	c.attempting = true
	c.attemptDone = make(chan struct{})
	done := c.attemptDone
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(
		context.Background(), preparedChildTerminationGrace+c.manager.reaper.settlementDur(),
	)
	c.runSettlementAttempt(ctx, false, done)
	cancel()
}

func (c *PreparedChild) settle(stopped bool, settleErr error) {
	c.mu.Lock()
	if c.state == preparedChildSettled {
		c.mu.Unlock()
		return
	}
	exit := ProcessExit{Code: processExitCode(c.waitErr), Stopped: stopped}
	if settleErr != nil {
		exit.Error = settleErr.Error()
	} else if !stopped && c.waitErr != nil {
		exit.Error = c.waitErr.Error()
	}
	c.exit = exit
	c.state = preparedChildSettled
	state := c.receipt.state
	gate := c.gate
	c.gate = nil
	stdin, stdout, stderr, session := c.stdin, c.stdout, c.stderr, c.session
	c.mu.Unlock()
	if state != nil {
		state.mu.Lock()
		state.prepared = false
		state.mu.Unlock()
	}
	if gate != nil {
		_ = gate.Close()
	}
	for _, file := range []*os.File{stdin, stdout, stderr} {
		if file != nil {
			_ = file.Close()
		}
	}
	if session != nil {
		_ = session.close()
	}
	c.manager.mu.Lock()
	delete(c.manager.children, c)
	<-c.manager.limit
	c.manager.notifyLocked()
	c.manager.mu.Unlock()
	close(c.done)
}

func processExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	if err == nil {
		return 0
	}
	return -1
}

type preparedPipes struct {
	readyRead, readyWrite, gateRead, gateWrite                                    *os.File
	stdinParent, stdinChild, stdoutParent, stdoutChild, stderrParent, stderrChild *os.File
	sessionParent, sessionChild                                                   *os.File
}

func prepareCommand(request SpawnRequest) (*exec.Cmd, *preparedPipes, error) {
	p := &preparedPipes{}
	var err error
	if p.readyRead, p.readyWrite, err = os.Pipe(); err != nil {
		return nil, nil, err
	}
	if p.gateRead, p.gateWrite, err = os.Pipe(); err != nil {
		p.closeAll()
		return nil, nil, err
	}
	if p.stdinParent, p.stdinChild, err = stdioPair(request.stdin, true); err != nil {
		p.closeAll()
		return nil, nil, err
	}
	if p.stdoutParent, p.stdoutChild, err = stdioPair(request.stdout, false); err != nil {
		p.closeAll()
		return nil, nil, err
	}
	if p.stderrParent, p.stderrChild, err = stdioPair(request.stderr, false); err != nil {
		p.closeAll()
		return nil, nil, err
	}
	if request.spawnedSession {
		if p.sessionParent, p.sessionChild, err = newSpawnedSessionFiles(); err != nil {
			p.closeAll()
			return nil, nil, err
		}
	}
	args := append([]string{"-c", childWrapper, "daemonkit-child", request.executable}, request.args...)
	cmd := exec.Command("/bin/sh", args...)
	cmd.Dir = request.dir
	cmd.Env = append([]string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C"}, request.env...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = p.stdinChild, p.stdoutChild, p.stderrChild
	cmd.ExtraFiles = []*os.File{p.readyWrite, p.gateRead}
	if p.sessionChild != nil {
		cmd.ExtraFiles = append(cmd.ExtraFiles, p.sessionChild)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd, p, nil
}

func stdioPair(mode StdioMode, input bool) (*os.File, *os.File, error) {
	if mode == StdioNull {
		file, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		return nil, file, err
	}
	read, write, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	if input {
		return write, read, nil
	}
	return read, write, nil
}

func awaitPrepared(ctx context.Context, ready *os.File) error {
	result := make(chan error, 1)
	go func() {
		var marker [1]byte
		_, err := io.ReadFull(ready, marker[:])
		if err == nil && marker[0] != 'r' {
			err = errors.New("proc: invalid wrapper readiness")
		}
		result <- err
	}()
	select {
	case err := <-result:
		_ = ready.Close()
		return err
	case <-ctx.Done():
		_ = ready.Close()
		<-result
		return ctx.Err()
	}
}

func (p *preparedPipes) closeChildEnds() {
	for _, file := range []*os.File{
		p.readyWrite, p.gateRead, p.stdinChild, p.stdoutChild, p.stderrChild, p.sessionChild,
	} {
		if file != nil {
			_ = file.Close()
		}
	}
}

func (p *preparedPipes) closeParentEnds() {
	for _, file := range []*os.File{
		p.readyRead, p.gateWrite, p.stdinParent, p.stdoutParent, p.stderrParent, p.sessionParent,
	} {
		if file != nil {
			_ = file.Close()
		}
	}
}

func (p *preparedPipes) takeSessionParent() *os.File {
	file := p.sessionParent
	p.sessionParent = nil
	return file
}

func (p *preparedPipes) closeAll() { p.closeChildEnds(); p.closeParentEnds() }
