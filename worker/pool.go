// Package worker runs bounded disposable commands under daemonkit process ownership.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

var (
	// ErrCapacity means every execution and queue slot is occupied.
	ErrCapacity = errors.New("worker: capacity exhausted")
	// ErrInputLimit means the immutable standard input exceeds its configured bound.
	ErrInputLimit = errors.New("worker: input limit exceeded")
	// ErrOutputLimit means standard output or standard error exceeded its configured bound.
	ErrOutputLimit = errors.New("worker: output limit exceeded")
	// ErrTimedOut means the queue and execution deadline elapsed.
	ErrTimedOut = errors.New("worker: timed out")
	// ErrSettlementIncomplete means exact termination or durable untracking did not settle.
	ErrSettlementIncomplete = errors.New("worker: settlement incomplete")
	// ErrClosed means the pool is terminal and admits no more work.
	ErrClosed = errors.New("worker: pool closed")
	// ErrCanceled means a caller or pool cancellation stopped the request.
	ErrCanceled = errors.New("worker: canceled")
	// ErrRuntimeOwnership means a Runtime claim is absent, stale, or in the wrong phase.
	ErrRuntimeOwnership = errors.New("worker: invalid runtime ownership")
	// ErrBudgetTooSmall means the total budget cannot preserve mandatory settlement time.
	ErrBudgetTooSmall = errors.New("worker: total budget is too small")
	// ErrInputDelivery means the complete immutable standard input was not delivered.
	ErrInputDelivery = errors.New("worker: standard input delivery failed")
)

const (
	terminationGrace  = 500 * time.Millisecond
	settlementReserve = time.Second
	totalReserve      = terminationGrace + settlementReserve
)

// Config fixes every pool, queue, runtime, and byte bound.
type Config struct {
	Capacity       int
	QueueCapacity  int
	MaxTotalRun    time.Duration
	MaxStdinBytes  int
	MaxStdoutBytes int
	MaxStderrBytes int
}

// CommandRequest is copied and validated before it can enter the queue.
type CommandRequest struct {
	Path         string
	Dir          string
	Args         []string
	Env          []string
	Stdin        []byte
	TotalTimeout time.Duration
}

// CommandResult is an immutable command observation and durable process receipt.
type CommandResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
	Receipt  proc.ProcessReceipt
}

// ExitError reports one normally settled nonzero command exit.
type ExitError struct {
	ExitCode int
}

func (e *ExitError) Error() string {
	if e == nil {
		return "worker: nonzero exit"
	}
	return fmt.Sprintf("worker: process exited with code %d", e.ExitCode)
}

// Pool owns one sealed proc.Manager and bounded disposable-command runtime.
type Pool struct {
	config    Config
	reaper    *proc.Reaper
	manager   *proc.Manager
	admission chan struct{}
	execution chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc

	mu           sync.Mutex
	closed       bool
	ready        bool
	history      bool
	claim        *RuntimeClaim
	inflight     int
	inflightDone chan struct{}
	terminal     error
}

// RuntimeClaim owns product and verifier workers for one daemon Runtime.
type RuntimeClaim struct {
	product   *Pool
	verifier  *Pool
	lifecycle chan struct{}

	mu                sync.Mutex
	productRecovered  bool
	verifierRecovered bool
	recovered         bool
	activated         bool
	released          bool
	terminal          error
	closeStarted      bool
	closeDone         chan struct{}
	closeErr          error
}

// NewPool constructs one exact worker runtime with no implicit defaults.
func NewPool(config Config, reaper *proc.Reaper) (*Pool, error) {
	return newPool(config, reaper)
}

func newPool(config Config, reaper *proc.Reaper) (*Pool, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	manager, err := proc.NewManager(config.Capacity, reaper)
	if err != nil {
		return nil, fmt.Errorf("worker: process manager: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	inflightDone := make(chan struct{})
	close(inflightDone)
	return &Pool{
		config: config, reaper: reaper, manager: manager,
		admission: make(chan struct{}, config.Capacity+config.QueueCapacity),
		execution: make(chan struct{}, config.Capacity),
		ctx:       ctx, cancel: cancel, inflightDone: inflightDone,
	}, nil
}

// ClaimRuntime assigns this open, unused pool to one pending daemon Runtime.
func (p *Pool) ClaimRuntime() (*RuntimeClaim, error) {
	if p == nil {
		return nil, ErrRuntimeOwnership
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.claim != nil || p.history || p.inflight != 0 {
		return nil, ErrRuntimeOwnership
	}
	if err := p.manager.ClaimRuntime(); err != nil {
		return nil, errors.Join(ErrRuntimeOwnership, err)
	}
	verifierConfig := p.config
	verifierConfig.Capacity = 1
	verifierConfig.QueueCapacity = 0
	verifier, err := newPool(verifierConfig, p.reaper)
	if err != nil {
		_ = p.manager.ReleaseRuntime()
		return nil, errors.Join(ErrRuntimeOwnership, err)
	}
	if err := verifier.manager.ClaimRuntime(); err != nil {
		_ = p.manager.ReleaseRuntime()
		return nil, errors.Join(ErrRuntimeOwnership, err)
	}
	claim := &RuntimeClaim{
		product: p, verifier: verifier, lifecycle: make(chan struct{}, 1), closeDone: make(chan struct{}),
	}
	claim.lifecycle <- struct{}{}
	p.claim = claim
	verifier.claim = claim
	return claim, nil
}

// Product returns the product worker pool owned by this claim.
func (c *RuntimeClaim) Product() *Pool {
	if c == nil {
		return nil
	}
	return c.product
}

// Recover settles product and verifier tasks before listener admission.
func (c *RuntimeClaim) Recover(ctx context.Context) error {
	if err := c.acquire(ctx); err != nil {
		return errors.Join(ErrRuntimeOwnership, err)
	}
	defer c.releaseLifecycle()
	if !c.current(false) {
		return ErrRuntimeOwnership
	}
	c.mu.Lock()
	if c.recovered || c.activated || c.released {
		c.mu.Unlock()
		return ErrRuntimeOwnership
	}
	productRecovered := c.productRecovered
	verifierRecovered := c.verifierRecovered
	c.mu.Unlock()
	if !productRecovered {
		if err := c.product.manager.Recover(ctx); err != nil {
			return fmt.Errorf("worker: recover product workers: %w", err)
		}
		c.mu.Lock()
		c.productRecovered = true
		c.mu.Unlock()
	}
	if !verifierRecovered {
		if err := c.verifier.manager.Recover(ctx); err != nil {
			return fmt.Errorf("worker: recover verifier worker: %w", err)
		}
		c.mu.Lock()
		c.verifierRecovered = true
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.recovered = true
	c.mu.Unlock()
	return nil
}

// Activate makes recovered product and verifier workers ready for admission.
func (c *RuntimeClaim) Activate() error {
	if c == nil {
		return ErrRuntimeOwnership
	}
	select {
	case <-c.lifecycle:
		defer c.releaseLifecycle()
	default:
		return ErrRuntimeOwnership
	}
	if !c.current(false) {
		return ErrRuntimeOwnership
	}
	c.mu.Lock()
	if !c.recovered || c.activated || c.released {
		c.mu.Unlock()
		return ErrRuntimeOwnership
	}
	c.activated = true
	c.mu.Unlock()
	c.product.setReady(true)
	c.verifier.setReady(true)
	return nil
}

// RunVerifier executes one private capacity-one verifier command.
func (c *RuntimeClaim) RunVerifier(ctx context.Context, request CommandRequest) (CommandResult, error) {
	if !c.current(true) {
		return CommandResult{}, ErrRuntimeOwnership
	}
	return c.verifier.Run(ctx, request)
}

// Release abandons one successfully recovered claim before activation.
func (c *RuntimeClaim) Release(ctx context.Context) error {
	if err := c.acquire(ctx); err != nil {
		return errors.Join(ErrRuntimeOwnership, err)
	}
	defer c.releaseLifecycle()
	if !c.current(false) || !c.canRelease() {
		return ErrRuntimeOwnership
	}
	if err := c.product.manager.ReleaseRuntime(); err != nil {
		return errors.Join(ErrRuntimeOwnership, err)
	}
	if err := c.verifier.manager.ReleaseRuntime(); err != nil {
		_ = c.terminalize(errors.Join(ErrSettlementIncomplete, err))
		return errors.Join(ErrRuntimeOwnership, err)
	}
	c.product.mu.Lock()
	c.product.claim = nil
	c.product.ready = false
	c.product.mu.Unlock()
	c.verifier.mu.Lock()
	c.verifier.claim = nil
	c.verifier.ready = false
	c.verifier.mu.Unlock()
	c.mu.Lock()
	c.released = true
	c.mu.Unlock()
	return nil
}

func (c *RuntimeClaim) acquire(ctx context.Context) error {
	if c == nil {
		return ErrRuntimeOwnership
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.lifecycle:
		return nil
	}
}

func (c *RuntimeClaim) releaseLifecycle() { c.lifecycle <- struct{}{} }

func (c *RuntimeClaim) current(requireActivated bool) bool {
	if c == nil || c.product == nil {
		return false
	}
	c.product.mu.Lock()
	current := c.product.claim == c
	c.product.mu.Unlock()
	c.mu.Lock()
	valid := !c.released && (!requireActivated || c.activated)
	c.mu.Unlock()
	return current && valid
}

func (c *RuntimeClaim) canRelease() bool {
	c.mu.Lock()
	valid := c.recovered && !c.activated && !c.released && c.terminal == nil
	c.mu.Unlock()
	if !valid {
		return false
	}
	return c.product.unused() && c.verifier.unused()
}

func (p *Pool) unused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.history && p.inflight == 0 && len(p.admission) == 0 && p.manager.Active() == 0
}

func (p *Pool) setReady(ready bool) {
	p.mu.Lock()
	p.ready = ready
	p.mu.Unlock()
}

func (p *Pool) markTerminal(err error) {
	p.mu.Lock()
	if p.terminal == nil {
		p.terminal = err
	}
	p.closed = true
	p.ready = false
	p.cancel()
	p.mu.Unlock()
}

func (c *RuntimeClaim) terminalize(err error) error {
	if c == nil {
		return err
	}
	c.mu.Lock()
	if c.terminal == nil {
		c.terminal = err
	}
	terminal := c.terminal
	c.mu.Unlock()
	c.product.markTerminal(terminal)
	c.verifier.markTerminal(terminal)
	return terminal
}

func (p *Pool) beginRun(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		err := errors.Join(ErrClosed, p.terminal)
		p.mu.Unlock()
		return err
	}
	claim := p.claim
	p.mu.Unlock()
	if claim == nil {
		return ErrRuntimeOwnership
	}
	if err := claim.acquire(ctx); err != nil {
		return runCause(err)
	}
	defer claim.releaseLifecycle()

	claim.mu.Lock()
	admitting := claim.activated && !claim.released && !claim.closeStarted && claim.terminal == nil
	claim.mu.Unlock()
	if !admitting {
		p.mu.Lock()
		closed, terminal := p.closed, p.terminal
		p.mu.Unlock()
		if closed {
			return errors.Join(ErrClosed, terminal)
		}
		return ErrRuntimeOwnership
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.Join(ErrClosed, p.terminal)
	}
	if p.claim != claim || !p.ready {
		return ErrRuntimeOwnership
	}
	select {
	case p.admission <- struct{}{}:
		p.history = true
		if p.inflight == 0 {
			p.inflightDone = make(chan struct{})
		}
		p.inflight++
		return nil
	default:
		return ErrCapacity
	}
}

func (p *Pool) finishRun() {
	<-p.admission
	p.mu.Lock()
	p.inflight--
	if p.inflight == 0 {
		close(p.inflightDone)
	}
	p.mu.Unlock()
}

func (p *Pool) waitRuns(ctx context.Context) error {
	p.mu.Lock()
	done := p.inflightDone
	p.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Active returns the product pool's currently owned child count.
func (p *Pool) Active() int {
	if p == nil {
		return 0
	}
	return p.manager.Active()
}

func (c Config) validate() error {
	if c.Capacity <= 0 {
		return errors.New("worker: capacity must be positive")
	}
	if c.QueueCapacity < 0 {
		return errors.New("worker: queue capacity must be nonnegative")
	}
	if c.MaxTotalRun <= totalReserve {
		return errors.New("worker: maximum total run must exceed the mandatory settlement reserve")
	}
	if c.MaxStdinBytes < 0 {
		return errors.New("worker: maximum stdin bytes must be nonnegative")
	}
	if c.MaxStdoutBytes <= 0 || c.MaxStderrBytes <= 0 {
		return errors.New("worker: maximum output bytes must be positive")
	}
	return nil
}

// Run executes one copied command or returns before spawning with a zero result.
//
//nolint:contextcheck // Pool cancellation and settlement reserves intentionally outlive the caller.
func (p *Pool) Run(ctx context.Context, request CommandRequest) (CommandResult, error) {
	if p == nil {
		return CommandResult{}, ErrClosed
	}
	request, err := p.copyAndValidate(request)
	if err != nil {
		return CommandResult{}, err
	}
	spawn, err := proc.NewSpawnRequest(proc.SpawnConfig{
		RecoveryClass:     proc.RecoveryTask,
		Executable:        request.Path,
		Args:              request.Args,
		Dir:               request.Dir,
		Env:               request.Env,
		Stdin:             proc.StdioPipe,
		Stdout:            proc.StdioPipe,
		Stderr:            proc.StdioPipe,
		RequiresPeerFence: false,
	})
	if err != nil {
		return CommandResult{}, fmt.Errorf("worker: command: %w", err)
	}

	totalDeadline := time.Now().Add(request.TotalTimeout)
	if callerDeadline, present := ctx.Deadline(); present && callerDeadline.Before(totalDeadline) {
		totalDeadline = callerDeadline
	}
	if err := ctx.Err(); err != nil {
		return CommandResult{}, runCause(err)
	}
	executionDeadline := totalDeadline.Add(-totalReserve)
	if !executionDeadline.After(time.Now()) {
		return CommandResult{}, ErrBudgetTooSmall
	}
	runCtx, cancel := context.WithDeadline(ctx, executionDeadline)
	cleanupCtx, cleanupCancel := context.WithDeadline(context.Background(), totalDeadline)
	stopPoolCancel := context.AfterFunc(p.ctx, cancel)
	defer func() {
		stopPoolCancel()
		cancel()
		cleanupCancel()
	}()
	if err := runCtx.Err(); err != nil {
		return CommandResult{}, runCause(err)
	}
	if err := p.beginRun(runCtx); err != nil {
		return CommandResult{}, err
	}
	defer p.finishRun()
	select {
	case p.execution <- struct{}{}:
		defer func() { <-p.execution }()
	case <-runCtx.Done():
		return CommandResult{}, runCause(runCtx.Err())
	}

	child, receipt, err := p.manager.Prepare(runCtx, spawn)
	if err != nil {
		return CommandResult{}, p.prepareError(runCtx, err)
	}
	result := CommandResult{Receipt: receipt}
	streams, err := takeWorkerPipes(child)
	if err != nil {
		streams.close()
		stopErr := p.stopChild(cleanupCtx, child)
		return result, errors.Join(err, stopErr)
	}
	if err := child.Start(runCtx); err != nil {
		streams.close()
		return result, p.childError(runCtx, err)
	}

	stdoutDone := make(chan streamResult, 1)
	stderrDone := make(chan streamResult, 1)
	stdinDone := make(chan error, 1)
	ioDone := make(chan ioResult, 1)
	limit := make(chan struct{}, 1)
	readStream := func(reader io.Reader, maximum int, done chan<- streamResult) {
		done <- collectBounded(reader, maximum, func() {
			select {
			case limit <- struct{}{}:
			default:
			}
		})
	}
	go readStream(streams.stdout, p.config.MaxStdoutBytes, stdoutDone)
	go readStream(streams.stderr, p.config.MaxStderrBytes, stderrDone)
	go func() { stdinDone <- writeInput(streams.stdin, request.Stdin) }()
	go func() {
		ioDone <- ioResult{stdout: <-stdoutDone, stderr: <-stderrDone, stdinErr: <-stdinDone}
	}()

	var cause error
	select {
	case <-child.Done():
	case <-runCtx.Done():
		cause = runCause(runCtx.Err())
	case <-limit:
		cause = ErrOutputLimit
	}
	if cause != nil {
		if stopErr := p.stopChild(cleanupCtx, child); stopErr != nil {
			cause = errors.Join(cause, stopErr)
		}
	}

	var completed ioResult
	select {
	case completed = <-ioDone:
	case <-cleanupCtx.Done():
		streams.close()
		completed = <-ioDone
		cause = errors.Join(cause, p.terminalize(errors.Join(ErrSettlementIncomplete, cleanupCtx.Err())))
	}
	streams.close()
	result.Stdout = append([]byte(nil), completed.stdout.data...)
	result.Stderr = append([]byte(nil), completed.stderr.data...)
	if (completed.stdout.limit || completed.stderr.limit) && !errors.Is(cause, ErrOutputLimit) {
		cause = errors.Join(cause, ErrOutputLimit)
	}
	if cause == nil && (completed.stdout.err != nil || completed.stderr.err != nil) {
		cause = errors.Join(cause, completed.stdout.err, completed.stderr.err)
	}
	if cause == nil && completed.stdinErr != nil {
		cause = errors.Join(ErrInputDelivery, completed.stdinErr)
	}
	exit, settled := child.Exit()
	if settled {
		result.ExitCode = exit.Code
	}
	if cause != nil {
		return result, cause
	}
	if !settled {
		return result, p.terminalize(ErrSettlementIncomplete)
	}
	if exit.Code != 0 {
		return result, &ExitError{ExitCode: exit.Code}
	}
	if exit.Error != "" {
		return result, errors.New(exit.Error)
	}
	return result, nil
}

func (p *Pool) copyAndValidate(request CommandRequest) (CommandRequest, error) {
	request.Args = append([]string(nil), request.Args...)
	request.Env = append([]string(nil), request.Env...)
	request.Stdin = append([]byte(nil), request.Stdin...)
	if !exactAbsolute(request.Path) {
		return CommandRequest{}, errors.New("worker: command path must be exact and absolute")
	}
	if !exactAbsolute(request.Dir) {
		return CommandRequest{}, errors.New("worker: command directory must be exact and absolute")
	}
	if request.TotalTimeout <= 0 || request.TotalTimeout > p.config.MaxTotalRun {
		return CommandRequest{}, errors.New("worker: command total timeout must be positive and within maximum total run")
	}
	if len(request.Stdin) > p.config.MaxStdinBytes {
		return CommandRequest{}, ErrInputLimit
	}
	for _, argument := range request.Args {
		if strings.ContainsRune(argument, '\x00') {
			return CommandRequest{}, errors.New("worker: command argument contains NUL")
		}
	}
	seen := map[string]struct{}{"PATH": {}, "LANG": {}}
	for _, variable := range request.Env {
		key, _, ok := strings.Cut(variable, "=")
		if !ok || key == "" || strings.ContainsRune(variable, '\x00') {
			return CommandRequest{}, errors.New("worker: command environment entry is invalid")
		}
		if _, exists := seen[key]; exists {
			return CommandRequest{}, fmt.Errorf("worker: duplicate command environment key %q", key)
		}
		seen[key] = struct{}{}
	}
	return request, nil
}

func exactAbsolute(path string) bool {
	return path != "" && !strings.ContainsRune(path, '\x00') && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func (p *Pool) prepareError(ctx context.Context, err error) error {
	var cause error
	if ctx.Err() != nil {
		cause = runCause(ctx.Err())
	}
	if errors.Is(err, proc.ErrChildSettlementIncomplete) {
		return errors.Join(cause, p.terminalize(errors.Join(ErrSettlementIncomplete, err)))
	}
	return errors.Join(cause, err)
}

func (p *Pool) childError(ctx context.Context, err error) error {
	var cause error
	if ctx.Err() != nil {
		cause = runCause(ctx.Err())
	}
	if errors.Is(err, proc.ErrChildSettlementIncomplete) {
		return errors.Join(cause, p.terminalize(errors.Join(ErrSettlementIncomplete, err)))
	}
	return errors.Join(cause, err)
}

func (p *Pool) stopChild(ctx context.Context, child *proc.PreparedChild) error {
	if err := child.Stop(ctx); err != nil {
		return p.terminalize(errors.Join(ErrSettlementIncomplete, err))
	}
	return nil
}

func (p *Pool) terminalize(err error) error {
	p.mu.Lock()
	claim := p.claim
	p.mu.Unlock()
	if claim != nil {
		return claim.terminalize(err)
	}
	p.markTerminal(err)
	return err
}

func runCause(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(ErrTimedOut, context.DeadlineExceeded)
	}
	return errors.Join(ErrCanceled, context.Canceled)
}

type streamResult struct {
	data  []byte
	limit bool
	err   error
}

type ioResult struct {
	stdout   streamResult
	stderr   streamResult
	stdinErr error
}

type workerPipes struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func takeWorkerPipes(child *proc.PreparedChild) (workerPipes, error) {
	var result workerPipes
	stdin, err := child.TakeStdin()
	if err != nil {
		return result, fmt.Errorf("worker: take stdin: %w", err)
	}
	result.stdin = stdin
	stdout, err := child.TakeStdout()
	if err != nil {
		return result, fmt.Errorf("worker: take stdout: %w", err)
	}
	result.stdout = stdout
	stderr, err := child.TakeStderr()
	if err != nil {
		return result, fmt.Errorf("worker: take stderr: %w", err)
	}
	result.stderr = stderr
	return result, nil
}

func (p workerPipes) close() {
	for _, stream := range []io.Closer{p.stdin, p.stdout, p.stderr} {
		if stream != nil {
			_ = stream.Close()
		}
	}
}

func collectBounded(reader io.Reader, limit int, overflow func()) streamResult {
	data := make([]byte, 0, limit)
	buffer := make([]byte, 32*1024)
	limited := false
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			remaining := limit - len(data)
			if remaining > n {
				remaining = n
			}
			if remaining > 0 {
				data = append(data, buffer[:remaining]...)
			}
			if n > remaining && !limited {
				limited = true
				overflow()
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return streamResult{data: data, limit: limited, err: err}
		}
	}
}

func writeInput(writer io.WriteCloser, input []byte) error {
	if writer == nil {
		return nil
	}
	written, writeErr := writer.Write(input)
	if writeErr == nil && written != len(input) {
		writeErr = io.ErrShortWrite
	}
	return errors.Join(writeErr, writer.Close())
}

// Close terminally settles product and verifier workers and stores its first result.
func (c *RuntimeClaim) Close(ctx context.Context) error {
	if c == nil {
		return ErrRuntimeOwnership
	}
	c.mu.Lock()
	if c.closeStarted {
		done := c.closeDone
		c.mu.Unlock()
		select {
		case <-done:
			return c.stickyCloseResult()
		default:
		}
		select {
		case <-done:
			return c.stickyCloseResult()
		case <-ctx.Done():
			select {
			case <-done:
				return c.stickyCloseResult()
			default:
				return ctx.Err()
			}
		}
	}
	if !c.activated || c.released {
		c.mu.Unlock()
		return ErrRuntimeOwnership
	}
	c.closeStarted = true
	c.mu.Unlock()
	if !c.currentWithoutClaimLock() {
		return c.finishClose(ErrRuntimeOwnership)
	}

	if err := ctx.Err(); err != nil {
		result := errors.Join(ErrSettlementIncomplete, err)
		return c.finishClose(c.terminalize(result))
	}
	if err := c.acquire(ctx); err != nil {
		result := errors.Join(ErrSettlementIncomplete, err)
		return c.finishClose(c.terminalize(result))
	}
	defer c.releaseLifecycle()

	terminal := c.terminalize(ErrClosed)
	productErr := c.product.manager.Shutdown(ctx)
	verifierErr := c.verifier.manager.Shutdown(ctx)
	productRunsErr := c.product.waitRuns(ctx)
	verifierRunsErr := c.verifier.waitRuns(ctx)
	result := errors.Join(productErr, verifierErr, productRunsErr, verifierRunsErr)
	if !errors.Is(terminal, ErrClosed) {
		result = terminal
		return c.finishClose(result)
	}
	if result != nil {
		result = errors.Join(ErrSettlementIncomplete, result)
	}
	return c.finishClose(result)
}

func (c *RuntimeClaim) currentWithoutClaimLock() bool {
	c.product.mu.Lock()
	current := c.product.claim == c
	c.product.mu.Unlock()
	return current
}

func (c *RuntimeClaim) finishClose(err error) error {
	c.mu.Lock()
	c.closeErr = err
	close(c.closeDone)
	c.mu.Unlock()
	return err
}

func (c *RuntimeClaim) stickyCloseResult() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}
