package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

var (
	errProbeClientExit   = errors.New("probe-client-exit")
	errUnexpectedNilExit = errors.New("unexpected nil termination")
	rpcTypeAllowlist     = regexp.MustCompile(`^[A-Z0-9_]{1,64}$`)
)

type terminalResult struct {
	once sync.Once
	done chan struct{}
	err  error
}

func newTerminalResult() *terminalResult {
	return &terminalResult{done: make(chan struct{})}
}

func (r *terminalResult) complete(err error) {
	r.once.Do(func() {
		r.err = err
		close(r.done)
	})
}

func (r *terminalResult) error() error {
	<-r.done
	return r.err
}

func (r *terminalResult) finished() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

type clientFailureSignal struct {
	once        sync.Once
	done        chan struct{}
	diagnostic  string
	cancelCause context.CancelCauseFunc
}

func newClientFailureSignal(cancelCause context.CancelCauseFunc) *clientFailureSignal {
	return &clientFailureSignal{done: make(chan struct{}), cancelCause: cancelCause}
}

func (f *clientFailureSignal) report(diagnostic string) {
	f.once.Do(func() {
		f.diagnostic = diagnostic
		close(f.done)
		if f.cancelCause != nil {
			f.cancelCause(safeDiagnosticError{message: diagnostic})
		}
	})
}

func (f *clientFailureSignal) message() string {
	<-f.done
	return f.diagnostic
}

type safeDiagnosticError struct {
	message string
}

func (e safeDiagnosticError) Error() string { return e.message }

type registrySnapshotContextKey struct{}

type registrySnapshotState struct {
	mu       sync.Mutex
	snapshot registrySnapshot
	valid    bool
}

func withRegistrySnapshotState(ctx context.Context) context.Context {
	return context.WithValue(ctx, registrySnapshotContextKey{}, &registrySnapshotState{})
}

func (s *registrySnapshotState) remember(snapshot registrySnapshot) {
	s.mu.Lock()
	s.snapshot = snapshot
	s.valid = true
	s.mu.Unlock()
}

func (s *registrySnapshotState) latest() (registrySnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot, s.valid
}

type clientPhaseState struct {
	mu      sync.Mutex
	name    string
	started time.Time
}

func (p *clientPhaseState) set(name string) {
	p.mu.Lock()
	p.name = name
	p.started = time.Now()
	p.mu.Unlock()
}

func (p *clientPhaseState) snapshot() (string, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.name, p.started
}

type clientLifecycle struct {
	ctx             context.Context
	label           string
	result          *terminalResult
	phase           clientPhaseState
	intentionalStop atomic.Bool
	snapshotMu      sync.Mutex
	lastSnapshot    registrySnapshot
	hasSnapshot     bool
}

func (l *clientLifecycle) rememberRegistrySnapshot(snapshot registrySnapshot) {
	l.snapshotMu.Lock()
	l.lastSnapshot = snapshot
	l.hasSnapshot = true
	l.snapshotMu.Unlock()
}

func (l *clientLifecycle) registrySnapshot() (registrySnapshot, bool) {
	l.snapshotMu.Lock()
	defer l.snapshotMu.Unlock()
	return l.lastSnapshot, l.hasSnapshot
}

func (l *clientLifecycle) diagnostic(phase string, elapsed time.Duration, cause string) string {
	snapshot, valid := l.registrySnapshot()
	return fmt.Sprintf("%s %s failed after %s (%s; %s)", l.label, phase, elapsed.Round(time.Millisecond), registrySnapshotDescription(snapshot, valid), cause)
}

func waitForClientCommand(ctx context.Context, lifecycle *clientLifecycle, done <-chan error, started time.Time) error {
	commandFailure := func() error {
		return errors.New(lifecycle.diagnostic("command", time.Since(started), "cause="+safeErrorClass(lifecycle.result.error())))
	}
	select {
	case err := <-done:
		if lifecycle.result.finished() {
			return commandFailure()
		}
		return err
	case <-ctx.Done():
		if lifecycle.result.finished() {
			return commandFailure()
		}
		return errors.New(lifecycle.diagnostic("command", time.Since(started), contextFailureDescription(ctx)))
	case <-lifecycle.result.done:
		return commandFailure()
	}
}

type clientRunOptions struct {
	probeClientExit func() error
}

func runClientWithOptions(phase *clientPhaseState, options clientRunOptions, run func() error) error {
	if options.probeClientExit != nil {
		phase.set("login")
		return options.probeClientExit()
	}
	return run()
}

func startClientLifecycle(ctx context.Context, label string, failures *clientFailureSignal, run func(*clientPhaseState) error) *clientLifecycle {
	lifecycle := &clientLifecycle{ctx: ctx, label: label, result: newTerminalResult()}
	lifecycle.phase.set("login")
	go func() {
		err := run(&lifecycle.phase)
		if err == nil && !lifecycle.intentionalStop.Load() {
			if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
				err = context.DeadlineExceeded
			} else {
				err = errUnexpectedNilExit
			}
		}
		lifecycle.result.complete(err)
		if !isIntentionalClientCleanup(lifecycle.intentionalStop.Load(), err) && failures != nil {
			phase, started := lifecycle.phase.snapshot()
			if phase == "" {
				phase, started = "client run", time.Now()
			}
			failures.report(lifecycle.diagnostic(phase, time.Since(started), "cause="+safeErrorClass(err)))
		}
	}()
	return lifecycle
}

func isIntentionalClientCleanup(intentional bool, err error) bool {
	return intentional && (err == nil || errors.Is(err, context.Canceled))
}

func safeErrorClass(err error) string {
	if err == nil {
		return "unexpected nil"
	}
	if errors.Is(err, errProbeClientExit) {
		return "injected probe"
	}
	if errors.Is(err, errUnexpectedNilExit) {
		return "unexpected nil"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return "net closed/EOF"
	}
	var rpcErr *tgerr.Error
	if errors.As(err, &rpcErr) && rpcTypeAllowlist.MatchString(rpcErr.Type) {
		return fmt.Sprintf("rpc(code=%d,type=%s)", rpcErr.Code, rpcErr.Type)
	}
	return fmt.Sprintf("other(%T)", err)
}

func contextFailureDescription(ctx context.Context) string {
	cause := context.Cause(ctx)
	if cause == nil {
		cause = ctx.Err()
	}
	if cause == nil {
		return "cause=unknown"
	}
	diagnostic, ok := errors.AsType[safeDiagnosticError](cause)
	if ok {
		return diagnostic.message
	}
	snapshot, valid := contextRegistrySnapshot(ctx)
	return fmt.Sprintf("cause=%s (%s)", safeErrorClass(cause), registrySnapshotDescription(snapshot, valid))
}

func contextRegistrySnapshot(ctx context.Context) (registrySnapshot, bool) {
	state, ok := ctx.Value(registrySnapshotContextKey{}).(*registrySnapshotState)
	if !ok {
		return registrySnapshot{}, false
	}
	return state.latest()
}

func runManagedCommands(ctx context.Context, cmds <-chan command, managerResult *terminalResult, phase *clientPhaseState, execute func(context.Context, command) error, stopManager func() error) error {
	for {
		select {
		case <-managerResult.done:
			phase.set("update manager")
			if err := managerResult.error(); err != nil {
				return err
			}
			return errUnexpectedNilExit
		default:
		}

		select {
		case <-ctx.Done():
			return stopManager()
		case <-managerResult.done:
			phase.set("update manager")
			if err := managerResult.error(); err != nil {
				return err
			}
			return errUnexpectedNilExit
		case cmd, ok := <-cmds:
			if !ok {
				return stopManager()
			}
			phase.set("command")
			commandCtx, cancelCommand := context.WithCancel(ctx)
			commandResult := make(chan error, 1)
			go func() { commandResult <- execute(commandCtx, cmd) }()
			select {
			case <-managerResult.done:
				phase.set("update manager")
				cancelCommand()
				<-commandResult
				if err := managerResult.error(); err != nil {
					return err
				}
				return errUnexpectedNilExit
			case <-ctx.Done():
				cancelCommand()
				<-commandResult
				return stopManager()
			case err := <-commandResult:
				cancelCommand()
				select {
				case <-managerResult.done:
					phase.set("update manager")
					if managerErr := managerResult.error(); managerErr != nil {
						return managerErr
					}
					return errUnexpectedNilExit
				default:
				}
				cmd.done <- err
				phase.set("idle")
			}
		}
	}
}

type registrySnapshot struct {
	connections  int
	zeroKeys     int
	distinctKeys int
	observedAt   time.Time
}

func snapshotRegistry(ctx context.Context, registry *mtproto.SessionRegistry, userID int64) (registrySnapshot, bool) {
	if ctx.Err() != nil {
		return registrySnapshot{}, false
	}
	conns := registry.Conns(userID)
	keys := make(map[int64]struct{}, len(conns))
	zeroKeys := 0
	for _, conn := range conns {
		keyID := conn.AuthKeyID()
		if keyID == 0 {
			zeroKeys++
			continue
		}
		keys[keyID] = struct{}{}
	}
	if ctx.Err() != nil {
		return registrySnapshot{}, false
	}
	snapshot := registrySnapshot{
		connections:  len(conns),
		zeroKeys:     zeroKeys,
		distinctKeys: len(keys),
		observedAt:   time.Now(),
	}
	if state, ok := ctx.Value(registrySnapshotContextKey{}).(*registrySnapshotState); ok {
		state.remember(snapshot)
	}
	return snapshot, true
}

func registrySnapshotDescription(snapshot registrySnapshot, valid bool) string {
	if !valid {
		return "no valid pre-cancellation registry snapshot"
	}
	return fmt.Sprintf("connections=%d zero-key=%d distinct-key=%d observation-age=%s", snapshot.connections, snapshot.zeroKeys, snapshot.distinctKeys, time.Since(snapshot.observedAt).Round(time.Millisecond))
}
