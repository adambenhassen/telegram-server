package e2e_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	osexec "os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

const clientTeardownProbeEnv = "TELEGRAM_TEST_CLIENT_TEARDOWN_PROBE"

func TestLifecycleTeardownFailureProbe(t *testing.T) {
	if os.Getenv(clientTeardownProbeEnv) != "1" {
		t.Skip("subprocess probe only")
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	failures := newClientFailureSignal(cancel)
	lifecycle := startClientLifecycle(ctx, "PROBE", failures, func(phase *clientPhaseState) error {
		phase.set("idle")
		return errProbeClientExit
	})
	<-failures.done
	<-lifecycle.result.done

	client := &smokeClient{
		lifecycle: lifecycle,
		cancel:    func() { cancel(context.Canceled) },
		manager:   updates.New(updates.Config{Handler: newUpdateCollector()}),
		cmds:      make(chan command),
		label:     "PROBE",
	}
	client.stopClient(t)
}

func TestEchoLifecycleDiagnostics(t *testing.T) {
	t.Run("unexpected idle exit fails shared client teardown", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := osexec.CommandContext(ctx, os.Args[0], "-test.run=^TestLifecycleTeardownFailureProbe$") // #nosec G204,G702 -- fixed test selector on this test binary verifies failure status.
		cmd.Env = append(os.Environ(), clientTeardownProbeEnv+"=1")
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("idle client failure was retained but teardown passed: %s", output)
		}
		if !strings.Contains(string(output), "PROBE idle") || !strings.Contains(string(output), "cause=injected probe") {
			t.Fatalf("teardown failed without the synthetic idle diagnostic: %s", output)
		}
	})

	t.Run("probe client exit before readiness is prompt and labelled", func(t *testing.T) {
		failures := newClientFailureSignal(nil)
		started := time.Now()
		var normalPath atomic.Bool
		client := startClientLifecycle(context.Background(), "A1", failures, func(phase *clientPhaseState) error {
			return runClientWithOptions(phase, clientRunOptions{probeClientExit: func() error { return errProbeClientExit }}, func() error {
				normalPath.Store(true)
				return nil
			})
		})

		select {
		case <-failures.done:
		case <-time.After(time.Second):
			t.Fatal("probe exit did not reach the test before the shared deadline")
		}
		if elapsed := time.Since(started); elapsed >= time.Second {
			t.Fatalf("probe exit took %s, want less than one second", elapsed)
		}
		if normalPath.Load() {
			t.Fatal("probe hook unexpectedly entered the normal login path")
		}
		if got := failures.message(); !strings.Contains(got, "A1 login") || !strings.Contains(got, "cause=injected probe") || !strings.Contains(got, "no valid pre-cancellation registry snapshot") {
			t.Fatalf("diagnostic = %q, want labelled login phase and injected probe cause", got)
		}
		if err := client.result.error(); !errors.Is(err, errProbeClientExit) {
			t.Fatalf("terminal result lost probe cause: %v", safeErrorClass(err))
		}
	})

	t.Run("manager exit while idle is prompt and repeatable", func(t *testing.T) {
		failures := newClientFailureSignal(nil)
		managerResult := newTerminalResult()
		managerErr := errors.New("unreviewed error payload")
		commands := make(chan command)
		client := startClientLifecycle(context.Background(), "B1", failures, func(phase *clientPhaseState) error {
			phase.set("idle")
			return runManagedCommands(context.Background(), commands, managerResult, phase, func(context.Context, command) error { return nil }, func() error { return nil })
		})

		managerResult.complete(managerErr)
		select {
		case <-failures.done:
		case <-time.After(time.Second):
			t.Fatal("idle update-manager exit was not observed promptly")
		}
		if got := failures.message(); !strings.Contains(got, "B1 update manager") || !strings.Contains(got, "cause=other(") {
			t.Fatalf("diagnostic = %q, want labelled update-manager phase and safe cause class", got)
		}
		if strings.Contains(failures.message(), "unreviewed error payload") {
			t.Fatal("raw manager error entered diagnostic output")
		}
		if got := client.result.error(); !errors.Is(got, managerErr) {
			t.Fatal("client terminal result did not retain the manager exit")
		}
		if got := managerResult.error(); !errors.Is(got, managerErr) {
			t.Fatal("manager result was consumed by the idle watcher")
		}
	})

	t.Run("manager exit interrupts an active command", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		failures := newClientFailureSignal(cancel)
		managerResult := newTerminalResult()
		managerErr := errors.New("unreviewed manager error payload")
		commandStarted := make(chan struct{})
		commandDone := make(chan error, 1)
		commands := make(chan command, 1)
		commands <- command{
			fn: func(ctx context.Context, _ *tg.Client) error {
				close(commandStarted)
				<-ctx.Done()
				return ctx.Err()
			},
			done: commandDone,
		}
		client := startClientLifecycle(ctx, "D1", failures, func(phase *clientPhaseState) error {
			phase.set("command")
			return runManagedCommands(ctx, commands, managerResult, phase, func(commandCtx context.Context, c command) error {
				return c.fn(commandCtx, nil)
			}, func() error { return managerResult.error() })
		})
		select {
		case <-commandStarted:
		case <-time.After(time.Second):
			t.Fatal("command did not start")
		}

		pendingCommand := make(chan error, 1)
		go func() {
			pendingCommand <- waitForClientCommand(ctx, client, commandDone, time.Now())
		}()

		started := time.Now()
		managerResult.complete(managerErr)
		var commandErr error
		select {
		case commandErr = <-pendingCommand:
		case <-time.After(time.Second):
			cancel(errors.New("test cleanup"))
			select {
			case <-client.result.done:
			case <-time.After(time.Second):
				t.Fatal("client shutdown did not finish after cleanup cancellation")
			}
			t.Fatal("manager exit did not interrupt the pending command promptly")
		}
		if elapsed := time.Since(started); elapsed >= time.Second {
			t.Fatalf("manager exit took %s to fail the pending command", elapsed)
		}
		if commandErr == nil || !strings.Contains(commandErr.Error(), "D1 command") || !strings.Contains(commandErr.Error(), "cause=other(") {
			t.Fatalf("pending command error = %v, want synthetic label, command phase and safe manager cause", commandErr)
		}
		if strings.Contains(commandErr.Error(), managerErr.Error()) {
			t.Fatal("raw manager error entered the pending command error")
		}
		if err := client.result.error(); !errors.Is(err, managerErr) {
			t.Fatalf("client terminal result = %v, want retained manager exit", err)
		}
		if got := failures.message(); !strings.Contains(got, "D1 update manager") || !strings.Contains(got, "cause=other(") {
			t.Fatalf("client failure = %q, want synthetic label, manager phase and safe cause", got)
		}
		if strings.Contains(failures.message(), managerErr.Error()) {
			t.Fatal("raw manager error entered the client failure diagnostic")
		}
		if got := managerResult.error(); !errors.Is(got, managerErr) {
			t.Fatalf("manager terminal result = %v, want retained manager exit", got)
		}
	})

	t.Run("intentional shutdown is normal cleanup", func(t *testing.T) {
		clientCtx, cancelClient := context.WithCancel(t.Context())
		defer cancelClient()
		failures := newClientFailureSignal(nil)
		managerResult := newTerminalResult()
		managerCtx, cancelManager := context.WithCancel(context.Background())
		defer cancelManager()
		var managerStopping atomic.Bool
		go func() {
			<-managerCtx.Done()
			if managerStopping.Load() {
				managerResult.complete(context.Canceled)
				return
			}
			managerResult.complete(errUnexpectedNilExit)
		}()
		commands := make(chan command)
		client := startClientLifecycle(clientCtx, "C", failures, func(phase *clientPhaseState) error {
			phase.set("idle")
			return runManagedCommands(clientCtx, commands, managerResult, phase, func(context.Context, command) error { return nil }, func() error {
				managerStopping.Store(true)
				cancelManager()
				return nil
			})
		})

		run := &smokeClient{lifecycle: client, cancel: cancelClient, manager: updates.New(updates.Config{Handler: newUpdateCollector()}), cmds: commands, label: "C"}
		run.stopClient(t)
		if err := client.result.error(); err != nil {
			t.Fatalf("intentional shutdown returned an error: %s", safeErrorClass(err))
		}
		select {
		case <-failures.done:
			t.Fatalf("intentional shutdown was reported as failure: %s", failures.message())
		default:
		}
	})

	t.Run("causal errors use the fixed allowlist", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			err  error
			want string
		}{
			{name: "nil", want: "unexpected nil"},
			{name: "unexpected nil exit", err: errUnexpectedNilExit, want: "unexpected nil"},
			{name: "cancelled", err: context.Canceled, want: "canceled"},
			{name: "deadline", err: context.DeadlineExceeded, want: "deadline"},
			{name: "injected", err: errProbeClientExit, want: "injected probe"},
			{name: "EOF", err: io.EOF, want: "net closed/EOF"},
			{name: "closed", err: net.ErrClosed, want: "net closed/EOF"},
			{name: "RPC", err: &tgerr.Error{Code: 420, Type: "FLOOD_WAIT"}, want: "rpc(code=420,type=FLOOD_WAIT)"},
			{name: "unsafe RPC type", err: &tgerr.Error{Code: 500, Type: "PRIVATE_VALUE" + "!"}, want: "other(*tgerr.Error)"},
			{name: "other", err: errors.New("unreviewed error payload"), want: "other(*errors.errorString)"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if got := safeErrorClass(tc.err); got != tc.want {
					t.Fatalf("safeErrorClass = %q, want %q", got, tc.want)
				}
			})
		}
	})

	t.Run("registry snapshots are discarded after cancellation", func(t *testing.T) {
		registry := mtproto.NewSessionRegistry()
		ctx := withRegistrySnapshotState(context.Background())
		snapshot, ok := snapshotRegistry(ctx, registry, 7)
		if !ok || snapshot.connections != 0 || snapshot.zeroKeys != 0 || snapshot.distinctKeys != 0 {
			t.Fatalf("empty registry snapshot = %+v, valid=%v", snapshot, ok)
		}
		if got := registrySnapshotDescription(snapshot, true); !strings.Contains(got, "connections=0 zero-key=0 distinct-key=0 observation-age=") {
			t.Fatalf("snapshot description = %q, want all three counts and an age", got)
		}
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		if _, ok := snapshotRegistry(cancelledCtx, registry, 7); ok {
			t.Fatal("kept a registry snapshot after context cancellation")
		}
		if got := contextFailureDescription(cancelledCtx); !strings.Contains(got, "connections=0 zero-key=0 distinct-key=0 observation-age=") {
			t.Fatalf("deadline description omitted its last live snapshot: %q", got)
		}
		if got := registrySnapshotDescription(registrySnapshot{}, false); got != "no valid pre-cancellation registry snapshot" {
			t.Fatalf("missing snapshot description = %q", got)
		}
		noSnapshotCtx, cancelNoSnapshot := context.WithCancel(withRegistrySnapshotState(context.Background()))
		cancelNoSnapshot()
		if got := contextFailureDescription(noSnapshotCtx); !strings.Contains(got, "no valid pre-cancellation registry snapshot") {
			t.Fatalf("missing-snapshot timeout description = %q", got)
		}
	})
}
