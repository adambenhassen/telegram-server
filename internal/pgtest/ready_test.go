package pgtest_test

import (
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

// TestContainerOptionsHaveNoLogWaitStrategy pins the reason readiness moved off
// the container log: the shared container is reused for days, and Docker's
// json-file log rotation eventually discards the Postgres startup lines a log
// wait strategy greps for. Reinstating one makes every reused container fail
// setup once its log rotates, while Postgres itself is perfectly healthy.
func TestContainerOptionsHaveNoLogWaitStrategy(t *testing.T) {
	t.Parallel()

	req := &testcontainers.GenericContainerRequest{}
	for _, opt := range pgtest.ContainerOptions() {
		if err := opt.Customize(req); err != nil {
			t.Fatalf("customize request: %v", err)
		}
	}
	if hasLogStrategy(req.WaitingFor) {
		t.Error("container readiness depends on log content; a rotated log fails an otherwise healthy container")
	}
}

// hasLogStrategy reports whether s is, or wraps, a log-matching wait strategy.
func hasLogStrategy(s wait.Strategy) bool {
	switch v := s.(type) {
	case *wait.LogStrategy:
		return true
	case *wait.MultiStrategy:
		return slices.ContainsFunc(v.Strategies, hasLogStrategy)
	}
	return false
}

// TestWaitAcceptingRejectsClosedEndpoint proves a Postgres that is not accepting
// connections still fails setup, with an error naming the endpoint, inside the
// timeout rather than hanging.
func TestWaitAcceptingRejectsClosedEndpoint(t *testing.T) {
	t.Parallel()

	// Bind then immediately release a port, so the address is well-formed but
	// nothing answers on it.
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	const timeout = 500 * time.Millisecond
	start := time.Now()
	err = pgtest.WaitAccepting(t.Context(), "postgres://postgres:postgres@"+addr+"/postgres?sslmode=disable", timeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("want error for an endpoint with no Postgres behind it, got nil")
	}
	if elapsed > 10*timeout {
		t.Errorf("wait is not bounded: took %s for a %s timeout", elapsed, timeout)
	}
	if !strings.Contains(err.Error(), "not accepting connections") || !strings.Contains(err.Error(), addr) {
		t.Errorf("error should name the endpoint and the condition, got: %v", err)
	}
}

// TestWaitAcceptingPassesForSharedContainer proves the probe accepts the real
// shared container, whatever its uptime or log state.
func TestWaitAcceptingPassesForSharedContainer(t *testing.T) {
	t.Parallel()

	if err := pgtest.Prewarm(); err != nil {
		t.Fatalf("prewarm: %v", err)
	}
	if err := pgtest.WaitAccepting(t.Context(), pgtest.AdminDSN(), 15*time.Second); err != nil {
		t.Errorf("shared container should be accepting connections: %v", err)
	}
}
