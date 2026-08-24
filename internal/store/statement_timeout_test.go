package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestStatementTimeoutCancelsStatementKeepsSession proves the pool-side half
// of the per-RPC bound: a single statement past the configured ceiling is
// cancelled by Postgres itself — query_canceled, not a context error — and the
// session it ran on survives, ready for the next statement.
func TestStatementTimeoutCancelsStatementKeepsSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// One second rather than something tighter: Open's schema check runs on
	// the same pool, and under a fully parallel suite a host can stall longer
	// than a sub-second ceiling forgives.
	const ceiling = time.Second
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(),
		store.WithBlobStore(testBlobs(t)),
		store.WithStatementTimeout(ceiling))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	})
	pool := store.StorePool(s)

	start := time.Now()
	_, sleepErr := pool.Exec(ctx, `SELECT pg_sleep(600)`)
	elapsed := time.Since(start)
	if sleepErr == nil {
		t.Fatal("statement past the ceiling succeeded, want cancellation")
	}
	if elapsed > ceiling*10 {
		t.Fatalf("cancellation took %s, want roughly the %s ceiling, not the sleep", elapsed, ceiling)
	}
	var pgErr *pgconn.PgError
	if !errors.As(sleepErr, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("err = %v, want query_canceled (57014)", sleepErr)
	}

	// The connection is not poisoned by the cancelled statement: the same
	// pooled session answers again immediately.
	if _, err := pool.Exec(ctx, `SELECT 1`); err != nil {
		t.Fatalf("statement after cancellation: %v", err)
	}

	// A context that carries no deadline still gets the server-side ceiling:
	// this is what bounds background work that never flows through a request.
	if _, err := pool.Exec(context.Background(), `SELECT pg_sleep(0)`); err != nil {
		t.Fatalf("short statement under the ceiling: %v", err)
	}
}
