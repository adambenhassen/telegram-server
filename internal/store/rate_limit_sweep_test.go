package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSweepRemovesExpiredRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort

	alice, err := s.CreateUser(ctx, "+15551294501")
	if err != nil {
		t.Fatal(err)
	}

	// Very short window so rows expire quickly.
	cfg := store.RateLimitConfig{Limit: 2, Window: 500 * time.Millisecond}

	// Consume the limit to create a rate_limits row.
	for range 2 {
		_, err := s.CheckRateLimit(ctx, alice.ID, "sweep_test", cfg)
		if err != nil {
			t.Fatalf("check: %v", err)
		}
	}

	// Row exists before sweep.
	countBefore, err := store.CountRateLimits(ctx, s, alice.ID)
	if err != nil {
		t.Fatalf("count before: %v", err)
	}
	if countBefore == 0 {
		t.Fatal("expected rate limit row before sweep")
	}

	// Wait for expiry.
	time.Sleep(600 * time.Millisecond)

	// Sweep.
	_, err = s.SweepExpiredRateLimits(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// Row should be gone.
	countAfter, err := store.CountRateLimits(ctx, s, alice.ID)
	if err != nil {
		t.Fatalf("count after: %v", err)
	}
	if countAfter != 0 {
		t.Fatalf("rate limits after sweep = %d, want 0", countAfter)
	}
}

func TestSweepVsCheckRace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort

	alice, err := s.CreateUser(ctx, "+15551294601")
	if err != nil {
		t.Fatal(err)
	}

	// Short window.
	cfg := store.RateLimitConfig{Limit: 1, Window: 500 * time.Millisecond}

	// Consume the single token.
	result, err := s.CheckRateLimit(ctx, alice.ID, "sweep_race", cfg)
	if err != nil {
		t.Fatalf("first check: %v", err)
	}
	if result != nil {
		t.Fatalf("first check: unexpected denial: %+v", result)
	}

	// Wait for the window to expire so the sweep can clean it up.
	time.Sleep(600 * time.Millisecond)

	// Manually run the sweep to delete the row.
	_, err = s.SweepExpiredRateLimits(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// Now check again — the row was swept, so this should succeed (not error).
	result, err = s.CheckRateLimit(ctx, alice.ID, "sweep_race", cfg)
	if err != nil {
		t.Fatalf("post-sweep check: %v", err)
	}
	if result != nil {
		t.Fatalf("post-sweep check: unexpected denial: %+v", result)
	}
}
