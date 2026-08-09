package store_test

import (
	"context"
	"fmt"
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

	cfg := store.RateLimitConfig{Limit: 2, Window: 500 * time.Millisecond}

	for range 2 {
		_, err := s.CheckRateLimit(ctx, alice.ID, "sweep_test", cfg)
		if err != nil {
			t.Fatalf("check: %v", err)
		}
	}

	countBefore, err := store.CountRateLimits(ctx, s, alice.ID)
	if err != nil {
		t.Fatalf("count before: %v", err)
	}
	if countBefore == 0 {
		t.Fatal("expected rate limit row before sweep")
	}

	time.Sleep(600 * time.Millisecond)

	_, err = s.SweepExpiredRateLimits(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}

	countAfter, err := store.CountRateLimits(ctx, s, alice.ID)
	if err != nil {
		t.Fatalf("count after: %v", err)
	}
	if countAfter != 0 {
		t.Fatalf("rate limits after sweep = %d, want 0", countAfter)
	}
}

// TestSweepCannotChangeTheAnswer is the property the deadline has to carry:
// the sweep is garbage collection, so whether it has run must never change what
// the limiter decides.
//
// It bites when a window is enlarged. The row keeps the deadline it opened
// with, the sweep collects it the moment that passes, and a limiter reading the
// live config instead would still be counting against it — so the same call is
// denied before the sweep and allowed after it, and a caller gets a fresh
// budget for free by waiting for the sweeper.
func TestSweepCannotChangeTheAnswer(t *testing.T) {
	t.Parallel()
	opened := store.RateLimitConfig{Limit: 1, Window: 50 * time.Millisecond}
	enlarged := store.RateLimitConfig{Limit: 1, Window: time.Hour}

	for _, swept := range []bool{false, true} {
		t.Run(fmt.Sprintf("swept=%v", swept), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dsn := pgtest.DSN(t)
			s, err := store.Open(ctx, dsn, pgtest.EncKey())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }() //nolint:errcheck // best-effort

			const subject = 1100
			const surface = "sweep_answer"

			if result, err := s.CheckRateLimit(ctx, subject, surface, opened); err != nil || result != nil {
				t.Fatalf("first request: result=%+v err=%v", result, err)
			}
			// The deadline this row opened with has now passed, which is exactly
			// the condition the sweep collects on.
			time.Sleep(100 * time.Millisecond)
			if swept {
				n, err := s.SweepExpiredRateLimits(ctx)
				if err != nil {
					t.Fatalf("sweep: %v", err)
				}
				if n != 1 {
					t.Fatalf("sweep deleted %d rows, want the 1 counter row past its deadline", n)
				}
			}

			result, err := s.CheckRateLimit(ctx, subject, surface, enlarged)
			if err != nil {
				t.Fatalf("post-enlarge: %v", err)
			}
			if result != nil {
				t.Errorf("denied after the recorded window closed: wait %s — the answer depends on whether the sweep has run", result.Wait)
			}
		})
	}
}

// TestSweepVsCheckRace proves that the ErrNoRows branch in CheckRateLimit
// fires when the row is deleted between the INSERT denial and the GET.
//
// It uses the test hook between the two reads to delete the row at the
// exact moment the branch is exercised. Delete the ErrNoRows branch at
// rate_limits.go:71 and this test fails: the GET returning ErrNoRows
// becomes an error instead of an allow.
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

	cfg := store.RateLimitConfig{Limit: 1, Window: 30 * time.Second}
	pool := store.StorePool(s)

	// Seed the row at its limit with an active window.
	_, err = pool.Exec(ctx,
		`INSERT INTO rate_limits (subject_id, surface, token_count, window_start, expires_at)
		 VALUES ($1, $2, 1, now(), now() + '30 seconds')`,
		alice.ID, "sweep_race",
	)
	if err != nil {
		t.Fatalf("seed row: %v", err)
	}

	// Install the hook: fires after INSERT denial, before GET.
	// Deletes the row so the GET returns ErrNoRows.
	store.SetDeniedHook(s, func() {
		_, _ = pool.Exec(ctx, //nolint:errcheck // test hook: best-effort delete
			`DELETE FROM rate_limits WHERE subject_id = $1 AND surface = $2`,
			alice.ID, "sweep_race",
		)
	})
	defer store.SetDeniedHook(s, nil)

	// Fire the check. INSERT fails (at limit), hook fires (deletes row),
	// GET returns ErrNoRows → branch fires → allowed.
	result, err := s.CheckRateLimit(ctx, alice.ID, "sweep_race", cfg)
	if err != nil {
		t.Fatalf("check returned error — ErrNoRows branch not hit: %v", err)
	}
	if result != nil {
		t.Fatalf("check denied — row not deleted in hook: %+v", result)
	}
}
