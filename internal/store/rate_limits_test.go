package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRateLimitBasic(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 100
	const surface = "test"

	// Requests 1-3 should pass.
	for i := range 3 {
		result, err := s.CheckRateLimit(ctx, subject, surface, cfg)
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		if result != nil {
			t.Fatalf("request %d: unexpected denial: %+v", i+1, result)
		}
	}

	// Request 4 should be denied.
	result, err := s.CheckRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("request 4: %v", err)
	}
	if result == nil {
		t.Fatal("request 4: expected denial, got allowed")
	}
	if result.Wait < time.Second {
		t.Errorf("request 4: wait = %v, want >= 1s", result.Wait)
	}
	if result.Wait > cfg.Window {
		t.Errorf("request 4: wait = %v, want <= %v", result.Wait, cfg.Window)
	}

	// Denied requests should not consume tokens — request 5 should also be denied.
	result2, err := s.CheckRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("request 5: %v", err)
	}
	if result2 == nil {
		t.Fatal("request 5: expected denial (denied requests consume nothing), got allowed")
	}
}

func TestRateLimitWindowExpiry(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	// Long window, aged past its deadline below rather than slept through: see
	// store.AgeRateLimitWindow.
	cfg := store.RateLimitConfig{Limit: 2, Window: time.Hour}
	ctx := context.Background()
	const subject = 200
	const surface = "expiry"

	// Exhaust the limit.
	for range 2 {
		result, err := s.CheckRateLimit(ctx, subject, surface, cfg)
		if err != nil || result != nil {
			t.Fatalf("initial request: result=%+v err=%v", result, err)
		}
	}

	// Denied.
	result, err := s.CheckRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("pre-expiry deny: %v", err)
	}
	if result == nil {
		t.Fatal("pre-expiry: expected denial, got allowed")
	}

	// Age the window past its deadline.
	if err := store.AgeRateLimitWindow(ctx, s, subject, surface, cfg.Window+time.Minute); err != nil {
		t.Fatalf("age window: %v", err)
	}

	// Should be allowed again.
	result, err = s.CheckRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("post-expiry: %v", err)
	}
	if result != nil {
		t.Fatalf("post-expiry: expected allowed, got denial: %+v", result)
	}
}

// TestRateLimitWindowRunsToItsRecordedDeadline pins which boundary decides a
// window: the deadline stored on the row when it opened, not window_start plus
// whatever the config says today.
//
// They are only the same while the config never changes. Deriving the boundary
// from the live config lets a shortened window reset a counter whose stored
// deadline is still hours away — a row the sweep would not touch, so the
// limiter and the sweep would be reading one row as two different windows.
func TestRateLimitWindowRunsToItsRecordedDeadline(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	ctx := context.Background()
	const subject = 1000
	const surface = "recorded_deadline"
	opened := store.RateLimitConfig{Limit: 1, Window: time.Hour}
	shortened := store.RateLimitConfig{Limit: 1, Window: 50 * time.Millisecond}

	if result, err := s.CheckRateLimit(ctx, subject, surface, opened); err != nil || result != nil {
		t.Fatalf("first request: result=%+v err=%v", result, err)
	}
	// Past the shortened window, far inside the one this row actually opened.
	time.Sleep(100 * time.Millisecond)

	result, err := s.CheckRateLimit(ctx, subject, surface, shortened)
	if err != nil {
		t.Fatalf("post-shorten: %v", err)
	}
	if result == nil {
		t.Error("the counter reset while its recorded deadline was still an hour out: the shorter window took effect mid-flight and handed back a budget the row had already spent")
	}
}

func TestRateLimitIndependentSubjects(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	ctx := context.Background()
	const surface = "indep"

	// Exhaust subject A.
	for range 2 {
		if _, err := s.CheckRateLimit(ctx, 300, surface, cfg); err != nil {
			t.Fatalf("subject A: %v", err)
		}
	}

	// Subject A should be denied.
	resultA, err := s.CheckRateLimit(ctx, 300, surface, cfg)
	if err != nil || resultA == nil {
		t.Fatalf("subject A at limit: result=%+v err=%v, want denial", resultA, err)
	}

	// Subject B should be unaffected.
	resultB, err := s.CheckRateLimit(ctx, 301, surface, cfg)
	if err != nil {
		t.Fatalf("subject B first request: %v", err)
	}
	if resultB != nil {
		t.Fatalf("subject B first request: unexpected denial: %+v", resultB)
	}

	// Subject C (a third subject) should also be unaffected.
	resultC, err := s.CheckRateLimit(ctx, 302, surface, cfg)
	if err != nil {
		t.Fatalf("subject C first request: %v", err)
	}
	if resultC != nil {
		t.Fatalf("subject C first request: unexpected denial: %+v", resultC)
	}
}

func TestRateLimitIndependentSurfaces(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 400

	// Exhaust surface "alpha".
	if _, err := s.CheckRateLimit(ctx, subject, "alpha", cfg); err != nil {
		t.Fatalf("alpha first: %v", err)
	}
	result, err := s.CheckRateLimit(ctx, subject, "alpha", cfg)
	if err != nil || result == nil {
		t.Fatalf("alpha at limit: result=%+v err=%v, want denial", result, err)
	}

	// Surface "beta" should be independent.
	result, err = s.CheckRateLimit(ctx, subject, "beta", cfg)
	if err != nil {
		t.Fatalf("beta first: %v", err)
	}
	if result != nil {
		t.Fatalf("beta first: unexpected denial: %+v", result)
	}
}

func TestRateLimitDisabled(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	ctx := context.Background()
	const subject = 500
	const surface = "disabled"

	// Zero limit disables enforcement.
	cfgZero := store.RateLimitConfig{Limit: 0, Window: 10 * time.Second}
	for range 10 {
		result, err := s.CheckRateLimit(ctx, subject, surface, cfgZero)
		if err != nil || result != nil {
			t.Fatalf("zero limit: result=%+v err=%v", result, err)
		}
	}
}

func TestRateLimitConcurrentBoundary(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 5, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 600
	const surface = "concurrent"

	// Pre-seed the row with 4 tokens consumed, leaving exactly 1 slot.
	// This avoids the race between the first INSERT and the denied goroutines
	// — all N goroutines contend for the same single slot.
	for range 4 {
		if _, err := s.CheckRateLimit(ctx, subject, surface, cfg); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	const n = 10 // more than the remaining slot

	type result struct {
		err    error
		denied bool
	}
	results := make([]result, n)

	var wg sync.WaitGroup
	wg.Add(n)
	ready := make(chan struct{})

	for i := range n {
		go func() {
			defer wg.Done()
			<-ready
			rl, err := s.CheckRateLimit(ctx, subject, surface, cfg)
			results[i] = result{err: err, denied: rl != nil}
		}()
	}

	close(ready)
	wg.Wait()

	var successes, denials, other int
	for _, r := range results {
		switch {
		case r.err != nil:
			other++
			t.Errorf("unexpected error: %v", r.err)
		case r.denied:
			denials++
		default:
			successes++
		}
	}

	if successes != 1 {
		t.Errorf("successes = %d, want 1 (one slot remaining)", successes)
	}
	if denials != n-1 {
		t.Errorf("denials = %d, want %d", denials, n-1)
	}
	if other != 0 {
		t.Errorf("unexpected errors = %d, want 0", other)
	}
}

// TestRateLimitWaitAccuracy pins what the wait on a denial means to the client:
// it is the time left in the window the row records, and it shrinks as that
// window ages.
//
// Neither leg is read off a wall clock. The window is an hour, so nothing but
// this test closes it; the clock the wait is measured against is pinned to the
// instant the window opened, so the remainder is an exact number of seconds
// rather than however much of a short window the round trip already spent; and
// the window ages by rewinding the row instead of by sleeping through it. A
// host that stalls this test for a second therefore moves none of the numbers
// below, which is what an elapsed-time measurement could not promise.
func TestRateLimitWaitAccuracy(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 1, Window: time.Hour}
	ctx := context.Background()
	const subject = 700
	const surface = "wait"

	// Consume the single token.
	if _, err := s.CheckRateLimit(ctx, subject, surface, cfg); err != nil {
		t.Fatalf("first request: %v", err)
	}

	expiresAt, err := store.RateLimitExpiresAt(ctx, s, subject, surface)
	if err != nil {
		t.Fatalf("read expires_at: %v", err)
	}
	opened := expiresAt.Add(-cfg.Window)
	store.SetNowFunc(s, func() time.Time { return opened })

	// Denied at the instant the window opened: the whole window is left.
	result, err := s.CheckRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("denial: %v", err)
	}
	if result == nil {
		t.Fatal("expected denial, got allowed")
	}
	if result.Wait != cfg.Window {
		t.Errorf("wait at window start = %v, want %v (the whole window)", result.Wait, cfg.Window)
	}

	// Age the row a quarter of the way through the window. The pinned clock has
	// not moved, so the row is now that much further into its own window.
	const aged = 15 * time.Minute
	if err := store.AgeRateLimitWindow(ctx, s, subject, surface, aged); err != nil {
		t.Fatalf("age window: %v", err)
	}

	result2, err := s.CheckRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("post-ageing denial: %v", err)
	}
	if result2 == nil {
		t.Fatal("post-ageing: expected denial, got allowed")
	}
	if want := cfg.Window - aged; result2.Wait != want {
		t.Errorf("wait after ageing the window %v = %v, want %v (the remainder)", aged, result2.Wait, want)
	}
	if result2.Wait >= result.Wait {
		t.Errorf("wait did not shrink as the window aged: %v, then %v", result.Wait, result2.Wait)
	}
}

// TestRateLimitWaitMinimumOneSecond guards the one-second floor in waitUntil,
// and only the floor. Every remainder above zero already reaches a second
// through the round-up, so a case naming one — 100ms, say — passes with the
// floor deleted and proves nothing; TestRateLimitWaitRoundsUp owns that rule.
// The cases here are the two where the round-up yields zero: the clock level
// with the row's deadline, and past it. Both are reachable in production, where
// the wait is a Go clock read against a Postgres timestamp and only Postgres
// decides whether the window is still open — an offset between the two puts the
// app past a deadline the row has not reached.
func TestRateLimitWaitMinimumOneSecond(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	// An hour of window so the row stays open however long the host takes, and
	// the remainder under test is the pinned clock's alone.
	cfg := store.RateLimitConfig{Limit: 1, Window: time.Hour}
	ctx := context.Background()
	const surface = "minwait"

	// Offset of the pinned clock from the row's stored deadline.
	cases := []struct {
		name    string
		subject int64
		offset  time.Duration
	}{
		{"clock level with the deadline", 800, 0},
		{"clock past the deadline", 801, 250 * time.Millisecond},
	}
	for _, tc := range cases {
		// Sequential on purpose: the pinned clock is per-Store state, and these
		// cases share one Store.
		t.Run(tc.name, func(t *testing.T) {
			// Consume the token.
			if _, err := s.CheckRateLimit(ctx, tc.subject, surface, cfg); err != nil {
				t.Fatalf("first request: %v", err)
			}

			expiresAt, err := store.RateLimitExpiresAt(ctx, s, tc.subject, surface)
			if err != nil {
				t.Fatalf("read expires_at: %v", err)
			}
			store.SetNowFunc(s, func() time.Time { return expiresAt.Add(tc.offset) })

			result, err := s.CheckRateLimit(ctx, tc.subject, surface, cfg)
			if err != nil {
				t.Fatalf("denial: %v", err)
			}
			if result == nil {
				t.Fatal("expected denial, got allowed")
			}
			if result.Wait != time.Second {
				t.Errorf("wait = %v, want exactly 1s (the floor)", result.Wait)
			}
		})
	}
}

func TestRateLimitWaitRoundsUp(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 950
	const surface = "roundup"

	// Consume the token.
	if _, err := s.CheckRateLimit(ctx, subject, surface, cfg); err != nil {
		t.Fatalf("first request: %v", err)
	}

	// The remainder has to be a known fraction of a second for a ceil to be
	// assertable at all. Left to real time it is however much of the window the
	// round trip to the denial has already spent, which on a loaded host is
	// seconds, and the test then measures the host rather than the rounding. So
	// the clock the wait is measured against is pinned 9.99s short of the row's
	// own deadline.
	expiresAt, err := store.RateLimitExpiresAt(ctx, s, subject, surface)
	if err != nil {
		t.Fatalf("read expires_at: %v", err)
	}
	store.SetNowFunc(s, func() time.Time { return expiresAt.Add(-9990 * time.Millisecond) })

	// Denial should round up to whole seconds: 9.99s rounds to 10s.
	result, err := s.CheckRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("denial: %v", err)
	}
	if result == nil {
		t.Fatal("expected denial, got allowed")
	}
	// Must be exactly the full window rounded to seconds (ceil from ~9.99s).
	if result.Wait != 10*time.Second {
		t.Errorf("wait = %v, want 10s (ceil to whole seconds)", result.Wait)
	}
}

// TestCheckRateLimitBudgetNoRow proves that a budget check on a surface with
// no row returns nil (allowed) without creating one. This is the invariant that
// prevents a read-only check from silently seeding a counter.
func TestCheckRateLimitBudgetNoRow(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 1100
	const surface = "budget_no_row"

	// Budget check on a clean surface should allow without creating a row.
	result, err := s.CheckRateLimitBudget(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("budget check: %v", err)
	}
	if result != nil {
		t.Fatalf("budget check: unexpected denial: %+v", result)
	}

	// No row should have been created.
	count, err := store.CountRateLimits(ctx, s, subject)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("budget check created a row: count = %d, want 0", count)
	}
}

// TestCheckRateLimitBudgetAndCharge proves the check-then-charge pattern:
// ChargeRateLimit creates/increments the counter, and CheckRateLimitBudget
// reads it without modifying it.
func TestCheckRateLimitBudgetAndCharge(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 1200
	const surface = "budget_and_charge"

	// Budget check on a clean surface: allowed, no row created.
	result, err := s.CheckRateLimitBudget(ctx, subject, surface, cfg)
	if err != nil || result != nil {
		t.Fatalf("initial budget check: result=%+v err=%v", result, err)
	}

	// Charge 3 times (one per failed attempt).
	for range 3 {
		if err := s.ChargeRateLimit(ctx, subject, surface, cfg); err != nil {
			t.Fatalf("charge: %v", err)
		}
	}

	// Budget check should now return a denial.
	result, err = s.CheckRateLimitBudget(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("budget check after charges: %v", err)
	}
	if result == nil {
		t.Fatal("budget check: expected denial after 3 charges, got allowed")
	}
	if result.Wait < time.Second {
		t.Errorf("wait = %v, want >= 1s", result.Wait)
	}

	// Budget check should not have incremented the counter.
	count, err := store.CountRateLimits(ctx, s, subject)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("budget check created a row: count = %d, want 1", count)
	}

	// A further charge should increment past the limit.
	if err := s.ChargeRateLimit(ctx, subject, surface, cfg); err != nil {
		t.Fatalf("charge past limit: %v", err)
	}

	// Budget check should still deny.
	result, err = s.CheckRateLimitBudget(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("budget check past limit: %v", err)
	}
	if result == nil {
		t.Fatal("budget check: expected denial after 4 charges, got allowed")
	}
}

// TestCheckRateLimitBudgetDisabled proves that a zero limit disables enforcement
// for the budget check.
func TestCheckRateLimitBudgetDisabled(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{} // zero = disabled
	ctx := context.Background()
	const subject = 1300
	const surface = "budget_disabled"

	// Budget check should always allow.
	for range 10 {
		result, err := s.CheckRateLimitBudget(ctx, subject, surface, cfg)
		if err != nil || result != nil {
			t.Fatalf("budget check: result=%+v err=%v", result, err)
		}
	}
}

// TestChargeRateLimitDisabled proves that a zero limit disables charging.
func TestChargeRateLimitDisabled(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{} // zero = disabled
	ctx := context.Background()
	const subject = 1400
	const surface = "charge_disabled"

	// Charges should be no-ops.
	for range 10 {
		if err := s.ChargeRateLimit(ctx, subject, surface, cfg); err != nil {
			t.Fatalf("charge: %v", err)
		}
	}

	// No row should have been created.
	count, err := store.CountRateLimits(ctx, s, subject)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("disabled charge created a row: count = %d, want 0", count)
	}
}

func TestRateLimitDenialNotError(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 900
	const surface = "sentinel"

	// Consume the token.
	if _, err := s.CheckRateLimit(ctx, subject, surface, cfg); err != nil {
		t.Fatalf("first request: %v", err)
	}

	// The denial returns a non-nil RateLimitResult, not an error.
	result, err := s.CheckRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("denial returned an error: %v", err)
	}
	if result == nil {
		t.Fatal("expected denial, got allowed")
	}
	// Wait should be positive and rounded to whole seconds.
	if result.Wait <= 0 {
		t.Errorf("wait = %v, want > 0", result.Wait)
	}
	if result.Wait%time.Second != 0 {
		t.Errorf("wait = %v, want whole seconds", result.Wait)
	}
}

// TestReserveAndRefund proves the reserve-then-refund pattern: a token is
// consumed before verification and refunded on success.
func TestReserveAndRefund(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 1500
	const surface = "reserve_refund"

	// Reserve 3 tokens (one per attempt).
	var reservations [3]*store.RateLimitReservation
	for i := range 3 {
		res, rl, err := s.ReserveRateLimit(ctx, subject, surface, cfg)
		if err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
		if rl != nil {
			t.Fatalf("reserve %d: unexpected denial: %+v", i, rl)
		}
		reservations[i] = res
	}

	// 4th reserve should be denied.
	_, rl, err := s.ReserveRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("reserve 4: %v", err)
	}
	if rl == nil {
		t.Fatal("reserve 4: expected denial, got allowed")
	}

	// Refund one token — should allow another reserve.
	if err := s.RefundRateLimit(ctx, subject, surface, reservations[0]); err != nil {
		t.Fatalf("refund: %v", err)
	}

	// Now one token should be available.
	res, rl, err := s.ReserveRateLimit(ctx, subject, surface, cfg)
	if err != nil {
		t.Fatalf("reserve after refund: %v", err)
	}
	if rl != nil {
		t.Fatalf("reserve after refund: unexpected denial: %+v", rl)
	}
	if res == nil {
		t.Fatal("reserve after refund: expected reservation")
	}

	// Cross-window case: reserve in window 1 at limit 1, age the row past
	// expiry, reserve once to open window 2, then refund the window-1
	// reservation. The refund must be a no-op (window_start = $3 rejects
	// the stale window).
	const subjectXW = 1501
	const surfaceXW = "cross_window"
	cfgXW := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Reserve in window 1 (consumes the only token).
	w1Res, _, err := s.ReserveRateLimit(ctx, subjectXW, surfaceXW, cfgXW)
	if err != nil {
		t.Fatalf("window 1 reserve: %v", err)
	}
	if w1Res == nil {
		t.Fatal("window 1 reserve: expected reservation")
	}

	// Age the row past expiry so the next reserve opens a new window.
	if err := ageRateLimitRow(dsn, subjectXW, surfaceXW, 11*time.Second); err != nil {
		t.Fatalf("age window: %v", err)
	}

	// Reserve once to open window 2.
	w2Res, _, err := s.ReserveRateLimit(ctx, subjectXW, surfaceXW, cfgXW)
	if err != nil {
		t.Fatalf("window 2 reserve: %v", err)
	}
	if w2Res == nil {
		t.Fatal("window 2 reserve: expected reservation")
	}

	// Refund the window-1 reservation — must be a no-op.
	if err := s.RefundRateLimit(ctx, subjectXW, surfaceXW, w1Res); err != nil {
		t.Fatalf("cross-window refund: %v", err)
	}

	// Window 2 budget should be unchanged: limit 1, already consumed 1.
	// Another reserve should be denied.
	_, rl, err = s.ReserveRateLimit(ctx, subjectXW, surfaceXW, cfgXW)
	if err != nil {
		t.Fatalf("window 2 second reserve: %v", err)
	}
	if rl == nil {
		t.Fatal("window 2 second reserve: expected denial (cross-window refund leaked a token)")
	}
}

// TestReserveConcurrentBurst proves that concurrent reserves are serialized
// by the row-level lock: exactly Limit tokens are consumed, the rest are denied.
func TestReserveConcurrentBurst(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	limit := 5
	cfg := store.RateLimitConfig{Limit: limit, Window: 10 * time.Second}
	ctx := context.Background()
	const subject = 1600
	const surface = "concurrent_burst"

	const goroutines = 20
	var (
		allowed atomic.Int32
		denied  atomic.Int32
		errOnce atomic.Bool
		errCh   = make(chan error, 1)
	)

	// Fire N goroutines that all try to reserve simultaneously.
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			_, rl, err := s.ReserveRateLimit(ctx, subject, surface, cfg)
			if err != nil {
				if errOnce.CompareAndSwap(false, true) {
					errCh <- err
				}
				return
			}
			if rl != nil {
				denied.Add(1)
			} else {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	allowedCount := allowed.Load()
	deniedCount := denied.Load()
	if allowedCount != int32(limit) {
		t.Errorf("allowed = %d, want %d (limit)", allowedCount, limit)
	}
	if deniedCount != int32(goroutines-limit) {
		t.Errorf("denied = %d, want %d (goroutines - limit)", deniedCount, goroutines-limit)
	}
}

// ageRateLimitRow rewinds a rate-limit row's timestamps by d, simulating
// d of wall clock passing since the window opened.
func ageRateLimitRow(dsn string, subjectID int64, surface string, d time.Duration) error {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(context.Background(),
		`UPDATE rate_limits
		    SET window_start = window_start - $3::INTERVAL,
		        expires_at   = expires_at   - $3::INTERVAL
		  WHERE subject_id = $1 AND surface = $2`,
		subjectID, surface, pgtype.Interval{Microseconds: d.Microseconds(), Valid: true})
	return err
}
