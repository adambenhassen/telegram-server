package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestUsernameLookupBurstCap exercises the per-minute burst cap (20 distinct
// handles) via the store-level API.
func TestUsernameLookupBurstCap(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	user, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Burst cap is 20 per minute. The 21st distinct lookup should be rejected.
	for i := range store.UsernameLookupBurstLimit {
		handle := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		if err := s.CheckAndChargeUsernameLookup(context.Background(), user.ID, handle); err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
	}

	err = s.CheckAndChargeUsernameLookup(context.Background(), user.ID, "zz")
	if !errors.Is(err, store.ErrUsernameLookupQuotaExceeded) {
		t.Fatalf("21st burst lookup: got %v, want ErrUsernameLookupQuotaExceeded", err)
	}
}

// TestUsernameLookupQuota exercises the 24-hour rolling window (100 distinct
// handles). It seeds the lookup table with timestamps spread across 24 hours
// so the burst cap does not interfere.
func TestUsernameLookupQuota(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	user, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Seed 100 distinct handles with timestamps spread across the 24-hour
	// window (one per minute), so the burst cap does not trigger.
	ctx := context.Background()
	now := time.Now()
	for i := range store.UsernameLookupLimit {
		handle := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i/676))
		ts := now.Add(-time.Duration(i) * time.Minute)
		if _, err := store.StorePool(s).Exec(ctx,
			`INSERT INTO username_lookups (caller_id, handle, looked_up_at) VALUES ($1, $2, $3)`,
			user.ID, handle, ts,
		); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// Next distinct lookup should exceed the 24-hour quota.
	err = s.CheckAndChargeUsernameLookup(ctx, user.ID, "zzz")
	if !errors.Is(err, store.ErrUsernameLookupQuotaExceeded) {
		t.Fatalf("101st lookup: got %v, want ErrUsernameLookupQuotaExceeded", err)
	}
}

// TestUsernameLookupRetrySame verifies that retrying the same handle does not
// increment the distinct count. It seeds the table with 100 distinct handles
// spread across 24 hours, then retries one of them.
func TestUsernameLookupRetrySame(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	user, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Seed 100 distinct handles spread across 24 hours (one per minute).
	ctx := context.Background()
	now := time.Now()
	for i := range store.UsernameLookupLimit {
		handle := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i/676))
		ts := now.Add(-time.Duration(i) * time.Minute)
		if _, err := store.StorePool(s).Exec(ctx,
			`INSERT INTO username_lookups (caller_id, handle, looked_up_at) VALUES ($1, $2, $3)`,
			user.ID, handle, ts,
		); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// Retry the first handle — distinct count stays at 100, so it should pass.
	retryHandle := string(rune('a')) + string(rune('a')) + string(rune('0'))
	if err := s.CheckAndChargeUsernameLookup(ctx, user.ID, retryHandle); err != nil {
		t.Fatalf("retry of same handle: %v", err)
	}
}

// TestUsernameLookupConcurrentBoundary verifies that the advisory lock in
// CheckAndChargeUsernameLookup serialises concurrent callers so exactly
// UsernameLookupBurstLimit succeed and the rest return
// ErrUsernameLookupQuotaExceeded.
//
// Without pg_advisory_xact_lock the prune/insert/count sequence races:
// multiple goroutines see the same pre-increment count and commit past the
// limit. Removing the lock causes this test to report more than
// UsernameLookupBurstLimit successes.
func TestUsernameLookupConcurrentBoundary(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	user, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	const n = store.UsernameLookupBurstLimit + 10

	type result struct{ err error }
	results := make([]result, n)

	var wg sync.WaitGroup
	wg.Add(n)
	ready := make(chan struct{})

	for i := range n {
		go func(i int) {
			defer wg.Done()
			<-ready
			handle := fmt.Sprintf("%02d", i)
			results[i].err = s.CheckAndChargeUsernameLookup(context.Background(), user.ID, handle)
		}(i)
	}

	close(ready)
	wg.Wait()

	var successes, quotaErrors, other int
	for _, r := range results {
		switch {
		case r.err == nil:
			successes++
		case errors.Is(r.err, store.ErrUsernameLookupQuotaExceeded):
			quotaErrors++
		default:
			other++
			t.Errorf("unexpected error: %v", r.err)
		}
	}

	if successes != store.UsernameLookupBurstLimit {
		t.Errorf("successes = %d, want %d", successes, store.UsernameLookupBurstLimit)
	}
	if quotaErrors != 10 {
		t.Errorf("quota errors = %d, want 10", quotaErrors)
	}
	if other != 0 {
		t.Errorf("unexpected errors = %d, want 0", other)
	}
}
