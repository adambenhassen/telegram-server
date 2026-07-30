package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestCheckAndChargeLookupConcurrentBoundary verifies that the advisory lock in
// CheckAndChargeLookup serialises concurrent callers so exactly LookupLimit
// succeed and the rest return ErrLookupQuotaExceeded.
//
// Without pg_advisory_xact_lock the prune/insert/count sequence races: multiple
// goroutines see the same pre-increment count and commit past the limit. Removing
// the lock causes this test to report more than LookupLimit successes.
func TestCheckAndChargeLookupConcurrentBoundary(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15559990001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	const n = store.LookupLimit + 10

	type result struct{ err error }
	results := make([]result, n)

	var wg sync.WaitGroup
	wg.Add(n)
	ready := make(chan struct{})

	for i := range n {
		go func(i int) {
			defer wg.Done()
			<-ready
			phone := fmt.Sprintf("+1555000%04d", i)
			results[i].err = s.CheckAndChargeLookup(ctx, u.ID, phone)
		}(i)
	}

	close(ready)
	wg.Wait()

	var successes, quotaErrors, other int
	for _, r := range results {
		switch {
		case r.err == nil:
			successes++
		case errors.Is(r.err, store.ErrLookupQuotaExceeded):
			quotaErrors++
		default:
			other++
			t.Errorf("unexpected error: %v", r.err)
		}
	}

	if successes != store.LookupLimit {
		t.Errorf("successes = %d, want %d", successes, store.LookupLimit)
	}
	if quotaErrors != 10 {
		t.Errorf("quota errors = %d, want 10", quotaErrors)
	}
	if other != 0 {
		t.Errorf("unexpected errors = %d, want 0", other)
	}
}
