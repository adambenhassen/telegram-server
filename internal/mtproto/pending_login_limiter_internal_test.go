package mtproto

import (
	"sync"
	"testing"
)

func TestPendingLoginLimiterClaimsSlotsAtomically(t *testing.T) {
	t.Parallel()

	lim := newPendingLoginLimiter(1)
	const attempts = 32
	var wg sync.WaitGroup
	results := make(chan bool, attempts)
	for range attempts {
		wg.Go(func() { results <- lim.acquire() })
	}
	wg.Wait()
	close(results)

	var acquired int
	for ok := range results {
		if ok {
			acquired++
		}
	}
	if acquired != 1 {
		t.Fatalf("concurrent acquire admitted %d connections with cap 1", acquired)
	}

	lim.release()
	if !lim.acquire() {
		t.Fatal("released pending-login slot was not reusable")
	}
}
