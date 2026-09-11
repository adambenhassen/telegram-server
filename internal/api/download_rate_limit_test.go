package api_test

import (
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestDownloadRateLimiterAdmitsLimitAndResetsWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	l := api.NewDownloadRateLimiterForTest(store.RateLimitConfig{Limit: 2, Window: time.Second})

	if wait, ok := l(now); !ok || wait != 0 {
		t.Fatalf("first admission = (%v, %v), want (0, true)", wait, ok)
	}
	if wait, ok := l(now.Add(100 * time.Millisecond)); !ok || wait != 0 {
		t.Fatalf("second admission = (%v, %v), want (0, true)", wait, ok)
	}
	wait, ok := l(now.Add(200 * time.Millisecond))
	if ok {
		t.Fatal("third call admitted before the window ended")
	}
	if wait != 800*time.Millisecond {
		t.Fatalf("denial wait = %v, want 800ms", wait)
	}
	if wait, ok := l(now.Add(time.Second)); !ok || wait != 0 {
		t.Fatalf("admission after reset = (%v, %v), want (0, true)", wait, ok)
	}
}

func TestDownloadRateLimiterDisabled(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for name, cfg := range map[string]store.RateLimitConfig{
		"zero limit":  {Limit: 0, Window: time.Second},
		"zero window": {Limit: 2},
	} {
		t.Run(name, func(t *testing.T) {
			l := api.NewDownloadRateLimiterForTest(cfg)
			for range 1000 {
				if wait, ok := l(now); !ok || wait != 0 {
					t.Fatalf("admission = (%v, %v), want (0, true)", wait, ok)
				}
			}
		})
	}
}

func TestDownloadRateLimiterConcurrentAdmissions(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	const limit = 400
	l := api.NewDownloadRateLimiterForTest(store.RateLimitConfig{Limit: limit, Window: time.Second})
	results := make(chan bool, limit+1)
	for range limit + 1 {
		go func() {
			_, ok := l(now)
			results <- ok
		}()
	}
	var admitted int
	for range limit + 1 {
		if <-results {
			admitted++
		}
	}
	if admitted != limit {
		t.Fatalf("admitted = %d, want %d", admitted, limit)
	}
}
